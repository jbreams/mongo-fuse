#define FUSE_USE_VERSION 26

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <bson.h>
#include <mongo.h>
#include "mongo-fuse.h"

#define BLACK 0
#define RED 1
#define NC(n) (n == NULL ? BLACK : n->c)

extern char * extents_name;

static void init_new_extent_doc(struct inode * e, struct enode * c, bson * doc) {
	bson_init(doc);
	bson_append_new_oid(doc, "_id");
	bson_append_oid(doc, "inode", &e->oid);
	bson_append_long(doc, "start", c->off);
	bson_append_start_array(doc, "blocks");
}

static void serialize_node(bson * doc, struct enode * n, int idx) {
	char idxstr[10];
	bson_numstr(idxstr, idx);
	bson_append_start_object(doc, idxstr);
	bson_append_binary(doc, "hash", 0, n->hash, 20);
	bson_append_int(doc, "len", n->len);
	bson_append_finish_object(doc);
}

int serialize_extent(struct inode * e, struct enode * root) {
	mongo * conn = get_conn();
	bson doc, cond;
	int nhashes = 0, res;
	off_t last_end = 0, cur_start = 0;
	struct enode * cur, *prev = NULL;

	cur = root;
	prev = cur;
	while(cur) {
		init_new_extent_doc(e, cur, &doc);
		cur_start = cur->off;
		while(cur && cur->off != last_end && nhashes < BLOCKS_PER_EXTENT) {
			struct enode * next;
			if(prev == cur->p) {
				next = cur->l;
				if(!next) {
					serialize_node(&doc, cur, nhashes++);
					next = cur->r ? cur->r : cur->p;
				}
			}
			else if(prev == cur->l) {
				serialize_node(&doc, cur, nhashes++);
				next = cur->r ? cur->r : cur->p;
			}
			else if(prev == cur->r)
				next = cur->p;
			prev = cur;
			last_end = cur->off + cur->len;
			cur = next;
		}
		bson_append_finish_array(&doc);
		bson_append_long(&doc, "end", last_end);
		bson_finish(&doc);

		res = mongo_insert(conn, extents_name, &doc, NULL);
		bson_destroy(&doc);
		if(res != MONGO_OK) {
			fprintf(stderr, "Error inserting extent\n");
			return -EIO;
		}

		bson_init(&cond);
		bson_append_oid(&cond, "inode", &e->oid);
		bson_append_start_object(&cond, "start");
		bson_append_long(&cond, "$gte", cur_start);
		bson_append_finish_object(&cond);
		bson_append_start_object(&cond, "end");
		bson_append_long(&cond, "$lte", last_end);
		bson_append_finish_object(&cond);
		bson_finish(&cond);

		res = mongo_remove(conn, extents_name, &cond, NULL);
		bson_destroy(&cond);
		if(res != MONGO_OK) {
			fprintf(stderr, "Error cleaning up extents\n");
			return -EIO;
		}

		nhashes = 0;
	}
	return 0;
}

int deserialize_extent(struct inode * e, off_t off, size_t len) {
	bson cond;
	mongo * conn = get_conn();
	mongo_cursor curs;
	int res;

	bson_init(&cond);
	bson_append_start_object(&cond, "$query");
	bson_append_oid(&cond, "inode", &e->oid);
	bson_append_start_object(&cond, "start");
	bson_append_long(&cond, "$lte", off);
	bson_append_finish_object(&cond);
	bson_append_start_object(&cond, "end");
	bson_append_long(&cond, "$gte", off + len);
	bson_append_finish_object(&cond);
	bson_append_finish_object(&cond);
	bson_append_start_object(&cond, "$orderby");
	bson_append_int(&cond, "start", 1);
	bson_append_int(&cond, "_id", 1);
	bson_append_finish_object(&cond);
	bson_finish(&cond);

	mongo_cursor_init(&curs, conn, extents_name);
	mongo_cursor_set_query(&curs, &cond);

	pthread_rwlock_wrlock(&e->rd_extent_lock);
	while((res = mongo_cursor_next(&curs)) == MONGO_OK) {
		const bson * curdoc = mongo_cursor_bson(&curs);
		bson_iterator topi, i, sub;
		bson_type bt;
		off_t curoff = 0;
		const char * key;
		bson_iteartor_init(&topi, curdoc);
		while(bson_iterator_next(&topi) != 0) {
			key = bson_iterator_key(&topi);
			if(strcmp(key, "blocks") == 0)
				bson_iterator_subiterator(&topi, &i);
			else if(strcmp(key, "start") == 0)
				curoff = bson_iterator_long(&topi);
		}

		while(bson_iterator_next(&i) != 0) {
			bson_iterator_subiterator(&i, &sub);
			char * hash = NULL;
			int len = 0;
			while(bson_iterator_next(&sub) != 0) {
				key = bson_iterator_key(&sub);
				if(strcmp(key, "hash") == 0)
					hash = (char*)bson_iterator_bin_data(&sub);
				else if(strcmp(key, "len") == 0)
					len = bson_iterator_int(&sub);
			}
			if((res = insert_hash(&e->rd_extent_root,
				curoff, len, hash)) != 0) {
				fprintf(stderr, "Error adding hash to extent tree\n");
				pthread_rwlock_unlock(&e->rd_extent_lock);
				return res;
			}
			curoff += len;
		}
	}
	mongo_cursor_destroy(&curs);
	bson_destroy(&cond);
	//e->rd_extent_updated = now;
	pthread_rwlock_unlock(&e->rd_extent_lock);
	return 0;
}

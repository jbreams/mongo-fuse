#define FUSE_USE_VERSION 26

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <bson.h>
#include <mongo.h>
#include "mongo-fuse.h"

extern char * extents_name;

int serialize_extent(struct inode * e, struct enode * root) {
	mongo * conn = get_conn();
	bson doc, cond;
	int res;
	struct enode_iter iter;
	struct enode * cur = start_iter(&iter, root, 0);

	while(cur) {
		bson_oid_t docid;
		off_t last_end = 0;
		const off_t cur_start = cur->off;
		int nhashes = 0;

		bson_oid_gen(&docid);
		bson_init(&doc);
		bson_append_oid(&doc, "_id", &docid);
		bson_append_oid(&doc, "inode", &e->oid);
		bson_append_long(&doc, "start", cur->off);
		fprintf(stderr, "Starting extent at %llu\n", cur->off);
		bson_append_start_array(&doc, "blocks");

		while(cur && nhashes < BLOCKS_PER_EXTENT) {
			char idxstr[10];
			bson_numstr(idxstr, nhashes++);
			bson_append_start_object(&doc, idxstr);
			if(cur->empty)
				bson_append_null(&doc, "hash");
			else
				bson_append_binary(&doc, "hash", 0,
					(const char*)cur->hash, 20);
			bson_append_int(&doc, "len", cur->len);
			bson_append_finish_object(&doc);

			last_end = cur->off + cur->len;
			cur = iter_next(&iter);
			if(cur && cur->off != last_end)
				break;
		}
		bson_append_finish_array(&doc);
		bson_append_long(&doc, "end", last_end);
		bson_finish(&doc);

		fprintf(stderr, "Ending extent at %llu\n", last_end);
		res = mongo_insert(conn, extents_name, &doc, NULL);
		bson_destroy(&doc);
		if(res != MONGO_OK) {
			fprintf(stderr, "Error inserting extent\n");
			iter_finish(&iter);
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
		bson_append_start_object(&cond, "_id");
		bson_append_oid(&cond, "$ne", &docid);
		bson_append_finish_object(&cond);
		bson_finish(&cond);

		res = mongo_remove(conn, extents_name, &cond, NULL);
		bson_destroy(&cond);
		if(res != MONGO_OK) {
			fprintf(stderr, "Error cleaning up extents\n");
			iter_finish(&iter);
			return -EIO;
		}
	}
	iter_finish(&iter);
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
	bson_append_long(&cond, "$gte", off);
	bson_append_long(&cond, "$lte", off + len);
	bson_append_finish_object(&cond);
	bson_append_finish_object(&cond);
	bson_append_start_object(&cond, "$orderby");
	bson_append_int(&cond, "start", 1);
	bson_append_int(&cond, "_id", 1);
	bson_append_finish_object(&cond);
	bson_finish(&cond);
	bson_print(&cond);

	mongo_cursor_init(&curs, conn, extents_name);
	mongo_cursor_set_query(&curs, &cond);

	pthread_rwlock_wrlock(&e->rd_extent_lock);
	while((res = mongo_cursor_next(&curs)) == MONGO_OK) {
		const bson * curdoc = mongo_cursor_bson(&curs);
		bson_iterator topi, i, sub;
		bson_type bt;
		off_t curoff = 0;
		const char * key;
		bson_iterator_init(&topi, curdoc);
		while(bson_iterator_next(&topi) != 0) {
			key = bson_iterator_key(&topi);
			if(strcmp(key, "blocks") == 0)
				bson_iterator_subiterator(&topi, &i);
			else if(strcmp(key, "start") == 0)
				curoff = bson_iterator_long(&topi);
		}

		while(bson_iterator_next(&i) != 0) {
			bson_iterator_subiterator(&i, &sub);
			uint8_t * hash = NULL;
			int len = 0;
			int empty = 0;
			while((bt = bson_iterator_next(&sub)) != 0) {
				key = bson_iterator_key(&sub);
				if(strcmp(key, "hash") == 0) {
					if(bt == BSON_NULL)
						empty = 1;
					else
						hash = (uint8_t*)bson_iterator_bin_data(&sub);
				}
				else if(strcmp(key, "len") == 0)
					len = bson_iterator_int(&sub);
			}
			if(empty)
				res = insert_empty(&e->rd_extent_root, curoff, len);
			else
				res = insert_hash(&e->rd_extent_root, curoff, len, hash);
			if(res != 0) {
				fprintf(stderr, "Error adding hash to extent tree\n");
				pthread_rwlock_unlock(&e->rd_extent_lock);
				return res;
			}
			curoff += len;
		}
	}
	mongo_cursor_destroy(&curs);
	bson_destroy(&cond);
	pthread_rwlock_unlock(&e->rd_extent_lock);
	return 0;
}

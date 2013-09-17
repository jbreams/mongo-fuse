#define FUSE_USE_VERSION 26

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include "mongo-fuse.h"

#define BLACK 0
#define RED 1
#define NC(n) (n == NULL ? BLACK : n->c)

extern char * extents_name;

static struct enode * grandparent(struct enode * n) {
	if(n && n->p)
		return n->p->p;
	return NULL;
}

static struct enode * uncle(struct enode * n) {
	struct enode * g = grandparent(n);
	if(!g)
		return NULL;
	if(n->p == g->l)
		return g->r;
	return g->l;
}

static struct enode * sibling(struct enode * n) {
	struct enode * p = n->p;
	if(n == p->l)
		return p->r;
	return p->l;
}

struct enode * find_node(struct enode * root, off_t offset) {
	struct enode * f = root;
	while(f && f->off != offset) {
		if(offset < f->off)
			f = f->l;
		else if(offset > f->off)
			f = f->r;
	}
	return f;
}

static void replace_node(struct enode * o, struct enode * n, struct enode ** root) {
	if(o->p == NULL) {
		*root = n;
	} else {
		if(o == o->p->l)
			o->p->l = n;
		else
			o->p->r = n;
	}
	if(n)
		n->p = o->p;
}

static void rotate_left(struct enode * n, struct enode ** root) {
	struct enode * r = n->r;
	replace_node(n, r, root);
	n->r = n->l;
	if(r->l)
		r->l->p = n;
	r->l = n;
	n->p = r;
}

static void rotate_right(struct enode * n, struct enode ** root) {
	struct enode * l = n->l;
	replace_node(n, l, root);
	n->l = l->r;
	if(l->r)
		l->r->p = n;
	l->r = n;
	n->p = l;
}

static void insert_cases(struct enode * n, struct enode ** root) {
	// case 1
	if(!n->p) {
		n->c = BLACK;
		return;
	}
	// case 2
	if(n->p->c == BLACK)
		return;
	// case 3
	struct enode * u = uncle(n), *g;
	if(u && u->c == RED) {
		n->p->c = BLACK;
		u->c = BLACK;
		g = grandparent(n);
		g->c = RED;
		insert_cases(g, root);
	} else {
		// case 4
		g = grandparent(n);
		if((n == n->p->r) && (n->p = g->l)) {
			rotate_left(n->p, root);
			n = n->l;
		}
		else if((n == n->p->l) && (n->p == g->r)) {
			rotate_right(n->p, root);
			n = n->r;
		}

		// case 5
		g = grandparent(n);
		n->p->c = BLACK;
		g->c = RED;
		if(n == n->p->l)
			rotate_right(g, root);
		else
			rotate_left(g, root);
	}
}

static void insert_hash_tree(struct enode * n, struct enode ** root) {
	struct enode * r = *root;
	while(1) {
		if(n->off < r->off) {
			if(r->l == NULL) {
				r->l = n;
				break;
			} else
				r = r->l;
		}
		else {
			if(r->r == NULL) {
				r->r = n;
				break;
			} else
				r = r->r;
 		}
	}
	n->p = r;
	insert_cases(n, root);
}

static void delete_cases(struct enode * n, struct enode ** root) {
	if(!n->p) // case 1
		return;
	// case 2
	struct enode * s = sibling(n);
	if(s->c == RED) {
		n->p->c = RED;
		s->c = BLACK;
		if(n == n->p->l)
			rotate_left(n->p, root);
		else
			rotate_right(n->p, root);
	}
	s = sibling(n);
	if(NC(s) == BLACK && NC(s->l) == BLACK && NC(s->r) == BLACK) {
		if(n->p->c == BLACK) { // case 3
			s->c = RED;
			delete_cases(n->p, root);
		} else { // case 4
			s->c = RED;
			n->p->c = BLACK;
		}
	} else { // case 5
		s = sibling(n);
		if(NC(s) == BLACK && NC(s->l) == RED && NC(s->r) == BLACK) {
			if(n == n->p->l) { 
				s->c = RED;
				s->l->c = BLACK;
				rotate_right(s, root);
			}
			else if(n == n->p->r) {
				s->c = RED;
				s->r->c = BLACK;
				rotate_left(s, root);
			}
		}
		// case 6
		s = sibling(n);
		s->c = NC(n->p);
		n->p->c = BLACK;
		if(n == n->p->l) {
			s->r->c = BLACK;
			rotate_left(n->p, root);
		}
		else {
			s->l->c = BLACK;
			rotate_right(n->p, root);
		}
	}
}

static void remove_one_node(struct enode * n, struct enode ** root) {
	struct enode * c = (n->r == NULL || (n->r->l == NULL && n->r->r == NULL)) ?
		n->l : n->r;
	replace_node(n, c, root);
	if(n->c == BLACK) {
		if(c->c == RED)
			c->c = BLACK;
		else
			delete_cases(c, root);
	}
	free(n);
}

static void remove_range(off_t off, size_t len, struct enode ** root) {
	struct enode * n = find_node(*root, off);
	if(!n)
		return;

	if(n == *root) {
		free(*root);
		*root = NULL;
		return;
	}

	off_t end = off + len;
	struct enode * rmhead = NULL, *c = n, *prev = NULL, *next;
	while(c && c->off + c->len < end) {
		if(c != n && prev == c->p) {
			next = c->l;
			if(!next) {
				c->n = rmhead;
				rmhead = c;
				next = c->r ? c->r : c->p;
			}
		}
		else if(prev == c->l) {
			c->n = rmhead;
			rmhead = c;
			next = c->r ? c->r : c->p;
		}
		else if(prev == c->r)
			next = c->p;
		prev = c;
		c = next;
	}

	while(rmhead) {
		next = rmhead->n;
		remove_one_node(rmhead, root);
		rmhead = next;
	}
}

static int insert_hash(struct enode ** r, off_t off, size_t len, char hash[20]) {
	struct enode * root = *r, *n = malloc(sizeof(struct enode));
	if(!n)
		return -ENOMEM;
	memset(n, 0, sizeof(struct enode));
	n->off = off;
	n->len = len;
	memcpy(n->hash, hash, 20);

	if(!root) {
		*r = n;
		return 0;
	}

	remove_range(off, len, r);
	n->c = RED;
	insert_hash_tree(n, r);
	return 0;
}

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

int serialize_extent(struct inode * e) {
	mongo * conn = get_conn();
	bson doc, cond;
	int nhashes = 0, res;
	off_t last_end = 0, cur_start = 0;
	struct enode * cur, *prev = NULL;

	pthread_mutex_lock(&e->wr_extent_lock);
	cur = e->wr_extent_root;
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
			pthread_mutex_unlock(&e->wr_extent_lock);
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
			pthread_mutex_unlock(&e->wr_extent_lock);
			fprintf(stderr, "Error cleaning up extents\n");
			return -EIO;
		}

		nhashes = 0;
	}
	pthread_mutex_unlock(&e->wr_extent_lock);
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
	pthread_rwlock_unlock(&e->rd_extent_lock);
	return 0;
}
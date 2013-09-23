#define FUSE_USE_VERSION 26

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include "mongo-fuse.h"

#define BLACK 0
#define RED 1
#define NC(n) (n == NULL ? BLACK : n->c)
#define IS_RED(n) (n && n->c == RED)

struct enode * rotate_single(struct enode * root, int dir) {
	struct enode * save = root->links[!dir];

	root->links[!dir] = save->links[dir];
	save->links[dir] = root;

	root->c = RED;
	save->c = BLACK;

	return save;
}

struct enode * rotate_double(struct enode * root, int dir) {
	root->links[!dir] = rotate_single(root->links[!dir], !dir);
	return rotate_single(root, dir);
}

int remove_one_node(struct enode ** root, off_t off) {
	if(!*root)
		return 0;

	struct enode head = {0};
	struct enode * q, *p = NULL, *g = NULL, *f = NULL;
	int dir = 1;

	q = &head;
	q->links[RIGHT] = *root;

	while(q->links[dir] != NULL) {
		int last = dir;

		g = p;
		p = q;
		q = q->links[dir];
		dir = q->off < off;

		if(q->off == off)
			f = q;

		if(!IS_RED(q) && !IS_RED(q->links[dir])) {
			if(IS_RED(q->links[!dir]))
				p = p->links[last] = rotate_single(q, dir);
			else if(!IS_RED(q->links[!dir])) {
				struct enode * s = p->links[!last];

				if(s == NULL)
					continue;

				if(!IS_RED(s->links[!last]) && !IS_RED(s->links[last])) {
					p->c = BLACK;
					s->c = RED;
					q->c = RED;
				} else {
					int dir2 = g->links[RIGHT] == p;

					if(IS_RED(s->links[last]))
						g->links[dir2] = rotate_double(p, last);
					else if(IS_RED(s->links[!last]))
						g->links[dir2] = rotate_single(p, last);

					q->c = g->links[dir2]->c = RED;
					g->links[dir2]->links[LEFT]->c = BLACK;
					g->links[dir2]->links[RIGHT]->c = BLACK;
				}
			}
		}
	}

	if(f != NULL) {
		f->off = q->off;
		f->len = q->len;
		memcpy(f->hash, q->hash, 20);
		p->links[p->links[RIGHT] == q] = q->links[q->links[LEFT] == NULL];
		free(q);
	}

	*root = head.links[RIGHT];
	if(*root != NULL)
		(*root)->c =BLACK;
	return f == NULL;
}

struct enode * start_iter(struct enode_iter * trav,
	struct enode * root, off_t off) {
	trav->root = trav->cur = root;
	trav->top = 0;
	if(!root)
		return NULL;
	while(trav->cur->links[LEFT] != NULL && trav->cur->off > off) {
		trav->path[trav->top++] = trav->cur;
		trav->cur = trav->cur->links[LEFT];
	}
	return trav->cur;
}

struct enode * iter_next(struct enode_iter * trav) {
	if(trav->cur->links[RIGHT] != NULL) {
		trav->path[trav->top++] = trav->cur;
		trav->cur = trav->cur->links[RIGHT];

		while(trav->cur->links[LEFT] != NULL) {
			trav->path[trav->top++] = trav->cur;
			trav->cur = trav->cur->links[LEFT];
		}
	}
	else {
		struct enode * last;
		do {
			if(trav->top == 0) {
				trav->cur = NULL;
				break;
			}

			last = trav->cur;
			trav->cur = trav->path[--trav->top];
		} while(last == trav->cur->links[RIGHT]);
	}

	return trav->cur;
}

void remove_range(off_t off, size_t len, struct enode ** root) {
	struct enode_iter it;
	struct enode * n, *h = NULL;
	off_t end;
	if(!*root)
		return;

	n = *root;
	if(!n->links[0] && !n->links[1]) {
		free(*root);
		*root = NULL;
		return;
	}

	n = start_iter(&it, *root, off);
	end = off + len;

	while(n->off < off)
		n = iter_next(&it);

	if(n->off + n->len > end)
		return;

	while(n && n->off + n->len < end) {
		n->n = h;
		h = n;
		n = iter_next(&it);
	}

	while(h) {
		n = h->n;
		remove_one_node(root, h->off);
		h = n;
	}
}

int insert_hash(struct enode ** r, off_t off, size_t len, char hash[20]) {
	if(*r == NULL) {
		struct enode * nr = malloc(sizeof(struct enode));
		memset(nr, 0, sizeof(struct enode));
		nr->off = off;
		nr->len = len;
		nr->c = BLACK;
		memcpy(nr->hash, hash, 20);
		*r = nr;
		return 0;
	}

	struct enode head = {0};
	struct enode *g, *t, *p, *q;
	int dir = LEFT, last;

	t = &head;
	g = p = NULL;
	q = t->links[RIGHT] = *r;

	for(;;) {
		if(q == NULL) {
			p->links[dir] = q = malloc(sizeof(struct enode));
			if(!q)
				return -ENOMEM;
			q->off = off;
			q->len = len;
			memcpy(q->hash, hash, 20);
		}
		else if(IS_RED(q->links[LEFT]) && IS_RED(q->links[RIGHT])) {
			q->c = RED;
			q->links[LEFT]->c = BLACK;
			q->links[RIGHT]->c = BLACK;
		}

		if(IS_RED(q) && IS_RED(p)) {
			int dir2 = t->links[RIGHT] == g;
			if(q == p->links[last])
				t->links[dir2] = rotate_single(g, !last);
			else
				t->links[dir2] = rotate_double(g, !last);
		}

		if(q->off == off)
			break;

		last = dir;
		dir = q->off < off;

		if(g != NULL)
			t = g;
		g = p;
		p = q;
		q = q->links[dir];
	}

	*r = head.links[RIGHT];
	(*r)->c = BLACK;
	return 0;
}

void free_extent_tree(struct enode * root) {
	if(!root)
		return;
	free_extent_tree(root->links[LEFT]);
	free_extent_tree(root->links[RIGHT]);
	free(root);
}

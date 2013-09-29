#define FUSE_USE_VERSION 26

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include "mongo-fuse.h"

#define BLACK 0
#define RED 1
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

int grow_stack(struct enode_iter * trav) {
	if(trav->top < TREE_HEIGHT_LIMIT || trav->top < trav->pathsize)
		return 0;

	int copy = 0;
	struct enode *new_stack;
	if(trav->pathsize == 0)  {
		trav->pathsize = 512;
		copy = 1;
		trav->path = NULL;
	}
	else
		trav->pathsize += 512;

	new_stack = realloc(trav->path, trav->pathsize * sizeof(struct enode*));
	if(!new_stack)
		return -ENOMEM;
	if(copy)
		memcpy(new_stack, trav->path_stack,
			sizeof(struct enode*) * TREE_HEIGHT_LIMIT);
	trav->path = (struct enode**)new_stack;
	return 0;
}

struct enode * start_iter(struct enode_iter * trav,
	struct enode * root, off_t off) {
	trav->root = trav->cur = root;
	trav->top = 0;
	trav->path = trav->path_stack;

	if(!root)
		return NULL;
	while(trav->cur->links[LEFT] != NULL && trav->cur->off > off) {
		trav->path[trav->top++] = trav->cur;
		if(grow_stack(trav) != 0)
			return NULL;
		trav->cur = trav->cur->links[LEFT];
	}
	return trav->cur;
}

struct enode * iter_next(struct enode_iter * trav) {
	if(trav->cur->links[RIGHT] != NULL) {
		trav->path[trav->top++] = trav->cur;
		if(grow_stack(trav) != 0)
			return NULL;
		trav->cur = trav->cur->links[RIGHT];

		while(trav->cur->links[LEFT] != NULL) {
			trav->path[trav->top++] = trav->cur;
			if(grow_stack(trav) != 0)
				return NULL;
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

void iter_finish(struct enode_iter * trav) {
	if(trav->path != trav->path_stack)
		free(trav->path);
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

	if(n->off + n->len > end) {
		iter_finish(&it);
		return;
	}

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
	iter_finish(&it);
}

void insert_enode(struct enode **r, struct enode * n) {
	if(*r == NULL) {
		*r = n;
		return;
	}

	struct enode head = {0};
	struct enode *g, *t, *p, *q;
	int dir = LEFT, last;

	t = &head;
	g = p = NULL;
	q = t->links[RIGHT] = *r;

	for(;;) {
		if(q == NULL)
			p->links[dir] = q = n;
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

		if(q->off == n->off)
			break;

		last = dir;
		dir = q->off < n->off;

		if(g != NULL)
			t = g;
		g = p;
		p = q;
		q = q->links[dir];
	}

	*r = head.links[RIGHT];
	(*r)->c = BLACK;
}

int insert_hash(struct enode ** r, off_t off, size_t len, uint8_t hash[20]) {
	struct enode * nr = malloc(sizeof(struct enode));
	if(!nr)
		return -ENOMEM;

	memset(nr, 0, sizeof(struct enode));
	nr->off = off;
	nr->len = len;
	memcpy(nr->hash, hash, 20);

	insert_enode(r, nr);
	return 0;
}

int insert_empty(struct enode **r, off_t off, size_t len) {
	struct enode * nr = malloc(sizeof(struct enode));
	if(!nr)
		return -ENOMEM;

	memset(nr, 0, sizeof(struct enode));
	nr->off = off;
	nr->len = len;
	nr->empty = 1;

	insert_enode(r, nr);
	return 0;
}

void free_extent_tree(struct enode * root) {
	if(!root)
		return;
	free_extent_tree(root->links[LEFT]);
	free_extent_tree(root->links[RIGHT]);
	free(root);
}

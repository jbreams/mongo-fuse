#define FUSE_USE_VERSION 26

#include <osxfuse/fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <mongo.h>
#include <stdlib.h>
#include <search.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "mongo-fuse.h"

int mongo_read(const char *path, char *buf, size_t size, off_t offset,
                      struct fuse_file_info *fi) {
    struct inode e;
    int res;
    struct extent * o, *el;
    char * block = buf;
    size_t tocopy;
    const size_t end = size + offset;

    if((res = get_inode(path, &e, offset < EXTENT_SIZE ? 1: 0)) != 0)
        return res;

    if(e.mode & S_IFDIR)
        return -EISDIR;

    if(offset > e.size)
        return 0;

    if(offset < EXTENT_SIZE) {
        size_t sizediff = EXTENT_SIZE - offset;
        tocopy = size > sizediff ? sizediff : size;
        memcpy(block, e.data + offset, tocopy);
        offset += tocopy;
        block += tocopy;
    }

    if(block - buf == size) {
        free_inode(&e);
        return tocopy;
    }

    res = resolve_extent(&e, offset, offset + size, &o, 1);
    free_inode(&e);
    if(res != 0)
        return res;

    el = o;
    while(block - buf != size) {
        if(end - offset < EXTENT_SIZE)
            tocopy = end - offset;
        else
            tocopy = EXTENT_SIZE;

        if(!el || el->start != offset) {
            memset(block, 0, tocopy);
            goto advance;
        }

        memcpy(block, el->data, tocopy);
        el = el->next;
advance:
        block += tocopy;
        offset += tocopy;
    }

    if(o)
        free_extents(o);
    return size;
}

int mongo_write(const char *path, const char *buf, size_t size,
                       off_t offset, struct fuse_file_info *fi)
{
    struct inode e;
    int res;
    struct extent * o = NULL, *last = NULL;
    char * block = (char*)buf;
    size_t tocopy;
    const size_t end = size + offset,
        last_block = (end / EXTENT_SIZE) * EXTENT_SIZE;

    if((res = get_inode(path, &e, offset < EXTENT_SIZE ? 1: 0)) != 0)
        return res;

    if(e.mode & S_IFDIR)
        return -EISDIR;

    e.modified = time(NULL);

    if(offset < EXTENT_SIZE) {
        size_t sizediff = EXTENT_SIZE - offset;
        tocopy = size > sizediff ? sizediff : size;
        memcpy(e.data + offset, block, tocopy);
        offset += tocopy;
        block += tocopy;
        e.datalen += tocopy;
    }

    if(block - buf == size) {
        if(end > e.size)
            e.size += tocopy;
        commit_inode(&e);
        free_inode(&e);
        return tocopy;
    }

    if(end - offset > EXTENT_SIZE) {
        o = NULL;
        while(end - offset > EXTENT_SIZE && offset != last_block) {
            struct extent * n = malloc(sizeof(struct extent));
            if(!n) {
                fprintf(stderr, "Error allocating block\n");
                res = -ENOMEM;
                goto cleanup;
            }
            n->size = EXTENT_SIZE;
            n->start = offset;
            memcpy(&n->inode, &e.oid, sizeof(bson_oid_t));
            memcpy(n->data, block, EXTENT_SIZE);
            n->next = o;
            o = n;
            block += tocopy;
            offset += tocopy;
        }
        commit_extents(&e, o);
        free_extents(o);
    }

    res = resolve_extent(&e, last_block, end, &last, 1);
    if(res != 0)
        goto cleanup;
    if(!last) {
        last = malloc(sizeof(struct extent));
        if(!last) {
            fprintf(stderr, "Error allocating last block!\n");
            res = -ENOMEM;
            goto cleanup;
        }
        last->start = last_block;
        memcpy(&last->inode, &e.oid, sizeof(bson_oid_t));
    }

    memcpy(last->data, block, end - offset);
    last->size = end - offset;
    last->next = o;

    res = commit_extents(&e, last);
    if(res != 0)
        goto cleanup;
    if(end > e.size)
        e.size = end;
    res = commit_inode(&e);
cleanup:
    free_inode(&e);
    free_extents(last);
    if(res != 0)
        return res;
    return size;
}


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
#include <assert.h>
#include "mongo-fuse.h"

int mongo_read(const char *path, char *buf, size_t size, off_t offset,
                      struct fuse_file_info *fi) {
    struct inode e;
    int res;
    struct extent * o, *el;
    char * block = buf;
    size_t tocopy;
    const off_t end = size + offset;

    if((res = get_inode(path, &e)) != 0)
        return res;

    if(e.mode & S_IFDIR)
        return -EISDIR;

    if(offset > e.size)
        return 0;

    res = resolve_extent(&e, compute_start(&e, offset), offset + size, &o);

    el = o;
    while(block - buf != size) {
        char * elock;
        size_t curblocksize = e.blocksize, sizediff = 0;
        if(!el) {
            memset(block, 0, end - offset);
            block += (end - offset);
            break;
        } else if(el == o && el->start < offset) {
            sizediff = offset - el->start;
            elock = el->data + sizediff;
            curblocksize = e.blocksize - sizediff;
        } else
            elock = el->data;

        if(end - offset < curblocksize)
            tocopy = end - offset;
        else
            tocopy = curblocksize;

        memcpy(block, elock, tocopy);
        el = el->next;
        block += tocopy;
        offset += tocopy;
    }

    if(o)
        free_extents(o);
    add_block_stat(path, size, 0);
    return block - buf;
}

int mongo_write(const char *path, const char *buf, size_t size,
                       off_t offset, struct fuse_file_info *fi)
{
    struct inode e;
    int res;
    struct extent * last = NULL, *new = NULL;
    char * block;
    const off_t end = size + offset;
    off_t curblock;
    size_t tocopy;

    if((res = get_inode(path, &e)) != 0)
        return res;

    if(e.mode & S_IFDIR)
        return -EISDIR;
/*
    if(size > sizeof(uint64_t)) {
        uint64_t zerocheck;
        block = ((char*)buf + size) - sizeof(uint64_t);
        while(block != buf) {
            zerocheck = *(uint64_t*)block;
            if(zerocheck != 0)
                break;
            block -= (block - buf > sizeof(uint64_t)) ? sizeof(uint64_t):block - buf;
        }
        if(zerocheck == 0) {
            if(end > e.size) {
                e.size = end;
                commit_inode(&e);
                free_inode(&e);
            }
            return size;
        }
    }*/
    block = (char*)buf;

    curblock = compute_start(&e, offset);
    while(offset < end) {
        size_t curblocksize = e.blocksize, sizediff = 0;
        struct extent * cur = NULL;
        char * elock;

        if(offset != curblock || end - curblock < e.blocksize) {
            res = resolve_extent(&e, curblock, curblock + e.blocksize, &cur);
            if(res != 0) {
                fprintf(stderr, "Error resolving extent at %llu for %s\n", curblock, path);
                goto cleanup;
            }
        }

        if(!cur) {
            cur = new_extent(&e);
            if(!cur) {
                res = -ENOMEM;
                fprintf(stderr, "Error allocating extent at %llu for %s\n", curblock, path);
                goto cleanup;
            }
            cur->start = curblock;
        }

        if(curblock < offset) {
            sizediff = offset - curblock;
            elock = cur->data + sizediff;
            curblocksize = e.blocksize - sizediff;
        } else
            elock = cur->data;

        if(end - offset < curblocksize)
            tocopy = end - offset;
        else
            tocopy = curblocksize;

        memcpy(elock, block, tocopy);
        cur->next = new;
        new = cur;
        block += tocopy;
        offset += tocopy;
        curblock = compute_start(&e, offset);
    }

    res = commit_extents(&e, new);
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
    else
        add_block_stat(path, size, 1);
    return size;
}


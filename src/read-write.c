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

    if((res = get_inode(path, &e, 0)) != 0)
        return res;

    if(e.mode & S_IFDIR)
        return -EISDIR;

    if(offset > e.size)
        return 0;


    /*

    if(offset < e.blocksize) {
        if((res = fill_data(&e)) != 0) {
            free_inode(&e);
            return res;
        }
        size_t sizediff = e.blocksize - offset;
        tocopy = size > sizediff ? sizediff : size;
        memcpy(block, e.data + offset, tocopy);
        offset += tocopy;
        block += tocopy;
    }

    if(block - buf == size) {
        free_inode(&e);
        add_block_stat(path, size, 0);
        return tocopy;
    }
*/

    res = resolve_extent(&e, compute_start(&e, offset), offset + size, &o);
    if(res != 0) {
        free_inode(&e);
        return res;
    }

    if(!o) {
        memset(block, 0, size);
        free_inode(&e);
        return size;
    }

    el = o;
    while(block - buf != size) {
        if(end - offset < e.blocksize)
            tocopy = end - offset;
        else
            tocopy = e.blocksize;

        if(!el) {
            memset(block, 0, end - offset);
            block += (end - offset);
            break;
        }
        else if(el->start < offset) {
            printf("%llu %llu %lu\n", el->start, offset, tocopy);
            memcpy(block, el->data + (offset - el->start), tocopy);
        }
        else
            memcpy(block, el->data, tocopy);

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
    char * block = (char*)buf;
    const off_t end = size + offset;
    off_t curblock, stopat;
    size_t tocopy;

    if((res = get_inode(path, &e, 0)) != 0)
        return res;

    if(e.mode & S_IFDIR)
        return -EISDIR;

    /*

    if(offset < e.blocksize) {
        printf("Writing to first block\n");
        if((res = fill_data(&e)) != 0) {
            free_inode(&e);
            return res;
        }
        tocopy = e.blocksize - offset;
        tocopy = size > tocopy ? tocopy : size;
        memcpy(e.data + offset, block, tocopy);
        offset += tocopy;
        block += tocopy;
        e.datalen += tocopy;
    }

    e.modified = time(NULL);
    if(offset == end) {
        if(end > e.size)
            e.size += tocopy;
        commit_inode(&e);
        free_inode(&e);
        add_block_stat(path, size, 1);
        return size;
    }*/

    curblock = compute_start(&e, offset);
    while(offset < end) {
        struct extent * cur = NULL;
        if(offset != curblock || end - curblock < e.blocksize) {
            res = resolve_extent(&e, curblock, curblock + e.blocksize, &cur);
            if(res != 0)
                goto cleanup;
        }

        if(!cur) {
            cur = new_extent(&e);
            if(!cur) {
                res = -ENOMEM;
                goto cleanup;
            }
            cur->start = curblock;
        }

        cur->next = new;
        new = cur;
        tocopy = end - offset;
        tocopy = tocopy > e.blocksize ? e.blocksize : tocopy;

        if(offset != curblock) {
            memcpy(cur->data + (offset - curblock), block, tocopy);
        }
        else
            memcpy(cur->data, block, tocopy);

        block += tocopy;
        offset += tocopy;
        curblock = compute_start(&e, offset);
        printf("%llu %llu %llu %llu %llu\n", tocopy, e.blocksize, curblock, offset, end);
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
    printf("Wrote it all okay %lu %d\n", size, res);
    return size;
}


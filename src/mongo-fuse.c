// mongo_fuse.c

/**
  gcc -Wall -DMONGO_HAVE_STDINT mongo-fuse.c `pkg-config fuse --cflags --libs` -o mongo-fuse -lmongoc
 */

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

static const char *mongo_path = "/hello";
char * blocks_name = "test.blocks";
char * inodes_name = "test.inodes";
mongo conn;

static int mongo_getattr(const char *path, struct stat *stbuf) {
    int res = 0;
    struct inode e;

    memset(stbuf, 0, sizeof(struct stat));
    if (strcmp(path, "/") == 0) {
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
        return res;
    }

    res = get_inode(path, &e, 0);
    if(res != 0)
        return res;

    stbuf->st_mode = e.mode;
    stbuf->st_uid = e.owner;
    stbuf->st_gid = e.group;
    stbuf->st_size = e.size;
    stbuf->st_ctime = e.created;
    stbuf->st_mtime = e.modified;
    stbuf->st_atime = e.modified;

    free_inode(&e);
    return res;
}

static int mongo_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                         off_t offset, struct fuse_file_info *fi) {
    (void) offset;
    (void) fi;
    bson query, fields;
    mongo_cursor curs;
    struct stat stbuf;
    size_t pathlen = strlen(path);
    char * regexp = malloc(pathlen + 10);
    int res;

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);

    sprintf(regexp, "^%s/[^/]+", path);
    bson_init(&query);
    bson_append_regex(&query, "dirents", regexp, "");
    bson_finish(&query);

    bson_init(&fields);
    bson_append_int(&fields, "data", 0);
    bson_finish(&fields);

    mongo_cursor_init(&curs, &conn, inodes_name);
    mongo_cursor_set_query(&curs, &query);
    mongo_cursor_set_fields(&curs, &fields);

    while((res = mongo_cursor_next(&curs)) == MONGO_OK) {
        bson_iterator i;
        const char * curpath, *key;
        bson_type bt;

        bson_iterator_init(&i, mongo_cursor_bson(&curs));
        while((bt = bson_iterator_next(&i))> 0) {
            key = bson_iterator_key(&i);
            if(strcmp(key, "mode") == 0)
                stbuf.st_mode = bson_iterator_int(&i);
            else if(strcmp(key, "owner") == 0)
                stbuf.st_uid = bson_iterator_long(&i);
            else if(strcmp(key, "group") == 0)
                stbuf.st_gid = bson_iterator_long(&i);
            else if(strcmp(key, "size") == 0)
                stbuf.st_size = bson_iterator_long(&i);
            else if(strcmp(key, "created") == 0)
                stbuf.st_ctime = bson_iterator_time_t(&i);
            else if(strcmp(key, "modified") == 0) {
                stbuf.st_mtime = bson_iterator_time_t(&i);
                stbuf.st_atime = stbuf.st_mtime;
            }
            else if(strcmp(key, "dirents") == 0) {
                bson_iterator sub;
                bson_iterator_subiterator(&i, &sub);
                while((bt = bson_iterator_next(&sub)) > 0) {
                    curpath = bson_iterator_string(&sub);
                    if(strncmp(curpath, path, pathlen) == 0)
                        break;
                }
            }
        }

        filler(buf, curpath + pathlen + 1, &stbuf, 0);
    }
    bson_destroy(&query);
    bson_destroy(&fields);
    free(regexp);

    if(curs.err != MONGO_CURSOR_EXHAUSTED) {
        fprintf(stderr, "Error reading directory %s: %d", path, curs.err);
        return -EIO;
    }

    return 0;
}

static int mongo_write(const char *path, const char *buf, size_t size,
                       off_t offset, struct fuse_file_info *fi)
{
    struct inode e;
    int res;
    struct extent * o, *el, *last = NULL;
    char * block = (char*)buf;
    size_t tocopy;
    const size_t end = size + offset;

    if((res = get_inode(path, &e, 1)) != 0)
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
    }

    if(block - buf == size) {
        commit_inode(&e);
        free_inode(&e);
        return tocopy;
    }

    res = resolve_extent(&e, offset, offset + size, &o);
    if(res != 0)
        return res;

    el = o;
    while(block - buf != size) {
        if(end - offset < EXTENT_SIZE)
            tocopy = end - offset;
        else
            tocopy = EXTENT_SIZE;

        if(!el || el->start != offset) {
            struct extent * n = malloc(sizeof(struct extent));
            n->size = tocopy;
            n->start = offset;
            memcpy(n->data, block, tocopy);
            if(last)
                last->next = n;
            else
                o = n;
            n->next = el;
            goto advance;
        }

        memcpy(el->data, block, tocopy);
        last = el;
        el = el->next;
advance:
        block += tocopy;
        offset += tocopy;
    }

    res = commit_extents(&e, o);
    free_inode(&e);
    free_extents(o);
    if(res != 0)
        return res;
    return size;
}

static int mongo_open(const char *path, struct fuse_file_info *fi)
{
    if (strcmp(path, mongo_path) != 0)
        return -ENOENT;

    if ((fi->flags & 3) != O_RDONLY)
        return -EACCES;

    return 0;
}

static int mongo_read(const char *path, char *buf, size_t size, off_t offset,
                      struct fuse_file_info *fi) {
    struct inode e;
    int res;
    struct extent * o, *el;
    char * block = buf;
    size_t tocopy;
    const size_t end = size + offset;

    if((res = get_inode(path, &e, 1)) != 0)
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

    res = resolve_extent(&e, offset, offset + size, &o);
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

static struct fuse_operations mongo_oper = {
    .getattr    = mongo_getattr,
    .readdir    = mongo_readdir,
    .open       = mongo_open,
    .read       = mongo_read,
    .write      = mongo_write
};

int main(int argc, char *argv[])
{
    return fuse_main(argc, argv, &mongo_oper, NULL);
}

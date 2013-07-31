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

    res = get_entry(path, &e, 0);
    if(res != 0)
        return res;

    stbuf->st_mode = e->mode;
    stbuf->st_uid = e->user;
    stbuf->st_gid = e->group;
    stbuf->st_size = e->size;
    stbuf->st_ctime = e->created;
    stbuf->st_mtime = e->mtime;
    stbuf->st_atime = e->mtime;

    free_entry(&e);
    return res;
}

static int mongo_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                         off_t offset, struct fuse_file_info *fi) {
    (void) offset;
    (void) fi;

    if (strcmp(path, "/") != 0)
        return -ENOENT;

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    filler(buf, mongo_path + 1, NULL, 0);

    return 0;
}

static int mongo_write(const char *path, const char *buf, size_t size,
		      off_t offset, struct fuse_file_info *fi)
{
    struct inode e;
    off_t * offs;
    int res, offcount;

    if(strcmp(path, "/") != 0)
        return -ENOENT;

    if((res = get_inode(path, &e, 1)) != 0)
        return res;

    if(e.mode & S_IFDIR)
        return -EISDIR;

	if(strcmp(path, "/") != 0)
		return -ENOENT;


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
    off_t base;
    int res, count, x;
    struct extents;
    size_t y, todo;

    if(strcmp(path, "/") != 0)
        return -ENOENT;

    if((res = get_inode(path, &e, 1)) != 0)
        return res;

    if(e.mode & S_IFDIR)
        return -EISDIR;

    if(offset > e.size)
        return 0;

    if(e.data) {
        y = size;
        if(y + offset > e.size)
            y = e.size - offset;
        memcpy(buf, e.data + offset, y);
        free_inode(&e);
        return y;
    }

    res = resolve_extent(&e, offset, offset + size, &o);
    free_inode(&e);
    if(res != 0)
        return res;

    if(offcount == 1) {
        offset %= EXTENT_SIZE;
        memcpy(buf, o->data + offset, size);
        free(o);
        return size;
    }

    os = (struct extent**)o;

    for(x = 0; x < offcount; x++) {
        if(x == 0) {
            y = OFFSET % EXTENT_SIZE;
            memcpy(buf, os[x]->data + (y), EXTENT_SIZE - y);
            size -= EXTENT_SIZE - y;
        } else if(x == offcount - 1) {
            memcpy(buf + done, os[x]->data, size - done);
        } else {
            memcpy(buf + done, os[x]->data, EXTENT_SIZE);
            done += EXTENT_SIZE;
        }
    }

    return size;
}

static struct fuse_operations mongo_oper = {
    .getattr    = mongo_getattr,
    .readdir    = mongo_readdir,
    .open        = mongo_open,
    .read        = mongo_read,
};

int main(int argc, char *argv[])
{
    return fuse_main(argc, argv, &mongo_oper, NULL);
}

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
    int res;
    int size;
    struct entry e;

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
    size_t len;
    (void) fi;

    if(strcmp(path, mongo_path) != 0)
        return -ENOENT;

    mongo conn;
    mongo_init( &conn );
    int res = mongo_client( &conn, "127.0.0.1", 27017 );
    if ( res != MONGO_OK ) {
        fprintf( stderr, "connection to mongo failed\n" );
        return -ENOENT;
    }

    bson doc;
    res = mongo_find_one( &conn, "test.blocks", bson_shared_empty(), bson_shared_empty(), &doc );
    if ( res != MONGO_OK ) {
        fprintf( stderr, "find one failed\n" );
        return -ENOENT;
    }

    bson_print( &doc );

    bson_iterator it;
    bson_iterator_init( &it, &doc );
    if ( bson_find( &it, &doc, "data" ) != BSON_STRING ) {
        fprintf( stderr, "no data field\n" );
        return -ENOENT;
    }

    const char* data = bson_iterator_string( &it );

    len = strlen(data);
    if (offset < len) {
        if (offset + size > len)
            size = len - offset;
        memcpy(buf, data + offset, size);
    }
    else
        size = 0;

    mongo_destroy( &conn );

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

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
#include <limits.h>
#include "mongo-fuse.h"

char * blocks_name = "test.blocks";
char * inodes_name = "test.inodes";
char * locks_name = "test.locks";
char * inodes_coll = "inodes";
char * dbname = "test";
const char * mongo_host = "127.0.0.1";
int mongo_port = 27017;

int mongo_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
    off_t offset, struct fuse_file_info *fi);
int mongo_mkdir(const char * path, mode_t mode);
int mongo_rmdir(const char * path);
int mongo_read(const char *path, char *buf, size_t size, off_t offset,
               struct fuse_file_info *fi);
int mongo_write(const char *path, const char *buf, size_t size,
               off_t offset, struct fuse_file_info *fi);

static int mongo_getattr(const char *path, struct stat *stbuf) {
    int res = 0;
    struct inode e;

    memset(stbuf, 0, sizeof(struct stat));
    res = get_inode(path, &e);
    if(res != 0) {
        if(strcmp(path, "/") == 0) {
            stbuf->st_mode = S_IFDIR | 0755;
            stbuf->st_nlink = 2;
            return mongo_mkdir("/", 0755);
        }
        return res;
    }

    stbuf->st_nlink = e.direntcount;
    stbuf->st_mode = e.mode;
    if(stbuf->st_mode & S_IFDIR)
        stbuf->st_nlink++;
    stbuf->st_uid = e.owner;
    stbuf->st_gid = e.group;
    stbuf->st_size = e.size;
    stbuf->st_ctime = e.created;
    stbuf->st_mtime = e.modified;
    stbuf->st_atime = e.modified;
    stbuf->st_dev = e.dev;

    free_inode(&e);
    return res;
}

static int mongo_rename(const char * path, const char * newpath) {
    struct inode e;
    struct dirent * nd, *last = NULL;
    const size_t newpathlen = strlen(newpath);
    int res;

    if((res = get_inode(path, &e)) != 0)
        return res;

    struct dirent * d = e.dirents;
    while(d && strcmp(d->path, path) != 0) {
        last = d;
        d = d->next;
    }

    nd = realloc(d, sizeof(struct dirent) + newpathlen);
    if(!nd) {
        free_inode(&e);
        return -ENOMEM;
    }

    if(last)
        last->next = nd;
    else
        e.dirents = nd;
    strcpy(nd->path, newpath);
    res = commit_inode(&e);
    free_inode(&e);
    return res;
}

static int mongo_open(const char *path, struct fuse_file_info *fi)
{
    struct inode e;
    int res;

    res = get_inode(path, &e);
    if(res == 0)
        free_inode(&e);
    return res;
}

static int mongo_create(const char * path, mode_t mode, struct fuse_file_info * fi) {
    int res = create_inode(path, mode, NULL);
    if(res == -EEXIST)
        return fi->flags & O_EXCL ? res : 0;
    return res;
}

static int mongo_symlink(const char * path, const char * target) {
    return create_inode(target, 0120777, path);
}

static int mongo_readlink(const char * path, char * out, size_t outlen) {
    struct inode e;
    int res;

    res = get_inode(path, &e);
    if(res != 0) {
        free_inode(&e);
        return res;
    }

    if(!(e.mode & S_IFLNK)) {
        free_inode(&e);
        return res;
    }

    strncpy(out, e.data, outlen > e.datalen ? e.datalen : outlen);
    free_inode(&e);

    return 0;
}

static int mongo_truncate(const char * path, off_t off) {
    struct inode e;
    int res;

    if((res = get_inode(path, &e)) != 0)
        return res;

    if(off == e.size) {
        free_inode(&e);
        return 0;
    }

    res = do_trunc(&e, off);
    if(res != 0) {
        free_inode(&e);
        return res;
    }
    commit_inode(&e);
    free_inode(&e);
    return 0;
}

static int mongo_link(const char * path, const char * newpath) {
    struct inode e;
    int res;
    size_t newpathlen = strlen(newpath);

    if((res = get_inode(path, &e)) != 0)
        return res;

    if(e.mode & S_IFDIR) {
        free_inode(&e);
        return -EPERM;
    }

    struct dirent * newlink = malloc(sizeof(struct dirent) + newpathlen);
    strcpy(newlink->path, newpath);
    newlink->next = e.dirents;
    newlink->len = newpathlen;
    e.dirents = newlink;
    e.direntcount++;
    res = commit_inode(&e);
    free_inode(&e);
    return res;
}

static int mongo_unlink(const char * path) {
    struct inode e;
    int res;
    mongo * conn = get_conn();

    if((res = get_inode(path, &e)) != 0)
        return res;

    if(e.direntcount > 1) {
        struct dirent * c = e.dirents, *l = NULL;
        while(c && strcmp(c->path, path) != 0) {
            l = c;
            c = c->next;
        }
        if(!l)
            e.dirents = c->next;
        else
            l->next = c->next;
        free(c);
        e.direntcount--;
        res = commit_inode(&e);
        free_inode(&e);
        return res;
    }

    bson cond;
    bson_init(&cond);
    bson_append_oid(&cond, "_id", &e.oid);
    bson_finish(&cond);

    res = mongo_remove(conn, inodes_name, &cond, NULL);
    bson_destroy(&cond);
    if(res != MONGO_OK) {
        fprintf(stderr, "Error removing inode entry for %s\n", path);
        return -EIO;
    }

    res = do_trunc(&e, 0);
    free_inode(&e);
    return res;
}

static int mongo_chmod(const char * path, mode_t mode) {
    struct inode e;
    int res;

    if((res = get_inode(path, &e)) != 0)
        return res;

    e.mode = (e.mode & S_IFMT) | mode;

    res = commit_inode(&e);
    free_inode(&e);
    return res;
}

static int mongo_chown(const char * path, uid_t user, gid_t group) {
    struct inode e;
    int res;

    if((res = get_inode(path, &e)) != 0)
        return res;

    e.owner = user;
    e.group = group;

    res = commit_inode(&e);
    free_inode(&e);
    return res;

}

static int mongo_utimens(const char * path, const struct timespec tv[2]) {
    struct inode e;
    int res;

    if((res = get_inode(path, &e)) != 0)
        return res;

    if(tv == NULL)
        e.modified = time(NULL);
    else
        e.modified = tv[1].tv_sec;

    res = commit_inode(&e);
    if(res != 0) {
        free_inode(&e);
        return res;
    }

    if(e.mode & S_IFDIR) {
        size_t pathlen = strlen(path);
        char * filename = (char*)path + pathlen;
        while(*(filename - 1) != '/') filename--;
        if(strcmp(filename, ".snapshot") == 0)
            res = snapshot_dir(path, pathlen, e.mode);
    }

    free_inode(&e);
    return res;
}

static int mongo_access(const char * path, int amode) {
    const struct fuse_context * fcx = fuse_get_context();
    struct inode e;
    int res;

    if(fcx->uid == 0)
        return 0;

    if((res = get_inode(path, &e)) != 0)
        return res;

    res = check_access(&e, amode) ? -EACCES : 0;
    free_inode(&e);
    return res;
}

#if FUSE_VERSION > 28
static int mongo_flock(const char * path, struct fuse_file_info * fi, int op) {
    struct inode e;
    int res;

    if((res = get_inode(path, &e)) != 0)
        return res;

    bson_date_t time;
    int write;
    if(op & LOCK_UN) {
        time = fi->lock_owner & 0x7FFFFFFFFFFFFFFF;
        write = (fi->lock_owner & 0x8000000000000000) >> 63;
        res = unlock_inode(&e, write, time);
        free_inode(&e);
        return res;
    }
    write = op & LOCK_EX;
    res = lock_inode(&e, write, &time, op & LOCK_NB);
    free_inode(&e);
    if(res != 0)
        return res;
    fi->lock_owner = ((uint64_t)time) | ((uint64_t)write << 63);
    return 0;
}
#endif

static struct fuse_operations mongo_oper = {
    .getattr    = mongo_getattr,
    .readdir    = mongo_readdir,
    .open       = mongo_open,
    .read       = mongo_read,
    .write      = mongo_write,
    .create     = mongo_create,
    .truncate   = mongo_truncate,
    .mkdir      = mongo_mkdir,
    .unlink     = mongo_unlink,
    .link       = mongo_link,
    .chmod      = mongo_chmod,
    .chown      = mongo_chown,
    .rmdir      = mongo_rmdir,
    .utimens    = mongo_utimens,
    .rename     = mongo_rename,
    .access     = mongo_access,
    .symlink    = mongo_symlink,
    .readlink   = mongo_readlink,
#if FUSE_VERSION > 28
    .flock      = mongo_flock
#endif
};

int main(int argc, char *argv[])
{
    setup_threading();
    int rc = fuse_main(argc, argv, &mongo_oper, NULL);
    teardown_threading();
    return rc;
}

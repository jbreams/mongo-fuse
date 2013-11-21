// mongo_fuse.c

/**
  gcc -Wall -DMONGO_HAVE_STDINT mongo-fuse.c `pkg-config fuse --cflags --libs` -o mongo-fuse -lmongoc
 */

#define FUSE_USE_VERSION 26

#include <fuse.h>
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
#include <fuse/fuse_opt.h>
#include <stddef.h>
#include "mongo-fuse.h"

char * blocks_name;
char * inodes_name;
char * extents_name;
char * dirents_name;
char * dbname;
char * dirents_coll = "dirents";
char * inodes_coll = "inodes";
mongo_host_port dbhost;
mongo_write_concern write_concern;

int mongo_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
    off_t offset, struct fuse_file_info *fi);
int mongo_mkdir(const char * path, mode_t mode);
int mongo_rmdir(const char * path);
int mongo_read(const char *path, char *buf, size_t size, off_t offset,
               struct fuse_file_info *fi);
int mongo_write(const char *path, const char *buf, size_t size,
               off_t offset, struct fuse_file_info *fi);
int mongo_rename(const char * path, const char * newpath);

static void getattr_impl(struct inode * e, struct stat * stbuf) {
    memset(stbuf, 0, sizeof(struct stat));
    stbuf->st_nlink = 1;
    stbuf->st_mode = e->mode;
    if(stbuf->st_mode & S_IFDIR)
        stbuf->st_nlink++;
    stbuf->st_uid = e->owner;
    stbuf->st_gid = e->group;
    stbuf->st_size = e->size;
    stbuf->st_ctime = e->created;
    stbuf->st_mtime = e->modified;
    stbuf->st_atime = e->modified;
}
static int mongo_getattr(const char *path, struct stat *stbuf) {
    int res = 0;
    struct inode e;

    res = get_inode(path, &e);
    if(res != 0)
        return res;

    getattr_impl(&e, stbuf);
    free_inode(&e);
    return res;
}

static int mongo_fgetattr(const char *path, struct stat *stbuf,
    struct fuse_file_info *fi) {
    struct inode * e = (struct inode *)fi->fh;
    getattr_impl(e, stbuf);
    return 0;
}

static int mongo_open(const char *path, struct fuse_file_info *fi)
{
    struct inode * e = malloc(sizeof(struct inode));
    int res;

    res = get_inode(path, e);
    if(res != 0) {
        free_inode(e);
        free(e);
        return res;
    }
    fi->fh = (uintptr_t)e;
    e->updated = time(NULL);
    e->wr_age = e->updated;

    return 0;
}

static int mongo_create(const char * path, mode_t mode, struct fuse_file_info * fi) {
    int res = create_inode(path, mode, NULL);
    if(res == -EEXIST && fi->flags & O_EXCL)
        return -EEXIST;
    else if(res != 0)
        return res;
    return mongo_open(path, fi);
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

static int mongo_ftruncate(const char * path, off_t off,
    struct fuse_file_info * fi) {
    struct inode * e = (struct inode*)fi->fh;
    int res;

    if(off == e->size)
        return 0;

    res = do_trunc(e, off);
    if(res != 0)
        return res;
    return commit_inode(e);
}

static int mongo_link(const char * path, const char * newpath) {
    struct inode e;
    int res;

    if((res = get_inode(path, &e)) != 0)
        return res;

    if(e.mode & S_IFDIR) {
        free_inode(&e);
        return -EPERM;
    }

    res = link_dirent(newpath, &e.oid);
    free_inode(&e);
    return res;
}

static int mongo_unlink(const char * path) {
    return unlink_dirent(path);
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

    if((res = get_inode(path, &e)) != 0) {
        if(res == -ENOENT && strcmp(path, "/") == 0) {
            res = mongo_mkdir("/", 0755);
        }
        return res;
    }

    res = check_access(&e, amode) ? -EACCES : 0;
    free_inode(&e);
    return res;
}

static int mongo_flush(const char * path, struct fuse_file_info * fi) {
    struct inode * e = (struct inode*)fi->fh;
    int res = 0;
    pthread_mutex_lock(&e->wr_lock);
    if(e->wr_extent) {
        res = serialize_extent(e, e->wr_extent);
        if(res != 0)
            goto end;
    }
    res = commit_inode(e);
    if(res != 0)
        goto end;
    e->wr_age = time(NULL);
end:
    pthread_mutex_unlock(&e->wr_lock);
    return res;
}

static int mongo_fsync(const char * path, int syncdata,
    struct fuse_file_info * fi) {
    return mongo_flush(path, fi);
}

static int mongo_release(const char * path, struct fuse_file_info * fi) {
    struct inode * e = (struct inode*)fi->fh;
    free_inode(e);
    free(e);
    return 0;
}

static void *mongo_initfs(struct fuse_conn_info * conn) {
    struct inode e;
    int res = get_inode("/", &e);
    if(res != 0) {
         mongo_mkdir("/", 0755);
    } else
        free_inode(&e);
    return NULL;
}

static struct fuse_operations mongo_oper = {
    .getattr    = mongo_getattr,
    .fgetattr   = mongo_fgetattr,
    .readdir    = mongo_readdir,
    .open       = mongo_open,
    .read       = mongo_read,
    .write      = mongo_write,
    .create     = mongo_create,
    .truncate   = mongo_truncate,
    .ftruncate  = mongo_ftruncate,
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
    .flush      = mongo_flush,
    .fsync      = mongo_fsync,
    .release    = mongo_release,
    .init       = mongo_initfs
};

void parse_args(struct fuse_args * rawargs) {
    // Struct for parsing args
    struct mongo_fuse_config {
        char * dbhost;
        char * mddbname;
        char * blockdbname;
        int journal;
        int writeconcern;
        int majorityconcern;
    } opts;

#define MF_OPT(t, p, v) { t, offsetof(struct mongo_fuse_config, p), v }

    static struct fuse_opt mongo_fuse_opts[] = {
        MF_OPT("dbhost=%s", dbhost, 0),
        MF_OPT("dbname=%s", mddbname, 0),
        MF_OPT("blockdbname=%s", blockdbname, 0),
        MF_OPT("journal", journal, 1),
        MF_OPT("majority", majorityconcern, 1),
        MF_OPT("w=%i", writeconcern, 0),
        FUSE_OPT_END
    };

    memset(&opts, 0, sizeof(opts));
    opts.writeconcern = 1;
    fuse_opt_parse(rawargs, &opts, mongo_fuse_opts, NULL);

    if(!opts.dbhost)
        opts.dbhost = "127.0.0.1";
    if(!opts.mddbname)
        opts.mddbname = "mongofuse";

    asprintf(&blocks_name, "%s.blocks", opts.blockdbname ? opts.blockdbname : opts.mddbname );
    asprintf(&inodes_name, "%s.inodes", opts.mddbname);
    asprintf(&extents_name, "%s.extents", opts.mddbname);
    asprintf(&dirents_name, "%s.dirents", opts.mddbname);
    dbname = strdup(opts.mddbname);
    mongo_parse_host(opts.dbhost, &dbhost);

    memset(&write_concern, 0, sizeof(write_concern));
    if(opts.journal == 1)
        mongo_write_concern_set_j(&write_concern, 1);

    if(opts.majorityconcern == 1)
        mongo_write_concern_set_mode(&write_concern, "majority");
    else
        mongo_write_concern_set_w(&write_concern, opts.writeconcern);
    mongo_write_concern_finish(&write_concern);
}

int main(int argc, char *argv[])
{
    struct fuse_args rawargs = FUSE_ARGS_INIT(argc, argv);
    parse_args(&rawargs);
    setup_threading();
    int rc = fuse_main(rawargs.argc, rawargs.argv, &mongo_oper, NULL);
    return rc;
}

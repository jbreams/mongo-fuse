#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <mongo.h>
#include <stdlib.h>
#include <search.h>
#include <sys/stat.h>
#include "mongo-fuse.h"
#include <osxfuse/fuse.h>
#include <limits.h>
#include <time.h>

extern const char * inodes_name;
extern const char * dbname;
extern const char * inodes_coll;
extern const char * dirents_name;
extern const char * dirents_coll;
extern char empty_hash[];

struct readdir_data {
    fuse_fill_dir_t filler;
    void * buf;
};

struct orphansnapshot_data {
    const char * topparent;
    int rootlen;
};

int readdir_cb(const char * path, void * p, size_t parentlen) {
    struct readdir_data * rd = (struct readdir_data*)p;
    size_t printlen = parentlen;
    if(parentlen > 1)
        printlen++;

    if(strcmp(path + printlen, ".snapshot") == 0)
        return 0;

    rd->filler(rd->buf, path + printlen, NULL, 0);

    return 0;
}

int mongo_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
    off_t offset, struct fuse_file_info *fi) {

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);

    struct readdir_data rb = {
        .filler = filler,
        .buf = buf
    };

    return read_dirents(path, readdir_cb, &rb);
}

int mongo_mkdir(const char * path, mode_t mode) {
    char snapshotdir[PATH_MAX];
    int res, isroot = (*(path + 1) == '\0');

    sprintf(snapshotdir, "%s/.snapshot", isroot ? path + 1 : path);
    if((res = create_inode(path, mode | S_IFDIR, NULL)) != 0)
        return res;

    res = create_inode(snapshotdir, mode | S_IFDIR, NULL);
    return res;
}

int orphan_snapshot(const char * path, void * p, size_t parentlen) {
    struct orphansnapshot_data * d = (struct orphansnapshot_data*)p;
    int res;
    struct inode e;
    size_t path_len = strlen(path);
    char newpath[PATH_MAX];
    const char * filename = path + path_len;
    while(*(filename - 1) != '/')
        filename--;
    
    if((res = get_inode(path, &e)) != 0)
        return res;

    if(e.mode & S_IFDIR) {
        res = read_dirents(path, orphan_snapshot, p);
        if(res != 0)
            return res;
    }

    if(strcmp(filename, ".snapshot") == 0) {
        sprintf(newpath, "/%*.s.snapshot/orphaned-%s",
            d->rootlen, d->topparent, d->topparent + d->rootlen + 1);
    }
    else {
        sprintf(newpath, "/%*.s.snapshot/orphaned-%s/%s",
            d->rootlen, d->topparent, d->topparent + d->rootlen, filename);
    }

    return rename_dirent(path, newpath);
}

int mongo_rmdir(const char * path) {
    struct inode e;
    int res;
    double dres;
    bson cond;
    char regexp[PATH_MAX + 25];
    mongo * conn = get_conn();

    sprintf(regexp, "^%s/[^/]+$", path);
    bson_init(&cond);
    bson_append_regex(&cond, "dirents", regexp, "");
    bson_finish(&cond);

    dres = mongo_count(conn, dbname, dirents_coll, &cond);
    bson_destroy(&cond);

    if(dres > 1)
        return -ENOTEMPTY;

    if(strstr(path, "/.snapshot") == NULL) {
        sprintf(regexp, "%s/.snapshot", path);
        if((res = get_inode(regexp, &e)) != 0)
            return res;

        sprintf(regexp, "^%s/.snapshot/", path);
        bson_init(&cond);
        bson_append_regex(&cond, "dirents", regexp, "");
        bson_finish(&cond);

        dres = mongo_count(conn, dbname, dirents_coll, &cond);
        bson_destroy(&cond);

        if(dres > 0) {
            struct orphansnapshot_data osd;
            size_t rootlen = strlen(path);
            while(path[--rootlen] != '/');
            osd.topparent = path;
            osd.rootlen = rootlen;
            res = orphan_snapshot(path, (void*)&osd, 0);
            free_inode(&e);
            if(res != 0)
                return res;
        }
    }

    return unlink_dirent(path);
}

int create_snapshot(const char * path, void * p, size_t parentlen) {
    bson_oid_t newid;
    int res;
    char newpath[PATH_MAX];
    char * filename = (char*)path + parentlen + 1;
    char * generation = (char*)p;
    struct elist * root = NULL;
    struct inode e;

    res = get_inode(path, &e);
    if(res != 0)
        return res;

    if(e.mode & S_IFDIR)
        return 0;

    res = deserialize_extent(&e, 0, e.size, &root);
    if(res != 0)
        return res;
    bson_oid_gen(&newid);
    memcpy(&e.oid, &newid, sizeof(bson_oid_t));
    res = serialize_extent(&e, root);
    free(root);

    res = commit_inode(&e);

    snprintf(newpath, PATH_MAX, "%*.s/.snapshot/%s/%s",
        (int)parentlen, path, generation, filename);
    if(res != 0) {
        fprintf(stderr, "Error committing inode for %s\n", newpath);
    }
    
    res = link_dirent(newpath, &newid);
    return res;
}

int snapshot_dir(const char * path, size_t pathlen, mode_t mode) {
    char dirpath[PATH_MAX + 1], regexp[PATH_MAX + 1];
    char snapshotname[20];
    struct tm curtime;
    time_t curtimet;
    int res;

    strcpy(dirpath, path);
    while(dirpath[pathlen] != '/') pathlen--;
    dirpath[pathlen] = '\0';

    curtimet = time(NULL);
    localtime_r(&curtimet, &curtime);
    strftime(snapshotname, 20, "%F %T", &curtime);

    sprintf(regexp, "%s/.snapshot/%s", dirpath, snapshotname);
    if((res = create_inode(regexp, mode, NULL)) != 0)
        return res;

    return read_dirents(dirpath, create_snapshot, snapshotname);
}

int mongo_rename(const char * path, const char * newpath) {
    return rename_dirent(path, newpath);
}


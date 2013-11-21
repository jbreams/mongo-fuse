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
extern char empty_hash[];

struct readdir_data {
    fuse_fill_dir_t filler;
    void * buf;
};

int read_dirents(const char * directory,
    int (*dirent_cb)(struct inode *e, void * p,
    const char * parent, size_t parentlen), void * p) {
    bson query;
    mongo_cursor curs;
    size_t pathlen = strlen(directory);
    char regexp[PATH_MAX + 10];
    int res;
    mongo * conn = get_conn();

    sprintf(regexp, "^%s/[^/]+$", pathlen == 1 ? directory + 1 : directory);
    bson_init(&query);
    bson_append_regex(&query, "dirents", regexp, "");
    bson_finish(&query);

    mongo_cursor_init(&curs, conn, inodes_name);
    mongo_cursor_set_query(&curs, &query);

    while((res = mongo_cursor_next(&curs)) == MONGO_OK) {
        struct inode e;
        init_inode(&e);
        res = read_inode(mongo_cursor_bson(&curs), &e);
        if(res != 0) {
            fprintf(stderr, "Error in read_inode\n");
            break;
        }

        res = dirent_cb(&e, p, directory, pathlen);
        free_inode(&e);
        if(res != 0)
            break;
    }
    bson_destroy(&query);
    mongo_cursor_destroy(&curs);

    if(curs.err != MONGO_CURSOR_EXHAUSTED) {
        fprintf(stderr, "Error listing directory contents\n");
        return -EIO;
    }
    return 0;
}

int readdir_cb(struct inode * e, void * p,
    const char * parent, size_t parentlen) {
    struct readdir_data * rd = (struct readdir_data*)p;
    struct stat stbuf;
    size_t printlen = parentlen;
    if(parentlen > 1)
        printlen++;

    stbuf.st_nlink = 1;
    stbuf.st_mode = e->mode;
    if(stbuf.st_mode & S_IFDIR)
        stbuf.st_nlink++;
    stbuf.st_uid = e->owner;
    stbuf.st_gid = e->group;
    stbuf.st_size = e->size;
    stbuf.st_ctime = e->created;
    stbuf.st_mtime = e->modified;
    stbuf.st_atime = e->modified;

    struct dirent * cde = e->dirents;
    while(cde) {
        if(strncmp(cde->path, parent, parentlen) != 0 ||
            strcmp(cde->path + printlen, ".snapshot") == 0) {
            cde = cde->next;
            continue;
        }
        rd->filler(rd->buf, cde->path + printlen, &stbuf, 0);
        cde = cde->next;
    }
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

int orphan_snapshot(struct inode *e, void * p,
    const char * parent, size_t parentlen) {
    const char * topparent = (const char*)p;
    struct dirent * cde = e->dirents, *desave = NULL;
    while(cde && strncmp(cde->path, parent, parentlen) != 0) {
        desave = cde;
        cde = cde->next;
    }
    const char * shortname = cde->path + cde->len;
    int rootlen = strlen(topparent);
    while(topparent[--rootlen] != '/');
    int res, nslashes = 1;

    if(e->mode & S_IFDIR) {
            nslashes--;
        if((res = read_dirents(cde->path,
            orphan_snapshot, (void*)topparent)) != 0)
            return res;
    }

    while(nslashes > 0 || *(shortname - 1) != '/') {
        if(*(shortname - 1) == '/')
            nslashes--;
        shortname--;
    }

    struct dirent * nd = malloc(sizeof(struct dirent) + PATH_MAX);
    if(!nd)
        return -ENOMEM;
    if(strcmp(shortname, ".snapshot") == 0) {
        nd->len = sprintf(nd->path, "/%*.s.snapshot/orphaned-%s",
            rootlen, topparent, topparent + rootlen + 1);
    } else {
        nd->len = sprintf(nd->path, "/%*.s.snapshot/orphaned-%s/%s",
            rootlen, topparent, topparent + rootlen + 1, shortname);
    }
    nd->next = cde->next;
    if(desave)
        desave->next = nd;
    else
        e->dirents = nd;
    commit_inode(e);
    if(desave)
        desave->next = cde;
    else
        e->dirents = cde;
    free(nd);

    return 0;
}

int mongo_rmdir(const char * path) {
    struct inode e;
    int res;
    double dres;
    bson cond;
    char regexp[PATH_MAX + 25];
    mongo * conn = get_conn();

    if((res = inode_exists(path)) != 0)
        return res;

    sprintf(regexp, "^%s/[^/]+$", path);
    bson_init(&cond);
    bson_append_regex(&cond, "dirents", regexp, "");
    bson_finish(&cond);

    dres = mongo_count(conn, dbname, inodes_coll, &cond);
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

        dres = mongo_count(conn, dbname, inodes_coll, &cond);
        bson_destroy(&cond);

        if(dres > 0) {
            res = orphan_snapshot(&e, (void*)path, NULL, 0);
            free_inode(&e);
            if(res != 0)
                return res;
        }
    }

    sprintf(regexp, "^%s", path);
    bson_init(&cond);
    bson_append_regex(&cond, "dirents", regexp, "");
    bson_finish(&cond);

    res = mongo_remove(conn, inodes_name, &cond, NULL);
    bson_destroy(&cond);
    if(res != MONGO_OK) {
        fprintf(stderr, "Error removing inode entry for %s\n", path);
        return -EIO;
    }
    return 0;
}

int create_snapshot(struct inode * e, void * p, const char * parent, size_t plen) {
    bson_oid_t newid;
    int res;
    const char * path = e->dirents->path;
    size_t pathlen = e->dirents->len;
    char * filename = (char*)path + pathlen;
    char * generation = (char*)p;
    struct elist * root = NULL;

    if(!root)
        return -ENOMEM;

    if(e->mode & S_IFDIR)
        return 0;

    res = deserialize_extent(e, 0, e->size, &root);
    if(res != 0)
        return res;
    bson_oid_gen(&newid);
    memcpy(&e->oid, &newid, sizeof(bson_oid_t));
    res = serialize_extent(e, root);
    free(root);
    
    while(*(filename-1) != '/') filename--;
    struct dirent * d = malloc(sizeof(struct dirent) + pathlen + 21);
    d->len = sprintf(d->path, "%s/.snapshot/%s/%s", parent, generation, filename);
    d->next = NULL;

    struct dirent * freeme = e->dirents;
    while(freeme) {
        struct dirent * freeme_next = freeme->next;
        free(freeme);
        freeme = freeme_next;
    }
    e->dirents = d;
    e->direntcount = 1;

    res = commit_inode(e);
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
    mongo * conn = get_conn();
    bson query, doc;
    int res;

    bson_init(&query);
    bson_append_string(&query, "dirents", path);
    bson_finish(&query);

    bson_init(&doc);
    bson_append_start_object(&doc, "$set");
    bson_append_string(&doc, "dirents.$", newpath);
    bson_append_finish_object(&doc);
    bson_finish(&doc);

    res = mongo_update(conn, inodes_name, &query, &doc,
        MONGO_UPDATE_BASIC, NULL);
    bson_destroy(&doc);
    bson_destroy(&query);

    if(res != MONGO_OK)
        return -EIO;
    return inode_exists(newpath);
}


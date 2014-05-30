#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <search.h>
#include <sys/stat.h>
#include "mongo-fuse.h"
#include <osxfuse/fuse.h>
#include <limits.h>
#include <time.h>

extern char empty_hash[];

struct readdir_data {
    fuse_fill_dir_t filler;
    void * buf;
};

int read_dirents(const char * directory,
    int (*dirent_cb)(struct inode *e, void * p,
    const char * parent, size_t parentlen), void * p) {
    size_t pathlen = strlen(directory);
    char regexp[PATH_MAX + 10];
    int res;
    mongoc_collection_t * coll = get_coll(COLL_INODES);
    bson_t query;
    const bson_t *doc;
    bson_error_t dberr;
    mongoc_cursor_t * curs;

    sprintf(regexp, "^%s/[^/]+$", pathlen == 1 ? directory + 1 : directory);
    bson_init(&query);
    bson_append_regex(&query, KEYEXP("dirents"), regexp, "");

    curs = mongoc_collection_find(coll,
        MONGOC_QUERY_NONE,
        0, // skip
        0, // limit
        0, // batch size
        &query,
        NULL, // fields
        NULL); // read prefs

    bson_destroy(&query);
    if(!curs) {
        logit(ERROR, "Error getting cursor while reading directory entries");
        return -EIO;
    }

    while(mongoc_cursor_next(curs, &doc)) {
        struct inode e;
        init_inode(&e);
        res = read_inode(doc, &e);
        if(res != 0) {
            fprintf(stderr, "Error in read_inode\n");
            break;
        }

        res = dirent_cb(&e, p, directory, pathlen);
        free_inode(&e);
        if(res != 0)
            break;
    }

    if(mongoc_cursor_error(curs, &dberr)) {
        mongoc_cursor_destroy(curs);
        logit(ERROR, "Error reading directory entries for %s: %s",
            directory, dberr.message);
        return -EIO;
    }

    mongoc_cursor_destroy(curs);
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
    bson_error_t dberr;
    int64_t dres;
    int res;
    bson_t cond;
    char regexp[PATH_MAX + 25];
    mongoc_collection_t * coll = get_coll(COLL_INODES);

    if((res = inode_exists(path)) != 0)
        return res;

    sprintf(regexp, "^%s/[^/]+$", path);
    bson_init(&cond);
    bson_append_regex(&cond, KEYEXP("dirents"), regexp, "");

    dres = mongoc_collection_count(coll,
        0, // flags
        &cond,
        0, // skip
        0, // limit
        NULL, // write preferences
        &dberr);

    bson_destroy(&cond);

    if(dres == -1) {
        logit(ERROR, "Error counting directory entries: %s", dberr.message);
        return -EIO;
    }

    if(dres > 1)
        return -ENOTEMPTY;

    if(strstr(path, "/.snapshot") == NULL) {
        sprintf(regexp, "%s/.snapshot", path);
        if((res = get_inode(regexp, &e)) != 0)
            return res;

        sprintf(regexp, "^%s/.snapshot/", path);
        bson_init(&cond);
        bson_append_regex(&cond, KEYEXP("dirents"), regexp, "");

        dres = mongoc_collection_count(coll,
            0, // flags
            &cond,
            0, // skip
            0, // limit
            NULL, // write preferences
            &dberr);

        bson_destroy(&cond);

        if(dres == -1) {
            logit(ERROR, "Error counting directory entries: %s", dberr.message);
            return -EIO;
        }

        if(dres > 0) {
            res = orphan_snapshot(&e, (void*)path, NULL, 0);
            free_inode(&e);
            if(res != 0)
                return res;
        }
    }

    sprintf(regexp, "^%s", path);
    bson_init(&cond);
    bson_append_regex(&cond, KEYEXP("dirents"), regexp, "");

    res = mongoc_collection_delete(coll,
        0, // flags
        &cond,
        NULL, // write concern
        &dberr);

    bson_destroy(&cond);

    if(!res) {
        logit(ERROR, "Error removing directory entry %s: %s", path, dberr.message);
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
    bson_oid_init(&newid, NULL);
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
    mongoc_collection_t * coll = get_coll(COLL_INODES);
    bson_t query, doc, setdoc;
    bson_error_t dberr;
    int res;

    bson_init(&query);
    bson_append_utf8(&query, KEYEXP("dirents"), path, strlen(path));

    bson_init(&doc);
    bson_append_document_begin(&doc, KEYEXP("$set"), &setdoc);
    bson_append_utf8(&setdoc, KEYEXP("dirents.$"), newpath, strlen(path));
    bson_append_document_end(&doc, &setdoc);

    res = mongoc_collection_update(coll,
        MONGOC_UPDATE_NONE,
        &query,
        &doc,
        NULL, // write concern
        &dberr);

    bson_destroy(&query);
    bson_destroy(&doc);

    if(!res) {
        logit(ERROR, "Error renaming inode %s: %s", path, dberr.message);
        return -EIO;
    }

    return inode_exists(newpath);
}

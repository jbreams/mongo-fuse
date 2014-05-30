#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <search.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/file.h>
#include <limits.h>
#include <time.h>
#include <pthread.h>
#include "mongo-fuse.h"
#include <osxfuse/fuse.h>
#include <execinfo.h>

extern const char * inodes_name;
extern const char * locks_name;

int inode_exists(const char * path) {
    bson_t query, fields, *doc;
    bson_error_t dberr;
    mongoc_collection_t * coll = get_coll(COLL_INODES);
    mongoc_cursor_t * curs;

    bson_init(&query);
    bson_append_utf8(&query, KEYEXP("dirents"), path, strlen(path));

    bson_init(&fields);
    bson_append_int32(&fields, KEYEXP("dirents"), 1);
    bson_append_int32(&fields, KEYEXP("_id"), 0);

    curs = mongoc_collection_find(coll,
        MONGOC_QUERY_NONE,
        0, // skip
        1, // limit,
        0, // batch size,
        &query,
        &fields,
        NULL); // read prefs

    bson_destroy(&query);
    bson_destroy(&fields);

    if(curs == NULL)
        return -EIO;

    if(!mongoc_cursor_next(curs, (const bson_t**)&doc)) {
        if(mongoc_cursor_error(curs, &dberr)) {
            logit(ERROR, "Could not query db for %s: %s", path, dberr.message);
            return -EIO;
        }
        return -ENOENT;
    }

    // Do something with the doc? Maybe cache it?
    mongoc_cursor_destroy(curs);
    return 0;
}

int commit_inode(struct inode * e) {
    bson_t cond, top, doc, direntsarray;
    bson_error_t dberr;
    mongoc_collection_t * coll = get_coll(COLL_INODES);
    char istr_buf[4];
    struct dirent * cde = e->dirents;
    int i;
    bool res;

    bson_init(&top);
    bson_append_document_begin(&top, KEYEXP("$set"), &doc);
    bson_append_oid(&doc, KEYEXP("_id"), &e->oid);

    bson_append_array_begin(&doc, KEYEXP("dirents"), &direntsarray);
    i = 0;
    while(cde) {
        const char * keystr;
        size_t keylen;
        keylen = bson_uint32_to_string(i++, &keystr, istr_buf, sizeof(istr_buf));
        bson_append_utf8(&direntsarray, keystr, keylen, cde->path, cde->len);
        cde = cde->next;
    }
    bson_append_array_end(&doc, &direntsarray);

    bson_append_int32(&doc, KEYEXP("mode"), e->mode);
    bson_append_int64(&doc, KEYEXP("owner"), e->owner);
    bson_append_int64(&doc, KEYEXP("group"), e->group);
    bson_append_int64(&doc, KEYEXP("size"), e->size);
    bson_append_time_t(&doc, KEYEXP("created"), e->created);
    bson_append_time_t(&doc, KEYEXP("modified"), e->modified);
    if(e->data && e->datalen > 0)
        bson_append_utf8(&doc, KEYEXP("data"), e->data, e->datalen);
    bson_append_document_end(&top, &doc);

    bson_init(&cond);
    bson_append_oid(&cond, KEYEXP("_id"), &e->oid);

    res = mongoc_collection_update(coll,
        MONGOC_UPDATE_UPSERT,
        &cond,
        &top,
        NULL, // write_concern,
        &dberr);

    bson_destroy(&cond);
    bson_destroy(&top);
    if(!res) {
        char oidstr[25];
        bson_oid_to_string(&e->oid, oidstr);
        logit(ERROR, "Error committing inode %s: %s",
            oidstr, dberr.message);
        return -EIO;
    }
    return 0;
}

void init_inode(struct inode * e) {
    memset(e, 0, sizeof(struct inode));
    pthread_mutex_init(&e->wr_lock, NULL);
}

int read_inode(const bson_t * doc, struct inode * out) {
    bson_iter_t iter, sub;

    bson_iter_init(&iter, doc);

    while(bson_iter_next(&iter)) {
        const char * key = bson_iter_key(&iter);
        if(strcmp(key, "_id") == 0)
            bson_oid_copy(bson_iter_oid(&iter), &out->oid);
        else if(strcmp(key, "mode") == 0)
            out->mode = bson_iter_int32(&iter);
        else if(strcmp(key, "owner") == 0)
            out->owner = bson_iter_int64(&iter);
        else if(strcmp(key, "group") == 0)
            out->group = bson_iter_int64(&iter);
        else if(strcmp(key, "size") == 0)
            out->size = bson_iter_int64(&iter);
        else if(strcmp(key, "created") == 0)
            out->created = bson_iter_time_t(&iter);
        else if(strcmp(key, "modified") == 0)
            out->modified = bson_iter_time_t(&iter);
        else if(strcmp(key, "data") == 0)
            out->data = bson_iter_dup_utf8(&iter, (uint32_t*)&out->datalen);
        else if(strcmp(key, "dirents") == 0) {
            while(out->dirents) {
                struct dirent * next = out->dirents->next;
                free(out->dirents);
                out->dirents = next;
            }

            bson_iter_recurse(&iter, &sub);
            while(bson_iter_next(&sub)) {
                uint32_t len;
                const char * pathstr = bson_iter_utf8(&sub, &len);
                struct dirent * cde = malloc(sizeof(struct dirent) + len + 1);
                if(!cde)
                    return -ENOMEM;
                strcpy(cde->path, pathstr);
                cde->len = len;
                cde->next = out->dirents;
                out->dirents = cde;
                out->direntcount++;
            }
        }
    }

    return 0;
}

int get_inode_impl(const char * path, struct inode * out) {
    bson_t query;
    const bson_t *doc;
    int res;
    mongoc_collection_t * coll = get_coll(COLL_INODES);
    mongoc_cursor_t * curs;

    bson_init(&query);
    bson_append_utf8(&query, KEYEXP("dirents"), path, strlen(path));

    curs = mongoc_collection_find(coll,
        MONGOC_QUERY_NONE,
        0, // skip
        1, // limit
        0, // batch size ??
        &query,
        NULL, // fields
        NULL); // write concern

    bson_destroy(&query);

    if(!curs) {
        logit(ERROR, "Error getting cursor for %s", path);
        return -EIO;
    }

    if(!mongoc_cursor_next(curs, &doc)) {
        bson_error_t dberr;
        if(mongoc_cursor_error(curs, &dberr)) {
            logit(ERROR, "Error getting inode for %s: %s", path, dberr.message);
            return -EIO;
        }
        return -ENOENT;
    }

    res = read_inode(doc, out);
    mongoc_cursor_destroy(curs);

    return res;
}

int get_cached_inode(const char * path, struct inode * out) {
    time_t now = time(NULL);
    int res;
    if(now - out->updated < 3)
        return 0;

    res = get_inode_impl(path, out);
    if(res == 0)
        out->updated = now;
    return res;
}

int get_inode(const char * path, struct inode * out) {
    init_inode(out);
    return get_inode_impl(path, out);
}

int check_access(struct inode * e, int amode) {
    const struct fuse_context * fcx = fuse_get_context();
    mode_t mode = e->mode;

    if(fcx->uid == 0 || amode == 0)
        return 0;

    if(fcx->uid == e->owner)
        mode >>= 6;
    else if(fcx->gid == e->group)
        mode >>= 3;

    return (((mode & S_IRWXO) & amode) == 0);
}

int create_inode(const char * path, mode_t mode, const char * data) {
    struct inode e;
    int pathlen = strlen(path);
    const struct fuse_context * fcx = fuse_get_context();
    int res;

    res = inode_exists(path);
    if(res == 0) {
        fprintf(stderr, "%s already exists\n", path);
        return -EEXIST;
    } else if(res == -EIO)
        return -EIO;

    init_inode(&e);
    bson_oid_init(&e.oid, NULL);
    e.dirents = malloc(sizeof(struct dirent) + pathlen);
    e.dirents->len = pathlen;
    strcpy(e.dirents->path, path);
    e.dirents->next = NULL;
    e.direntcount = 1;

    e.mode = mode;
    e.owner = fcx->uid;
    e.group = fcx->gid;
    e.created = time(NULL);
    e.modified = time(NULL);
    if(data) {
        e.datalen = strlen(data);
        e.data = strdup(data);
        e.size = e.datalen;
    } else {
        e.data = NULL;
        e.datalen = 0;
        e.size = 0;
    }
    e.wr_extent = NULL;
    e.wr_age = 0;

    res = commit_inode(&e);
    free_inode(&e);
    return res;
}

void free_inode(struct inode *e) {
    if(e->data)
        free(e->data);
    while(e->dirents) {
        struct dirent * next = e->dirents->next;
        free(e->dirents);
        e->dirents = next;
    }
    if(e->wr_extent)
        free(e->wr_extent);
}

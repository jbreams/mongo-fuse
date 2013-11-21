#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <mongo.h>
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

int commit_inode(struct inode * e) {
    bson cond, doc;
    mongo * conn = get_conn();
    int res;

    bson_init(&doc);
    bson_append_oid(&doc, "_id", &e->oid);
    bson_append_int(&doc, "mode", e->mode);
    bson_append_long(&doc, "owner", e->owner);
    bson_append_long(&doc, "group", e->group);
    bson_append_long(&doc, "size", e->size);
    bson_append_time_t(&doc, "created", e->created);
    bson_append_time_t(&doc, "modified", e->modified);
    if(e->data && e->datalen > 0)
        bson_append_string_n(&doc, "data", e->data, e->datalen);
    bson_finish(&doc);

    bson_init(&cond);
    bson_append_oid(&cond, "_id", &e->oid);
    bson_finish(&cond);

    res = mongo_update(conn, inodes_name, &cond, &doc,
        MONGO_UPDATE_UPSERT, NULL);
    bson_destroy(&cond);
    bson_destroy(&doc);
    if(res != MONGO_OK) {
        fprintf(stderr, "Error committing inode %s\n",
            mongo_get_server_err_string(conn));
        return -EIO;
    }
    return 0;
}

void init_inode(struct inode * e) {
    memset(e, 0, sizeof(struct inode));
    pthread_mutex_init(&e->wr_lock, NULL);
}

int read_inode(const bson * doc, struct inode * out) {
    bson_iterator i;
    bson_type bt;
    const char * key;

    bson_iterator_init(&i, doc);
    while((bt = bson_iterator_next(&i)) > 0) {
        key = bson_iterator_key(&i);
        if(strcmp(key, "_id") == 0)
            memcpy(&out->oid, bson_iterator_oid(&i), sizeof(bson_oid_t));
        else if(strcmp(key, "mode") == 0)
            out->mode = bson_iterator_int(&i);
        else if(strcmp(key, "owner") == 0)
            out->owner = bson_iterator_long(&i);
        else if(strcmp(key, "group") == 0)
            out->group = bson_iterator_long(&i);
        else if(strcmp(key, "size") == 0)
            out->size = bson_iterator_long(&i);
        else if(strcmp(key, "created") == 0)
            out->created = bson_iterator_time_t(&i);
        else if(strcmp(key, "modified") == 0)
            out->modified = bson_iterator_time_t(&i);
        else if(strcmp(key, "data") == 0) {
            out->datalen = bson_iterator_string_len(&i);
            out->data = malloc(out->datalen + 1);
            strcpy(out->data, bson_iterator_string(&i));
        }
    }

    return 0;
}

int get_inode_impl(const char * path, struct inode * out) {
    bson query, doc;
    bson_oid_t inode_id;
    int res;
    mongo * conn = get_conn();

    if((res = resolve_dirent(path, &inode_id)) != 0)
        return res;

    bson_init(&query);
    bson_append_oid(&query, "_id", &inode_id);
    bson_finish(&query);

    res = mongo_find_one(conn, inodes_name, &query,
         bson_shared_empty(), &doc);
    bson_destroy(&query);

    if(res != MONGO_OK) {
        fprintf(stderr, "Error finding inode %s\n", path);
        return -EIO;
    }

    res = read_inode(&doc, out);
    bson_destroy(&doc);

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
    const struct fuse_context * fcx = fuse_get_context();
    bson_oid_t inode_id;
    int res;

    res = resolve_dirent(path, &inode_id);
    if(res == 0) {
        return -EEXIST;
    }
    else if(res == -EIO)
        return -EIO;

    init_inode(&e);
    bson_oid_gen(&inode_id);
    memcpy(&e.oid, &inode_id, sizeof(bson_oid_t));

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
    if(res != 0)
        return res;
    res = link_dirent(path, &inode_id);

    return res;
}

void free_inode(struct inode *e) {
    if(e->data)
        free(e->data);
    if(e->wr_extent)
        free(e->wr_extent);
}

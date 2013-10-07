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

extern const char * inodes_name;
extern const char * locks_name;

int inode_exists(const char * path) {
    bson query, fields;
    mongo * conn = get_conn();
    mongo_cursor curs;
    int res;

    bson_init(&query);
    bson_append_string(&query, "dirents", path);
    bson_finish(&query);

    bson_init(&fields);
    bson_append_int(&fields, "dirents", 1);
    bson_append_int(&fields, "_id", 0);
    bson_finish(&fields);

    mongo_cursor_init(&curs, conn, inodes_name);
    mongo_cursor_set_query(&curs, &query);
    mongo_cursor_set_fields(&curs, &fields);
    mongo_cursor_set_limit(&curs, 1);

    res = mongo_cursor_next(&curs);
    bson_destroy(&query);
    bson_destroy(&fields);
    mongo_cursor_destroy(&curs);

    if(res == 0)
        return 0;
    if(curs.err != MONGO_CURSOR_EXHAUSTED)
        return -EIO;
    return -ENOENT;
}

int commit_inode(struct inode * e) {
    bson cond, doc;
    mongo * conn = get_conn();
    char istr[4];
    struct dirent * cde = e->dirents;
    int res;

    bson_init(&doc);
    bson_append_start_object(&doc, "$set");
    bson_append_start_array(&doc, "dirents");
    res = 0;
    while(cde) {
        bson_numstr(istr, res++);
        bson_append_string(&doc, istr, cde->path);
        cde = cde->next;
    }
    bson_append_finish_array(&doc);

    bson_append_int(&doc, "mode", e->mode);
    bson_append_long(&doc, "owner", e->owner);
    bson_append_long(&doc, "group", e->group);
    bson_append_long(&doc, "size", e->size);
    if(e->dev > 0)
        bson_append_long(&doc, "dev", e->dev);
    bson_append_time_t(&doc, "created", e->created);
    bson_append_time_t(&doc, "modified", e->modified);
    if(e->data && e->datalen > 0)
        bson_append_string_n(&doc, "data", e->data, e->datalen);
    bson_append_finish_object(&doc);
    bson_finish(&doc);

    bson_init(&cond);
    bson_append_oid(&cond, "_id", &e->oid);
    bson_finish(&cond);

    res = mongo_update(conn, inodes_name, &cond, &doc,
        MONGO_UPDATE_UPSERT, NULL);
    bson_destroy(&cond);
    bson_destroy(&doc);
    if(res != MONGO_OK) {
        fprintf(stderr, "Error committing inode\n");
        return -EIO;
    }
    return 0;
}

void init_inode(struct inode * e) {
    memset(e, 0, sizeof(struct inode));
    pthread_rwlock_init(&e->rd_extent_lock, NULL);
    pthread_mutex_init(&e->wr_extent_lock, NULL);
}

int read_inode(const bson * doc, struct inode * out) {
    bson_iterator i, sub;
    bson_type bt;
    const char * key;

    init_inode(out);
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
        else if(strcmp(key, "dev") == 0)
            out->dev = bson_iterator_long(&i);
        else if(strcmp(key, "data") == 0) {
            out->datalen = bson_iterator_string_len(&i);
            out->data = malloc(out->datalen + 1);
            strcpy(out->data, bson_iterator_string(&i));
        }
        else if(strcmp(key, "dirents") == 0) {
            bson_iterator_subiterator(&i, &sub);
            while((bt = bson_iterator_next(&sub)) > 0) {
                int len = bson_iterator_string_len(&sub);
                struct dirent * cde = malloc(sizeof(struct dirent) + len);
                if(!cde)
                    return -ENOMEM;
                strcpy(cde->path, bson_iterator_string(&sub));
                cde->len = bson_iterator_string_len(&sub);
                cde->next = out->dirents;
                out->dirents = cde;
                out->direntcount++;
            }
        }
    }

    return 0;
}

int get_cached_inode(const char * path, struct inode * out) {
    time_t now = time(NULL);
    int res;
    if(now - out->updated < 3)
        return 0;

    res = get_inode(path, out);
    if(res == 0)
        out->updated = now;
    return res;
}

int get_inode(const char * path, struct inode * out) {
    bson query, doc;
    int res;
    mongo * conn = get_conn();

    bson_init(&query);
    bson_append_string(&query, "dirents", path);
    bson_finish(&query);

    res = mongo_find_one(conn, inodes_name, &query,
         bson_shared_empty(), &doc);

    if(res != MONGO_OK) {
        bson_destroy(&query);
        return -ENOENT;
    }

    bson_destroy(&query);
    res = read_inode(&doc, out);
    bson_destroy(&doc);

    return res;
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
    bson_oid_gen(&e.oid);
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
    e.rd_extent_root = NULL;
    e.wr_extent_root = NULL;
    e.wr_extent_updated = 0;
    e.rd_extent_updated = 0;

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
    if(e->rd_extent_root)
        free_extent_tree(e->rd_extent_root);
    if(e->wr_extent_root)
        free_extent_tree(e->wr_extent_root);
}

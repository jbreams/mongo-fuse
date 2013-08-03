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

extern const char * inodes_name;
extern mongo conn;

int commit_inode(struct inode * e) {
    bson cond, *doc;
    mongo * conn = get_conn();
    char istr[4];
    struct dirent * cde = e->dirents;
    int res;

    doc = bson_alloc();
    if(!doc)
        return -ENOMEM;

    bson_init(doc);
    bson_append_start_object(doc, "$set");
    bson_append_start_array(doc, "dirents");
    res = 0;
    while(cde) {
        bson_numstr(istr, res++);
        bson_append_string(doc, istr, cde->path);
        cde = cde->next;
    }
    bson_append_finish_array(doc);

    bson_append_int(doc, "mode", e->mode);
    bson_append_long(doc, "owner", e->owner);
    bson_append_long(doc, "group", e->group);
    bson_append_long(doc, "size", e->size);
    if(e->dev > 0)
        bson_append_long(doc, "dev", e->dev);
    bson_append_time_t(doc, "created", e->created);
    bson_append_time_t(doc, "modified", e->modified);
    if(e->data && e->datalen > 0)
        bson_append_binary(doc, "data", 0, e->data, e->datalen);
    bson_append_finish_object(doc);
    bson_finish(doc);

    bson_print(doc);

    bson_init(&cond);
    bson_append_oid(&cond, "_id", &e->oid);
    bson_finish(&cond);

    res = mongo_update(conn, inodes_name, &cond, doc, MONGO_UPDATE_UPSERT, NULL);
    bson_destroy(&cond);
    bson_destroy(doc);
    bson_dealloc(doc);
    if(res != MONGO_OK) {
        fprintf(stderr, "Error committing inode\n");
        return -EIO;
    }
    return 0;
}

int read_inode(const bson * doc, struct inode * out) {
    bson_iterator i;
    bson_type bt;
    int res;
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
        else if(strcmp(key, "dev") == 0)
            out->dev = bson_iterator_long(&i);
        else if(strcmp(key, "data") == 0) {
            if(bt == BSON_STRING) {
                out->datalen = bson_iterator_string_len(&i);
                strcpy(out->data, bson_iterator_string(&i));
            } else if(bt == BSON_BINDATA) {
                out->datalen = bson_iterator_bin_len(&i);
                memcpy(out->data, bson_iterator_bin_data(&i), out->datalen);
            }
        }
        else if(strcmp(key, "dirents") == 0) {
            bson_iterator sub;
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

int get_inode(const char * path, struct inode * out, int getdata) {
    bson query, doc, fields;
    bson_iterator i;
    bson_type bt;
    int res;
    const char * key;
    mongo * conn = get_conn();

    bson_init(&query);
    bson_append_string(&query, "dirents", path);
    bson_finish(&query);

    if(!getdata) {
        bson_init(&fields);
        bson_append_int(&fields, "data", 0);
        bson_finish(&fields);
    }

    res = mongo_find_one(conn, inodes_name, &query,
            getdata ? bson_shared_empty():&fields, &doc);

    if(res != MONGO_OK) {
        fprintf(stderr, "find one failed for %s: %d\n", path, res);
        bson_destroy(&query);
        return -ENOENT;
    }

    memset(out, 0, sizeof(struct inode));
    bson_destroy(&query);
    if(getdata) {
        out->data = malloc(EXTENT_SIZE);
        memset(out->data, 0, EXTENT_SIZE);
    }
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

    res = get_inode(path, &e, 0);
    if(res == 0) {
        free_inode(&e);
        return -EEXIST;
    }

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
}


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
    bson cond, doc;
    char istr[4];
    struct dirent * cde = e->dirents;
    int res;

    bson_init(&cond);
    bson_append_oid(&cond, "_id", &e->oid);
    bson_finish(&cond);

    bson_init(&doc);
    bson_append_oid(&doc, "_id", &e->oid);
    bson_append_start_array(&doc, "dirents");
    res = 0;
    while(cde) {
        bson_numstr(istr, res++);
        printf("Adding %s\n", cde->path);
        bson_append_string(&doc, istr, cde->path);
        cde = cde->next;
    }
    bson_append_finish_array(&doc);

    bson_append_int(&doc, "mode", e->mode);
    bson_append_long(&doc, "owner", e->owner);
    bson_append_long(&doc, "group", e->group);
    bson_append_long(&doc, "size", e->size);
    bson_append_time_t(&doc, "created", e->created);
    bson_append_time_t(&doc, "modified", e->modified);
    if(e->data && e->datalen > 0)
        bson_append_binary(&doc, "data", 0, e->data, e->size);

    bson_finish(&doc);
    res = mongo_update(&conn, inodes_name, &cond, &doc, MONGO_UPDATE_UPSERT, NULL);
    bson_destroy(&cond);
    bson_destroy(&doc);
    if(res != MONGO_OK) {
        fprintf(stderr, "Error committing inode\n");
        return -EIO;
    }
    return 0;
}

int get_inode(const char * path, struct inode * out, int getdata) {
    bson query, doc, fields;
    bson_iterator i;
    bson_type bt;
    int res;
    const char * key;

    bson_init(&query);
    bson_append_string(&query, "dirents", path);
    bson_finish(&query);

    if(!getdata) {
        bson_init(&fields);
        bson_append_int(&fields, "data", 0);
        bson_finish(&fields);
    }

    res = mongo_find_one(&conn, inodes_name, &query,
            getdata ? bson_shared_empty():&fields, &doc);

    if(res != MONGO_OK) {
        fprintf(stderr, "find one failed: %d\n", res);
        bson_destroy(&query);
        return -ENOENT;
    }

    memset(out, 0, sizeof(struct inode));
    bson_destroy(&query);
    bson_iterator_init(&i, &doc);
    if(getdata) {
        out->data = malloc(EXTENT_SIZE);
        memset(out->data, 0, EXTENT_SIZE);
    }

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
        else if(strcmp(key, "data") == 0 && getdata) {
            if(bt == BSON_STRING) {
                out->datalen = bson_iterator_string_len(&i);
                strcpy(out->data, bson_iterator_string(&i));
            } else if(bt == BSON_BINDATA) {
                out->datalen = bson_iterator_bin_len(&i);
                memcpy(out->data, bson_iterator_bin_data(&i), out->datalen);
            }
        }
        else if(strcmp(key, "dirents")) {
            bson_iterator sub;
            bson_iterator_subiterator(&i, &sub);
            while((bt = bson_iterator_next(&sub)) > 0) {
                int len = bson_iterator_string_len(&sub);
                struct dirent * cde = malloc(sizeof(struct dirent) + len);
                strcpy(cde->path, bson_iterator_string(&sub));
                cde->next = out->dirents;
                out->dirents = cde;
            }
        }
    }
    bson_destroy(&doc);

    return 0;
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


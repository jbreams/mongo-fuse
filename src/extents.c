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

extern const char * blocks_name;
extern mongo conn;

void free_extents(struct extent * head) {
    while(head) {
        struct extent * n = head->next;
        free(head);
        head = n;
    }
}

int commit_extent(struct inode * ent, struct extent *e) {
    int res;
    bson doc, cond;

    bson_ensure_space(&doc, EXTENT_SIZE + 128);
    bson_append_oid(&doc, "_id", &e->oid);
    bson_append_oid(&doc, "inode", &ent->oid);
    bson_append_long(&doc, "start", e->start);
    bson_append_binary(&doc, "data", 0, e->data, e->size);
    bson_finish(&doc);

    bson_init(&cond);
    bson_append_oid(&cond, "_id", &e->oid);
    bson_finish(&cond);

    res = mongo_update(&conn, blocks_name, &cond, &doc, MONGO_UPDATE_UPSERT, NULL);
    if(res != MONGO_OK) {
        fprintf(stderr, "Error committing block\n");
        return -EIO;
    }
    bson_destroy(&doc);
    bson_destroy(&cond);
    return 0;
}

int commit_extents(struct inode * ent, struct extent * e) {
    while(e) {
        int res = commit_extent(ent, e);
        if(res != 0)
            return res;
        e = e->next;
    }
    return 0;
}

int resolve_extent(struct inode * e, off_t start,
    off_t end, struct extent ** realout) {
    bson query;
    int res, x, count;
    struct extent * out = NULL;
    mongo_cursor curs;
    bson_iterator i;
    bson_type bt;
    const char * key;
    struct extent * tail = NULL;

    start = (start/EXTENT_SIZE)*EXTENT_SIZE;

    bson_init(&query);
    bson_append_start_object(&query, "$query");
    bson_append_oid(&query, "inode", &e->oid);
    if(end - start < EXTENT_SIZE) {
        bson_append_long(&query, "start", start);
        count = 1;
    } else {
        x = end % EXTENT_SIZE;
        end = ((end / EXTENT_SIZE) + (x?1:0)) * EXTENT_SIZE;
        count = end - start / EXTENT_SIZE;

        bson_append_start_object(&query, "start");
        bson_append_long(&query, "$gte", start);
        bson_append_long(&query, "$lte", end);
        bson_append_finish_object(&query);
    }
    bson_append_finish_object(&query);
    bson_append_start_object(&query, "$orderby");
    bson_append_int(&query, "start", -1);
    bson_append_finish_object(&query);
    bson_finish(&query);

    mongo_cursor_init(&curs, &conn, blocks_name);
    mongo_cursor_set_query(&curs, &query);

    while((res = mongo_cursor_next(&curs)) == MONGO_OK) {
        out = malloc(sizeof(struct extent));
        memset(out, 0, sizeof(struct extent));
        out->next = tail;
        tail = out;

        const bson * curbson = mongo_cursor_bson(&curs);
        bson_iterator_init(&i, curbson);

        while((bt = bson_iterator_next(&i)) > 0) {
            key = bson_iterator_key(&i);
            if(strcmp(key, "_id") == 0)
                memcpy(&out->oid, bson_iterator_oid(&i), sizeof(bson_oid_t));
            else if(strcmp(key, "start") == 0)
                out->start = bson_iterator_long(&i);
            else if(strcmp(key, "data") == 0) {
                out->size = bson_iterator_bin_len(&i);
                memcpy(out->data,
                    bson_iterator_bin_data(&i), out->size);
            }
        }
    }

    bson_destroy(&query);
    if(curs.err != MONGO_CURSOR_EXHAUSTED) {
        fprintf(stderr, "Error getting extents %d", curs.err);
        free(out);
        return -EIO;
    }

    *realout = out;

    return 0;
}


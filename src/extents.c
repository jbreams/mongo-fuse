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
extern int blocks_name_len;
const char * block_suffixes[] = {
    "4k", "8k", "16k", "32k", "64k", "128k", "256k", "512k", "1m"
};

void free_extents(struct extent * head) {
    while(head) {
        struct extent * n = head->next;
        free(head);
        head = n;
    }
}

off_t compute_start(struct inode * e, off_t offset) {
    return (offset / e->blocksize) * e->blocksize;
}

void get_block_collection(struct inode * e, char * name) {
    int i;
    switch(e->blocksize) {
        case 4096:
        default:
            i = 0;
            break;
        case 8192:
            i = 1;
            break;
        case 16384:
            i = 2;
            break;
        case 32768:
            i = 3;
            break;
        case 65536:
            i = 4;
            break;
        case 132072:
            i = 5;
            break;
        case 262144:
            i = 6;
            break;
        case 524288:
            i = 7;
            break;
        case 1048576:
            i = 8;
            break;
    }

    sprintf(name, "%s_%s", blocks_name, block_suffixes[i]);
}

int commit_extent(struct inode * ent, struct extent *e) {
    int res;
    bson * doc, cond;
    mongo * conn = get_conn();
    char blocks_coll[32];

    doc = bson_alloc();
    if(!doc)
        return -ENOMEM;
    bson_init(doc);
    bson_append_start_object(doc, "_id");
    bson_append_oid(doc, "inode", &ent->oid);
    bson_append_long(doc, "start", e->start);
    bson_append_finish_object(doc);
    bson_append_binary(doc, "data", 0, e->data, ent->blocksize);
    bson_finish(doc);

    bson_init(&cond);
    bson_append_start_object(&cond, "_id");
    bson_append_oid(&cond, "inode", &ent->oid);
    bson_append_long(&cond, "start", e->start);
    bson_append_finish_object(&cond);
    bson_finish(&cond);

    get_block_collection(ent, blocks_coll);

    res = mongo_update(conn, blocks_coll, &cond, doc, MONGO_UPDATE_UPSERT, NULL);
    bson_destroy(doc);
    bson_dealloc(doc);
    bson_destroy(&cond);

    if(res != MONGO_OK) {
        fprintf(stderr, "Error committing block\n");
        return -EIO;
    }
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

struct extent * new_extent(struct inode * e) {
    struct extent * n = malloc(sizeof(struct extent) + e->blocksize);
    if(!n)
        return NULL;
    memset(n, 0, sizeof(struct extent) + e->blocksize);
    memcpy(&n->inode, &e->oid, sizeof(bson_oid_t));
    return n;
}

int resolve_extent(struct inode * e, off_t start,
    off_t end, struct extent ** realout) {
    bson query;
    int res;
    struct extent * out = NULL, *tail = NULL;
    mongo_cursor curs;
    bson_iterator i;
    bson_type bt;
    const char * key;
    mongo * conn = get_conn();
    char block_coll[32];

    bson_init(&query);
    bson_append_start_object(&query, "$query");
    bson_append_oid(&query, "_id.inode", &e->oid);
    if(end - start < e->blocksize) {
        bson_append_long(&query, "_id.start", start);
    } else {
        int mod = end % e->blocksize;
        end = ((end / e->blocksize) + (mod?1:0)) * e->blocksize;

        bson_append_start_object(&query, "_id.start");
        bson_append_long(&query, "$gte", start);
        bson_append_long(&query, "$lt", end);
        bson_append_finish_object(&query);
    }
    bson_append_finish_object(&query);
    bson_append_start_object(&query, "$orderby");
    bson_append_int(&query, "_id.start", -1);
    bson_append_finish_object(&query);
    bson_finish(&query);

    get_block_collection(e, block_coll);

    mongo_cursor_init(&curs, conn, block_coll);
    mongo_cursor_set_query(&curs, &query);

    while((res = mongo_cursor_next(&curs)) == MONGO_OK) {
        out = new_extent(e);
        out->next = tail;
        tail = out;

        const bson * curbson = mongo_cursor_bson(&curs);
        bson_iterator_init(&i, curbson);

        while((bt = bson_iterator_next(&i)) > 0) {
            key = bson_iterator_key(&i);
            if(strcmp(key, "_id") == 0) {
                bson_iterator sub;
                bson_iterator_subiterator(&i, &sub);
                while(bson_iterator_next(&sub)) {
                    key = bson_iterator_key(&sub);
                    if(strcmp(key, "start") == 0) {
                        out->start = bson_iterator_long(&sub);
                        break;
                    }
                }
            }
            else if(strcmp(key, "data") == 0) {
                size_t size = bson_iterator_bin_len(&i);
                memcpy(out->data,
                    bson_iterator_bin_data(&i), size);
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


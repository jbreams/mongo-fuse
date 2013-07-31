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

int commit_exent(struct inode * ent, struct extent *e) {
    int res;
    bson doc, cond;

    if(e->committed)
        return 0;

    bson_ensure_space(&doc, e->size + 128);
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
    e->committed = 1;
    return 0;
}

static int extent_cmp(const void * ra, const void * rb) {
    const struct extent * a = ra, *b = rb;
    if(a->start < b->start)
        return -1;
    else if(a->start > b->start)
        return 1;
    return 0;
}

int resolve_extent(struct inode * e, off_t start,
    off_t end, struct extent ** realout, int * countout) {
    bson query;
    int res, x, count, y;
    struct extent * out = NULL;
    mongo_cursor curs;
    bson_iterator i;
    bson_type bt;
    const char * key;


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
    bson_append_int(&query, "start", 1);
    bson_append_finish_object(&query);
    bson_finish(&query);

    mongo_cursor_init(&curs, &conn, blocks_name);
    mongo_cursor_set_query(&curs, &query);

    res = mongo_cursor_next(&curs);
    bson_destroy(&query);

    out = malloc(sizeof(struct extent) * count);
    memset(out, 0, sizeof(struct extent) * count);
    *realout = out;
    *countout = count;

    x = 0;
    while((res = mongo_cursor_next(&curs)) == MONGO_OK) {
        const bson * curbson = mongo_cursor_bson(&curs);
        bson_iterator_init(&i, curbson);

        out[x].committed = 1;
        while((bt = bson_iterator_next(&i)) > 0) {
            key = bson_iterator_key(&i);
            if(strcmp(key, "_id") == 0)
                memcpy(&out[x].oid, bson_iterator_oid(&i), sizeof(bson_oid_t));
            else if(strcmp(key, "start") == 0)
                out[x].start = bson_iterator_long(&i);
            else if(strcmp(key, "data") == 0) {
                out[x].size = bson_iterator_bin_len(&i);
                memcpy(out[x].data,
                    bson_iterator_bin_data(&i), out[x].size);
            }
        }
    }

    if(curs.err != MONGO_CURSOR_EXHAUSTED) {
        fprintf(stderr, "Error getting extents %d", curs.err);
        free(out);
        return -EIO;
    }

    if(x == count)
        return 0;

    for(y = 0; y < count; y++) {
        struct extent s;
        off_t curoff = start + (EXTENT_SIZE * y);
        s.start = curoff;
        if(bsearch(&s, out, x, sizeof(struct extent), extent_cmp) != NULL)
            continue;

        out[x].start = curoff;
        bson_oid_gen(&out[x++].oid);
    }

    qsort(out, count, sizeof(struct extent), extent_cmp);

    return 0;
}


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
#ifdef HAVE_SNAPPY
#include <snappy-c.h>
#endif
#include "sha1.h"

extern const char * dbname;
extern const char * blocks_name;
extern int blocks_name_len;
const char * block_suffixes[] = {
    "4k", "8k", "16k", "32k", "64k", "128k", "256k", "512k", "1m"
};

struct inode_id {
    bson_oid_t oid;
    uint64_t start;
};

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
    bson doc, cond;
    mongo * conn = get_conn();
    char blocks_coll[32], *colll = blocks_coll;
    ssize_t realsize = ent->blocksize;
    int sum = 0;
    double count;
    uint8_t hash[20];
    struct inode_id id;

    sha1((uint8_t*)e->data, ent->blocksize, hash);
    get_block_collection(ent, blocks_coll);

    id.start = e->start;
    memcpy(&id.oid, &ent->oid, sizeof(bson_oid_t));

    if(e->foundhash) {
        if(memcmp(e->hash, hash, sizeof(hash)) == 0)
            return 0;
        bson_init(&cond);
        bson_append_binary(&cond, "_id", 0, (char*)e->hash, sizeof(hash));
        bson_finish(&cond);

        bson_init(&doc);
        bson_append_start_object(&doc, "$pull");
        bson_append_binary(&doc, "refs", 0, (char*)&id, sizeof(id));
        bson_append_finish_object(&doc);
        bson_finish(&doc);

        res = mongo_update(conn, blocks_coll, &cond, &doc, 0, NULL);
        bson_destroy(&doc);
        bson_destroy(&cond);
        if(res != MONGO_OK)
            return -EIO;
    }

    bson_init(&cond);
    bson_append_binary(&cond, "_id", 0, (char*)hash, sizeof(hash));
    bson_finish(&cond);

    while(*colll != '.') colll++;
    colll++;

    count = mongo_count(conn, dbname, colll, &cond);
    bson_destroy(&cond);

    for(;realsize >= 0 && sum == 0; realsize--)
        sum |= e->data[realsize];

    bson_init(&doc);
    bson_append_start_object(&doc, "$addToSet");
    bson_append_binary(&doc, "refs", 0, (char*)&id, sizeof(id));
    bson_append_finish_object(&doc);
    if(count < 1) {
        bson_append_start_object(&doc, "$set");
#ifdef HAVE_SNAPPY
        char * comp_out = get_compress_buf();
        size_t comp_size = snappy_max_compressed_length(ent->blocksize);
        if((res = snappy_compress(e->data, ent->blocksize,
            comp_out, &comp_size)) != SNAPPY_OK) {
            fprintf(stderr, "Error compressing input: %d\n", res);
            return -EIO;
        }
        bson_append_binary(&doc, "data", 0, comp_out, comp_size);
#else
        bson_append_binary(&doc, "data", 0, e->data, ent->blocksize);
#endif
        bson_append_finish_object(&doc);
    }
    bson_finish(&doc);

    bson_init(&cond);
    bson_append_binary(&cond, "_id", 0, (char*)hash, sizeof(hash));
    bson_finish(&cond);

    res = mongo_update(conn, blocks_coll, &cond, &doc,
        MONGO_UPDATE_UPSERT, NULL);
    bson_destroy(&doc);
    bson_destroy(&cond);

    if(res != MONGO_OK) {
        fprintf(stderr, "Error committing block %s\n", conn->lasterrstr);
        return -EIO;
    }
    return 0;
}

int resolve_extent(struct inode * e, off_t start,
    struct extent ** realout, int getdata) {
    bson query, fields;
    int res;
    mongo_cursor curs;
    bson_iterator i;
    bson_type bt;
    const char * key;
    mongo * conn = get_conn();
    char block_coll[32];
    struct extent * out = new_extent(e);
    struct inode_id id;

    if(out == NULL)
        return -ENOMEM;

    memcpy(&id.oid, &e->oid, sizeof(bson_oid_t));
    id.start = start;

    bson_init(&query);
    bson_append_binary(&query, "refs", 0, (char*)&id, sizeof(id));
    bson_finish(&query);

    bson_init(&fields);
    bson_append_int(&fields, "refs", 0);
    if(!getdata)
        bson_append_int(&fields, "data", 0);
    bson_finish(&fields);

    get_block_collection(e, block_coll);

    mongo_cursor_init(&curs, conn, block_coll);
    mongo_cursor_set_query(&curs, &query);
    mongo_cursor_set_fields(&curs, &fields);
    mongo_cursor_set_limit(&curs, 1);

    while((res = mongo_cursor_next(&curs)) == MONGO_OK) {
        const bson * curbson = mongo_cursor_bson(&curs);
        bson_iterator_init(&i, curbson);
        bson_print(curbson);

        while((bt = bson_iterator_next(&i)) > 0) {
            key = bson_iterator_key(&i);
            if(strcmp(key, "_id") == 0) {
                memcpy(out->hash, bson_iterator_bin_data(&i),
                    sizeof(out->hash));
                out->foundhash = 1;
            }
            else if(strcmp(key, "data") == 0) {
                size_t size = bson_iterator_bin_len(&i);
#ifdef HAVE_SNAPPY
                size_t outsize = e->blocksize;
                if((res = snappy_uncompress(bson_iterator_bin_data(&i),
                    size, out->data, &outsize)) != SNAPPY_OK) {
                    fprintf(stderr, "Error uncompressing block %d\n", res);
                    free(out);
                    return -EIO;
                }
#else
                memcpy(out->data,
                    bson_iterator_bin_data(&i), size);
#endif
            }
        }
    }

    bson_destroy(&query);
    bson_destroy(&fields);
    mongo_cursor_destroy(&curs);

    if(curs.err != MONGO_CURSOR_EXHAUSTED) {
        fprintf(stderr, "Error getting extents %d", curs.err);
        free(out);
        return -EIO;
    }

    fprintf(stderr, "Block %llu: %d\n", start, out->foundhash);
    *realout = out->foundhash ? out : NULL;
    return 0;
}

int do_trunc(struct inode * e, off_t off) {
    bson cond, doc;
    int res, i = 0;
    mongo * conn = get_conn();
    char blocks_coll[32], istr[5];
    struct inode_id id;

    if(e->mode & S_IFDIR)
        return -EISDIR;

    if(off > e->size) {
        e->size = off;
        res = commit_inode(e);
        return res;
    }

    memcpy(&id.oid, &e->oid, sizeof(bson_oid_t));
    bson_init(&cond);
    bson_append_start_object(&cond, "refs");
    bson_append_start_array(&cond, "$in");
    off = compute_start(e, off);
    while(off < e->size) {
        bson_numstr(istr, i++);
        id.start = off;
        bson_append_binary(&cond, istr, 0, (char*)&id, sizeof(id));
    }
    bson_append_finish_array(&cond);
    bson_append_finish_object(&cond);
    bson_finish(&cond);

    bson_init(&doc);
    bson_append_start_object(&doc, "$pull");
    bson_append_binary(&doc, "refs", 0, (char*)&id, sizeof(id));
    bson_append_finish_object(&doc);
    bson_finish(&doc);

    get_block_collection(e, blocks_coll);
    res = mongo_update(conn, blocks_coll, &cond,
        &doc, MONGO_UPDATE_MULTI, NULL);
    bson_destroy(&cond);
    bson_destroy(&doc);

    if(res != MONGO_OK) {
        fprintf(stderr, "Error truncating blocks\n");
        return -EIO;
    }

    return commit_inode(e);
}


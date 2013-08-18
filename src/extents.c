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
#include <snappy-c.h>
#ifdef __APPLE__
#include <CommonCrypto/CommonDigest.h>
#else
#include <openssl/sha.h>
#endif

extern const char * dbname;
extern const char * blocks_name;
extern const char * inodes_name;
extern int blocks_name_len;
static const char * block_suffixes[] = {
    "4k", "8k", "16k", "32k", "64k", "128k", "256k", "512k", "1m"
};

off_t compute_start(struct inode * e, off_t offset) {
    return (offset / e->blocksize) * e->blocksize;
}

int get_blocksize_index(struct inode * e) {
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
    return i;
}

void get_block_collection(struct inode * e, char * name) {
    int i = get_blocksize_index(e);
    sprintf(name, "%s_%s", blocks_name, block_suffixes[i]);
}

int commit_extent(struct inode * ent, struct extent *e) {
    int res;
    bson doc, cond;
    mongo * conn = get_conn();
    char blocks_coll[32], *colll = blocks_coll;
    ssize_t realend = ent->blocksize;
    double count = 0;
    uint8_t hash[20];
    uint32_t offset, reallen;
    struct inode_id id;

#ifdef __APPLE__
    CC_SHA1(e->data, ent->blocksize, hash);
#else
    SHA1((uint8_t*)e->data, ent->blocksize, hash);
#endif
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

    for(;realend >= 0 && e->data[realend] == '\0'; realend--);
    realend++;
    for(offset = 0; offset < realend && e->data[offset] == 0; offset++);
    offset -= offset > 0 ? 1 : 0;
    reallen = realend - offset;

    if(reallen > 0) {
        bson_init(&cond);
        bson_append_binary(&cond, "_id", 0, (char*)hash, sizeof(hash));
        bson_finish(&cond);

        while(*colll != '.') colll++;
        colll++;

        count = mongo_count(conn, dbname, colll, &cond);
        bson_destroy(&cond);
    } else
        return 0;

    bson_init(&doc);
    bson_append_start_object(&doc, "$addToSet");
    bson_append_binary(&doc, "refs", 0, (char*)&id, sizeof(id));
    bson_append_finish_object(&doc);
    if(count < 1) {
        bson_append_start_object(&doc, "$set");
        if(reallen > 0) {
            char * comp_out = get_compress_buf();
            size_t comp_size = snappy_max_compressed_length(reallen);
            if((res = snappy_compress(e->data + offset, reallen,
                comp_out, &comp_size)) != SNAPPY_OK) {
                fprintf(stderr, "Error compressing input: %d\n", res);
                return -EIO;
            }
            bson_append_binary(&doc, "data", 0, comp_out, comp_size);
        } else
            bson_append_null(&doc, "data");
        bson_append_int(&doc, "offset", offset);
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
        size_t outsize = e->blocksize;
        char * comp_out = get_compress_buf();
        uint32_t offset;

        bson_iterator_init(&i, curbson);
        while((bt = bson_iterator_next(&i)) > 0) {
            key = bson_iterator_key(&i);
            if(strcmp(key, "_id") == 0) {
                memcpy(out->hash, bson_iterator_bin_data(&i),
                    sizeof(out->hash));
                out->foundhash = 1;
            }
            else if(strcmp(key, "data") == 0) {
                size_t size = bson_iterator_bin_len(&i);
                if((res = snappy_uncompress(bson_iterator_bin_data(&i),
                    size, comp_out, &outsize)) != SNAPPY_OK) {
                    fprintf(stderr, "Error uncompressing block %d\n", res);
                    free(out);
                    return -EIO;
                }
            }
            else if(strcmp(key, "offset") == 0)
                offset = bson_iterator_int(&i);
        }
        memcpy(&out->data + offset, comp_out, outsize);
    }

    bson_destroy(&query);
    bson_destroy(&fields);
    mongo_cursor_destroy(&curs);

    if(curs.err != MONGO_CURSOR_EXHAUSTED) {
        fprintf(stderr, "Error getting extents %d", curs.err);
        free(out);
        return -EIO;
    }

    *realout = out->foundhash ? out : NULL;
    return 0;
}

int do_trunc(struct inode * e, off_t off) {
    bson cond, doc;
    int res;
    mongo * conn = get_conn();
    char blocks_coll[32];
    struct inode_id id;
    off_t moving_off = off > 0 ? compute_start(e, off) + e->blocksize : 0;

    if(e->mode & S_IFDIR)
        return -EISDIR;

    if(off > e->size) {
        e->size = off;
        return 0;
    }

    if(off == 0) {
        bson_oid_t oldid;
        bson removecond;
        memcpy(&oldid, &e->oid, sizeof(bson_oid_t));

        bson_init(&removecond);
        bson_append_oid(&removecond, "_id", &oldid);
        bson_finish(&removecond);

        add_unlink(e);
        bson_oid_gen(&e->oid);

        res = mongo_remove(conn, inodes_name, &removecond, NULL);
        bson_destroy(&removecond);
        if(res != MONGO_OK) {
            fprintf(stderr, "Error removing inode during truncate\n");
            return -EIO;
        }
        return 0;
    }

    fprintf(stderr, "Truncating file %llu bytes to %llu\n", e->size, off);
    get_block_collection(e, blocks_coll);
    memcpy(&id.oid, &e->oid, sizeof(bson_oid_t));
    while(moving_off < e->size) {
        id.start = moving_off;
        bson_init(&cond);
        bson_append_binary(&cond, "refs", 0, (char*)&id, sizeof(id));
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
        moving_off += e->blocksize;
    }
    e->size = off;
    return 0;
}


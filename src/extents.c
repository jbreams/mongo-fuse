#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <mongo.h>
#include <stdlib.h>
#include <strings.h>
#include <sys/stat.h>
#include "mongo-fuse.h"
#include <osxfuse/fuse.h>
#include <snappy-c.h>
#ifdef __APPLE__
#include <CommonCrypto/CommonDigest.h>
#else
#include <openssl/sha.h>
#endif
#include <pthread.h>
#include <xmmintrin.h>
#include <assert.h>

extern const char * dbname;
extern const char * blocks_name;
extern const char * inodes_name;
extern const char * maps_name;
extern const char * blocks_coll;
extern int blocks_name_len;

const char empty_hash[] = "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";

off_t compute_start(struct inode * e, off_t offset) {
    return (offset / e->blocksize) * e->blocksize;
}

int ensure_blockmaps(struct inode * e, off_t end) {
    int nmaps = (end / (e->blocksize * BLOCKS_PER_MAP)) + 1;
    if(nmaps <= e->nmaps)
        return 0;
    struct block_map ** new_list = malloc(sizeof(struct block_map*) * nmaps);
    if(!new_list)
        return -ENOMEM;
    memset(new_list, 0, sizeof(struct block_map*) * nmaps);
    if(e->maps) {
        memcpy(new_list, e->maps, sizeof(struct block_map*) * e->nmaps);
        free(e->maps);
    }
    e->maps = new_list;
    e->nmaps = nmaps;
    return 0;
}

int read_blockmap(struct inode * e, int map, off_t start) {
    mongo_cursor curs;
    bson cond;
    const bson * curbson;
    bson_iterator i;
    mongo * conn = get_conn();
    struct block_map * curmap;
    bson_type bt;
    int res;

    if(!e->maps[map]) {
        e->maps[map] = malloc(sizeof(struct block_map));
        if(!e->maps[map])
            return -ENOMEM;
    }
    curmap = e->maps[map];
    memset(curmap, 0, sizeof(struct block_map));
    curmap->start = start;
    memcpy(&curmap->inode, &e->oid, sizeof(bson_oid_t));

    bson_init(&cond);
    bson_append_oid(&cond, "inode", &e->oid);
    bson_append_long(&cond, "start", start);
    bson_finish(&cond);

    mongo_cursor_init(&curs, conn, maps_name);
    mongo_cursor_set_query(&curs, &cond);
    mongo_cursor_set_limit(&curs, 1);

    res = mongo_cursor_next(&curs);

    if(res != MONGO_OK) {
        mongo_cursor_destroy(&curs);
        if(curs.err == MONGO_CURSOR_EXHAUSTED) {
            bson doc;
            bson_init(&doc);
            bson_append_start_object(&doc, "$setOnInsert");
            bson_append_binary(&doc, "padding", 0, get_compress_buf(), 74684);
            bson_append_start_array(&doc, "blocks");
            bson_append_finish_array(&doc);
            bson_append_finish_object(&doc);
            bson_finish(&doc);

            res = mongo_update(conn, maps_name, &cond, &doc,
                MONGO_UPDATE_UPSERT, NULL);
            bson_destroy(&doc);
            bson_destroy(&cond);
            return map;
        }
        bson_destroy(&cond);
        return -EIO;
    }
    bson_destroy(&cond);
    curbson = mongo_cursor_bson(&curs);
    bson_iterator_init(&i, curbson);

    while((bt = bson_iterator_next(&i)) != 0) {
        const char * key = bson_iterator_key(&i);
        if(strcmp(key, "_id") == 0)
            memcpy(&curmap->oid, bson_iterator_oid(&i), sizeof(bson_oid_t));
        else if(strcmp(key, "inode") == 0)
            memcpy(&curmap->inode, bson_iterator_oid(&i), sizeof(bson_oid_t));
        else if(strcmp(key, "start") == 0)
            curmap->start = bson_iterator_long(&i);
        else if(strcmp(key, "blocks") == 0) {
            bson_iterator sub;
            bson_iterator_subiterator(&i, &sub);
            while((bt = bson_iterator_next(&sub)) != 0) {
                key = bson_iterator_key(&sub);
                int index = atoi(key);
                if(bt == BSON_NULL)
                    memset(curmap->blocks[index], 0, 20);
                else {
                    memcpy(curmap->blocks[index],
                        bson_iterator_bin_data(&sub), 20);
                }
            }
        }
        else if(strcmp(key, "padding") == 0)
            curmap->has_padding = 1;
    }
    mongo_cursor_destroy(&curs);
    return 0;
}

int commit_blockmap(struct inode * e, struct block_map *map) {
    bson cond, doc;
    mongo * conn = get_conn();
    int res;

    if(!map)
        return 0;

    int i, nchanges = 0;
    for(i = 0; i < BLOCKS_PER_MAP/32; i++)
        if(map->changed[i] != 0)
            nchanges++;
    if(nchanges == 0)
        return 0;

    bson_init(&cond);
    bson_append_oid(&cond, "inode", &map->inode);
    bson_append_long(&cond, "start", map->start);
    bson_finish(&cond);

    bson_init(&doc);
    bson_append_start_object(&doc, "$set");

    for(i = 0; i < BLOCKS_PER_MAP; i++) {
        char indexstr[12];
        if(!(map->changed[WORD_OFFSET(i)] & (1 << BIT_OFFSET(i))))
            continue;
        sprintf(indexstr, "blocks.%d", i);
        if(memcmp(empty_hash, map->blocks[i], 20) == 0)
            bson_append_null(&doc, indexstr);
        else
            bson_append_binary(&doc, indexstr, 0,
                (const char*)map->blocks[i], 20);
    }

    bson_append_finish_object(&doc);
    if(map->has_padding) {
        bson_append_start_object(&doc, "$unset");
        bson_append_string(&doc, "padding", "");
        bson_append_finish_object(&doc);
    }
    bson_finish(&doc);

    res = mongo_update(conn, maps_name, &cond, &doc, MONGO_UPDATE_UPSERT, NULL);
    bson_destroy(&doc);
    bson_destroy(&cond);
    if(res != MONGO_OK)
        return -EIO;
    memset(map->changed, 0, sizeof(map->changed));

    return 0;
}

int get_blockmap(struct inode * e, off_t pos) {
    int map = (pos / (e->blocksize * BLOCKS_PER_MAP));
    off_t start = map * e->blocksize * BLOCKS_PER_MAP;
    time_t now = time(NULL);
    int res;

    if((res = ensure_blockmaps(e, pos)) != 0)
        return res;

    if(e->maps[map]) {
        if(now - e->maps[map]->updated < 3)
            return map;
        else {
            e->maps[map]->updated = now;
            if((res = commit_blockmap(e, e->maps[map])) != 0)
                return res;
        }
    }

    if((res = read_blockmap(e, map, start)) != 0)
        return res;
    e->maps[map]->updated = now;
    return map;
}

int set_block_hash(struct inode * e, off_t off, uint8_t hash[20]) {
    int mapindex = get_blockmap(e, off);
    if(mapindex < 0)
        return mapindex;
    struct block_map * map = e->maps[mapindex];
    int block = (off - map->start) / e->blocksize;
    if(memcmp(map->blocks[block], hash, 20) == 0)
        return 0;
    if(memcmp(map->blocks[block], empty_hash, 20) != 0)
        decref_block(map->blocks[block]);
    memcpy(map->blocks[block], hash, 20);
    map->changed[WORD_OFFSET(block)] |= (1 << BIT_OFFSET(block));
    return 0;
}

int clear_block_hash(struct inode * e, off_t off) {
    int mapindex = get_blockmap(e, off);
    if(mapindex < 0)
        return mapindex;
    struct block_map * map = e->maps[mapindex];
    int block = (off - map->start) / e->blocksize;
    if(memcmp(map->blocks[block], empty_hash, 20) == 0)
        return 0;
    decref_block(map->blocks[block]);
    memset(map->blocks[block], 0, 20);
    map->changed[WORD_OFFSET(block)] |= (1 << BIT_OFFSET(block));
    return 0;
}

int get_block_hash(struct inode * e, off_t off, uint8_t hashout[20]) {
    int mapindex = get_blockmap(e, off);
    if(mapindex < 0)
        return mapindex;
    struct block_map * map = e->maps[mapindex];
    int block = (off - map->start) / e->blocksize;
    memcpy(hashout, map->blocks[block], 20);
    return 0;
}

int commit_extent(struct inode * ent, struct extent *e) {
    int res;
    bson doc, cond;
    mongo * conn = get_conn();
    uint8_t hash[20];
    uint32_t offset = 0, reallen;
    int32_t realend = ent->blocksize;
    char * zlock;


    /* Uncomment this for incredibly slow length calculations.
    for(;realend >= 0 && e->data[realend] == '\0'; realend--);
    realend++;
    for(offset = 0; offset < realend && e->data[offset] == 0; offset++);
    offset -= offset > 0 ? 1 : 0;
    *
    * The code below uses SSE4 instructions to find the first/last
    * zero bytes by doing 16-byte comparisons at a time. This should give
    * a ~16x speed boost on blocks with lots of zero bytes over the dumb
    * method above.
    */
    __m128i zero = _mm_setzero_si128();
    if(e->data[ent->blocksize - 1] == '\0') {
        zlock = e->data + ent->blocksize - 16;
        while(zlock >= e->data) {
            __m128i x = _mm_loadu_si128((__m128i*)zlock);
            res = _mm_movemask_epi8(_mm_cmpeq_epi8(zero, x));
            if(res != 0xffff) {
                realend = zlock - e->data + 16;
                while(realend > 0 && e->data[realend] == '\0')
                    realend--;
                realend++;
                break;
            }
            zlock -= 16;
        }
    }
    if(e->data[0] == '\0') {
        zlock = e->data;
        while(zlock - e->data < realend) {
            __m128i x = _mm_loadu_si128((__m128i*)zlock);
            res = _mm_movemask_epi8(_mm_cmpeq_epi8(zero, x));
            if(res != 0xffff) {
                offset = zlock - e->data;
                while(offset < realend && e->data[offset] == '\0')
                    offset++;
                offset -= offset > 0 ? 1 :0;
                break;
            }
            zlock += 16;
        }
    }
    reallen = realend - offset;

    if(reallen == 0) {
        if((res = clear_block_hash(ent, e->start)) != 0)
            return res;
        return 0;
    }

#ifdef __APPLE__
    CC_SHA1(e->data, ent->blocksize, hash);
#else
    SHA1((uint8_t*)e->data, ent->blocksize, hash);
#endif

    bson_init(&cond);
    bson_append_binary(&cond, "_id", 0, (char*)hash, sizeof(hash));
    bson_finish(&cond);

    bson_init(&doc);
    bson_append_start_object(&doc, "$setOnInsert");
    char * comp_out = get_compress_buf();
    size_t comp_size = snappy_max_compressed_length(reallen);
    if((res = snappy_compress(e->data + offset, reallen,
        comp_out, &comp_size)) != SNAPPY_OK) {
        fprintf(stderr, "Error compressing input: %d\n", res);
        return -EIO;
    }
    bson_append_binary(&doc, "data", 0, comp_out, comp_size);
    bson_append_int(&doc, "offset", offset);
    bson_append_finish_object(&doc);
    bson_append_start_object(&doc, "$inc");
    bson_append_int(&doc, "refs", 1);
    bson_append_finish_object(&doc);
    bson_finish(&doc);

    res = mongo_update(conn, blocks_name, &cond, &doc,
        MONGO_UPDATE_UPSERT, NULL);
    bson_destroy(&doc);
    bson_destroy(&cond);

    if(res != MONGO_OK) {
        fprintf(stderr, "Error committing block %s\n", conn->lasterrstr);
        return -EIO;
    }

    return set_block_hash(ent, e->start, hash);
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
    struct extent * out = new_extent(e);
    uint8_t hash[20];

    if(out == NULL)
        return -ENOMEM;

    if((res = get_block_hash(e, start, hash)) != 0)
        return res;

    if(memcmp(hash, empty_hash, sizeof(hash)) == 0) {
        *realout = NULL;
        return 0;
    }

    if(!getdata) {
        memcpy(out->hash, hash, 20);
        out->start = start;
        *realout = out;
        memset(out->data, 0, e->blocksize);
        return 0;
    }

    bson_init(&query);
    bson_append_binary(&query, "_id", 0, (char*)hash, 20);
    bson_finish(&query);

    bson_init(&fields);
    bson_append_int(&fields, "_id", 0);
    bson_append_int(&fields, "data", 1);
    bson_append_int(&fields, "offset", 1);
    bson_finish(&fields);

    mongo_cursor_init(&curs, conn, blocks_name);
    mongo_cursor_set_query(&curs, &query);
    mongo_cursor_set_fields(&curs, &fields);
    mongo_cursor_set_limit(&curs, 1);

    res = mongo_cursor_next(&curs);
    bson_destroy(&query);
    bson_destroy(&fields);
    if(res != MONGO_OK) {
        mongo_cursor_destroy(&curs);
        return -EIO;
    }

    bson_iterator_init(&i, mongo_cursor_bson(&curs));
    size_t outsize, compsize = 0;
    const char * compdata = NULL;
    uint32_t offset = 0;

    while((bt = bson_iterator_next(&i)) > 0) {
        key = bson_iterator_key(&i);
        if(strcmp(key, "data") == 0) {
            compsize = bson_iterator_bin_len(&i);
            compdata = bson_iterator_bin_data(&i);
        }
        else if(strcmp(key, "offset") == 0)
            offset = bson_iterator_int(&i);
    }

    if(!compdata) {
        fprintf(stderr, "No data in block?\n");
        return -EIO;
    }

    outsize = e->blocksize - offset;
    if((res = snappy_uncompress(compdata, compsize,
        out->data + offset, &outsize)) != SNAPPY_OK) {
        fprintf(stderr, "Error uncompressing block %d\n", res);
        return -EIO;
    }
    if(offset > 0)
        memset(out->data, 0, offset);
    compsize = outsize + offset;
    if(compsize < e->blocksize)
        memset(out->data + compsize, 0, e->blocksize - compsize);
    out->start = start;
    memcpy(out->hash, hash, 20);
    mongo_cursor_destroy(&curs);

    if(curs.err != MONGO_CURSOR_EXHAUSTED) {
        fprintf(stderr, "Error getting extents %d", curs.err);
        free(out);
        return -EIO;
    }

    *realout = out;
    return 0;
}

int do_trunc(struct inode * e, off_t off) {
    bson cond;
    int res;
    mongo * conn = get_conn();
    off_t new_size = off;
    if(e->mode & S_IFDIR)
        return -EISDIR;

    if(off > e->size) {
        e->size = off;
        return 0;
    }

    off = compute_start(e, off);
    while(off < e->size) {
        uint8_t hash[20];
        if((res = get_block_hash(e, off, hash)) != 0)
            return res;
        if(memcmp(hash, empty_hash, sizeof(hash)) != 0)
            decref_block(hash);
        off += e->blocksize;
    }

    e->size = new_size;

    bson_init(&cond);
    bson_append_oid(&cond, "inode", &e->oid);
    if(new_size > 0) {
        int highmap = new_size / (BLOCKS_PER_MAP * e->blocksize);
        bson_append_start_object(&cond, "start");
        bson_append_long(&cond, "$gte",
            highmap * (BLOCKS_PER_MAP * e->blocksize));
        bson_append_finish_object(&cond);
    }
    bson_finish(&cond);

    res = mongo_remove(conn, maps_name, &cond, NULL);
    bson_destroy(&cond);
    if(res != 0)
        return -EIO;
    return 0;
}


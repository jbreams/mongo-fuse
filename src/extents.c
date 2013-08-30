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
#include <pthread.h>

extern const char * dbname;
extern const char * blocks_name;
extern const char * inodes_name;
extern const char * maps_name;
extern int blocks_name_len;
static const char * block_suffixes[] = {
    "4k", "8k", "16k", "32k", "64k", "128k", "256k", "512k", "1m"
};

#define WORD_OFFSET(b) ((b) / 32)
#define BIT_OFFSET(b)  ((b) % 32)
static char empty_hash[] = "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";

struct extent * block_cache[BLOCK_CACHE_SIZE];
pthread_mutex_t block_cache_lock = PTHREAD_MUTEX_INITIALIZER;

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
    bson_destroy(&cond);

    if(res != MONGO_OK) {
        mongo_cursor_destroy(&curs);
        if(curs.err == MONGO_CURSOR_EXHAUSTED)
            return map;
        return -EIO;
    }
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
    bson_append_oid(&cond, "inode", &e->oid);
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
    char blocks_coll[32];
    ssize_t realend = ent->blocksize;
    uint8_t hash[20];
    uint32_t offset, reallen;
    uint16_t cache_hash;
    struct extent * cached_ext;
    size_t ext_size = sizeof(struct extent) + ent->blocksize;

    for(;realend >= 0 && e->data[realend] == '\0'; realend--);
    realend++;
    for(offset = 0; offset < realend && e->data[offset] == 0; offset++);
    offset -= offset > 0 ? 1 : 0;
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
    get_block_collection(ent, blocks_coll);

    bson_init(&doc);
    bson_append_binary(&cond, "_id", 0, (char*)hash, sizeof(hash));
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
    bson_finish(&doc);

    res = mongo_insert(conn, blocks_coll, &doc, NULL);
    bson_destroy(&doc);

    if(res != MONGO_OK && conn->lasterrcode != 11000) {
        fprintf(stderr, "Error committing block %s\n", conn->lasterrstr);
        return -EIO;
    }

    cache_hash = *(uint16_t*)hash;
    cache_hash &= (BLOCK_CACHE_SIZE - 1);
    pthread_mutex_lock(&block_cache_lock);
    if(block_cache[cache_hash]) {
        cached_ext = block_cache[cache_hash];
        if(memcmp(cached_ext->hash, hash, sizeof(hash)) != 0) {
            free(cached_ext);
            cached_ext = malloc(ext_size);
            if(!cached_ext) {
                pthread_mutex_unlock(&block_cache_lock);
                return -ENOMEM;
            }
            memcpy(cached_ext, e, ext_size);
            block_cache[cache_hash] = cached_ext;
        }
    } else {
        cached_ext = malloc(ext_size);
        if(!cached_ext) {
            pthread_mutex_unlock(&block_cache_lock);
            return -ENOMEM;
        }
        memcpy(cached_ext, e, ext_size);
        block_cache[cache_hash] = cached_ext;
    }
    pthread_mutex_unlock(&block_cache_lock);

    return set_block_hash(ent, e->start, hash);
}

int resolve_extent(struct inode * e, off_t start,
    struct extent ** realout, int getdata) {
    bson query;
    int res;
    mongo_cursor curs;
    bson_iterator i;
    bson_type bt;
    const char * key;
    mongo * conn = get_conn();
    char block_coll[32];
    struct extent * out = new_extent(e);
    uint8_t hash[20];
    int foundhash = 0;
    uint16_t cache_hash;

    if(out == NULL)
        return -ENOMEM;

    if((res = get_block_hash(e, start, hash)) != 0)
        return res;

    if(!getdata) {
        if(memcmp(hash, empty_hash, 20) == 0)
            *realout = NULL;
        else {
            memcpy(out->hash, hash, 20);
            *realout = out;
        }
        return 0;
    }

    cache_hash = *(uint16_t*)hash;
    cache_hash &= (BLOCK_CACHE_SIZE - 1);
    pthread_mutex_lock(&block_cache_lock);
    if(block_cache[cache_hash] &&
        memcmp(block_cache[cache_hash]->hash, hash, 20) == 0) {
        size_t to_copy = sizeof(struct extent) + e->blocksize;
        memcpy(out, block_cache[cache_hash], to_copy);
        pthread_mutex_unlock(&block_cache_lock);
        return 0;
    }
    pthread_mutex_unlock(&block_cache_lock);

    bson_init(&query);
    bson_append_binary(&query, "_id", 0, (char*)hash, 20);
    bson_finish(&query);

    get_block_collection(e, block_coll);
    mongo_cursor_init(&curs, conn, block_coll);
    mongo_cursor_set_query(&curs, &query);
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
                foundhash = 1;
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
        out->start = start;
        memcpy(&out->data + offset, comp_out, outsize);
    }

    bson_destroy(&query);
    mongo_cursor_destroy(&curs);

    if(curs.err != MONGO_CURSOR_EXHAUSTED) {
        fprintf(stderr, "Error getting extents %d", curs.err);
        free(out);
        return -EIO;
    }

    *realout = foundhash ? out : NULL;
    return 0;
}

int do_trunc(struct inode * e, off_t off) {
    bson cond;
    int res;
    mongo * conn = get_conn();

    if(e->mode & S_IFDIR)
        return -EISDIR;

    if(off > e->size) {
        e->size = off;
        return 0;
    }

    bson_init(&cond);
    bson_append_oid(&cond, "inode", &e->oid);
    if(off > 0) {
        int highmap = off / (BLOCKS_PER_MAP * e->blocksize);
        bson_append_start_object(&cond, "start");
        bson_append_long(&cond, "$gte",
            highmap * (BLOCKS_PER_MAP * e->blocksize));
        bson_append_finish_object(&cond);
    }
    bson_finish(&cond);

    res = mongo_remove(conn, maps_name, &cond, NULL);
    bson_destroy(&cond);
    e->size = off;
    return 0;
}


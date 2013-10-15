#define FUSE_USE_VERSION 26

#include <osxfuse/fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <search.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <limits.h>
#include <math.h>
#include "mongo-fuse.h"
#include <snappy-c.h>
#ifdef __APPLE__
#include <CommonCrypto/CommonDigest.h>
#else
#include <openssl/sha.h>
#endif
#include <xmmintrin.h>

extern char * blocks_name;
extern char * extents_name;

static int resolve_block(struct inode * e, uint8_t hash[HASH_LEN], char * buf) {
    bson query;
    int res;
    mongo_cursor curs;
    bson_iterator i;
    bson_type bt;
    const char * key;
    mongo * conn = get_conn();

    bson_init(&query);
    bson_append_binary(&query, "_id", 0, (char*)hash, 20);
    bson_finish(&query);

    mongo_cursor_init(&curs, conn, blocks_name);
    mongo_cursor_set_query(&curs, &query);
    mongo_cursor_set_limit(&curs, 1);

    res = mongo_cursor_next(&curs);
    bson_destroy(&query);
    if(res != MONGO_OK) {
        mongo_cursor_destroy(&curs);
        return -EIO;
    }

    bson_iterator_init(&i, mongo_cursor_bson(&curs));
    size_t outsize, compsize = 0;
    const char * compdata = NULL;
    uint32_t offset = 0, size = 0;

    while((bt = bson_iterator_next(&i)) > 0) {
        key = bson_iterator_key(&i);
        if(strcmp(key, "data") == 0) {
            compsize = bson_iterator_bin_len(&i);
            compdata = bson_iterator_bin_data(&i);
        }
        else if(strcmp(key, "offset") == 0)
            offset = bson_iterator_int(&i);
        else if(strcmp(key, "size") == 0)
            size = bson_iterator_int(&i);
    }

    if(curs.err != MONGO_CURSOR_EXHAUSTED) {
        fprintf(stderr, "Error getting extents %d", curs.err);
        return -EIO;
    }

    if(!compdata) {
        fprintf(stderr, "No data in block?\n");
        return -EIO;
    }

    outsize = MAX_BLOCK_SIZE;
    if((res = snappy_uncompress(compdata, compsize,
        buf + offset, &outsize)) != SNAPPY_OK) {
        fprintf(stderr, "Error uncompressing block %d\n", res);
        return -EIO;
    }
    if(offset > 0)
        memset(buf, 0, offset);
    compsize = outsize + offset;
    if(compsize < size)
        memset(buf + compsize, 0, size - compsize);
    mongo_cursor_destroy(&curs);

    return 0;
}

int mongo_read(const char *path, char *buf, size_t size, off_t offset,
               struct fuse_file_info *fi) {
    struct inode * e;
    int res;
    const off_t end = size + offset;
    size_t tocopy, skip, idx;
    struct elist * list;
    char * extent_buf = get_extent_buf();

    e = (struct inode*)fi->fh;
    if((res = get_cached_inode(path, e)) != 0)
        return res;

    if(e->mode & S_IFDIR)
        return -EISDIR;

    if(offset > e->size)
        return 0;

    list = init_elist();
    if(!list)
        return -ENOMEM;

    fprintf(stderr, "Starting read of %llu %lu\n", offset, size);

    if((res = deserialize_extent(e, offset, size, list)) != 0)
        return res;

    if(list->nnodes == 0) {
        memset(buf, 0, size);
        free(list);
        return size;
    }

    const size_t l_nodes = list->nnodes;
    for(idx = 0; idx < l_nodes; idx++) {
        const struct enode * cur = &list->list[idx];
        const off_t curend = cur->off + cur->len;
        char * olock, *ilock;

        ilock = extent_buf;
        olock = buf;
        tocopy = cur->len;

        if(cur->off > offset) {
            ilock += 
        }

        if(cur->empty) {
            fprintf(stderr, "Writing empty at skip: %lu tocopy: %lu offset: %llu curoff: %llu curlen: %lu\n",
                skip, tocopy, offset, cur->off, cur->len);
         //   memset(block, 0, tocopy);
            continue;
        }

        fprintf(stderr, "Writing block at skip: %lu tocopy: %lu offset: %llu curoff: %llu curlen: %lu\n",
            skip, tocopy, offset, cur->off, cur->len);
        res = resolve_block(e, (uint8_t*)cur->hash, extent_buf);
        if(res != 0)
            return res;
        memcpy(block, extent_buf + skip, tocopy);
    }

    free(list);
    return size;
}

int mongo_write(const char *path, const char *buf, size_t size,
                off_t offset, struct fuse_file_info *fi)
{
    struct inode * e;
    int res;
    size_t reallen;
    int32_t realend = size, blk_offset = 0;
    const off_t write_end = size + offset;
    char * zlock;
    bson doc, cond;
    mongo * conn = get_conn();
    uint8_t hash[20];
    time_t now = time(NULL);

    if(size > MAX_BLOCK_SIZE)
        return -EFBIG;

    e = (struct inode*)fi->fh;
    if((res = get_cached_inode(path, e)) != 0)
        return res;

    if(e->mode & S_IFDIR)
        return -EISDIR;

    /* Uncomment this for incredibly slow length calculations.
    for(;realend >= 0 && buf[realend] == '\0'; realend--);
    realend++;
    for(blk_offset = 0; blk_offset < realend && buf[blk_offset] == 0; blk_offset++);
    blk_offset -= blk_offset > 0 ? 1 : 0;
    * The code below uses SSE4 instructions to find the first/last
    * zero bytes by doing 16-byte comparisons at a time. This should give
    * a ~16x speed boost on blocks with lots of zero bytes over the dumb
    * method above.
    */
    __m128i zero = _mm_setzero_si128();
    if(buf[size - 1] == '\0') {
        zlock = (char*)buf + size - 16;
        while(zlock >= buf) {
            __m128i x = _mm_loadu_si128((__m128i*)zlock);
            res = _mm_movemask_epi8(_mm_cmpeq_epi8(zero, x));
            if(res != 0xffff) {
                realend = zlock - buf + 16;
                while(realend > 0 && buf[realend] == '\0')
                    realend--;
                realend++;
                break;
            }
            zlock -= 16;
        }
    }
    if(buf[0] == '\0') {
        zlock = (char*)buf;
        while(zlock - buf < realend) {
            __m128i x = _mm_loadu_si128((__m128i*)zlock);
            res = _mm_movemask_epi8(_mm_cmpeq_epi8(zero, x));
            if(res != 0xffff) {
                blk_offset = zlock - buf;
                while(blk_offset < realend && buf[blk_offset] == '\0')
                    blk_offset++;
                blk_offset -= blk_offset > 0 ? 1 : 0;
                break;
            }
            zlock += 16;
        }
    }
    reallen = realend - blk_offset;
    if(reallen == 0) {
        pthread_mutex_lock(&e->wr_lock);
        if(e->wr_extent == NULL) {
            e->wr_extent = init_elist();
            if(!e->wr_extent) {
                pthread_mutex_unlock(&e->wr_lock);
                return -ENOMEM;
            }
        }
        res = insert_empty(&e->wr_extent, offset, size);
        pthread_mutex_unlock(&e->wr_lock);
        return res;
    }

#ifdef __APPLE__
    CC_SHA1(buf, size, hash);
#else
    SHA1(buf, size, hash);
#endif

    bson_init(&cond);
    bson_append_binary(&cond, "_id", 0, (char*)hash, sizeof(hash));
    bson_finish(&cond);

    bson_init(&doc);
    bson_append_start_object(&doc, "$setOnInsert");
    char * comp_out = get_compress_buf();
    size_t comp_size = snappy_max_compressed_length(reallen);
    if((res = snappy_compress(buf + blk_offset, reallen,
        comp_out, &comp_size)) != SNAPPY_OK) {
        fprintf(stderr, "Error compressing input: %d\n", res);
        return -EIO;
    }
    bson_append_binary(&doc, "data", 0, comp_out, comp_size);
    bson_append_int(&doc, "offset", blk_offset);
    bson_append_int(&doc, "size", size);
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

    pthread_mutex_lock(&e->wr_lock);
    if(write_end > e->size)
        e->size = write_end;

    if(e->wr_extent == NULL) {
        e->wr_extent = init_elist();
        if(!e->wr_extent) {
            pthread_mutex_unlock(&e->wr_lock);
            return -ENOMEM;
        }
    }
    res = insert_hash(&e->wr_extent, offset, size, hash);

    if(now - e->wr_age > 3) {
        res = serialize_extent(e, e->wr_extent);
        if(res != 0) {
            pthread_mutex_unlock(&e->wr_lock);
            return res;    
        }
        e->wr_age = now;
        res = commit_inode(e);
    }
    pthread_mutex_unlock(&e->wr_lock);

    if(res != 0)
        return res;
    return size;
}

int do_trunc(struct inode * e, off_t off) {
    bson cond;
    int res;
    mongo * conn = get_conn();

    if(off > e->size) {
        e->size = off;
        return 0;
    }

    pthread_mutex_lock(&e->wr_lock);
    if(e->wr_extent) {
        if(off < 0 &&(res = serialize_extent(e, e->wr_extent)) != 0)
            return res;
        e->wr_extent->nnodes = 0;
    }
    pthread_mutex_unlock(&e->wr_lock);

    bson_init(&cond);
    bson_append_oid(&cond, "inode", &e->oid);
    if(off > 0) {
        bson_append_start_object(&cond, "start");
        bson_append_long(&cond, "$gte", 0);
        bson_append_finish_object(&cond);
    }
    bson_finish(&cond);

    res = mongo_remove(conn, extents_name, &cond, NULL);
    bson_destroy(&cond);
    if(res != 0) {
        fprintf(stderr, "Error removing extents in do_truncate\n");
        return -EIO;
    }

    e->size = off;

    return 0;
}

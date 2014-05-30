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

static int resolve_block(struct inode * e, uint8_t hash[HASH_LEN], char * buf) {
    bson_t query, *doc;
    bson_iter_t iter;
    bson_error_t dberr;
    mongoc_collection_t * coll = get_coll(COLL_BLOCKS);
    mongoc_cursor_t * curs;
    int res;

    bson_init(&query);
    bson_append_binary(&query, KEYEXP("_id"), 0, hash, 20);

    curs = mongoc_collection_find(coll,
        MONGOC_QUERY_NONE,
        0, // skip
        1, // limit
        0, // batch_size
        &query,
        NULL, // fields
        NULL); // read_prefs

    bson_destroy(&query);
    
    if(!curs) {
        logit(ERROR, "Error creating cursor while searching for block");
        return -EIO;
    }

    if(!mongoc_cursor_next(curs, (const bson_t**)&doc)) {
        if(mongoc_cursor_error(curs, &dberr))
            logit(ERROR, "Error searching for block: %s", dberr.message);
        else
            logit(WARN, "Block requested doesn't exist");
        mongoc_cursor_destroy(curs);
        return -EIO;
    }

    bson_iter_init(&iter, doc);
    size_t outsize, compsize = 0;
    const char * compdata = NULL;
    uint32_t offset = 0, size = 0;

    while(bson_iter_next(&iter)) {
        const char * key = bson_iter_key(&iter);
        if(strcmp(key, "data") == 0) {
            bson_subtype_t subtype;
            bson_iter_binary(&iter, &subtype, 
                (uint32_t*)&compsize, (const uint8_t**)&compdata);
        }
        else if(strcmp(key, "offset") == 0)
            offset = bson_iter_int32(&iter);
        else if(strcmp(key, "size") == 0)
            size = bson_iter_int32(&iter);
    }

    if(!compdata) {
        logit(ERROR, "No data in block?");
        mongoc_cursor_destroy(curs);
        return -EIO;
    }

    outsize = MAX_BLOCK_SIZE;
    if((res = snappy_uncompress(compdata, compsize,
        buf + offset, &outsize)) != SNAPPY_OK) {
        fprintf(stderr, "Error uncompressing block %d\n", res);
        mongoc_cursor_destroy(curs);
        return -EIO;
    }
    if(offset > 0)
        memset(buf, 0, offset);
    compsize = outsize + offset;
    if(compsize < size)
        memset(buf + compsize, 0, size - compsize);
    mongoc_cursor_destroy(curs);

    return 0;
}

int mongo_read(const char *path, char *buf, size_t size, off_t offset,
               struct fuse_file_info *fi) {
    struct inode * e;
    int res;
    const off_t end = size + offset;
    size_t idx;
    struct elist * list = NULL;
    char * extent_buf = get_extent_buf();

    e = (struct inode*)fi->fh;
    if((res = get_cached_inode(path, e)) != 0)
        return res;

    if(e->mode & S_IFDIR)
        return -EISDIR;

    pthread_mutex_lock(&e->wr_lock);
    if(e->wr_extent) {
        if((res = serialize_extent(e, e->wr_extent)) != 0) {
            pthread_mutex_unlock(&e->wr_lock);
            return res;
        }
        e->wr_extent->nnodes = 0;
    }
    e->wr_age = time(NULL);
    pthread_mutex_unlock(&e->wr_lock);

    if((res = deserialize_extent(e, offset, size, &list)) != 0)
        return res;

    if(list->nnodes == 0) {
        memset(buf, 0, size);
        free(list);
        return size;
    }

    if(list->list[0].off > offset)
        memset(buf, 0, list->list[0].off - offset);

    for(idx = 0; idx < list->nnodes; idx++) {
        const struct enode * cur = &list->list[idx];
        const off_t curend = cur->off + cur->len;
        size_t inskip = 0, tocopy = cur->len, outskip = 0;
 
        if(cur->off > end || curend < offset)
            continue;
 
        if(cur->off < offset)
            inskip = offset - cur->off;
        if(cur->off > offset)
            outskip = cur->off - offset;
        tocopy = end > curend ? curend : end;
        tocopy -= (cur->off + inskip);
 
        if(cur->empty) {
            memset(buf + outskip, 0, tocopy);
            continue;
        }
 
        res = resolve_block(e, (uint8_t*)cur->hash, extent_buf);
        if(res != 0)
            return res;
        memcpy(buf + outskip, extent_buf + inskip, tocopy);
    }

    free(list);
    return size;
}

int update_filesize(struct inode * e, off_t newsize) {
    bson_t cond, doc, set;
    bson_error_t dberr;
    mongoc_collection_t * coll = get_coll(COLL_INODES);
    bool res;

    if(newsize < e->size)
        return 0;

    e->size = newsize;

    bson_init(&cond);
    bson_append_oid(&cond, KEYEXP("_id"), &e->oid);

    bson_init(&doc);
    bson_append_document_begin(&doc, KEYEXP("$set"), &set);
    bson_append_int64(&set, KEYEXP("size"), newsize);
    bson_append_document_end(&doc, &set);

    res = mongoc_collection_update(coll,
        MONGOC_UPDATE_NONE,
        &cond,
        &doc,
        NULL, // write concern
        &dberr);

    bson_destroy(&cond);
    bson_destroy(&doc);

    if(!res) {
        char oidstr[25];
        bson_oid_to_string(&e->oid, oidstr);
        logit(ERROR, "Error updating file size for %s: %s",
            oidstr, dberr.message);
        return -EIO;
    }

    return 0;
}

int mongo_write(const char *path, const char *buf, size_t size,
                off_t offset, struct fuse_file_info *fi)
{
    struct inode * e;
    int res;
    size_t reallen;
    int32_t realend = size, blk_offset = 0;
    const off_t write_end = size + offset;
    char * lock;
    bson_t doc, cond, setoninsert;
    bson_error_t dberr;
    mongoc_collection_t * coll = get_coll(COLL_BLOCKS);
    uint8_t hash[20];
    time_t now = time(NULL);

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
    * a ~16 speed boost on blocks with lots of zero bytes over the dumb
    * method above.
    */
    
    if(size >= 16) {
        __m128i zero = _mm_setzero_si128();
        lock = (char*)buf + size - 16;
        while(lock >= buf) {
            __m128i x = _mm_loadu_si128((__m128i*)lock);
            res = _mm_movemask_epi8(_mm_cmpeq_epi8(zero, x));
            if(res == 0xffff) {
                lock -= 16;
                continue;
            }
            realend = lock - buf + fls(res ^ 0xffff);
            break;
        }
        if(lock <= buf)
            realend = 0;

        lock = (char*)buf;
        while(lock - buf < realend) {
            __m128i x = _mm_loadu_si128((__m128i*)lock);
            res = _mm_movemask_epi8(_mm_cmpeq_epi8(zero, x));
            if(res == 0xffff) {
                lock += 16;
                continue;
            }
            blk_offset = lock - buf + ffs(res ^ 0xffff) - 1;
            break;
        }
    }

    reallen = realend - blk_offset;
    if(reallen == 0) {
        pthread_mutex_lock(&e->wr_lock);
        res = insert_empty(&e->wr_extent, offset, size);
        goto end;
    }

#ifdef __APPLE__
    CC_SHA1(buf, size, hash);
#else
    SHA1(buf, size, hash);
#endif

    bson_init(&cond);
    bson_append_binary(&cond, KEYEXP("_id"), 0, hash, sizeof(hash));

    bson_init(&doc);
    bson_append_document_begin(&doc, KEYEXP("$setOnInsert"), &setoninsert);

    char * comp_out = get_compress_buf();
    size_t comp_size = snappy_max_compressed_length(reallen);
    if((res = snappy_compress(buf + blk_offset, reallen,
        comp_out, &comp_size)) != SNAPPY_OK) {
        fprintf(stderr, "Error compressing input: %d\n", res);
        return -EIO;
    }

    bson_append_binary(&setoninsert, KEYEXP("data"), 0,
        (const uint8_t*)comp_out, comp_size);
    bson_append_int64(&setoninsert, KEYEXP("offset"), blk_offset);
    bson_append_int64(&setoninsert, KEYEXP("size"), size);
    bson_append_time_t(&setoninsert, KEYEXP("created"), now);
    bson_append_document_end(&doc, &setoninsert);

    res = mongoc_collection_update(coll,
        MONGOC_UPDATE_UPSERT,
        &cond,
        &doc,
        NULL, // write concern
        &dberr);
    bson_destroy(&doc);
    bson_destroy(&cond);    

    if(!res) {
        logit(ERROR, "Error commiting block: %s", dberr.message);
        return -EIO;
    }

    pthread_mutex_lock(&e->wr_lock);
    res = insert_hash(&e->wr_extent, offset, size, hash);

end:
    if(write_end > e->size)
        e->size = write_end;

    if(now - e->wr_age > 3) {
        res = serialize_extent(e, e->wr_extent);
        if(res != 0) {
            pthread_mutex_unlock(&e->wr_lock);
            return res;    
        }
        e->wr_age = now;
    }
    pthread_mutex_unlock(&e->wr_lock);
    if(res != 0)
        return res;

    res = update_filesize(e, write_end);

    if(res != 0)
        return res;
    return size;
}

int do_trunc(struct inode * e, off_t off) {
    bson_t cond;
    bson_error_t dberr;
    int res;
    mongoc_collection_t * coll = get_coll(COLL_EXTENTS);

    if(off > e->size) {
        e->size = off;
        return 0;
    }

    pthread_mutex_lock(&e->wr_lock);
    if(e->wr_extent) {
        if(off < 0 && (res = serialize_extent(e, e->wr_extent)) != 0)
            return res;
        e->wr_extent->nnodes = 0;
    }
    e->wr_age = time(NULL);
    pthread_mutex_unlock(&e->wr_lock);

    bson_init(&cond);
    bson_append_oid(&cond, KEYEXP("inode"), &e->oid);
    if(off > 0) {
        bson_t start;
        bson_append_document_begin(&cond, KEYEXP("start"), &start);
        bson_append_int64(&start, KEYEXP("$gte"), off);
        bson_append_document_end(&cond, &start);
    }

    res = mongoc_collection_delete(coll,
        0, // flags,
        &cond,
        NULL, // write concern,
        &dberr);
    bson_destroy(&cond);
    if(!res) {
        char oidstr[25];
        bson_oid_to_string(&e->oid, oidstr);
        logit(ERROR, "Error removing extents for %s: %s", oidstr, dberr.message);
        return -EIO;
    }

    e->size = off;

    return 0;
}

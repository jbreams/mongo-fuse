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

static int resolve_block(struct inode * e, uint8_t hash[20],
    struct extent * out) {
    bson query, fields;
    int res;
    mongo_cursor curs;
    bson_iterator i;
    bson_type bt;
    const char * key;
    mongo * conn = get_conn();

    if(out == NULL)
        return -ENOMEM;

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
    mongo_cursor_destroy(&curs);

    if(curs.err != MONGO_CURSOR_EXHAUSTED) {
        fprintf(stderr, "Error getting extents %d", curs.err);
        free(out);
        return -EIO;
    }

    return 0;
}

int mongo_read(const char *path, char *buf, size_t size, off_t offset,
               struct fuse_file_info *fi) {
    struct inode * e;
    int res;
    struct extent * o;
    char * block = buf;
    size_t tocopy;
    const off_t end = size + offset;
    struct enode * cur, *next = NULL, *prev = NULL, *start;

    e = (struct inode*)fi->fh;
    if((res = get_cached_inode(path, e)) != 0)
        return res;

    if(e->mode & S_IFDIR)
        return -EISDIR;

    if(offset > e->size)
        return 0;

    if((res = deserialize_extent(e, offset, size)) != 0)
        return res;

    pthread_rwlock_rdlock(&e->rd_extent_lock);
    cur = e->rd_extent_root;
    while(cur) {
        if(offset == cur->off)
            break;
        if(offset < cur->off) {
            if(cur->off + cur->len > offset)
                break;
            cur = cur->l;
        }
        else if(offset > cur->off)
            cur = cur->r;
    }
    if(!cur) {
        pthread_rwlock_unlock(&e->rd_extent_lock);
        memset(buf, 0, size);
        return size;
    }

    o = new_extent(e);
    start = cur;
    while(block - buf != size) {
        if(!cur || cur->off > end)
            break;
        if(start == cur) {
            if((res = resolve_block(e, (char*)cur->hash, o)) != 0) {
                pthread_rwlock_unlock(&e->rd_extent_lock);
                return res;
            }
            char * elock = o->data + (offset - cur->off);
            tocopy = cur->len - (offset - cur->off);
            memcpy(block, elock, tocopy);
            block += tocopy;
            offset += tocopy;
            prev = cur;
            next = cur->r ? cur->r : cur->p;
            continue;
        }
        if(prev == cur->p) {
            next = cur->l;
            if(!next)
                next = cur->r ? cur->r : cur->p;
        }
        else if(prev == cur->l)
            next = cur->r ? cur->r : cur->p;
        else if(prev == cur->r) {
            prev = cur;
            cur = cur->p;
            continue;
        }

        if(cur->off != offset) {
            memset(block, 0, offset - cur->off);
            block += offset - cur->off;
            offset += offset - cur->off;
        }

        if((res = resolve_block(e, (char*)cur->hash, o)) != 0) {
            pthread_rwlock_unlock(&e->rd_extent_lock);
            return res;
        }
        if(cur->off + cur->len > end)
            tocopy = end - (cur->off + cur->len);
        else
            tocopy = cur->len;
        memcpy(block, o->data, tocopy);
        prev = cur;
        cur = next;
    }

    if(block - buf != size) {
        tocopy = size - (block - buf);
        memset(block, 0, tocopy);
        block += tocopy;
        offset += tocopy;
    }

    return block - buf;
}

int mongo_write(const char *path, const char *buf, size_t size,
                off_t offset, struct fuse_file_info *fi)
{
    struct inode * e;
    int res;
    char * block = (char*)buf;
    const off_t end = size + offset;
    off_t curblock;
    size_t tocopy, reallen;
    int32_t realend = size, blk_offset = 0;
    char * zlock;
    bson doc, cond;
    mongo * conn = get_conn();
    uint8_t hash[20];
    time_t now = time(NULL);

    e = (struct inode*)fi->fh;
    if((res = get_cached_inode(path, e)) != 0)
        return res;

    if(e->mode & S_IFDIR)
        return -EISDIR;

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
    if(e->data[0] == '\0') {
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
    if(reallen == 0)
        return 0;

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
    bson_append_int(&doc, "offset", offset);
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

    pthread_mutex_lock(&e->wr_extent_lock);
    res = insert_hash(&e->wr_extent_root,
        offset + blk_offset, reallen, hash);
    if(res == 0 && now - e->wr_extent_updated > 3)
        res = serialize_extent(e, e->wr_extent_root);
    pthread_mutex_unlock(&e->wr_extent_lock);
    if(res != 0)
        return res;
    return size;
}

int do_trunc(struct inode * e, off_t off) {
    bson cond;
    int res;
    mongo * conn = get_conn();
    struct enode * trunc_root = NULL;
    off_t new_size = off;
    if(e->mode & S_IFDIR)
        return -EISDIR;

    if(off > e->size) {
        e->size = off;
        return 0;
    }

    pthread_rwlock_wrlock(&e->rd_extent_lock);
    free_extent_tree(e->rd_extent_root);
    e->rd_extent_root = NULL;
    if((res = deserialize_extent(e, 0, off)) != 0) {
        pthread_rwlock_unlock(&e->rd_extent_lock);
        return res;
    }

    bson_init(&cond);
    bson_append(&cond, "inode", &e->oid);
    bson_finish(&cond);

    res = mongo_remove(cond, extents_name, &cond, NULL);
    bson_destroy(&cond)
    if(res != 0) {
        pthread_rwlock_unlock(&e->rd_extent_lock);
        fprintf(stderr, "Error removing extents in do_truncate\n");
        return -EIO;
    }

    if((res = serialize_extent(e, e->rd_extent_root)) != 0) {
        pthread_rwlock_unlock(&e->rd_extent_lock);
        fprintf(stderr, "Error updating extents in do_truncate\n");
        return -EIO;
    }
    pthread_mutex_lock(&e->wr_extent_lock);
    free_extent_tree(&e->wr_extent_root);
    e->wr_extent_root = NULL;
    pthread_mutex_unlock(&e->wr_extent_lock);
    pthread_rwlock_unlock(&e->rd_extent_lock);

    return 0;
}

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

int commit_block(struct inode * ent, struct extent *e) {
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
    if(reallen == 0)
        return 0;

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

    return 0;
}

/*
int mongo_write(const char *path, const char *buf, size_t size,
                off_t offset, struct fuse_file_info *fi)
{
    struct inode * e;
    int res;
    char * block = (char*)buf;
    const off_t end = size + offset;
    off_t curblock;
    size_t tocopy;

    e = (struct inode*)fi->fh;
    if((res = get_cached_inode(path, e)) != 0)
        return res;

    if(e->is_blocksizefile)
        write_blocksize(e, buf, size);

    if(e->mode & S_IFDIR)
        return -EISDIR;

    curblock = compute_start(e, offset);
    while(offset < end) {
        size_t curblocksize = e->blocksize, sizediff = 0;
        struct extent * cur = NULL;
        int getdata = (offset != curblock || end - curblock < e->blocksize);
        char * elock;
        if((res = resolve_extent(e, curblock, &cur, getdata)) != 0)
            goto cleanup;

        if(!cur) {
            cur = new_extent(e);
            memset(cur, 0, sizeof(struct extent) + e->blocksize);
            cur->start = curblock;
        }

        if(curblock < offset) {
            sizediff = offset - curblock;
            elock = cur->data + sizediff;
            curblocksize = e->blocksize - sizediff;
        } else
            elock = cur->data;

        if(end - offset < curblocksize)
            tocopy = end - offset;
        else
            tocopy = curblocksize;

        memcpy(elock, block, tocopy);
        if((res = commit_extent(e, cur)) != 0)
            goto cleanup;

        block += tocopy;
        offset += tocopy;
        curblock = compute_start(e, offset);
    }

    if(end > e->size)
        e->size = end;
    res = commit_inode(e);

cleanup:
    if(res != 0)
        return res;
    else
        add_block_stat(path, size, 1);
    return size;
}
*/

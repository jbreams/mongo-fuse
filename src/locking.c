#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <mongo.h>
#include <stdlib.h>
#include <search.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/file.h>
#include <time.h>
#include "mongo-fuse.h"
#include <osxfuse/fuse.h>

extern const char * inodes_name;
extern const char * locks_name;

#if FUSE_VERSION > 28
void read_lock(const bson * b, int * nr, int * nw) {
    bson_iterator i;
    bson_type bt;
    const char * key;
    int writer = 0, added = 0;

    bson_iterator_init(&i, b);
    while((bt = bson_iterator_next(&i)) != 0) {
        key = bson_iterator_key(&i);
        if(strcmp(key, "_id") == 0) {
            bson_iterator sub;
            bson_iterator_subiterator(&i, &sub);
            while((bt = bson_iterator_next(&sub)) != 0) {
                key = bson_iterator_key(&sub);
                if(strcmp(key, "acquire") == 0){
                    added = bson_iterator_bool(&sub);
                }
            }
        }
        else if(strcmp(key, "writer") == 0)
            writer = bson_iterator_bool(&i);
    }
    if(writer)
        *nw += added ? 1 : -1;
    else
        *nr += added ? 1 : -1;
}

int unlock_inode(struct inode * e, int writer, bson_date_t locktime) {
    bson doc;
    mongo * conn = get_conn();
    int res;

    bson_init(&doc);
    bson_append_start_object(&doc, "_id");
    bson_append_date(&doc, "time", locktime);
    bson_append_oid(&doc, "inode", &e->oid);
    bson_append_bool(&doc, "acquire", 0);
    bson_append_finish_object(&doc);
    bson_append_bool(&doc, "writer", writer);
    bson_finish(&doc);

    res = mongo_insert(conn, locks_name, &doc, NULL);
    bson_destroy(&doc);
    if(res != MONGO_OK)
        return -EIO;
    return 0;
}

int lock_inode_impl(struct inode * e, int writer,
    bson_date_t * locktime, int noblock) {
    bson query, doc;
    mongo * conn = get_conn();
    mongo_cursor curs;
    int res, nreaders = 0, nwriters = 0, nrecs = 0, nrecs2 = 0;
    struct timeval tv;

    gettimeofday(&tv, NULL);
    *locktime = (tv.tv_sec * 1000) + (tv.tv_usec / 1000);

    bson_init(&doc);
    bson_append_start_object(&doc, "_id");
    bson_append_date(&doc, "time", *locktime);
    bson_append_oid(&doc, "inode", &e->oid);
    bson_append_bool(&doc, "acquire", 1);
    bson_append_finish_object(&doc);
    bson_append_bool(&doc, "writer", writer);
    bson_finish(&doc);

    res = mongo_insert(conn, locks_name, &doc, NULL);
    bson_destroy(&doc);
    if(res != MONGO_OK) {
        if(conn->lasterrcode == 11000)
            return -EAGAIN;
        return -EIO;
    }

    bson_init(&query);
    bson_append_start_object(&query, "$query");
    bson_append_oid(&query, "_id.inode", &e->oid);
    bson_append_start_object(&query, "_id.time");
    bson_append_date(&query, "$lt", *locktime);
    bson_append_finish_object(&query);
    bson_append_finish_object(&query);
    bson_append_start_object(&query, "$orderby");
    bson_append_int(&query, "$natural", 1);
    bson_append_finish_object(&query);
    bson_finish(&query);

    mongo_cursor_init(&curs, conn, locks_name);
    mongo_cursor_set_query(&curs, &query);

    while((res = mongo_cursor_next(&curs)) == MONGO_OK) {
        const bson * curbson = mongo_cursor_bson(&curs);
        read_lock(curbson, &nreaders, &nwriters);
        nrecs++;
    }

    mongo_cursor_destroy(&curs);
    if(writer && nreaders == 0 && nwriters == 0) {
        bson_destroy(&query);
        return 0;
    }
    else if(!writer && nwriters == 0) {
        bson_destroy(&query);
        return 0;
    }
    if(nonblock)
        return -EWOULDBLOCK;

    mongo_cursor_init(&curs, conn, locks_name);
    mongo_cursor_set_query(&curs, &query);
    mongo_cursor_set_options(&curs,
        MONGO_TAILABLE | MONGO_NO_CURSOR_TIMEOUT | MONGO_AWAIT_DATA);

    nreaders = 0;
    nwriters = 0;
    while((res = mongo_cursor_next(&curs)) == MONGO_OK) {
        const bson * curbson = mongo_cursor_bson(&curs);
        read_lock(curbson, &nreaders, &nwriters);
        nrecs2++;
        if(nrecs2 < nrecs)
            continue;

        if(writer && nreaders == 0 && nwriters == 0)
            break;
        else if(!writer && nwriters == 0)
            break;
    }

    bson_destroy(&query);
    mongo_cursor_destroy(&curs);

    if(res == MONGO_OK)
        return 0;
    return -EIO;
}

int lock_inode(struct inode * e, int writer, bson_date_t * locktime, int noblock) {
    int res;
    while(1) {
        res = lock_inode_impl(e, writer, locktime, noblock);
        if(res == -EAGAIN) {
            struct timespec ts;
            ts.tv_sec = 0;
            ts.tv_nsec = 10000000;
            nanosleep(&ts, NULL);
        }
        else
            return res;
    }
}
#endif

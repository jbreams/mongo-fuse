#include <pthread.h>
#include <stdlib.h>
#include <stdarg.h>
#include "mongo-fuse.h"

static pthread_key_t tls_key;
extern mongoc_uri_t * dial_uri;
extern int loglevel;

struct thread_data {
    mongoc_client_t * conn;
    mongoc_collection_t * coll_cache[COLL_MAX];

    // This is a buffer for compression output that should hold the
    // largest block size plus any overhead from snappy.
    // See https://code.google.com/p/snappy/source/browse/trunk/snappy.cc#55
    char compress_buf[32 + MAX_BLOCK_SIZE + MAX_BLOCK_SIZE / 6];
    char extent_buf[MAX_BLOCK_SIZE];
};

void free_thread_data(void * tdr) {
    struct thread_data * td = (struct thread_data*)tdr;
    int i;
    for(i = 0; i < COLL_MAX; i++) {
        if(td->coll_cache[i] == NULL)
            continue;
        mongoc_collection_destroy(td->coll_cache[i]);
    }
    mongoc_client_destroy(td->conn);
    free(td);
}

void setup_threading() {
    pthread_key_create(&tls_key, free_thread_data);
}

struct thread_data * get_thread_data() {
    struct thread_data * td = pthread_getspecific(tls_key);
    if(td)
        return td;
    td = calloc(1, sizeof(struct thread_data));
    td->conn = mongoc_client_new_from_uri(dial_uri);
    if(td->conn == NULL) {
        free(td);
        return NULL;
    }
    pthread_setspecific(tls_key, td);
    return td;
}

char * get_extent_buf() {
    return get_thread_data()->extent_buf;
}

char * get_compress_buf() {
    return get_thread_data()->compress_buf;
}

mongoc_collection_t * get_coll(int coll) {
    struct thread_data * td = get_thread_data();

    if(td->coll_cache[coll] != NULL)
        return td->coll_cache[coll];

    if(coll >= COLL_MAX) {
        logit(ERROR, "Requesting invalid collection %d", coll);
        return NULL;
    }

    const char * coll_names[] = { "inodes", "blocks", "extents" };
    const char * dbname = mongoc_uri_get_database(dial_uri);

    td->coll_cache[coll] = mongoc_client_get_collection(
        td->conn, dbname, coll_names[coll]);
    return td->coll_cache[coll];
}

void logit(int level, const char * fmt, ...) {
    if(level == DEBUG && loglevel < DEBUG)
        return;
    const char * level_prefixes[] = { "INFO", "WARN", "ERROR", "DEBUG" };

    va_list ap;
    va_start(ap, fmt);
    fprintf(stderr, "%s: ", level_prefixes[level]);
    vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n");
}
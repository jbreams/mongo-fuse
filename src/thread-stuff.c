#include <pthread.h>
#include <mongo.h>
#include <bson.h>
#include <stdlib.h>
#include "mongo-fuse.h"

static pthread_key_t tls_key;
extern const char * inodes_name;
extern const char * blocks_name;
extern mongo_host_port dbhost;
extern mongo_write_concern write_concern;

struct thread_data {
    mongo conn;
    int bson_id;
    // This is a buffer for compression output that should hold the
    // largest block size plus any overhead from snappy.
    // See https://code.google.com/p/snappy/source/browse/trunk/snappy.cc#55
    char compress_buf[32 + MAX_BLOCK_SIZE + MAX_BLOCK_SIZE / 6];
    char extent_buf[MAX_BLOCK_SIZE];
};

void free_thread_data(void* rp) {
    struct thread_data * td = rp;
    mongo_destroy(&td->conn);
    free(td);
}

int get_bson_number() {
    static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    static int x = 0;
    int out;
    pthread_mutex_lock(&mutex);
    out = x++;
    pthread_mutex_unlock(&mutex);
    return out;
}

void setup_threading() {
    pthread_key_create(&tls_key, free_thread_data);
    bson_set_oid_inc(get_bson_number);
}

static struct thread_data * get_thread_data() {
    struct thread_data * td = pthread_getspecific(tls_key);
    if(td)
        return td;
    td = malloc(sizeof(struct thread_data));
    memset(td, 0, sizeof(struct thread_data));
    mongo_init(&td->conn);
    pthread_setspecific(tls_key, td);
    return td;
}

char * get_extent_buf() {
    return get_thread_data()->extent_buf;
}

char * get_compress_buf() {
    return get_thread_data()->compress_buf;
}

struct mongo * get_conn() {
    struct thread_data * td = get_thread_data();
    if(mongo_is_connected(&td->conn))
        return &td->conn;

    if(mongo_client(&td->conn, dbhost.host, dbhost.port) != MONGO_OK) {
        fprintf(stderr, "Error connecting to mongodb %s:%d\n",
            dbhost.host, dbhost.port);
        return NULL;
    }

    mongo_set_write_concern(&td->conn, &write_concern);

    return &td->conn;
}


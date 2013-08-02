#include <pthread.h>
#include <mongo.h>
#include <bson.h>
#include "mongo-fuse.h"

static pthread_key_t tls_key;
extern const char * mongo_host;
extern int mongo_port;

struct thread_data {
    mongo conn;
    struct inode lastfile;
    struct extent * lastextent;
};

void free_thread_data(void* rp) {
    struct thread_data * td = rp;
    free_inode(&td->lastfile);
    if(td->lastextent)
        free_extents(td->lastextent);
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

void clear_last_file() {
    struct thread_data * td = get_thread_data();
    memset(&td->lastfile, 0, sizeof(struct inode));
}

struct inode * get_last_file() {
    return &get_thread_data()->lastfile;
}

struct extent * get_last_extent() {
    return get_thread_data()->lastextent;
}

void set_last_extent(struct extent * e) {
    struct thread_data * td = get_thread_data();
    td->lastextent = e;
}

struct mongo * get_conn() {
    struct thread_data * td = get_thread_data();
    if(mongo_is_connected(&td->conn))
        return &td->conn;

    if(mongo_client(&td->conn, mongo_host, mongo_port) != MONGO_OK) {
        fprintf(stderr, "Error connecting to mongodb %s:%d\n",
            mongo_host, mongo_port);
        return NULL;
    }

    return &td->conn;
}


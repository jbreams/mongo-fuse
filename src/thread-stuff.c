#include <pthread.h>
#include <mongo.h>
#include <bson.h>
#include <stdlib.h>
#include "mongo-fuse.h"

static pthread_key_t tls_key;
extern const char * mongo_host;
extern int mongo_port;
extern const char * inodes_name;


struct thread_data {
    mongo conn;
    int bson_id;
    struct inode lastfile;
    struct extent * lastextent;
};

struct block_stat {
    struct block_stat * next;
    size_t size, pathlen;
    char write;
    char path[1];
};

pthread_cond_t compute_stats_cond = PTHREAD_COND_INITIALIZER;
pthread_mutex_t compute_stats_mutex = PTHREAD_MUTEX_INITIALIZER,
    block_stat_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_t stats_thread;
struct block_stat * block_stat_head = NULL;
size_t nops = 0, keep_computing_stats = 1;

void add_block_stat(const char * path, size_t size, int write) {
    size_t pathlen = strlen(path);
    size_t l_nops;

    struct block_stat * n = malloc(sizeof(struct block_stat) + pathlen);
    if(!n)
        return;
    n->size = size;
    n->write = write;
    n->pathlen = pathlen;
    strncpy(n->path, path, pathlen);

    pthread_mutex_lock(&block_stat_mutex);
    n->next = block_stat_head;
    block_stat_head = n;
    l_nops = nops++;
    pthread_mutex_unlock(&block_stat_mutex);

    if(l_nops > 100)
        pthread_cond_signal(&compute_stats_cond);
}

uint32_t round_up_pow2(uint32_t v) {
    if(((v - 1) & v) != 0) {
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v++;
    }
    return v;
}

static void * stats_thread_fn(void * arg) {
    static const unsigned int log2s[] = {
        0xAAAAAAAA, 0xCCCCCCCC, 0xF0F0F0F0, 0xFF00FF00, 0xFFFF0000};

    while(keep_computing_stats) {
        struct block_stat * head;
        mongo * conn = get_conn();
        pthread_cond_wait(&compute_stats_cond, &compute_stats_mutex);
        pthread_mutex_lock(&block_stat_mutex);
        head = block_stat_head;
        block_stat_head = NULL;
        nops = 0;
        pthread_mutex_unlock(&block_stat_mutex);
        printf("Woke up stats thread!\n");

        while(head) {
            bson doc, query;
            struct block_stat * save;
            int res;
            int v = round_up_pow2(head->size), r;
            char fieldname[11];

            char * pl = head->path + head->pathlen;
            while(*pl != '/') pl--;
            if(pl - head->path == 0)
                *(++pl) = '\0';
            else
                *pl = '\0';

            if(v < 4096)
                v = 4096;
            else if(v > 1048576)
                v = 1048576;

            r = (v & log2s[0]) != 0;
            r |= ((v & log2s[4]) != 0) << 4;
            r |= ((v & log2s[3]) != 0) << 3;
            r |= ((v & log2s[2]) != 0) << 2;
            r |= ((v & log2s[1]) != 0) << 1;
            r -= 12;

            sprintf(fieldname, "%s.%d", head->write ? "writes":"reads", r);

            bson_init(&doc);
            bson_append_start_object(&doc, "$inc");
            bson_append_int(&doc, fieldname, 1);
            bson_append_finish_object(&doc);
            bson_finish(&doc);

            bson_init(&query);
            bson_append_string(&query, "dirents", head->path);
            bson_finish(&query);

            res = mongo_update(conn, inodes_name, &query, &doc,
                MONGO_UPDATE_BASIC, NULL);
            if(res != 0)
                fprintf(stderr, "Error updating block stats\n");
            bson_destroy(&query);
            bson_destroy(&doc);
            save = head->next;
            free(head);
            head = save;
        }
    }
    return NULL;
}

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
    if(pthread_create(&stats_thread, NULL, stats_thread_fn, NULL) != 0)
        fprintf(stderr, "Couldn't create a stats thread! WTF??\n");
}

void teardown_threading() {
    keep_computing_stats = 0;
    pthread_cond_signal(&compute_stats_cond);
    pthread_join(stats_thread, NULL);
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


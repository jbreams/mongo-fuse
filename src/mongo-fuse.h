#include <pthread.h>
#include <stdint.h>
#include <mongoc.h>
#include <bson.h>
#include <sys/types.h>
#define FUSE_USE_VERSION 26

struct extent {
    char hash[20];
    uint64_t start;
    size_t size;
    char data[1];
};

#define KEYEXP(name) name, sizeof(name) - 1

//#define BLOCKS_PER_EXTENT 2
#define BLOCKS_PER_EXTENT 512
#define MAX_BLOCK_SIZE 65536
#define TREE_HEIGHT_LIMIT 64
#define HASH_LEN 20

#define INFO 0
#define WARN 1
#define ERROR 2
#define DEBUG 3

#define COLL_INODES 0
#define COLL_BLOCKS 1
#define COLL_EXTENTS 2
#define COLL_MAX 3

struct dirent {
    struct dirent * next;
    size_t len;
    char path[1];
};

struct enode {
    off_t off;
    size_t len;
    uint32_t seq;
    char empty;
    uint8_t hash[HASH_LEN];
};

struct elist {
    size_t nnodes;
    size_t nslots;
    struct enode list[1];
};

struct inode {
    time_t updated;
    bson_oid_t oid;
    struct dirent * dirents;
    int direntcount;
    uint32_t mode;
    uint64_t owner;
    uint64_t group;
    uint64_t size;
    time_t created;
    time_t modified;
    char * data;
    size_t datalen;

    struct elist * wr_extent;
    pthread_mutex_t wr_lock;
    time_t wr_age;
};

void setup_threading();
void teardown_threading();
char * get_compress_buf();
char * get_extent_buf();
void logit(int level, const char * fmt, ...);
mongoc_collection_t * get_coll(int coll);

int insert_hash(struct elist ** list, off_t off,
    size_t len, const uint8_t hash[HASH_LEN]);
int insert_empty(struct elist ** list, off_t off, size_t len);
int deserialize_extent(struct inode * e, off_t off,
    size_t len, struct elist ** pout);
int serialize_extent(struct inode * e, struct elist * list);
struct elist * init_elist();

void init_inode(struct inode * e);
void free_inode(struct inode *e);
int get_inode(const char * path, struct inode * out);
int get_cached_inode(const char * path, struct inode * out);
int commit_inode(struct inode * e);
int create_inode(const char * path, mode_t mode, const char * data);
int check_access(struct inode * e, int amode);
int read_inode(const bson_t * doc, struct inode * out);
int inode_exists(const char * path);

int do_trunc(struct inode * e, off_t off);

int read_dirents(const char * directory,
    int (*dirent_cb)(struct inode *e, void * p,
    const char * parent, size_t parentlen), void * p);
int snapshot_dir(const char * path, size_t pathlen, mode_t mode);


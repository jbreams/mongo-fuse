#include <pthread.h>
#include <stdint.h>
#include <mongo.h>
#include <sys/types.h>
#define FUSE_USE_VERSION 26

// From lookup3.c for cacheing/hashing.
uint32_t hashlittle( const void *key, size_t length, uint32_t initval);
#define INITVAL 0xdeadbeef

struct extent {
    char hash[20];
    uint64_t start;
    size_t size;
    char data[1];
};

//#define BLOCKS_PER_EXTENT 2
#define BLOCKS_PER_EXTENT 512
#define MAX_BLOCK_SIZE 65536
#define HASH_LEN 20

struct dirent {
    struct dirent * next;
    bson_oid_t inode;
    uint32_t hash;
    time_t cached_on;
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
    uint32_t hash;
    time_t cached_on;

    bson_oid_t oid;
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

    struct inode * next;
};

mongo * get_conn();
void setup_threading();
void teardown_threading();
char * get_compress_buf();
char * get_extent_buf();

int rename_dirent(const char * path, const char * newpath);
int unlink_dirent(const char * path);
int link_dirent(const char * path, bson_oid_t * inode);
int resolve_dirent(const char * path, bson_oid_t * out);
int read_dirents(const char * directory,
    int (*dirent_cb)(const char * path, void * p, size_t parentlen),
    void * p);

int insert_hash(struct elist ** list, off_t off,
    size_t len, uint8_t hash[HASH_LEN]);
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
int read_inode(const bson * doc, struct inode * out);
int inode_exists(const char * path);

int do_trunc(struct inode * e, off_t off);

int snapshot_dir(const char * path, size_t pathlen, mode_t mode);


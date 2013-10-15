#include <pthread.h>
#include <stdint.h>
#include <mongo.h>
#include <sys/types.h>
#define FUSE_USE_VERSION 26

struct extent {
    char hash[20];
    uint64_t start;
    size_t size;
    char data[1];
};

//#define BLOCKS_PER_EXTENT 2
#define BLOCKS_PER_EXTENT 512
#define MAX_BLOCK_SIZE 65536
#define TREE_HEIGHT_LIMIT 64
#define HASH_LEN 20
#define LEFT 0
#define RIGHT 1

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
    char skip;
    uint8_t hash[HASH_LEN];
};

struct elist {
    size_t nnodes;
    size_t nslots;
    char sorted;
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
    uint64_t dev;
    time_t created;
    time_t modified;
    char * data;
    size_t datalen;

    struct elist * wr_extent;
    pthread_mutex_t wr_lock;
    time_t wr_age;
};

mongo * get_conn();
void setup_threading();
void teardown_threading();
char * get_compress_buf();
char * get_extent_buf();

int insert_hash(struct elist ** list, off_t off,
    size_t len, uint8_t hash[HASH_LEN]);
int insert_empty(struct elist ** list, off_t off, size_t len);
int deserialize_extent(struct inode * e, off_t off,
    size_t len, struct elist * out);
int serialize_extent(struct inode * e, struct elist * list);
size_t get_offset_index(struct elist * list, off_t offset);
struct elist * init_elist();

void free_inode(struct inode *e);
int get_inode(const char * path, struct inode * out);
int get_cached_inode(const char * path, struct inode * out);
int commit_inode(struct inode * e);
int create_inode(const char * path, mode_t mode, const char * data);
int check_access(struct inode * e, int amode);
int read_inode(const bson * doc, struct inode * out);
int inode_exists(const char * path);
#if FUSE_VERSION > 28
int lock_inode(struct inode * e, int writer, bson_date_t * locktime, int noblock);
int unlock_inode(struct inode * e, int writer, bson_date_t locktime);
#endif

int do_trunc(struct inode * e, off_t off);

int read_dirents(const char * directory,
    int (*dirent_cb)(struct inode *e, void * p,
    const char * parent, size_t parentlen), void * p);
int snapshot_dir(const char * path, size_t pathlen, mode_t mode);


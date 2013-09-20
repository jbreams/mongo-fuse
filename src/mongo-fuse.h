#include <pthread.h>
#include <stdint.h>
#define MONGO_HAVE_STDINT
#include <mongo.h>
#include <sys/types.h>
#define FUSE_USE_VERSION 26

struct extent {
    char hash[20];
    uint64_t start;
    size_t size;
    char data[1];
};

#define BLOCKS_PER_MAP 1024
#define BLOCKS_PER_EXTENT 512

struct dirent {
    struct dirent * next;
    size_t len;
    char path[1];
};

struct enode {
    int c;
    struct enode *p, *l, *r, *n;
    off_t off;
    size_t len;
    char hash[20];
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
    uint32_t blocksize;
    uint64_t reads[8];
    uint64_t writes[8];
    char * data;
    size_t datalen;
    int is_blocksizefile;

    pthread_mutex_t wr_extent_lock;
    struct enode * wr_extent_root;
    time_t wr_extent_updated;

    pthread_rwlock_t rd_extent_lock;
    struct enode * rd_extent_root;
};

mongo * get_conn();
void setup_threading();
void teardown_threading();
char * get_compress_buf();
struct extent * new_extent(size_t datasize);

int serialize_extent(struct inode * e, struct enode * root);
int deserialize_extent(struct inode * e, off_t off, size_t len);
int insert_hash(struct enode ** r, off_t off, size_t len, char hash[20]);

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


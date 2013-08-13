
#define FUSE_USE_VERSION 26

struct extent {
    struct extent * next;
    bson_oid_t inode;
    uint64_t start;
    char data[1];
};

struct dirent {
    struct dirent * next;
    size_t len;
    char path[1];
};

struct inode {
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
    uint8_t locked;
    char * data;
    size_t datalen;
};

struct inode * get_last_file();
void set_last_extent(struct extent * e);
struct extent * get_last_extent();
mongo * get_conn();
void setup_threading();
void teardown_threading();
void add_block_stat(const char * path, size_t size, int write);
uint32_t round_up_pow2(uint32_t v);
char * get_compress_buf();

void free_inode(struct inode *e);
int get_inode(const char * path, struct inode * out);
int commit_inode(struct inode * e);
int create_inode(const char * path, mode_t mode, const char * data);
int check_access(struct inode * e, int amode);
int read_inode(const bson * doc, struct inode * out);
#if FUSE_VERSION > 28
int lock_inode(struct inode * e, int writer, bson_date_t * locktime, int noblock);
int unlock_inode(struct inode * e, int writer, bson_date_t locktime);
#endif

int commit_extents(struct inode * ent, struct extent *e);
int resolve_extent(struct inode * e, off_t start,
    off_t end, struct extent ** realout);
void free_extents(struct extent * head);
void get_block_collection(struct inode * e, char * name);
struct extent * new_extent(struct inode * e);
off_t compute_start(struct inode * e, off_t offset);

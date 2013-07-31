
#define EXTENT_SIZE 262144
#define FUSE_USE_VERSION 26

struct extent {
    char committed;
    bson_oid_t oid;
    uint32_t size;
    uint64_t start;
    char data[EXTENT_SIZE];
};

struct dirent {
    struct dirent * next;
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
    time_t created;
    time_t modified;
    char * data;
};

void free_inode(struct inode *e);
int get_inode(const char * path, struct inode * out, int getdata);
int commit_inode(struct inode * e);

int commit_exent(struct inode * ent, struct extent *e);
int resolve_extent(struct inode * e, off_t start,
    off_t end, struct extent ** realout, int * countout);


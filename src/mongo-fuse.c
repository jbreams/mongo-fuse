// mongo_fuse.c

/**
  gcc -Wall -DMONGO_HAVE_STDINT mongo-fuse.c `pkg-config fuse --cflags --libs` -o mongo-fuse -lmongoc
 */

#define FUSE_USE_VERSION 26

#include <osxfuse/fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <mongo.h>
#include <stdlib.h>
#include <search.h>
#include <sys/stat.h>

static const char *mongo_path = "/hello";
char * blocks_name = "test.blocks";
char * inodes_name = "test.inodes";
mongo conn;

#define EXTENT_SIZE 8388608
#define FREE_EXTENTS 4

struct extent {
    char committed;
    bson_oid_t oid;
    uint32_t size;
    uint64_t start;
    char * data;
};

struct dirent {
    char * path;
    struct dirent * next;
};

struct entry {
    struct dirent * dirents;
    int direntcount;
    uint64_t inode;
    uint32_t mode;
    uint64_t owner;
    uint64_t group;
    uint64_t size;
    time_t created;
    time_t modified;
    char * data;
    int extentcount;
    /* This should be a data structure that's much faster to search
     * for sparse files. But for this first try, a simple tree of extents
     * should be okay! */
    struct extent * extents;
};

static int commit_entry(struct entry * e) {
    bson cond, doc;
    char istr[4];
    struct dirent * cde = e->dirents;
    int res;

    bson_numstr(istr, 0);

    bson_init(&cond);
    bson_append_long(&cond, "_id", e->inode);
    bson_finish(&cond);

    bson_init(&doc);
    bson_append_long(&doc, "_id", e->inode);
    bson_append_start_array(&doc, "dirents");
    while(cde) {
        bson_append_string(&doc, istr, cde->path);
        bson_incnumstr(istr);
        cde = cde->next;
    }

    bson_append_int(&doc, "mode", e->mode);
    bson_append_long(&doc, "owner", e->owner);
    bson_append_long(&doc, "group", e->group);
    bson_append_long(&doc, "size", e->size);
    bson_append_time_t(&doc, "created", e->created);
    bson_append_time_t(&doc, "modified", e->modified);
    if(e->data)
        bson_append_binary(&doc, "data", 0, e->data, e->size);
    bson_append_int(&doc, "extentcount", e->extentcount);

    res = mongo_update(&conn, blocks_name, &cond, &doc, MONGO_UPDATE_UPSERT, NULL);
    bson_destroy(&cond);
    bson_destroy(&doc);
    if(res != MONGO_OK) {
        fprintf(stderr, "Error committing inode\n");
        return -EIO;
    }
    return 0;
}


static void free_extent(void * p) {
    struct extent * e = p;
    if(e->data)
        free(e->data);
    free(e);
}

static void free_entry(struct entry *e) {
    int i;
    if(e->extentcount > 0) {
        tdestroy(e->extents, free_extent);
    }
    if(e->data)
        free(e->data);
    for(i = 0; i < e->direntcount; i++)
        free(e->dirents[i]);
    free(e->dirents);
}

static void commit_exent(const void * node, VISIT value, int level) {
    struct extent * e = (struct extent*)node;
    int res;
    bson doc, cond;

    if(e->committed)
        return;

    bson_ensure_space(&doc, e->size + 128);
    bson_append_oid(&doc, "_id", &e->oid);
    bson_append_long(&doc, "start", e->start);
    bson_append_binary(&doc, "data", 0, e->data, e->size);
    bson_finish(&doc);

    bson_init(&cond);
    bson_append_oid(&cond, "_id", &e->oid);
    bson_finish(&cond);

    res = mongo_update(&conn, blocks_name, &cond, &doc, MONGO_UPDATE_UPSERT, NULL);
    if(res != MONGO_OK)
        fprintf(stderr, "Error committing block\n");
    bson_destroy(&doc);
    bson_destroy(&cond);
    e->committed = 1;
}

static int get_extent_contents(struct extent * out) {
    bson query, doc;
    bson_iterator i;
    bson_type bt;
    int res;

    bson_init(&query);
    bson_append_oid(&query, "_id", &out->oid);
    bson_finish(&query);

    res = mongo_find_one(&conn, blocks_name, &query, bson_shared_empty(), &doc);
    bson_destroy(&query);
    if(res != MONGO_OK) {
        fprintf(stderr, "error finding block\n");
        return -EIO;
    }

    bt = bson_find(&i, &doc, "data");
    out->size = bson_iterator_bin_len(&i);
    out->data = malloc(out->size);
    memcpy(out->data, bson_iterator_bin_data(&i), out->size);
    return 0;
}

static int extent_cmp(const void * ra, const void * rb) {
    const struct extent *a = ra, *b = rb;
    if(a->start < b->start)
        return -1;
    else if(a->start > b->start)
        return 1;
    return 0;
}

static int resolve_extent(struct entry * e, off_t off, struct extent ** o) {
    struct extent * search, *found;

    *o = NULL;
    if(off < e->size && e->data)
        return 0;

    search = malloc(sizeof(struct extent));
    if(!search)
        return -ENOMEM;
    search->committed = 0;
    search->start = off / EXTENT_SIZE;

    found = tsearch(search, (void**)&e->extents, extent_cmp);
    if(found != search) {
        *o = found;
        if(!found->data)
            return get_extent_contents(found);
        return 0;
    }
    *o = search;

    bson_oid_gen(&search->oid);
    search->size = off % EXTENT_SIZE;
    search->data = malloc(search->size);
    if(search->data == NULL)
        return -ENOMEM;
    memset(search->data, 0, search->size);
    *o = search;

    return 0;
}

static int get_entry(const char * path, struct entry * out, int getdata) {
    bson query, doc, fields;
    bson_iterator i;
    bson_type bt;
    mongo_cursor curs;
    int res, c;
    const char * key;

    bson_init(&query);
    bson_append_string(&query, "dirents", path);
    bson_finish(&query);

    if(!getdata) {
        bson_init(&fields);
        bson_append_int(&fields, "data", 0);
        bson_finish(&fields);
    }

    res = mongo_find_one(&conn, inodes_name, &query,
            getdata ? bson_shared_empty():&fields, &doc);

    if(res != MONGO_OK) {
        fprintf(stderr, "find one failed: %d\n", res);
        bson_destroy(&query);
        return -ENOENT;
    }

    memset(out, 0, sizeof(struct entry));
    bson_destroy(&query);
    bson_iterator_init(&i, &doc);
    while((bt = bson_iterator_next(&i)) > 0) {
        key = bson_iterator_key(&i);
        if(strcmp(key, "_id") == 0)
            out->inode = bson_iterator_long(&i);
        else if(strcmp(key, "mode") == 0)
            out->mode = bson_iterator_int(&i);
        else if(strcmp(key, "owner") == 0)
            out->owner = bson_iterator_long(&i);
        else if(strcmp(key, "group") == 0)
            out->owner = bson_iterator_long(&i);
        else if(strcmp(key, "size") == 0)
            out->owner = bson_iterator_long(&i);
        else if(strcmp(key, "created") == 0)
            out->created = bson_iterator_time_t(&i);
        else if(strcmp(key, "modified") == 0)
            out->modified = bson_iterator_time_t(&i);
        else if(strcmp(key, "extentcount") == 0)
            out->extentcount = bson_iterator_int(&i);
        else if(strcmp(key, "data") == 0) {
            int datalen = bson_iterator_bin_len(&i);
            out->data = malloc(datalen);
            memcpy(out->data, bson_iterator_bin_data(&i), datalen);
        }
        else if(strcmp(key, "dirents")) {
            bson_iterator sub;
            bson_iterator_subiterator(&i, &sub);
            struct dirent * cde = malloc(sizeof(struct dirent));
            while((bt = bson_iterator_next(&sub)) > 0) {
                cde->path = strdup(bson_iterator_string(&sub));
                cde->next = e->dirents;
                e->dirents = cde;
                cde = malloc(sizeof(struct dirent));
            }
        }
    }
    bson_destroy(&doc);

    if(out->inode == 0)
        return -ENOENT;

    if(out->extentcount == 0) {
        if(!getdata)
            bson_destroy(&fields);
        return 0;
    }

    bson_init(&query);
    bson_append_long(&query, "inode", out->inode);
    bson_finish(&query);

    mongo_cursor_init(&curs, &conn, blocks_name);
    mongo_cursor_set_query(&curs, &query);
    if(!getdata)
        mongo_cursor_set_fields(&curs, &fields);

    c = 0;

    while((res = mongo_cursor_next(&curs)) == MONGO_OK) {
        bson_iterator_init(&i, mongo_cursor_bson(&curs));
        struct extent * curextent = malloc(sizeof(struct extent)), *found;
        curextent->committed = 1;
        while((bt = bson_iterator_next(&i)) > 0) {
            key = bson_iterator_key(&i);
            if(strcmp(key, "_id") == 0)
                memcpy(&curextent->oid, bson_iterator_oid(&i), sizeof(bson_oid_t));
            else if(strcmp(key, "start") == 0)
                curextent->start = bson_iterator_long(&i);
            else if(strcmp(key, "data") == 0 && getdata) {
                curextent->size = bson_iterator_bin_len(&i);
                curextent->data = malloc(curextent->size);
                memcpy(curextent->data, bson_iterator_bin_data(&i), curextent->size);
            }
        }
        found = (struct extent*)tsearch(curextent, (void**)&out->extents, extent_cmp);
        if(found != curextent) {
            fprintf(stderr, "Duplicate extent for start %lld\n", curextent->start);
            free_extent(curextent);
            free_entry(out);
            return -EIO;
        }
    }

    mongo_cursor_destroy(&curs);
    bson_destroy(&query);
    if(!getdata)
        bson_destroy(&fields);

    if(res != MONGO_OK) {
        fprintf(stderr, "Error getting extent %d: %d", c, curs.err);
        return -EIO;
    }

    return 0;
}


static int mongo_getattr(const char *path, struct stat *stbuf) {
    int res = 0;
    struct entry e;

    memset(stbuf, 0, sizeof(struct stat));
    if (strcmp(path, "/") == 0) {
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
        return res;
    }

    res = get_entry(path, &e, 0);
    if(res != 0)
        return res;

    stbuf->st_mode = e->mode;
    if(stbuf->st_mode & S_IFREG)
        stbuf->st_nlink = 1;
    else if(stbuf->st_mode & s_IFDIR)
        stbuf->st_nlink = 2;
    stbuf->st_size = e->size;
    stbuf->st_ino = e->inode;
    stbuf->st_uid = e->user;
    stbuf->st_gid = e->group;

    free_entry(&e);
    return res;
}


static int mongo_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                         off_t offset, struct fuse_file_info *fi) {
    (void) offset;
    (void) fi;

    if (strcmp(path, "/") != 0)
        return -ENOENT;

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    filler(buf, mongo_path + 1, NULL, 0);

    return 0;
}

static int mongo_write(const char *path, const char *buf, size_t size,
		      off_t offset, struct fuse_file_info *fi)
{
    int res;
    int size;
    struct entry e;

	if(strcmp(path, "/") != 0)
		return -ENOENT;


	return size;
}

static int mongo_open(const char *path, struct fuse_file_info *fi)
{
    if (strcmp(path, mongo_path) != 0)
        return -ENOENT;

    if ((fi->flags & 3) != O_RDONLY)
        return -EACCES;

    return 0;
}

static int mongo_read(const char *path, char *buf, size_t size, off_t offset,
                      struct fuse_file_info *fi) {
    size_t len;
    (void) fi;

    if(strcmp(path, mongo_path) != 0)
        return -ENOENT;

    mongo conn;
    mongo_init( &conn );
    int res = mongo_client( &conn, "127.0.0.1", 27017 );
    if ( res != MONGO_OK ) {
        fprintf( stderr, "connection to mongo failed\n" );
        return -ENOENT;
    }

    bson doc;
    res = mongo_find_one( &conn, "test.blocks", bson_shared_empty(), bson_shared_empty(), &doc );
    if ( res != MONGO_OK ) {
        fprintf( stderr, "find one failed\n" );
        return -ENOENT;
    }

    bson_print( &doc );

    bson_iterator it;
    bson_iterator_init( &it, &doc );
    if ( bson_find( &it, &doc, "data" ) != BSON_STRING ) {
        fprintf( stderr, "no data field\n" );
        return -ENOENT;
    }

    const char* data = bson_iterator_string( &it );

    len = strlen(data);
    if (offset < len) {
        if (offset + size > len)
            size = len - offset;
        memcpy(buf, data + offset, size);
    }
    else
        size = 0;

    mongo_destroy( &conn );

    return size;
}

static struct fuse_operations mongo_oper = {
    .getattr    = mongo_getattr,
    .readdir    = mongo_readdir,
    .open        = mongo_open,
    .read        = mongo_read,
};

int main(int argc, char *argv[])
{
    return fuse_main(argc, argv, &mongo_oper, NULL);
}

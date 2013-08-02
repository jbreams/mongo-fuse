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
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "mongo-fuse.h"

char * blocks_name = "test.blocks";
char * inodes_name = "test.inodes";
char * inodes_coll = "inodes";
char * dbname = "test";
const char * mongo_host = "127.0.0.1";
int mongo_port = 27017;

static int mongo_mkdir(const char * path, mode_t mode);

static int mongo_getattr(const char *path, struct stat *stbuf) {
    int res = 0;
    struct inode e;

    memset(stbuf, 0, sizeof(struct stat));
    res = get_inode(path, &e, 0);
    if(res != 0) {
        if(strcmp(path, "/") == 0) {
            stbuf->st_mode = S_IFDIR | 0755;
            stbuf->st_nlink = 2;
            return mongo_mkdir("/", 0755);
        }
        return res;
    }

    stbuf->st_nlink = e.direntcount;
    stbuf->st_mode = e.mode;
    if(stbuf->st_mode & S_IFDIR)
        stbuf->st_nlink++;
    stbuf->st_uid = e.owner;
    stbuf->st_gid = e.group;
    stbuf->st_size = e.size;
    stbuf->st_ctime = e.created;
    stbuf->st_mtime = e.modified;
    stbuf->st_atime = e.modified;
    stbuf->st_dev = e.dev;

    free_inode(&e);
    return res;
}

static int mongo_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                         off_t offset, struct fuse_file_info *fi) {
    (void) offset;
    (void) fi;
    bson query, fields;
    mongo_cursor curs;
    struct stat stbuf;
    size_t pathlen = strlen(path);
    char * regexp = malloc(pathlen + 10);
    int res;
    mongo * conn = get_conn();

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);

    if(strcmp(path, "/") == 0) {
        path++;
    } else
        pathlen++;
    sprintf(regexp, "^%s/[^/]+$", path);
    printf("%s %s\n", regexp, path);
    bson_init(&query);
    bson_append_regex(&query, "dirents", regexp, "");
    bson_finish(&query);

    bson_init(&fields);
    bson_append_int(&fields, "data", 0);
    bson_finish(&fields);

    mongo_cursor_init(&curs, conn, inodes_name);
    mongo_cursor_set_query(&curs, &query);
    mongo_cursor_set_fields(&curs, &fields);

    while((res = mongo_cursor_next(&curs)) == MONGO_OK) {
        bson_iterator i;
        const char * curpath, *key;
        bson_type bt;

        bson_iterator_init(&i, mongo_cursor_bson(&curs));
        while((bt = bson_iterator_next(&i))> 0) {
            key = bson_iterator_key(&i);
            if(strcmp(key, "mode") == 0)
                stbuf.st_mode = bson_iterator_int(&i);
            else if(strcmp(key, "owner") == 0)
                stbuf.st_uid = bson_iterator_long(&i);
            else if(strcmp(key, "group") == 0)
                stbuf.st_gid = bson_iterator_long(&i);
            else if(strcmp(key, "size") == 0)
                stbuf.st_size = bson_iterator_long(&i);
            else if(strcmp(key, "created") == 0)
                stbuf.st_ctime = bson_iterator_time_t(&i);
            else if(strcmp(key, "modified") == 0) {
                stbuf.st_mtime = bson_iterator_time_t(&i);
                stbuf.st_atime = stbuf.st_mtime;
            }
            else if(strcmp(key, "dirents") == 0) {
                bson_iterator sub;
                bson_iterator_subiterator(&i, &sub);
                while((bt = bson_iterator_next(&sub)) > 0) {
                    curpath = bson_iterator_string(&sub);
                    if(strncmp(curpath, path, pathlen) == 0)
                        break;
                }
            }
        }

        filler(buf, curpath + pathlen, &stbuf, 0);
    }
    bson_destroy(&query);
    bson_destroy(&fields);
    free(regexp);

    if(curs.err != MONGO_CURSOR_EXHAUSTED) {
        fprintf(stderr, "Error reading directory %s: %d", path, curs.err);
        return -EIO;
    }

    return 0;
}

static int mongo_rename(const char * path, const char * newpath) {
    struct inode e;
    struct dirent * nd;
    const size_t newpathlen = strlen(newpath);
    int res;

    if((res = get_inode(path, &e, 0)) != 0)
        return res;

    struct dirent * d = e.dirents;
    while(d && strcmp(d->path, path) != 0)
        d = d->next;

    nd = realloc(d, sizeof(struct dirent) + newpathlen);
    if(!nd) {
        free_inode(&e);
        return -ENOMEM;
    }

    strcpy(nd->path, newpath);
    res = commit_inode(&e);
    free_inode(&e);
    return res;
}

static int mongo_write(const char *path, const char *buf, size_t size,
                       off_t offset, struct fuse_file_info *fi)
{
    struct inode e;
    int res;
    struct extent * o = NULL, *last = NULL;
    char * block = (char*)buf;
    size_t tocopy;
    const size_t end = size + offset,
        last_block = (end / EXTENT_SIZE) * EXTENT_SIZE;

    if((res = get_inode(path, &e, offset < EXTENT_SIZE ? 1: 0)) != 0)
        return res;

    if(e.mode & S_IFDIR)
        return -EISDIR;

    e.modified = time(NULL);

    if(offset < EXTENT_SIZE) {
        size_t sizediff = EXTENT_SIZE - offset;
        tocopy = size > sizediff ? sizediff : size;
        memcpy(e.data + offset, block, tocopy);
        offset += tocopy;
        block += tocopy;
        e.datalen += tocopy;
    }

    if(block - buf == size) {
        if(end > e.size) {
            e.size += tocopy;
            e.datalen += tocopy;
        }
        commit_inode(&e);
        free_inode(&e);
        return tocopy;
    }

    if(end - offset > EXTENT_SIZE) {
        o = NULL;
        while(end - offset > EXTENT_SIZE && offset != last_block) {
            struct extent * n = malloc(sizeof(struct extent));
            if(!n) {
                fprintf(stderr, "Error allocating block\n");
                res = -ENOMEM;
                goto cleanup;
            }
            n->size = EXTENT_SIZE;
            n->start = offset;
            memcpy(&n->inode, &e.oid, sizeof(bson_oid_t));
            memcpy(n->data, block, EXTENT_SIZE);
            n->next = o;
            o = n;
            block += tocopy;
            offset += tocopy;
        }
        commit_extents(&e, o);
        free_extents(o);
    }

    res = resolve_extent(&e, last_block, end, &last, 1);
    if(res != 0)
        goto cleanup;
    if(!last) {
        last = malloc(sizeof(struct extent));
        if(!last) {
            fprintf(stderr, "Error allocating last block!\n");
            res = -ENOMEM;
            goto cleanup;
        }
        last->start = last_block;
        memcpy(&last->inode, &e.oid, sizeof(bson_oid_t));
    }

    memcpy(last->data, block, end - offset);
    last->size = end - offset;
    last->next = o;

    res = commit_extents(&e, last);
    if(res != 0)
        goto cleanup;
    if(end > e.size)
        e.size = end;
    res = commit_inode(&e);
cleanup:
    free_inode(&e);
    free_extents(last);
    if(res != 0)
        return res;
    return size;
}

static int mongo_open(const char *path, struct fuse_file_info *fi)
{
    struct inode e;
    int res;

    res = get_inode(path, &e, 0);
    if(res == 0)
        free_inode(&e);
    return res;
}

static int mongo_create(const char * path, mode_t mode, struct fuse_file_info * fi) {
    struct inode e;
    int res;
    int pathlen = strlen(path);
    const struct fuse_context * fcx = fuse_get_context();

    res = get_inode(path, &e, 0);
    if(res == 0) {
        free_inode(&e);
        fprintf(stderr, "Exiting because it already exists\n");
        return 0;
    }
    else if(res != -ENOENT) {
        fprintf(stderr, "Exiting create with error %d\n", res);
        return res;
    }

    bson_oid_gen(&e.oid);
    e.dirents = malloc(sizeof(struct dirent) + pathlen);
    strcpy(e.dirents->path, path);
    e.dirents->next = NULL;
    e.direntcount = 1;

    e.mode = mode;
    e.owner = fcx->uid;
    e.group = fcx->gid;
    e.size = 0;
    e.created = time(NULL);
    e.modified = time(NULL);
    e.data = NULL;

    res = commit_inode(&e);
    free_inode(&e);
    return res;
}

static int mongo_mkdir(const char * path, mode_t mode) {
    return mongo_create(path, mode | S_IFDIR, NULL);
}

static int mongo_rmdir(const char * path) {
    struct inode e;
    int res;
    double dres;
    bson cond;
    size_t pathlen = strlen(path);
    char * regexp = malloc(pathlen + 10);
    mongo * conn = get_conn();

    sprintf(regexp, "^%s/[^/]+", path + 1);
    bson_init(&cond);
    bson_append_regex(&cond, "dirents", regexp, "");
    bson_finish(&cond);

    dres = mongo_count(conn, dbname, inodes_coll, &cond);
    bson_destroy(&cond);
    free(regexp);

    if(dres > 1)
        return -ENOTEMPTY;

    if((res = get_inode(path, &e, 0)) != 0)
        return res;

    bson_init(&cond);
    bson_append_oid(&cond, "_id", &e.oid);
    bson_finish(&cond);

    res = mongo_remove(conn, inodes_name, &cond, NULL);
    bson_destroy(&cond);
    if(res != MONGO_OK) {
        fprintf(stderr, "Error removing inode entry for %s\n", path);
        return -EIO;
    }
    return 0;
}

static int mongo_read(const char *path, char *buf, size_t size, off_t offset,
                      struct fuse_file_info *fi) {
    struct inode e;
    int res;
    struct extent * o, *el;
    char * block = buf;
    size_t tocopy;
    const size_t end = size + offset;

    if((res = get_inode(path, &e, offset < EXTENT_SIZE ? 1: 0)) != 0)
        return res;

    if(e.mode & S_IFDIR)
        return -EISDIR;

    if(offset > e.size)
        return 0;

    if(offset < EXTENT_SIZE) {
        size_t sizediff = EXTENT_SIZE - offset;
        tocopy = size > sizediff ? sizediff : size;
        memcpy(block, e.data + offset, tocopy);
        offset += tocopy;
        block += tocopy;
    }

    if(block - buf == size) {
        free_inode(&e);
        return tocopy;
    }

    res = resolve_extent(&e, offset, offset + size, &o, 1);
    free_inode(&e);
    if(res != 0)
        return res;

    el = o;
    while(block - buf != size) {
        if(end - offset < EXTENT_SIZE)
            tocopy = end - offset;
        else
            tocopy = EXTENT_SIZE;

        if(!el || el->start != offset) {
            memset(block, 0, tocopy);
            goto advance;
        }

        memcpy(block, el->data, tocopy);
        el = el->next;
advance:
        block += tocopy;
        offset += tocopy;
    }

    if(o)
        free_extents(o);
    return size;
}

static int do_trunc(struct inode * e, off_t off) {
    bson cond;
    int res;
    mongo * conn = get_conn();

    if(e->mode & S_IFDIR)
        return -EISDIR;

    if(off > e->size) {
        e->size = off;
        return commit_inode(e);
    }

    bson_init(&cond);
    bson_append_oid(&cond, "inode", &e->oid);

    if(off <= EXTENT_SIZE) {
        e->size = off;
        e->datalen = off;
    } else {
        size_t start = off / EXTENT_SIZE;
        if(off % EXTENT_SIZE != 0)
            start += EXTENT_SIZE;
        bson_append_start_object(&cond, "start");
        bson_append_long(&cond, "$gt", start);
        bson_append_finish_object(&cond);
    }
    bson_finish(&cond);
    res = mongo_remove(conn, blocks_name, &cond, NULL);
    bson_destroy(&cond);
    if(res != MONGO_OK) {
        fprintf(stderr, "Error truncating blocks\n");
        return -EIO;
    }

    return commit_inode(e);
}

static int mongo_truncate(const char * path, off_t off) {
    struct inode e;
    int res;

    if((res = get_inode(path, &e, 1)) != 0)
        return res;

    res = do_trunc(&e, off);
    free_inode(&e);
    return res;
}

static int mongo_link(const char * newpath, const char * path) {
    struct inode e;
    int res;

    if((res = get_inode(path, &e, 0)) != 0)
        return res;

    if(e.mode & S_IFDIR) {
        free_inode(&e);
        return -EPERM;
    }

    struct dirent * newlink = malloc(sizeof(struct dirent) + strlen(newpath));
    strcpy(newlink->path, newpath);
    newlink->next = e.dirents;
    e.direntcount++;
    return commit_inode(&e);
}

static int mongo_unlink(const char * path) {
    struct inode e;
    int res;
    mongo * conn = get_conn();

    if((res = get_inode(path, &e, 0)) != 0)
        return res;

    if(e.direntcount > 1) {
        struct dirent * c = e.dirents, *l = NULL;
        while(c && strcmp(c->path, path) != 0) {
            l = c;
            c = c->next;
        }
        if(!l)
            e.dirents = c;
        else
            l->next = c->next;
        free(c);
        e.direntcount--;
        res = commit_inode(&e);
        free_inode(&e);
        return res;
    }

    bson cond;
    bson_init(&cond);
    bson_append_oid(&cond, "_id", &e.oid);
    bson_finish(&cond);

    res = mongo_remove(conn, inodes_name, &cond, NULL);
    bson_destroy(&cond);
    if(res != MONGO_OK) {
        fprintf(stderr, "Error removing inode entry for %s\n", path);
        return -EIO;
    }

    bson_init(&cond);
    bson_append_oid(&cond, "inode", &e.oid);
    bson_finish(&cond);

    res = mongo_remove(conn, blocks_name, &cond, NULL);
    bson_destroy(&cond);
    if(res != MONGO_OK) {
        fprintf(stderr, "Error removing blocks for %s\n", path);
        return -EIO;
    }
    return 0;
}

static int mongo_chmod(const char * path, mode_t mode) {
    struct inode e;
    int res;

    if((res = get_inode(path, &e, 0)) != 0)
        return res;

    e.mode = (e.mode & S_IFMT) | mode;

    res = commit_inode(&e);
    free_inode(&e);
    return res;
}

static int mongo_chown(const char * path, uid_t user, gid_t group) {
    struct inode e;
    int res;

    if((res = get_inode(path, &e, 0)) != 0)
        return res;

    e.owner = user;
    e.group = group;

    res = commit_inode(&e);
    free_inode(&e);
    return res;

}

static int mongo_utimens(const char * path, const struct timespec tv[2]) {
    struct inode e;
    int res;

    if((res = get_inode(path, &e, 0)) != 0)
        return res;

    if(tv == NULL)
        e.modified = time(NULL);
    else
        e.modified = tv[1].tv_sec;

    res = commit_inode(&e);
    free_inode(&e);
    return res;
}

static struct fuse_operations mongo_oper = {
    .getattr    = mongo_getattr,
    .readdir    = mongo_readdir,
    .open       = mongo_open,
    .read       = mongo_read,
    .write      = mongo_write,
    .create     = mongo_create,
    .truncate   = mongo_truncate,
    .mkdir      = mongo_mkdir,
    .unlink     = mongo_unlink,
    .link       = mongo_link,
    .chmod      = mongo_chmod,
    .chown      = mongo_chown,
    .rmdir      = mongo_rmdir,
    .utimens    = mongo_utimens,
    .rename     = mongo_rename
};

int main(int argc, char *argv[])
{
    setup_threading();
    return fuse_main(argc, argv, &mongo_oper, NULL);
}

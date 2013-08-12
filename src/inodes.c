#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <mongo.h>
#include <stdlib.h>
#include <search.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/file.h>
#include <time.h>
#include "mongo-fuse.h"
#include <osxfuse/fuse.h>

extern const char * inodes_name;
extern const char * locks_name;
extern mongo conn;

int commit_inode(struct inode * e) {
    bson cond, doc;
    mongo * conn = get_conn();
    char istr[4];
    struct dirent * cde = e->dirents;
    int res;

    bson_init(&doc);
    bson_append_start_object(&doc, "$set");
    bson_append_start_array(&doc, "dirents");
    res = 0;
    while(cde) {
        bson_numstr(istr, res++);
        bson_append_string(&doc, istr, cde->path);
        cde = cde->next;
    }
    bson_append_finish_array(&doc);

    bson_append_int(&doc, "mode", e->mode);
    bson_append_long(&doc, "owner", e->owner);
    bson_append_long(&doc, "group", e->group);
    bson_append_long(&doc, "size", e->size);
    bson_append_int(&doc, "blocksize", e->blocksize);
    if(e->dev > 0)
        bson_append_long(&doc, "dev", e->dev);
    bson_append_time_t(&doc, "created", e->created);
    bson_append_time_t(&doc, "modified", e->modified);
    if(e->data && e->datalen > 0)
        bson_append_binary(&doc, "data", 0, e->data, e->datalen);
    bson_append_finish_object(&doc);
    bson_finish(&doc);

    bson_init(&cond);
    bson_append_oid(&cond, "_id", &e->oid);
    bson_finish(&cond);

    res = mongo_update(conn, inodes_name, &cond, &doc, MONGO_UPDATE_UPSERT, NULL);
    bson_destroy(&cond);
    bson_destroy(&doc);
    if(res != MONGO_OK) {
        fprintf(stderr, "Error committing inode\n");
        return -EIO;
    }
    return 0;
}

int fill_data(struct inode * e) {
    bson query, doc, fields;
    mongo * conn = get_conn();
    bson_iterator i;
    bson_type bt;
    int res;

    bson_init(&query);
    bson_append_oid(&query, "_id", &e->oid);
    bson_finish(&query);

    bson_init(&fields);
    bson_append_int(&fields, "data", 1);
    bson_append_int(&fields, "_id", 0);
    bson_finish(&fields);

    res = mongo_find_one(conn, inodes_name, &query, &fields, &doc);
    bson_destroy(&query);
    bson_destroy(&fields);

    if(res != MONGO_OK) {
        fprintf(stderr, "Error retrieving data\n");
        return -EIO;
    }

    bson_iterator_init(&i, &doc);
    bt = bson_iterator_next(&i);

    if(e->data)
        free(e->data);
    e->data = malloc(e->blocksize);
    if(e->data == NULL) {
        bson_destroy(&doc);
        return -ENOMEM;
    }
    memset(e->data, 0, e->blocksize);

    if(bt == 0 || strcmp(bson_iterator_key(&i), "data") != 0) {
        e->datalen = 0;
        bson_destroy(&doc);
        return 0;
    }

    if(bt == BSON_STRING) {
        e->datalen = bson_iterator_string_len(&i);
        strcpy(e->data, bson_iterator_string(&i));
    } else if(bt == BSON_BINDATA) {
        e->datalen = bson_iterator_bin_len(&i);
        memcpy(e->data, bson_iterator_bin_data(&i), e->datalen);
    }

    bson_destroy(&doc);
    return 0;
}

int read_inode(const bson * doc, struct inode * out) {
    bson_iterator i, sub;
    bson_type bt;
    const char * key;

    memset(out, 0, sizeof(struct inode));
    bson_iterator_init(&i, doc);
    while((bt = bson_iterator_next(&i)) > 0) {
        key = bson_iterator_key(&i);
        if(strcmp(key, "_id") == 0)
            memcpy(&out->oid, bson_iterator_oid(&i), sizeof(bson_oid_t));
        else if(strcmp(key, "mode") == 0)
            out->mode = bson_iterator_int(&i);
        else if(strcmp(key, "owner") == 0)
            out->owner = bson_iterator_long(&i);
        else if(strcmp(key, "group") == 0)
            out->group = bson_iterator_long(&i);
        else if(strcmp(key, "size") == 0)
            out->size = bson_iterator_long(&i);
        else if(strcmp(key, "created") == 0)
            out->created = bson_iterator_time_t(&i);
        else if(strcmp(key, "modified") == 0)
            out->modified = bson_iterator_time_t(&i);
        else if(strcmp(key, "dev") == 0)
            out->dev = bson_iterator_long(&i);
        else if(strcmp(key, "blocksize") == 0)
            out->blocksize = bson_iterator_int(&i);
        else if(strcmp(key, "locked") == 0)
            out->locked = bson_iterator_int(&i);
        else if(strcmp(key, "data") == 0) {
            if(bt == BSON_STRING) {
                out->datalen = bson_iterator_string_len(&i);
                out->data = malloc(out->datalen + 1);
                strcpy(out->data, bson_iterator_string(&i));
            } else if(bt == BSON_BINDATA) {
                out->datalen = bson_iterator_bin_len(&i);
                out->data = malloc(out->datalen);
                memcpy(out->data, bson_iterator_bin_data(&i), out->datalen);
            }
        }
        else if(strcmp(key, "dirents") == 0) {
            bson_iterator_subiterator(&i, &sub);
            while((bt = bson_iterator_next(&sub)) > 0) {
                int len = bson_iterator_string_len(&sub);
                struct dirent * cde = malloc(sizeof(struct dirent) + len);
                if(!cde)
                    return -ENOMEM;
                strcpy(cde->path, bson_iterator_string(&sub));
                cde->len = bson_iterator_string_len(&sub);
                cde->next = out->dirents;
                out->dirents = cde;
                out->direntcount++;
            }
        }
        else if(strcmp(key, "reads") == 0 || strcmp(key, "writes") == 0) {
            int write = (*key == 'w');
            bson_iterator_subiterator(&i, &sub);
            while((bt = bson_iterator_next(&sub)) != 0) {
                uint64_t curval = bson_iterator_long(&sub);
                int index;
                key = bson_iterator_key(&sub);
                index = atoi(key);
                if(write)
                    out->writes[index] = curval;
                else
                    out->reads[index] = curval;
            }
        }
    }

    return 0;
}

int get_inode(const char * path, struct inode * out, int getdata) {
    bson query, doc, fields;
    int res;
    mongo * conn = get_conn();

    bson_init(&query);
    bson_append_string(&query, "dirents", path);
    bson_finish(&query);

    if(!getdata) {
        bson_init(&fields);
        bson_append_int(&fields, "data", 0);
        bson_finish(&fields);
    }

    res = mongo_find_one(conn, inodes_name, &query,
        getdata ? bson_shared_empty():&fields, &doc);

    if(!getdata)
        bson_destroy(&fields);

    if(res != MONGO_OK) {
        bson_destroy(&query);
        return -ENOENT;
    }

    bson_destroy(&query);
    res = read_inode(&doc, out);
    bson_destroy(&doc);

    return res;
}

int check_access(struct inode * e, int amode) {
    const struct fuse_context * fcx = fuse_get_context();
    mode_t mode = e->mode;

    if(fcx->uid == 0 || amode == 0)
        return 0;

    if(fcx->uid == e->owner)
        mode >>= 6;
    else if(fcx->gid == e->group)
        mode >>= 3;

    return (((mode & S_IRWXO) & amode) == 0);
}

int crunch_stats(struct inode * p) {
    int i, ireadmax, iwritemax;
    uint64_t readmax = 0, writemax = 0;

    for(i = 0; i < 8; i++) {
        if(p->reads[i] > readmax) {
            readmax = p->reads[i];
            ireadmax = i;
        }
    }
    for(i = 0; i < 8; i++) {
        if(p->writes[i] > writemax) {
            writemax = p->writes[i];
            iwritemax = i;
        }
    }

    if(writemax == 0 && readmax == 0)
        return 0;

    if(writemax > readmax)
        return (1<<(iwritemax + 12));
    else
        return (1<<(ireadmax + 12));
}

int choose_block_size(const char * path, size_t len) {
    int blocksize = 0, res;
    char *parentpath = strdup(path), *ppl = (char*)parentpath + len;
    struct inode parent;

    while(blocksize == 0 && ppl - path > 1) {
        while(*ppl != '/') ppl--;
        if(ppl - path > 0)
            *ppl = '\0';

        res = get_inode(parentpath, &parent, 0);
        if(res != 0 || !(parent.mode & S_IFDIR)) {
            free_inode(&parent);
            free(parentpath);
            return -ENOENT;
        }

        blocksize = crunch_stats(&parent);
        free_inode(&parent);
    }

    free(parentpath);
    if(blocksize == 0)
        blocksize = 65536;
    return blocksize;
}

int create_inode(const char * path, mode_t mode, const char * data) {
    struct inode e;
    int pathlen = strlen(path);
    const struct fuse_context * fcx = fuse_get_context();
    int res, blocksize;

    res = get_inode(path, &e, 0);
    if(res == 0) {
        free_inode(&e);
        return -EEXIST;
    }

    if(mode & S_IFREG) {
        blocksize = choose_block_size(path, pathlen);
        if(blocksize < 0)
            return blocksize;
        e.blocksize = blocksize;
    }
    else
        e.blocksize = 0;

    bson_oid_gen(&e.oid);
    e.dirents = malloc(sizeof(struct dirent) + pathlen);
    e.dirents->len = pathlen;
    strcpy(e.dirents->path, path);
    e.dirents->next = NULL;
    e.direntcount = 1;

    e.mode = mode;
    e.owner = fcx->uid;
    e.group = fcx->gid;
    e.created = time(NULL);
    e.modified = time(NULL);
    if(data) {
        e.datalen = strlen(data);
        e.data = strdup(data);
        e.size = e.datalen;
    } else {
        e.data = NULL;
        e.datalen = 0;
        e.size = 0;
    }

    res = commit_inode(&e);
    free_inode(&e);
    return res;
}

void free_inode(struct inode *e) {
    if(e->data)
        free(e->data);
    while(e->dirents) {
        struct dirent * next = e->dirents->next;
        free(e->dirents);
        e->dirents = next;
    }
}

#if FUSE_VERSION > 28
void read_lock(const bson * b, int * nr, int * nw) {
    bson_iterator i;
    bson_type bt;
    const char * key;
    int writer = 0, added = 0;

    bson_iterator_init(&i, b);
    while((bt = bson_iterator_next(&i)) != 0) {
        key = bson_iterator_key(&i);
        if(strcmp(key, "_id") == 0) {
            bson_iterator sub;
            bson_iterator_subiterator(&i, &sub);
            while((bt = bson_iterator_next(&sub)) != 0) {
                key = bson_iterator_key(&sub);
                if(strcmp(key, "acquire") == 0){
                    added = bson_iterator_bool(&sub);
                }
            }
        }
        else if(strcmp(key, "writer") == 0)
            writer = bson_iterator_bool(&i);
    }
    if(writer)
        *nw += added ? 1 : -1;
    else
        *nr += added ? 1 : -1;
}

int unlock_inode(struct inode * e, int writer, bson_date_t locktime) {
    bson doc;
    mongo * conn = get_conn();
    int res;

    bson_init(&doc);
    bson_append_start_object(&doc, "_id");
    bson_append_date(&doc, "time", locktime);
    bson_append_oid(&doc, "inode", &e->oid);
    bson_append_bool(&doc, "acquire", 0);
    bson_append_finish_object(&doc);
    bson_append_bool(&doc, "writer", writer);
    bson_finish(&doc);

    res = mongo_insert(conn, locks_name, &doc, NULL);
    bson_destroy(&doc);
    if(res != MONGO_OK)
        return -EIO;
    return 0;
}

int lock_inode_impl(struct inode * e, int writer,
    bson_date_t * locktime, int noblock) {
    bson query, doc;
    mongo * conn = get_conn();
    mongo_cursor curs;
    int res, nreaders = 0, nwriters = 0, nrecs = 0, nrecs2 = 0;
    struct timeval tv;

    gettimeofday(&tv, NULL);
    *locktime = (tv.tv_sec * 1000) + (tv.tv_usec / 1000);

    bson_init(&doc);
    bson_append_start_object(&doc, "_id");
    bson_append_date(&doc, "time", *locktime);
    bson_append_oid(&doc, "inode", &e->oid);
    bson_append_bool(&doc, "acquire", 1);
    bson_append_finish_object(&doc);
    bson_append_bool(&doc, "writer", writer);
    bson_finish(&doc);

    res = mongo_insert(conn, locks_name, &doc, NULL);
    bson_destroy(&doc);
    if(res != MONGO_OK) {
        if(conn->lasterrcode == 11000)
            return -EAGAIN;
        return -EIO;
    }

    bson_init(&query);
    bson_append_start_object(&query, "$query");
    bson_append_oid(&query, "_id.inode", &e->oid);
    bson_append_start_object(&query, "_id.time");
    bson_append_date(&query, "$lt", *locktime);
    bson_append_finish_object(&query);
    bson_append_finish_object(&query);
    bson_append_start_object(&query, "$orderby");
    bson_append_int(&query, "$natural", 1);
    bson_append_finish_object(&query);
    bson_finish(&query);

    mongo_cursor_init(&curs, conn, locks_name);
    mongo_cursor_set_query(&curs, &query);

    while((res = mongo_cursor_next(&curs)) == MONGO_OK) {
        const bson * curbson = mongo_cursor_bson(&curs);
        read_lock(curbson, &nreaders, &nwriters);
        nrecs++;
    }

    mongo_cursor_destroy(&curs);
    if(writer && nreaders == 0 && nwriters == 0) {
        bson_destroy(&query);
        return 0;
    }
    else if(!writer && nwriters == 0) {
        bson_destroy(&query);
        return 0;
    }
    if(nonblock)
        return -EWOULDBLOCK;

    mongo_cursor_init(&curs, conn, locks_name);
    mongo_cursor_set_query(&curs, &query);
    mongo_cursor_set_options(&curs,
        MONGO_TAILABLE | MONGO_NO_CURSOR_TIMEOUT | MONGO_AWAIT_DATA);

    nreaders = 0;
    nwriters = 0;
    while((res = mongo_cursor_next(&curs)) == MONGO_OK) {
        const bson * curbson = mongo_cursor_bson(&curs);
        read_lock(curbson, &nreaders, &nwriters);
        nrecs2++;
        if(nrecs2 < nrecs)
            continue;

        if(writer && nreaders == 0 && nwriters == 0)
            break;
        else if(!writer && nwriters == 0)
            break;
    }

    bson_destroy(&query);
    mongo_cursor_destroy(&curs);

    if(res == MONGO_OK)
        return 0;
    return -EIO;
}

int lock_inode(struct inode * e, int writer, bson_date_t * locktime, int noblock) {
    int res;
    while(1) {
        res = lock_inode_impl(e, writer, locktime, noblock);
        if(res == -EAGAIN) {
            struct timespec ts;
            ts.tv_sec = 0;
            ts.tv_nsec = 10000000;
            nanosleep(&ts, NULL);
        }
        else
            return res;
    }
}
#endif

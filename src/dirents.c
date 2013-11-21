#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <mongo.h>
#include <stdlib.h>
#include <search.h>
#include <sys/stat.h>
#include "mongo-fuse.h"
#include <osxfuse/fuse.h>
#include <limits.h>
#include <time.h>

extern const char * inodes_name;
extern const char * dirents_name;
extern const char * dirents_coll;
extern const char * extents_name;
extern const char * dbname;

static struct dirent ** dirent_cache = NULL;
static int cache_size = 0;
static int cache_count = 0;
static int cache_mask = 0;
static pthread_mutex_t cache_lock = PTHREAD_MUTEX_INITIALIZER;

int cache_dirent(struct dirent * found) {
	const uint32_t hash = found->hash & cache_mask;
	pthread_mutex_lock(&cache_lock);
	if(!dirent_cache) {
		cache_size = (1 << 8);
		cache_mask = cache_size - 1;
		size_t malloc_size = sizeof(struct dirent *) * cache_size;
		dirent_cache = malloc(malloc_size);
		if(!dirent_cache) {
			pthread_mutex_unlock(&cache_lock);
			return -ENOMEM;
		}
		memset(dirent_cache, 0, malloc_size);
	}

	found->next = dirent_cache[hash];
	dirent_cache[hash] = found;

	if(++cache_count > cache_size) {
		struct dirent ** old_cache = dirent_cache;
		size_t old_cache_size = cache_size, i;
		cache_size <<= 1;
		cache_mask = cache_size - 1;
		size_t malloc_size = sizeof(struct dirent *) * cache_size;
		dirent_cache = malloc(malloc_size);
		if(!dirent_cache) {
			dirent_cache = old_cache;
			cache_size = old_cache_size;
			pthread_mutex_unlock(&cache_lock);
			return -ENOMEM;
		}
		memset(dirent_cache, 0, malloc_size);
		for(i = 0; i < old_cache_size; i++) {
			if(!old_cache[i])
				continue;
			found = old_cache[i];
			while(found) {
				struct dirent * fnext = found->next;
				size_t new_loc = found->hash & cache_mask;
				found->next = dirent_cache[new_loc];
				dirent_cache[new_loc] = found;
				found = fnext;
			}
		}
		free(old_cache);
	}
	pthread_mutex_unlock(&cache_lock);
	return 0;
}

void uncache_dirent(const char * path, size_t path_len) {
	uint32_t hash = hashlittle(path, path_len, INITVAL);
	hash &= cache_mask;

	struct dirent * found = dirent_cache[hash];
	struct dirent * prev = NULL;
	while(found && strcmp(found->path, path) != 0) {
		prev = found;
		found = found->next;
	}
	if(found) {
		if(!prev)
			dirent_cache[hash] = found->next;
		else
			prev->next = found->next;
		free(found);
		cache_count--;
	}
}

int resolve_dirent(const char * path, bson_oid_t * out) {
	mongo * conn = get_conn();
	bson query, doc, fields;
	bson_iterator it;
	size_t path_len = strlen(path);
	int res;
	const uint32_t hash = hashlittle(path, path_len, INITVAL);
	struct dirent * found;
	time_t now = time(NULL);

	pthread_mutex_lock(&cache_lock);
	if(dirent_cache) {
		uint32_t loc = hash & cache_mask;
		found = dirent_cache[loc];

		while(found && strcmp(found->path, path) != 0)
			found = found->next;

		if(found) {
			if(now - found->cached_on > 3)
				uncache_dirent(path, path_len);
			else {
				memcpy(out, &found->inode, sizeof(bson_oid_t));
				pthread_mutex_unlock(&cache_lock);
				return 0;
			}
		}
	}
	pthread_mutex_unlock(&cache_lock);

	bson_init(&query);
	bson_append_string(&query, "dirents", path);
	bson_finish(&query);

	bson_init(&fields);
	bson_append_int(&fields, "_id", 1);
	bson_finish(&fields);

	res = mongo_find_one(conn, dirents_name, &query, &fields, &doc);
	bson_destroy(&query);
	bson_destroy(&fields);

	if(res != MONGO_OK) {
		return -ENOENT;
	}

	bson_find(&it, &doc, "_id");
	memcpy(out, bson_iterator_oid(&it), sizeof(bson_oid_t));
	bson_destroy(&doc);

	found = calloc(1, sizeof(struct dirent) + path_len);
	memcpy(&found->inode, out, sizeof(bson_oid_t));
	found->hash = hash;
	found->len = path_len;
	found->cached_on = now;
	strcpy(found->path, path);

	return cache_dirent(found);
}

int link_dirent(const char * path, bson_oid_t * inode) {
	mongo * conn = get_conn();
	bson query, doc;
	size_t path_len = strlen(path);
	int res;
	const uint32_t hash = hashlittle(path, path_len, INITVAL);
	struct dirent * found;
	time_t now = time(NULL);

	bson_init(&query);
	bson_append_oid(&query, "_id", inode);
	bson_finish(&query);

	bson_init(&doc);
	bson_append_start_object(&doc, "$addToSet");
	bson_append_string(&doc, "dirents", path);
	bson_append_finish_object(&doc);
	bson_finish(&doc);

	res = mongo_update(conn, dirents_name, &query, &doc,
		MONGO_UPDATE_UPSERT, NULL);
	bson_destroy(&doc);
	bson_destroy(&query);

	if(res != MONGO_OK)
		return -EIO;

	found = calloc(1, sizeof(struct dirent) + path_len);
	memcpy(&found->inode, inode, sizeof(bson_oid_t));
	found->hash = hash;
	found->len = path_len;
	found->cached_on = now;
	strcpy(found->path, path);

	return cache_dirent(found);
}

int unlink_dirent(const char * path) {
	bson_oid_t inode_id;
	mongo * conn = get_conn();
	bson query, doc, value;
	int res, nlinks = 0;
	size_t path_len = strlen(path);
	bson_iterator tit, sit;
	bson_type bt;

	if((res = resolve_dirent(path, &inode_id)) != 0)
		return res;

	bson_init(&query);
	bson_append_string(&query, "findAndModify", dirents_coll);
	bson_append_start_object(&query, "query");
	bson_append_oid(&query, "_id", &inode_id);
	bson_append_finish_object(&query);
	bson_append_start_object(&query, "update");
	bson_append_start_object(&query, "$pull");
	bson_append_string(&query, "dirents", path);
	bson_append_finish_object(&query);
	bson_append_finish_object(&query);
	bson_append_bool(&query, "new", 1);
	bson_finish(&query);

	res = mongo_run_command(conn, dbname, &query, &doc);
	bson_destroy(&query);
	if(res != MONGO_OK)
		return -EIO;

	pthread_mutex_lock(&cache_lock);
	uncache_dirent(path, path_len);
	pthread_mutex_unlock(&cache_lock);

	bt = bson_find(&tit, &doc, "value");
	if(bt == BSON_NULL)
		return 0;

	bson_iterator_subobject_init(&tit, &value, 0);
	bt = bson_find(&tit, &value, "dirents");
	bson_iterator_subiterator(&tit, &sit);
	while(bson_iterator_next(&sit) != 0)
		nlinks++;
	bson_destroy(&doc);

	if(nlinks > 0)
		return 0;
	
	bson_init(&query);
	bson_append_oid(&query, "_id", &inode_id);
	bson_finish(&query);

	res = mongo_remove(conn, dirents_name, &query, NULL);
	if(res != MONGO_OK) {
		bson_destroy(&query);
		fprintf(stderr, "Error unlinking dirent for %s\n", path);
		return -EIO;
	}

	res = mongo_remove(conn, inodes_name, &query, NULL);
	bson_destroy(&query);
	if(res != MONGO_OK) {
		fprintf(stderr, "Error unlinking inode for %s\n", path);
		return -EIO;
	}

	bson_init(&query);
	bson_append_oid(&query, "inode", &inode_id);
	bson_finish(&query);

	res = mongo_remove(conn, extents_name, &query, NULL);
	bson_destroy(&query);
	if(res != MONGO_OK) {
		fprintf(stderr, "Error removing extents for %s\n", path);
		return -EIO;
	}

	return 0;
}

int rename_dirent(const char * path, const char * newpath) {
    mongo * conn = get_conn();
    bson query, doc;
    bson_oid_t inodeid;
    int res;

    bson_init(&query);
    bson_append_string(&query, "dirents", path);
    bson_finish(&query);

    bson_init(&doc);
    bson_append_start_object(&doc, "$set");
    bson_append_string(&doc, "dirents.$", newpath);
    bson_append_finish_object(&doc);
    bson_finish(&doc);

    res = mongo_update(conn, inodes_name, &query, &doc,
        MONGO_UPDATE_BASIC, NULL);
    bson_destroy(&doc);
    bson_destroy(&query);

    if(res != MONGO_OK)
        return -EIO;

    if((res = resolve_dirent(newpath, &inodeid)) != 0)
    	return res;

    pthread_mutex_lock(&cache_lock);
    uncache_dirent(path, strlen(path));
    pthread_mutex_unlock(&cache_lock);
    return 0;
}

int read_dirents(const char * directory,
    int (*dirent_cb)(const char * path, void * p, size_t parentlen),
    void * p) {
    bson query;
    mongo_cursor curs;
    size_t dirnamelen = strlen(directory);
    char regexp[PATH_MAX + 10];
    int res;
    mongo * conn = get_conn();
    time_t now = time(NULL);

    sprintf(regexp, "^%s/[^/]+$", dirnamelen == 1 ? directory + 1 : directory);
    bson_init(&query);
    bson_append_regex(&query, "dirents", regexp, "");
    bson_finish(&query);

    mongo_cursor_init(&curs, conn, dirents_name);
    mongo_cursor_set_query(&curs, &query);

    while((res = mongo_cursor_next(&curs)) == MONGO_OK) {
    	bson_iterator tit, sit;
    	bson_oid_t * inode_id;
    	const char * path;
    	uint32_t hash;
    	size_t path_len;
    	struct dirent * found;

    	bson_iterator_init(&tit, mongo_cursor_bson(&curs));
    	while(bson_iterator_next(&tit) != 0) {
    		const char * key = bson_iterator_key(&tit);
    		if(strcmp(key, "_id") == 0)
    			inode_id = bson_iterator_oid(&tit);
    		if(strcmp(key, "dirents") == 0)
    			bson_iterator_subiterator(&tit, &sit);
    	}

    	while(bson_iterator_next(&sit) != 0) {
    		path = bson_iterator_string(&sit);
    		path_len = bson_iterator_string_len(&sit);
    		if(strncmp(path, directory, dirnamelen) == 0)
    			break;
    	}

        res = dirent_cb(path, p, dirnamelen);
        if(res != 0)
            break;

        hash = hashlittle(path, path_len, INITVAL);
		found = calloc(1, sizeof(struct dirent) + path_len);
		memcpy(&found->inode, inode_id, sizeof(bson_oid_t));
		found->hash = hash;
		found->len = path_len;
		found->cached_on = now;
		strcpy(found->path, path);

        res = cache_dirent(found);
        if(res != 0)
            break;
    }
    bson_destroy(&query);
    mongo_cursor_destroy(&curs);

    if(curs.err != MONGO_CURSOR_EXHAUSTED) {
        fprintf(stderr, "Error listing directory contents\n");
        return -EIO;
    }
    return 0;
}
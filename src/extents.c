#define FUSE_USE_VERSION 26

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include "mongo-fuse.h"

struct elist * init_elist() {
	const size_t malloc_size = sizeof(struct elist) +
		(sizeof(struct enode) * BLOCKS_PER_EXTENT);
	struct elist * out = calloc(1, malloc_size);
	if(!out)
		return NULL;
	out->nslots = BLOCKS_PER_EXTENT;
	return out;
}

int ensure_elist(struct elist ** pout) {
	struct elist * out = *pout;
	if(out == NULL) {
		out = init_elist();
		if(!out) {
			*pout = NULL;
			return -ENOMEM;
		}
		*pout = out;
	}
	if(out->nnodes + 1 < out->nslots)
		return 0;
	out->nslots += BLOCKS_PER_EXTENT;
	out = realloc(out, sizeof(struct elist) +
		(sizeof(struct enode) * out->nslots));
	if(!out)
		return -ENOMEM;
	*pout = out;
	return 0;
}

int insert_hash(struct elist ** pout, off_t off, size_t len,
	const uint8_t hash[HASH_LEN]) {
	int res, idx;
	if((res = ensure_elist(pout)) != 0)
		return res;

	struct elist * out = *pout;

	idx = out->nnodes++;
	out->list[idx].off = off;
	out->list[idx].len = len;
	out->list[idx].empty = 0;
	out->list[idx].seq = idx;
	memcpy(out->list[idx].hash, hash, HASH_LEN);

	return 0;
}

int insert_empty(struct elist ** pout, off_t off, size_t len) {
	int res, idx;
	if((res = ensure_elist(pout)) != 0)
		return res;

	struct elist * out = *pout;

	idx = out->nnodes++;
	out->list[idx].off = off;
	out->list[idx].len = len;
	out->list[idx].empty = 1;
	out->list[idx].seq = idx;
	
	return 0;
}

int enode_cmp(const void * ra, const void * rb) {
	struct enode * a = (struct enode *)ra;
	struct enode * b = (struct enode *)rb;

	int res = (a->seq > b->seq) - (a->seq < b->seq);
	if(res == 0)
		res = (a->off > b->off) - (a->off < b->off);
	return res;
}

int serialize_extent(struct inode * e, struct elist * list) {
	mongoc_collection_t * coll = get_coll(COLL_EXTENTS);
	bson_t doc, cond;
	bson_error_t dberr;
	int res, idx, towrite = 0;

	if(list->nnodes == 0)
		return 0;
	qsort(list->list, list->nnodes, sizeof(struct enode), enode_cmp);

	for(idx = 0; idx < list->nnodes;) {
		bson_oid_t docid;
		bson_t blocklist, sub;
		off_t last_end = 0;
		struct enode * cur = &list->list[idx];
		const off_t cur_start = cur->off;
		int nhashes = 0;

		bson_oid_init(&docid, NULL);
		bson_init(&doc);
		bson_append_oid(&doc, KEYEXP("_id"), &docid);
		bson_append_oid(&doc, KEYEXP("inode"), &e->oid);
		bson_append_int64(&doc, KEYEXP("start"), cur->off);
		bson_append_array_begin(&doc, KEYEXP("blocks"), &blocklist);
		towrite = 0;
		for(; idx < list->nnodes; idx++) {
			bson_t blockentry;
			cur = &list->list[idx];
			char idxbuf[10];
			const char *idxstr;

			if(last_end > 0 && cur->off != last_end)
				break;

			size_t idxlen = bson_uint32_to_string(
				nhashes++, &idxstr, idxbuf, sizeof(idxbuf));
			
			bson_append_document_begin(&blocklist, idxstr, idxlen, &blockentry);
			if(cur->empty)
				bson_append_null(&blockentry, KEYEXP("hash"));
			else
				bson_append_binary(&blockentry, KEYEXP("hash"), 0,
					cur->hash, HASH_LEN);
			bson_append_int32(&blockentry, KEYEXP("len"), cur->len);
			bson_append_document_end(&blocklist, &blockentry);

			last_end = cur->off + cur->len;
			towrite++;
		}
		
		bson_append_array_end(&doc, &blocklist);
		bson_append_int64(&doc, KEYEXP("end"), last_end);

		res = mongoc_collection_insert(coll,
			0, // flags
			&doc,
			NULL, // write concern
			&dberr);

		bson_destroy(&doc);
		
		if(!res) {
			logit(ERROR, "Error inserting extent: %s", dberr.message);
			return -EIO;
		}

		// { 
		//   _id: { $lt: ObjectId(this) }, 
		//   start: { $gte: cur_start },
		//   end: { $lte: last_end }
	    // }
		bson_init(&cond);
		bson_append_document_begin(&cond, KEYEXP("_id"), &sub);
		bson_append_oid(&cond, KEYEXP("$lt"), &docid);
		bson_append_document_end(&cond, &sub);
		bson_append_oid(&cond, KEYEXP("inode"), &e->oid);
		bson_append_document_begin(&cond, KEYEXP("start"), &sub);
		bson_append_int64(&sub, KEYEXP("$gte"), cur_start);
		bson_append_document_end(&cond, &sub);
		bson_append_document_begin(&cond, KEYEXP("end"), &sub);
		bson_append_int64(&sub, KEYEXP("$lte"), last_end);
		bson_append_document_end(&cond, &sub);

		res = mongoc_collection_delete(coll,
			0, // flags
			&cond,
			NULL, // write concern
			&dberr);
		bson_destroy(&cond);

		if(!res)
			logit(WARN, "Error cleaning up extent: %s", dberr.message);
	}

	list->nnodes = 0;
	return 0;
}

int deserialize_extent(struct inode * e, off_t off, size_t len, struct elist ** pout) {
	bson_t cond, query, orderby, sub;
	const bson_t * curdoc;
	mongoc_collection_t * coll = get_coll(COLL_EXTENTS);
	mongoc_cursor_t * curs;
	int res;
	const off_t end = off + len;
	struct elist * out = NULL;

	/* start <= end && end >= start */
	/* {
	  $query: {
	  	inode: docid
	    start: { $lte: $(off + len) },
	    end: { $gte: $(off) }
	  },
	  $orderby: {
        start: 1,
        _id: 1
	  }
	} */
	bson_init(&cond);
	bson_append_document_begin(&cond, KEYEXP("$query"), &query);
	bson_append_oid(&query, KEYEXP("inode"), &e->oid);
	bson_append_document_begin(&query, KEYEXP("start"), &sub);
	bson_append_int64(&sub, KEYEXP("$lte"), off + len);
	bson_append_document_end(&query, &sub);
	bson_append_document_begin(&query, KEYEXP("end"), &sub);
	bson_append_int64(&sub, KEYEXP("$gte"), off);
	bson_append_document_end(&query, &sub);
	bson_append_document_end(&cond, &query);
	bson_append_document_begin(&cond, KEYEXP("$orderby"), &orderby);
	bson_append_int32(&orderby, KEYEXP("start"), 1);
	bson_append_int32(&orderby, KEYEXP("_id"), 1);
	bson_append_document_end(&cond, &orderby);

	curs = mongoc_collection_find(coll,
		MONGOC_QUERY_NONE,
		0, // skip
		0, // limit
		0, // batch size
		&cond,
		NULL, // fields
		NULL); // read prefs

	while(mongoc_cursor_next(curs, &curdoc)) {
		bson_iter_t topi, i, sub;
		off_t curoff = 0;
		const char * key;

		bson_iter_init(&topi, curdoc);
		while(bson_iter_next(&topi)) {
			key = bson_iter_key(&topi);
			if(strcmp(key, "blocks") == 0)
				bson_iter_recurse(&topi, &i);
			else if(strcmp(key, "start") == 0)
				curoff = bson_iter_int64(&topi);
		}

		while(bson_iter_next(&i)) {
			bson_iter_recurse(&i, &sub);
			const uint8_t * hash;
			int curlen = 0;
			off_t curend;
			int empty = 0;

			while(bson_iter_next(&sub)) {
				key = bson_iter_key(&sub);
				bson_type_t bt = bson_iter_type(&sub);
				if(strcmp(key, "hash") == 0) {
					if(bt == BSON_TYPE_NULL)
						empty = 1;
					else {
						bson_subtype_t subtype;
						uint32_t hashsize;
						bson_iter_binary(
							&sub,
							&subtype,
							&hashsize,
							&hash);
					}
				}
				else if(strcmp(key, "len") == 0)
					curlen = bson_iter_int32(&sub);
			}

			curend = curoff + curlen;
			if(!(curoff < end && curend > off)) {
				curoff += curlen;
				continue;
			}

			if(empty)
				res = insert_empty(&out, curoff, curlen);
			else
				res = insert_hash(&out, curoff, curlen, hash);
			if(res != 0) {
				fprintf(stderr, "Error adding hash to extent tree\n");
				return res;
			}
			curoff += curlen;
		}
	}
	mongoc_cursor_destroy(curs);
	bson_destroy(&cond);
	*pout = out;

	return 0;
}

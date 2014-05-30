mongo-fuse: *.c
	cc -Wall -I/usr/local/include/libmongoc-1.0 -I/usr/local/include/libbson-1.0 -g -o mongo-fuse -losxfuse -lmongoc-1.0 -lbson-1.0 -lsnappy -lcrypto -DMONGO_HAVE_STDINT -D_FILE_OFFSET_BITS=64 -I/usr/local/include/osxfuse *.c

all: mongo-fuse

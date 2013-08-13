mongo-fuse: *.c
	cc -Wall -g -o mongo-fuse -losxfuse -lmongoc -lbson -lsnappy -DHAVE_SNAPPY -DMONGO_HAVE_STDINT -D_FILE_OFFSET_BITS=64 -I/usr/local/include/osxfuse *.c

all: mongo-fuse

# See README.txt.

PROTOC=/opt/bin/protoc
PROTO_PKGCONFIG=/opt/lib/pkgconfig


PKG_CONFIG_PATH+=$(PROTO_PKGCONFIG)
export PKG_CONFIG_PATH

.PHONY: all cpp clean

all: cpp 

cpp:   test_readblock

clean:
	rm -f test_readblock
	rm -f protoc_middleman *.pb.cc *.pb.h 

protoc_middleman: proto/hdfs.proto proto/datatransfer.proto
	protoc --cpp_out=. proto/*.proto --proto_path=proto/
	@touch protoc_middleman

test_readblock: test_readblock.cc protoc_middleman
	pkg-config --cflags protobuf  # fails if protobuf is not installed
	c++ -O2 test_readblock.cc datatransfer.pb.cc hdfs.pb.cc -o teset_readblock \
	  `pkg-config --cflags --libs protobuf` -static


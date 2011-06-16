#include <arpa/inet.h>
#include <stdio.h>
#include <netdb.h>
#include <cstdlib>
#include <cstring>
#include <endian.h>
#include <iostream>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/io/coded_stream.h>

#include "datatransfer.pb.h"
#include "hdfs.pb.h"
#include "crc32.h"

#define DATA_TRANSFER_VERSION 27
#define OP_READ_BLOCK 81

using namespace google::protobuf;
using namespace google::protobuf::io;
using namespace std;

uint32_t read_u64(CodedInputStream *cis) {
  uint64_t ret;
  cis->ReadRaw(&ret, sizeof(ret));
  return be64toh(ret);
}

uint32_t read_u32(CodedInputStream *cis) {
  uint32_t ret;
  cis->ReadRaw(&ret, sizeof(ret));
  return ntohl(ret);
}

uint16_t read_short(CodedInputStream *cis) {
  uint16_t ret;
  cis->ReadRaw(&ret, sizeof(ret));
  return ntohs(ret);
}

void read_limited(CodedInputStream *cis, size_t limit,
                  Message *msg) {
  CodedInputStream::Limit l = cis->PushLimit(limit);

  if (!msg->ParseFromCodedStream(cis)) {
    std::cerr << "could not parse" << std::endl;
    exit(1);
  }

  cis->PopLimit(l);
}

void read_vint_prefixed(CodedInputStream *cis,
                        Message *msg) {
  uint32_t size;
  cis->ReadVarint32(&size);
  read_limited(cis, size, msg);
}

void read_checksum(CodedInputStream *cis) {
  uint8_t type;
  uint32_t bpc;

  cis->ReadRaw(&type, sizeof(type));
  cis->ReadRaw(&bpc, sizeof(bpc));
  bpc = ntohl(bpc);

  cout << "checksum type: " << (int)type << " bpc: " << bpc << endl;
}

void read_packet_header(CodedInputStream *cis, PacketHeaderProto *hdr) {
  uint32_t packetlen = read_u32(cis);
  uint16_t protolen = read_short(cis);

  read_limited(cis, protolen, hdr);
}

void read_packet(CodedInputStream *cis, const PacketHeaderProto &hdr) {
  int chunks = 1 + (hdr.datalen() - 1) / 512;
  int checksumSize = chunks * 4;

  size_t size = checksumSize + hdr.datalen();
  uint8_t *buf = (uint8_t *)malloc(size);
  if (!cis->ReadRaw(buf, size)) {
    cerr << "Could not read " << size << " butes" << endl;
    free(buf);
    exit(1);
  }

  uint32_t *checksums = reinterpret_cast<uint32_t *>(buf);
  uint8_t *datastart = buf + checksumSize;

  // verify checksums
  uint8_t *chunk_start = datastart;
  int rem = hdr.datalen();
  for (int i = 0; i < chunks; i++) {
    uint32_t cksum = crc_init();
    size_t len = min(rem, 512);
    cksum = crc_update(cksum, chunk_start, len);
    cksum = htonl(crc_val(cksum));
    if (cksum != *checksums) {
      cerr << "Checksums did not match at " << (chunk_start - datastart)
        << ": expected=" << (*checksums) << " got: " << cksum
        << endl;
      exit(1);
    }
    checksums++;

    chunk_start += len;
    rem -= len;
  }
  assert(rem == 0);

  write(1, datastart, hdr.datalen());

  free(buf);
}

void setup_read_block(OpReadBlockProto *op) {
    ClientOperationHeaderProto *hdr = op->mutable_header();
    hdr->set_clientname("test");
    BaseHeaderProto *base = hdr->mutable_baseheader();
    ExtendedBlockProto *block = base->mutable_block();
    block->set_poolid("BP-1890200317-172.29.5.34-1308203250354");
    block->set_blockid(-6240183935164208125L);
    block->set_numbytes(1000L);
    block->set_generationstamp(1001L);
    BlockTokenIdentifierProto *token = base->mutable_token();
    token->set_identifier("");
    token->set_password("");
    token->set_kind("");
    token->set_service("");

    op->set_offset(0);
    op->set_len(128*1024*1024);
}

void send_op(int sock, uint8_t opcode, const Message &op) {
  std::string buf;
  buf.reserve(5 + op.ByteSize());
  StringOutputStream sos(&buf);
  CodedOutputStream cos(&sos);
  uint16_t version = htons(DATA_TRANSFER_VERSION);
  cos.WriteRaw(&version, sizeof(version));
  cos.WriteRaw(&opcode, sizeof(opcode));
  cos.WriteVarint32(op.GetCachedSize());
  op.SerializeWithCachedSizes(&cos);
  // op.PrintDebugString();

  int rem = buf.length();
  uint8_t *cbuf = (uint8_t *)buf.c_str();
  while (rem > 0) {
    ssize_t n = write(sock, cbuf, rem);
    if (n < 0) {
      perror("Could not write");
      exit(1);
    }
    rem -= n;
    cbuf += n;
  }
}

int connect_dn(char *dn, int port) {
  struct hostent *server = gethostbyname(dn);
  if (server == NULL) {
    cerr << "bad host" << endl;
    return -1;
  }
  struct sockaddr_in serv_addr;
 
  bzero((char *) &serv_addr, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  bcopy((char *)server->h_addr,
       (char *)&serv_addr.sin_addr.s_addr,
       server->h_length);
  serv_addr.sin_port = htons(port);

  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0) {
    perror("Could not create socket");
    return -1;
  }

  if (connect(sockfd,(struct sockaddr *) &serv_addr,sizeof(serv_addr)) < 0) {
    perror("Could not connect");
    close(sockfd);
    return -1;
  }
  return sockfd;
}

int main(int argc, char* argv[])
{
  try
  {
    if (argc != 3)
    {
      std::cerr << "Usage: blocking_tcp_echo_client <host> <port>\n";
      return 1;
    }

    int port = atoi(argv[2]);
    int sockfd = connect_dn(argv[1], port);
    if (sockfd < 0) {
      exit(1);
    }

    OpReadBlockProto op;
    setup_read_block(&op);
    send_op(sockfd, OP_READ_BLOCK, op);


      FileInputStream fis(sockfd);
      CodedInputStream cis(&fis);
      cis.SetTotalBytesLimit(INT_MAX, INT_MAX);

      BlockOpResponseProto resp;
      read_vint_prefixed(&cis, &resp);
      read_checksum(&cis);
      // resp.PrintDebugString();

      long first_chunk_offset = read_u64(&cis);
      cout << "first chunk offset: " << first_chunk_offset << endl;

    PacketHeaderProto hdr;
    
    while (!hdr.lastpacketinblock()) {
      read_packet_header(&cis, &hdr);
      // hdr.PrintDebugString();
      read_packet(&cis, hdr);
    }
  }
  catch (std::exception& e)
  {
    std::cerr << "Exception: " << e.what() << "\n";
  }

  return 0;
}

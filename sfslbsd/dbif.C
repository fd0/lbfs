
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "chunking.h"
#include "lbfs.h"

#define DEBUG_ONLY 0

// algorithm for "adding" a file:
//
//  - from new list of chunks (new_chunks vector), assemble a vector
//    (tmp_chunks vector) of ptrs to reusable chunks and null ptrs 
//    (unknown chunks). at the same time, write reusable data.
//  - retrieve unknown chunks from client.
//  - assemble temporary file.
//  - remove old list of chunks from db.
//  - add new list of chunks to db.
//  - move temporary into file system.


int 
main(int argc, char *argv[])
{
  if (argc != 3) {
    printf("usage: %s newfile oldfile\n", argv[0]);
    return -1;
  }

  const char *oldfile = argv[2];
  const char *newfile = argv[1];
  ssize_t unknown_bytes = 0;

  vec<lbfs_chunk *> new_chunks;
  if (chunk_file(newfile, CHUNK_SIZES(0), &new_chunks) < 0) {
    printf("cannot open %s for chunking\n", newfile);
    return -1;
  }

#if DEBUG_ONLY
  for (unsigned i = 0; i < new_chunks.size(); i++) {
    lbfs_chunk *c = new_chunks[i];
    printf("%s %d: %ld, 0x%016qx\n", c->path, i, c->pos, c->fingerprint);
  }
  return 0;
#endif

  vec<lbfs_chunk *> reusable_chunks;
  lbfs_search_reusable_chunks(new_chunks, reusable_chunks);
 
  // SERVER: send request to client.

  char tmpfile[] = "/tmp/sfslbsdXXXXXXXX";
  int tmpfd = mkstemp (tmpfile);
  lbfs_load_reusable_chunks(tmpfd, new_chunks, reusable_chunks);

  bool move_tmp = true;
  int nfd = open(newfile, O_RDONLY);
  for (unsigned i = 0; i < new_chunks.size(); i++) {
    if (!reusable_chunks[i]) {
      char *buf = new char[new_chunks[i]->size];
      if (lseek(nfd, new_chunks[i]->pos, SEEK_SET) != new_chunks[i]->pos ||
          read (nfd, buf, new_chunks[i]->size) != new_chunks[i]->size) {
	printf("warning: %s corrupted, can't add\n", tmpfile);
	move_tmp = false;
      }
      lseek(tmpfd, new_chunks[i]->pos, SEEK_SET);
      write(tmpfd, buf, new_chunks[i]->size);
      unknown_bytes += new_chunks[i]->size;
      delete buf;
    }
  }
  close(nfd);

  int ret = -1;
  if (move_tmp) 
    ret = lbfs_add_file(oldfile, tmpfile);

  for (unsigned i = 0; i < new_chunks.size(); i++) { 
    if (reusable_chunks[i])
      delete reusable_chunks[i];
    delete new_chunks[i];
  }

  printf("%d bytes unknown\n", unknown_bytes);
  return ret;
}


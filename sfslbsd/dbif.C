
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "chunking.h"
#include "lbfs.h"

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
  if (chunk_file(newfile, &new_chunks) < 0) {
    printf("cannot open %s for chunking\n", newfile);
    return -1;
  }

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

  lbfs_add_file(oldfile, new_chunks);

  for (unsigned i = 0; i < new_chunks.size(); i++) { 
    if (reusable_chunks[i])
      delete reusable_chunks[i];
    delete new_chunks[i];
  }

  if (move_tmp && rename(tmpfile, oldfile)) {
    printf("cannot move %s to %s\n", tmpfile, oldfile);
    return -1;
  }

  printf("%d bytes unknown\n", unknown_bytes);
  return 0;
}


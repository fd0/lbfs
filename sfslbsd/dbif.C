
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "rabinpoly.h"
#include "chunking.h"
#include "lbfsdb.h"

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
  ssize_t reused_bytes = 0;
  ssize_t unknown_bytes = 0;

  vec<lbfs_chunk *> new_chunks;
  if (chunk_file(newfile, &new_chunks) < 0) {
    printf("cannot open %s for chunking\n", newfile);
    return -1;
  }

  lbfs_db db(FMAP_DB);
  db.open();
  
  char tmpfile[] = "/tmp/sfslbsdXXXXXXXX";
  int tmpfd = mkstemp (tmpfile);
  bool move_tmp = true;

  // compute reusable chunks and unknown chunks, for reusable chunks, grab
  // data from existing files
  vec<lbfs_chunk *> tmp_chunks;
  tmp_chunks.setsize(new_chunks.size());
  for (unsigned i = 0; i < new_chunks.size(); i++) {
    lbfs_chunk c;
    if (!db.search(new_chunks[i]->fingerprint, &c)) {
      tmp_chunks[i] = new_chunks[i];
      assert(c.size == new_chunks[i]->size);
      int fd = open(c.path, O_RDONLY);
      if (fd < 0 || lseek(fd, c.pos, SEEK_SET) != c.pos)
        tmp_chunks[i] = 0;
      else {
	char *buf = new char[c.size];
        if (read(fd, buf, c.size) != c.size)
          tmp_chunks[i] = 0;
	else {
	  lseek(tmpfd, new_chunks[i]->pos, SEEK_SET);
          write(tmpfd, buf, c.size);
	  reused_bytes += c.size;
	}
        delete buf;
      }
      close(fd);
    }
    else
      tmp_chunks[i] = 0L;
  }

  // retrieve unknown bytes
  int nfd = open(newfile, O_RDONLY);
  for (unsigned i = 0; i < new_chunks.size(); i++) {
    if (!tmp_chunks[i]) {
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

  // make database consistent
  vec<lbfs_chunk *> old_chunks;
  chunk_file(oldfile, &old_chunks);
  for (unsigned i = 0; i < old_chunks.size(); i++) { 
    if (db.remove_chunk(old_chunks[i]->fingerprint, old_chunks[i]) != 0) 
      printf("db inconsistent: old chunk for %s does not exist!\n", oldfile);
    delete old_chunks[i];
  }
  for (unsigned i = 0; i < new_chunks.size(); i++) { 
    lbfs_chunk c(oldfile, new_chunks[i]->pos, 
	         new_chunks[i]->size, new_chunks[i]->fingerprint);
    int ret = db.add_chunk(new_chunks[i]->fingerprint, &c);
    assert(ret == 0);
    delete new_chunks[i];
  }

  // copy the tmp file we created if we are successful in adding the file
  if (move_tmp && rename(tmpfile, oldfile)) {
    printf("cannot move %s to %s\n", tmpfile, oldfile);
    return -1;
  }

  printf("%d / %d bytes reused\n", reused_bytes, reused_bytes+unknown_bytes);
  return 0;
}



#include <sys/types.h>
#include <stdio.h>
#include <db.h>

#include "chunking.h"
#include "lbfsdb.h"

// algorithm for "adding" a file:
//
//  - get new list of chunks
//  - assemble list of reusable chunks from database
//  - assemble list of unknown chunks
//  - retrieve unknown chunks from client
//  - remove old list of chunks from db
//  - add new list of chunks to db
//
// XXX - what's the correct order for the last 3 actions?

int 
main(int argc, char *argv[])
{
  if (argc != 3) {
    printf("usage: %s newfile oldfile\n", argv[0]);
    return -1;
  }

  const char *oldfile = argv[2];
  const char *newfile = argv[1];

  vec<lbfs_chunk *> new_chunks;
  if (chunk_file(newfile, &new_chunks) < 0) {
    printf("cannot open %s for chunking, chop pork liver instead\n", newfile);
    return -1;
  }

  vec<lbfs_chunk *> old_chunks;
  vec<lbfs_chunk *> reuse_chunks;
  vec<lbfs_chunk *> unknown_chunks;

  lbfs_db db(FMAP_DB);
  db.open();

  printf("%s: total %d new chunks\n", newfile, new_chunks.size());
  for (unsigned i = 0; i < new_chunks.size(); i++) {
    lbfs_chunk *c;
    if (!db.search(new_chunks[i]->fingerprint, &c)) {
      printf("db has chunk %d: %s, %ld, %d, 0x%016qx\n", 
             i, c->path, c->pos, c->size, c->fingerprint);
      reuse_chunks.push_back(c);
    } 
    else {
      unknown_chunks.push_back(new_chunks[i]);
      printf("chunk %d unknown\n", i);
    }
  }
  
  chunk_file(oldfile, &old_chunks);
  for (unsigned i = 0; i < old_chunks.size(); i++) { 
    if (db.remove_chunk(old_chunks[i]->fingerprint, old_chunks[i]) != 0) 
      printf("db inconsistent: old chunk for %s does not exist!\n", oldfile);
    delete old_chunks[i];
  }
  
  for (unsigned i = 0; i < new_chunks.size(); i++) { 
    lbfs_chunk c(oldfile, new_chunks[i]->pos, 
	         new_chunks[i]->size, new_chunks[i]->fingerprint);
    if (db.add_chunk(new_chunks[i]->fingerprint, &c) != 0) 
      printf("db inconsistent: cannot add chunk for %s!\n", oldfile);
    delete new_chunks[i];
  }

  // move over the file: in real server, we would ask
  // for unknown chunks, get them, and assemble the new
  // file from other chunks
  if (rename(newfile, oldfile)) {
    printf("cannot move %s to %s\n", newfile, oldfile);
    return -1;
  }

  return 0;
}


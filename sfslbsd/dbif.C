
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "chunking.h"
#include "lbfsdb.h"
#include "lbfs.h"

int 
main(int argc, char *argv[])
{
  if (argc != 3) {
    printf("usage: %s newfile oldfile\n", argv[0]);
    return -1;
  }

  const char *newfile = argv[1];

  vec<lbfs_chunk *> new_chunks;
  if (chunk_file(newfile, CHUNK_SIZES(0), &new_chunks) < 0) {
    printf("cannot open %s for chunking\n", newfile);
    return -1;
  }

  vec<lbfs_chunk_loc *> reusable_chunks;
  lbfs_search_reusable_chunks(new_chunks, reusable_chunks);
 
  for (unsigned i=0; i<new_chunks.size(); i++) {
    if (reusable_chunks[i])
      printf("reuse %ld %d\n",
	     reusable_chunks[i]->pos, reusable_chunks[i]->size);
    delete new_chunks[i];
    delete reusable_chunks[i];
  }

  return 0;
}



#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "chunking.h"
#include "lbfsdb.h"

int
lbfs_search_reusable_chunks(vec<lbfs_chunk *> &new_chunks,
                            vec<lbfs_chunk_loc *> &reusable_chunks)
{
  if (new_chunks.size() == 0)
    return -1;

  lbfs_db db;
  db.open();

  reusable_chunks.setsize(new_chunks.size());
  for (unsigned i = 0; i < new_chunks.size(); i++) {
    lbfs_db::chunk_iterator *iter = 0;
    if (db.get_chunk_iterator(new_chunks[i]->fingerprint, &iter) == 0) {
      if (iter) {
        struct lbfs_chunk_loc *c = new struct lbfs_chunk_loc;
        if (iter->get(c) == 0)
          reusable_chunks[i] = c;
        else {
	  reusable_chunks[i] = 0L;
	  delete c;
	}
        delete iter;
      }
    }
    else 
      reusable_chunks[i] = 0L;
  }
  return 0;
}


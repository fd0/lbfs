
#include <sys/types.h>
#include <stdio.h>
#include <db.h>

#include "rabinpoly.h"
#include "chunking.h"
#include "lbfsdb.h"

int 
main(int argc, char *argv[]) 
{
  lbfs_db db(FMAP_DB);
  db.open();

  vec<u_int64_t> fv;
  vec<lbfs_chunk *> cv;

  chunk_file(argv[1], &fv, &cv);
  for(unsigned i=0; i<fv.size(); i++) {
    if (db.add_chunk(fv[i], cv[i]) != 0) 
      printf("add returned non-zero\n");
    delete cv[i];
  }
}


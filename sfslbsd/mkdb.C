
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

  vec<lbfs_chunk>

  add_file(argv[1], &db);
      if (db->add_chunk(f, &c) != 0)
        printf("add returned non-zero\n");
}


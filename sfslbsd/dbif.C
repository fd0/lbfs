
#include <sys/types.h>
#include <stdio.h>
#include <db.h>

#include "lbfsdb.h"


int 
main(int argc, char *argv[])
{ 
  lbfs_db db(FMAP_DB);
  db.open();

  int ret;
  lbfs_chunk c;
  
  ret = db.search(0x72ecbd3004fc0078LL, &c);
  if (ret < 0) printf("ret %d\n", ret);
  else printf("%s, offset %ld, size %d\n", c.path, c.pos, c.size);

  ret = db.search(0x39aa588cc89a7078LL, &c);
  if (ret < 0) printf("ret %d\n", ret);
  else printf("%s, offset %ld, size %d\n", c.path, c.pos, c.size);
  
  ret = db.search(0x54b6fa377b896078LL, &c);
  if (ret < 0) printf("ret %d\n", ret);
  else printf("%s, offset %ld, size %d\n", c.path, c.pos, c.size);
      
  lbfs_chunk c2("dbif2.o", 77102, 1551);
  ret = db.remove_chunk(0x54b6fa377b896078LL, &c2); 
  if (ret < 0) printf("ret cdel %d\n", ret);

  
  ret = db.search(0x54b6fa377b896078LL, &c);
  if (ret < 0) printf("ret get %d\n", ret);
  else printf("%s, offset %ld, size %d\n", c.path, c.pos, c.size);

}


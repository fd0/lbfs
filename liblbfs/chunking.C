
#include <sys/types.h>
#include <stdio.h>
#include <db.h>

#include "rabinpoly.h"
#include "lbfsdb.h"

static bool
mapfile (const u_char **bufp, size_t *sizep, const char *path)
{
  int fd = open (path, O_RDONLY);
  if (fd < 0)
    return false;
  struct stat sb;
  if (fstat (fd, &sb) < 0) {
    close (fd);
    return false;
  }
  if (!S_ISREG (sb.st_mode)) {
    close (fd);
    errno = EISDIR;		// XXX /dev/null
    return false;
  }
  if (!(*sizep = sb.st_size))
    *bufp = NULL;
  else {
    void *vbp = mmap (NULL, *sizep, PROT_READ, MAP_FILE|MAP_SHARED, fd, 0);
    if (vbp == reinterpret_cast<void *> (MAP_FAILED)) {
      close (fd);
      return false;
    }
    *bufp = static_cast<u_char *> (vbp);
  }
  close (fd);
  return true;
}


int
add_file(const char *path, lbfs_db *db)
{
  const u_char *fp;
  size_t fl;
  if (!mapfile (&fp, &fl, path))
    return -1;

  u_int64_t poly = FINGERPRINT_PT;
  window w (poly);
  u_int64_t f = 0;
  w.reset();
 
  size_t last_i = 0;
  size_t i = 0;
  for (i = 0; i < fl;) {
    f = w.slide8 (fp[i]);
    if ((f % BREAKMARK_K) == BREAKMARK_X) {
      printf ("C%d 0x%016qx\n", i, f);
      lbfs_chunk c(path, last_i, i-last_i);
      if (db->add_chunk(f, &c) != 0)
        printf("add returned non-zero\n");
      w.reset();
      last_i = i;
    }
    i++;
  }
  printf ("C%d 0x%016qx\n", i, f);
  lbfs_chunk c(path, last_i, i-last_i);
  if (db->add_chunk(f, &c) != 0) 
    printf("add returned non-zero\n");
  return 0;
}

int 
main(int argc, char *argv[]) 
{
  lbfs_db db(FMAP_DB);
  db.open();
  add_file(argv[1], &db);
}


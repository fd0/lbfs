
#include <sys/types.h>
#include <stdio.h>
#include <db.h>

#include "lbfsdb.h"
#include "rabinpoly.h"

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
chunk_file(const char *path, vec<lbfs_chunk *> *cvp)
{
  const u_char *fp;
  size_t fl;
  if (!mapfile (&fp, &fl, path))
    return -1;

  u_int64_t poly = FINGERPRINT_PT;
  u_int64_t f_break = 0;
  u_int64_t f_chunk = 0;
  window w (poly);
  w.reset();
 
  size_t last_i = 0;
  size_t i = 0;
  for (i = 0; i < fl; i++) {
    f_break = w.slide8 (fp[i]);
    f_chunk = w.append8 (f_chunk, fp[i]);
    if ((f_break % BREAKMARK_K) == BREAKMARK_X) {
      lbfs_chunk *c = new lbfs_chunk(path, last_i, i-last_i, f_chunk);
      cvp->push_back(c);
      w.reset();
      f_chunk = 0;
      last_i = i;
    }
  }
  lbfs_chunk *c = new lbfs_chunk(path, last_i, i-last_i, f_chunk); 
  cvp->push_back(c);

  munmap((void*)fp, fl);
  return 0;
}


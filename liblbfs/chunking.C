
#include <sys/types.h>
#include <stdio.h>
#include <db.h>

#include "lbfsdb.h"
#include "rabinpoly.h"

int
mapfile (const u_char **bufp, size_t *sizep, const char *path)
{
  int fd = open (path, O_RDONLY);
  if (fd < 0)
    return -1;
  struct stat sb;
  if (fstat (fd, &sb) < 0) {
    close (fd);
    return -1;
  }
  if (!S_ISREG (sb.st_mode)) {
    close (fd);
    errno = EISDIR;		// XXX /dev/null
    return -1;
  }
  if (!(*sizep = sb.st_size))
    *bufp = NULL;
  else {
    void *vbp = mmap (NULL, *sizep, PROT_READ, MAP_FILE|MAP_SHARED, fd, 0);
    if (vbp == reinterpret_cast<void *> (MAP_FAILED)) {
      close (fd);
      return -1;
    }
    *bufp = static_cast<u_char *> (vbp);
  }
  close (fd);
  return 0;
}

int
chunk_data(const char *path, unsigned csize, 
           const unsigned char *data, size_t size, 
	   vec<lbfs_chunk *> *cvp)
{
  u_int64_t poly = FINGERPRINT_PT;
  u_int64_t f_break = 0;
  u_int64_t f_chunk = 0;
  window w (poly);
  w.reset();
 
  size_t last_i = 0;
  size_t i = 0;
  for (i = 0; i < size; i++) {
    f_break = w.slide8 (data[i]);
    if ((f_break % csize) == BREAKMARK_VALUE) {
      lbfs_chunk *c = new lbfs_chunk(path, last_i, i-last_i, f_chunk);
      cvp->push_back(c);
      w.reset();
      f_chunk = 0;
      last_i = i;
    }
    f_chunk = w.append8 (f_chunk, data[i]);
  }
  lbfs_chunk *c = new lbfs_chunk(path, last_i, i-last_i, f_chunk); 
  cvp->push_back(c);

  return 0;
}

int
chunk_file(const char *path, unsigned csize, vec<lbfs_chunk *> *cvp)
{
  const u_char *fp;
  size_t fl;
  if (mapfile (&fp, &fl, path) != 0)
    return -1;
  int ret = chunk_data(path, csize, fp, fl, cvp);
  munmap(static_cast<void*>(const_cast<u_char*>(fp)), fl);
  return ret;
}


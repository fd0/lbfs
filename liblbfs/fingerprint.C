
#include <sys/types.h>
#include <stdio.h>
#include <db.h>

#include "lbfsdb.h"
#include "rabinpoly.h"
#include "fingerprint.h"

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

u_int64_t 
fingerprint(const unsigned char *data, size_t count)
{
  u_int64_t poly = FINGERPRINT_PT;
  window w (poly);
  w.reset();
  u_int64_t fp = 0;
  for (size_t i = 0; i < count; i++)
    fp = w.append8 (fp, data[i]);
  return fp;
}

Chunker::Chunker(unsigned s, vec<lbfs_chunk *> *cvp)
  : _w(FINGERPRINT_PT), _chunk_size(s), _cvp(cvp)
{
  _last_pos = 0;
  _cur_pos = 0;
  _fp = 0;
  _w.reset();
}

Chunker::~Chunker()
{
}

void
Chunker::stop()
{
  lbfs_chunk *c = new lbfs_chunk(_last_pos, _cur_pos-_last_pos, _fp);
  _cvp->push_back(c);
}

void
Chunker::chunk(const unsigned char *data, size_t size)
{
  u_int64_t f_break = 0;
  for (size_t i=0; i<size; i++, _cur_pos++) {
    f_break = _w.slide8 (data[i]);
    if ((f_break % _chunk_size) == BREAKMARK_VALUE) {
      lbfs_chunk *c = new lbfs_chunk(_last_pos, _cur_pos-_last_pos, _fp);
      _cvp->push_back(c);
      _w.reset();
      _fp = 0;
      _last_pos = _cur_pos;
    }
    _fp = _w.append8 (_fp, data[i]);
  }
}

int chunk_file(unsigned chunk_size, vec<lbfs_chunk *> *cvp,
               const char *path)
{
  const u_char *fp;
  size_t fl;
  if (mapfile (&fp, &fl, path) != 0) return -1;
  Chunker chunker(chunk_size, cvp);
  chunker.chunk(fp, fl);
  chunker.stop();
  munmap(static_cast<void*>(const_cast<u_char*>(fp)), fl);
  return 0;
}

int chunk_data(unsigned chunk_size, vec<lbfs_chunk *> *cvp,
               const unsigned char *data, size_t size)
{
  Chunker chunker(chunk_size, cvp);
  chunker.chunk(data, size);
  chunker.stop();
  return 0;
}


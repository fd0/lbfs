
#include <sys/types.h>
#include <stdio.h>
#include <db.h>

#include "sha1.h"
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

Chunker::Chunker(unsigned s, vec<lbfs_chunk *> *v, bool hash)
  : _w(FINGERPRINT_PT), _chunk_size(s), _hash(hash), cvp(v)
{
  _last_pos = 0;
  _cur_pos = 0;
  _fp = 0;
  _w.reset();
  _hbuf_cursor = 0;
  if (_hash) {
    _hbuf = new unsigned char[32768];
    _hbuf_size = 32768;
  }
  else {
    _hbuf = 0;
    _hbuf_size = 0;
  }
}

Chunker::~Chunker()
{
  if (_hash) {
    for (unsigned i = 0; i < hv.size(); i++)
      delete hv[i];
    if (_hbuf_size > 0) {
      delete[] _hbuf;
      _hbuf_size = 0;
      _hbuf_cursor = 0;
    }
  }
}

void
Chunker::handle_hash(const unsigned char *data, size_t size)
{
  if (size > 0) {
    while (_hbuf_cursor+size > _hbuf_size) {
      unsigned char *nb = new unsigned char[_hbuf_size*2];
      memmove(nb, _hbuf, _hbuf_cursor);
      _hbuf_size *= 2;
      delete[] _hbuf;
      _hbuf = nb;
    }
    memmove(_hbuf+_hbuf_cursor, data, size);
    _hbuf_cursor += size;
  }
}

void
Chunker::stop()
{
  lbfs_chunk *c = new lbfs_chunk(_last_pos, _cur_pos-_last_pos, _fp);
  if (_hash) { 
    sfs_hash *h = new sfs_hash; 
    sha1_hash(h->base(), _hbuf, _hbuf_cursor); 
    _hbuf_cursor = 0; 
    hv.push_back(h); 
  }
  cvp->push_back(c);
}

void
Chunker::chunk(const unsigned char *data, size_t size)
{
  u_int64_t f_break = 0;
  size_t pos0 = _cur_pos;
  for (size_t i=0; i<size; i++, _cur_pos++) {
    f_break = _w.slide8 (data[i]);
    if ((f_break % _chunk_size) == BREAKMARK_VALUE) {
      lbfs_chunk *c = new lbfs_chunk(_last_pos, _cur_pos-_last_pos, _fp);
      _w.reset();
      _fp = 0;

      if (_hash) {
	off_t start = _last_pos>pos0 ? _last_pos-pos0 : 0;
	handle_hash(data+start, i-start+1);
        sfs_hash *h = new sfs_hash;
	sha1_hash(h->base(), _hbuf, _hbuf_cursor);
	_hbuf_cursor = 0;
	hv.push_back(h);
      }
      cvp->push_back(c);

      _last_pos = _cur_pos;
    }
    _fp = _w.append8 (_fp, data[i]);
  }
      
  if (_hash) { 
    off_t start = _last_pos>pos0 ? _last_pos-pos0 : 0;
    handle_hash(data+start, size-start);
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


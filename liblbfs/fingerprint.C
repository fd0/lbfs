
#include <sys/types.h>
#include <stdio.h>

#include "sha1.h"
#include "rabinpoly.h"
#include "fingerprint.h"

const char *CLI_FPDB = getenv("LBFS_CLIDB") ? getenv("LBFS_CLIDB") 
                                            : "/var/tmp/fp-cli.db";
const char *SRV_FPDB = getenv("LBFS_SRVDB") ? getenv("LBFS_SRVDB") 
                                            : "/var/tmp/fp-srv.db";

unsigned Chunker::min_size_suppress = 0;
unsigned Chunker::max_size_suppress = 0;

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

Chunker::Chunker()
  : _w(FINGERPRINT_PT)
{
  _last_pos = 0;
  _cur_pos = 0;
  _w.reset();
  _hbuf_cursor = 0;
  _hbuf = New unsigned char[32768];
  _hbuf_size = 32768;
  _pfb = 0;
}

Chunker::~Chunker()
{
  if (_hbuf_size > 0) {
    delete[] _hbuf;
    _hbuf_size = 0;
    _hbuf_cursor = 0;
  }
  for (unsigned i = 0; i < _cv.size(); i++)
    delete _cv[i];
  prefetched_buffer *b = _pfb;
  prefetched_buffer *n;
  while (b) {
    n = b->next;
    delete b->data;
    delete b;
    b = n;
  }
  _pfb = 0;
}

void
Chunker::handle_hash(const unsigned char *data, size_t size)
{
  if (size > 0) {
    while (_hbuf_cursor+size > _hbuf_size) {
      unsigned char *nb = New unsigned char[_hbuf_size*2];
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
  if (_cur_pos != _last_pos) {
    chunk *c = New chunk(_last_pos, _cur_pos-_last_pos, _hbuf);
    assert(_cur_pos-_last_pos == _hbuf_cursor);
    _hbuf_cursor = 0; 
    _cv.push_back(c);
  }
}

void
Chunker::chunk_data(const unsigned char *data, uint64 off, size_t size)
{
  if (off != _cur_pos) {
    prefetched_buffer *b = New prefetched_buffer (data, off, size);
    b->next = _pfb;
    _pfb = b;
  }
  else {
    chunk_data (data, size);
    prefetched_buffer *b = _pfb;
    prefetched_buffer *p = 0;
    while (b) {
      if (b->off == _cur_pos) {
	if (!p) // first one
          _pfb = b->next;
	else
	  p->next = b->next;
	chunk_data (b->data, b->off, b->size);
	delete b->data;
	delete b;
	return;
      }
      p = b;
      b = b->next;
    }
  }
}

void
Chunker::chunk_data(const unsigned char *data, size_t size)
{
  u_int64_t f_break = 0;
  size_t start_i = 0;
  for (size_t i=0; i<size; i++, _cur_pos++) {
    f_break = _w.slide8 (data[i]);
    size_t cs = _cur_pos - _last_pos;
    if ((f_break % chunk_size) == BREAKMARK_VALUE && cs < MIN_CHUNK_SIZE) 
      min_size_suppress++;
    else if (cs == MAX_CHUNK_SIZE)
      max_size_suppress++;
    if (((f_break % chunk_size) == BREAKMARK_VALUE && cs >= MIN_CHUNK_SIZE) 
	|| cs >= MAX_CHUNK_SIZE) {
      _w.reset();
      if (i-start_i > 0) 
	handle_hash(data+start_i, i-start_i);
      chunk *c = New chunk(_last_pos, cs, _hbuf);
      if (_hbuf_cursor != cs)
	warn << "_hbuf_cursor = " << _hbuf_cursor << ", cs = " << cs << "\n";
      assert(_hbuf_cursor == cs);
      _hbuf_cursor = 0;
      _cv.push_back(c);
      _last_pos = _cur_pos;
      start_i = i;
    }
  }
  handle_hash(data+start_i, size-start_i);
}

int chunk_file(vec<chunk *>& cvp, const char *path)
{
  int fd = open(path, O_RDONLY);
  if (fd < 0) return -1;
  unsigned char buf[4096];
  int count;
  Chunker chunker;
  while ((count = read(fd, buf, 4096))>0)
    chunker.chunk_data(buf, count);
  chunker.stop();
  close(fd);
  chunker.copy_chunk_vector(cvp);
  return 0;
}

int chunk_data(vec<chunk *>& cvp, const unsigned char *data, size_t size)
{
  Chunker chunker;
  chunker.chunk_data(data, size);
  chunker.stop();
  chunker.copy_chunk_vector(cvp);
  return 0;
}


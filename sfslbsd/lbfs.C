
#include "sfsrwsd.h"
#include "lbfsdb.h"

struct read3obj {
  typedef callback<void, unsigned char *, int, str>::ref cb_t;
  cb_t cb;
  ref<aclnt> c;

  nfs_fh3 fh;
  off_t pos; 
  uint32 count;
  uint32 want;
  unsigned char *buf;
    
  read3res res;

  void gotdata3 (clnt_stat stat) {
    
    if (stat || res.status) {
      (*cb) (buf, count, stat2str (res.status, stat));
      delete this;
    }
    else {
      memmove(buf+count, res.resok->data.base(), res.resok->count);
      count += res.resok->count;
      if (want > res.resok->count) {
	want -= res.resok->count;
	pos += res.resok->count;
      }
      else
	want = 0;

      if (want == 0 || res.resok->eof) {
        (*cb) (buf, count, NULL);
        delete this;
      }
      else
	do_read();
    }
  }
  
  void do_read() {
    read3args arg;
    arg.file = fh;
    arg.offset = pos;
    arg.count = want;
    c->call (NFSPROC3_READ, &arg, &res,
	     wrap (this, &read3obj::gotdata3), auth_root);
  }
  
  read3obj (ref<aclnt> c, nfs_fh3 &f, off_t p, uint32 cnt, cb_t cb) 
    : cb (cb), c (c), fh(f)
  {
    count = 0;
    pos = p;
    want = cnt;
    buf = new unsigned char [cnt];
    do_read();
  }
};

void
readfh3 (ref<aclnt> c, nfs_fh3 &fh, read3obj::cb_t cb, off_t pos, uint32 count)
{
  vNew read3obj (c, fh, pos, count, cb);
}




#include "sfsrwsd.h"
#include "lbfsdb.h"

void lookupfh3 (ref<aclnt> c, const nfs_fh3 &start, str path,
		callback<void, const nfs_fh3 *, const FATTR3 *, str>::ref cb);

struct read3obj {
  typedef callback<void, unsigned char *, size_t, str>::ref cb_t;
  cb_t cb;
  ref<aclnt> c;

  const nfs_fh3 fh;
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
  
  read3obj (ref<aclnt> c, const nfs_fh3 &f, off_t p, uint32 cnt, cb_t cb)
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
readfh3 (ref<aclnt> c, const nfs_fh3 &fh, read3obj::cb_t cb, 
         off_t pos, uint32 count)
{
  vNew read3obj (c, fh, pos, count, cb);
}


struct mkdir3obj {
  typedef callback<void, const nfs_fh3 *, str>::ref cb_t;
  cb_t cb;
  ref<aclnt> c;

  diropres3 res;
  nfs_fh3 dir;
  filename3 fname;
  sattr3 attr;
  int done_mkdir;

  void gotfh3 (const nfs_fh3 *fh, const FATTR3 *, str err) {
    if (!err) {
      nfs_fh3 cb_fh;
      cb_fh = *fh;
      (*cb) (&cb_fh, NULL);
      delete this;
    }
    else if (!done_mkdir)
      do_mkdir();
    else {
      (*cb) (NULL, err);
      delete this;
    }
  }

  void gotdir (clnt_stat stat) {
    if (stat || res.status) {
      (*cb) (NULL, stat2str (res.status, stat));
      delete this;
    }
    else {
      done_mkdir = 1;
      do_lookup();
    }
  }
 
  void do_mkdir()
  {
    mkdir3args arg;
    arg.where.dir = dir;
    arg.where.name = fname;
    arg.attributes = attr;
    c->call (NFSPROC3_MKDIR, &arg, &res,
	     wrap (this, &mkdir3obj::gotdir), auth_root);
  }
  
  void do_lookup()
  {
    lookupfh3(c, dir, fname, wrap(this, &mkdir3obj::gotfh3));
  }

  mkdir3obj (ref<aclnt> c, const nfs_fh3 &d, const str &name, 
             sattr3 &a, cb_t cb)
    : cb (cb), c (c), dir(d), fname(name), attr(a)
  {
    done_mkdir = 0;
    do_lookup();
  }
};


// creates dir if it does not exist, otherwise return fh in cb
void
mkdir3 (ref<aclnt> c, const nfs_fh3 &dir, const str &name, sattr3 attr,
        mkdir3obj::cb_t cb)
{
  vNew mkdir3obj (c, dir, name, attr, cb);
}



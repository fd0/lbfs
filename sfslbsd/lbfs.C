
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


struct copy3obj {
  typedef callback<void, commit3res *, str>::ref cb_t;
  cb_t cb;
  ref<aclnt> c;

  const nfs_fh3 &src;
  const nfs_fh3 &dst;

  getattr3res ares;
  commit3res cres;

  int outstanding_reads;
  int outstanding_writes;
  int error;
  u_int64_t size;
  u_int64_t next_read;
  
  static const int READ_BLOCK_SZ = 8192;

  void gotcommit(clnt_stat stat)
  { 
    if (stat || cres.status) 
      (*cb) (NULL, stat2str (cres.status, stat));
    else
      (*cb) (&cres, NULL);
    delete this;
  }

  void check_finish()
  {
    if (outstanding_reads == 0 && outstanding_writes == 0) {
      if (!error && next_read == size) {
	commit3args arg;
	arg.file = dst;
	arg.offset = 0;
	arg.count = size;
        c->call (NFSPROC3_COMMIT, &arg, &cres,
	         wrap(this, &copy3obj::gotcommit), auth_root);
      }
      else 
	delete this;
    }
  }

  void gotwrite (u_int64_t pos, u_int32_t count, read3res *rres, 
                 write3res *wres, clnt_stat stat) 
  {
    if (stat || wres->status) {
      (*cb) (NULL, stat2str (wres->status, stat));
      delete rres;
      delete wres;
      outstanding_writes--;
      error++;
      check_finish();
      return;
    }
    else {
      if (rres->resok->count < count) {
        write3args arg;
        arg.file = dst;
        arg.offset = pos + rres->resok->count;
        arg.count = count - rres->resok->count;
        arg.stable = UNSTABLE;
        arg.data.set
	  (rres->resok->data.base()+rres->resok->count, arg.count, 
	   freemode::NOFREE);
        write3res *wres2 = new write3res;
        c->call (NFSPROC3_WRITE, &arg, wres2,
	         wrap(this, &copy3obj::gotwrite, 
		      arg.offset, arg.count, rres, wres2), auth_root);
      }
      else 
	delete rres;
    }
    delete wres;

    if (next_read < size) {
      count = size > (next_read+READ_BLOCK_SZ) 
	      ? READ_BLOCK_SZ : (size-next_read);
      do_read(next_read, count);
      next_read += count;
    }

    outstanding_writes--;
    check_finish();
  }

  void gotread (u_int64_t pos, u_int32_t count, read3res *res, clnt_stat stat) 
  {
    if (stat || res->status) {
      (*cb) (NULL, stat2str (res->status, stat));
      delete res;
      outstanding_reads--;
      error++;
      check_finish();
    }
    else {
      if (res->resok->count < count) {
        do_read(pos+res->resok->count, count-res->resok->count);
	count = res->resok->count;
      }
      write3args arg;
      arg.file = dst;
      arg.offset = pos;
      arg.count = count;
      arg.stable = UNSTABLE;
      arg.data.set(res->resok->data.base(), count, freemode::NOFREE);
      write3res *wres = new write3res;
      c->call (NFSPROC3_WRITE, &arg, wres,
	       wrap(this, &copy3obj::gotwrite, 
		    arg.offset, arg.count, res, wres), auth_root);
      outstanding_writes++;
      outstanding_reads--;
    } 
  }
 
  void do_read(u_int64_t pos, u_int32_t count)
  {
    read3res *rres = new read3res;
    read3args arg;
    arg.file = src;
    arg.offset = pos;
    arg.count = count;
    c->call (NFSPROC3_READ, &arg, rres,
	     wrap (this, &copy3obj::gotread, arg.offset, arg.count, rres), 
	     auth_root);
    outstanding_reads++;
  }

  void gotattr (clnt_stat stat) {
    if (stat || ares.status) {
      (*cb) (NULL, stat2str(ares.status, stat));
      delete this;
    }
    else {
      FATTR3 * attr = ares.attributes.addr();
      size = attr->size;
      next_read = 0;
      for(int i=0; i<5 && next_read < size; i++) {
        int count = size > (next_read+READ_BLOCK_SZ) 
	              ? READ_BLOCK_SZ : (size-next_read);
        do_read(next_read, count);
        next_read += count;
      }
    }
  }
  
  void do_getattr()
  {
    c->call (NFSPROC3_GETATTR, &src, &ares,
	     wrap (this, &copy3obj::gotattr), auth_root);
  }

  copy3obj (ref<aclnt> c, const nfs_fh3 &s, const nfs_fh3 &d, cb_t cb)
    : cb (cb), c (c), src(s), dst(d)
  {
    error = outstanding_reads = outstanding_writes = 0;
    do_getattr();
  }
};

// cb may be called more than once
void
copy3 (ref<aclnt> c, const nfs_fh3 &src, const nfs_fh3 &dst,
       copy3obj::cb_t cb)
{
  vNew copy3obj (c, src, dst, cb);
}


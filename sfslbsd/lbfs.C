
#include "ihash.h"
#include "sfsrwsd.h"
#include "nfs3_prot.h"
#include "lbfsdb.h"

tmpfh_record::tmpfh_record(const nfs_fh3 &f, const char *s, unsigned l)
{
  fh = f;
  assert (l <= TMPFN_MAX-1);
  memmove(&name[0], s, l);
  name[l] = '\0';
  len = l;
}

tmpfh_record::~tmpfh_record()
{
}

void lookupfh3 (ref<aclnt> c, const nfs_fh3 &start, str path,
		callback<void, const nfs_fh3 *, const FATTR3 *, str>::ref cb);

struct read_obj {
  typedef callback<void, const unsigned char *, size_t, off_t>::ref read_cb_t;
  typedef callback<void, size_t, read3res *, str>::ref cb_t;
  read_cb_t read_cb;
  cb_t cb;
  ref<aclnt> c;

  const nfs_fh3 fh;
  off_t pos; 
  uint32 count;
  uint32 want;
    
  read3res res;

  void gotdata (clnt_stat stat) {
    
    if (stat || res.status) {
      (*cb) (count, &res, stat2str (res.status, stat));
      delete this;
    }
    else {
      read_cb(reinterpret_cast<unsigned char*>(res.resok->data.base()),
	      res.resok->count, pos);
      count += res.resok->count;
      if (want > res.resok->count) {
	want -= res.resok->count;
	pos += res.resok->count;
      }
      else
	want = 0;
      if (want == 0 || res.resok->eof) {
        (*cb) (count, &res, NULL);
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
	     wrap (this, &read_obj::gotdata), auth_root);
  }
  
  read_obj (ref<aclnt> c, const nfs_fh3 &f, off_t p, uint32 cnt, 
            read_cb_t rcb, cb_t cb)
    : read_cb(rcb), cb(cb), c(c), fh(f)
  {
    count = 0;
    pos = p;
    want = cnt;
    do_read();
  }
};

void
nfs3_read (ref<aclnt> c, const nfs_fh3 &fh, 
           read_obj::read_cb_t rcb, 
           read_obj::cb_t cb, 
           off_t pos, uint32 count)
{
  vNew read_obj (c, fh, pos, count, rcb, cb);
}


struct mkdir_obj {
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
	     wrap (this, &mkdir_obj::gotdir), auth_root);
  }
  
  void do_lookup()
  {
    lookupfh3(c, dir, fname, wrap(this, &mkdir_obj::gotfh3));
  }

  mkdir_obj (ref<aclnt> c, const nfs_fh3 &d, const str &name, 
             sattr3 &a, cb_t cb)
    : cb (cb), c (c), dir(d), fname(name), attr(a)
  {
    done_mkdir = 0;
    do_lookup();
  }
};


// creates dir if it does not exist, otherwise return fh in cb
void
nfs3_mkdir (ref<aclnt> c, const nfs_fh3 &dir, const str &name, sattr3 attr,
            mkdir_obj::cb_t cb)
{
  vNew mkdir_obj (c, dir, name, attr, cb);
}


struct copy_obj {
  typedef callback<void, unsigned const char *, size_t, off_t>::ref read_cb_t;
  typedef callback<void, const FATTR3 *, commit3res *, str>::ref cb_t;
  read_cb_t read_cb;
  cb_t cb;
  ref<aclnt> c;

  const nfs_fh3 &src;
  const nfs_fh3 &dst;

  getattr3res ares;
  commit3res cres;

  int outstanding_reads;
  int outstanding_writes;
  int errors;
  u_int64_t size;
  u_int64_t next_read;
  
  static const int READ_BLOCK_SZ = 8192;

  void gotdstattr(clnt_stat stat)
  {
    if (stat || cres.status) 
      (*cb) (NULL, NULL, stat2str (cres.status, stat));
    else {
      FATTR3 * attr = ares.attributes.addr();
      (*cb) (attr, &cres, NULL);
    }
    delete this;
  }

  void gotcommit(clnt_stat stat)
  { 
    if (stat || cres.status) {
      (*cb) (NULL, NULL, stat2str (cres.status, stat));
      delete this;
    }
    else
      c->call (NFSPROC3_GETATTR, &dst, &ares,
	       wrap (this, &copy_obj::gotdstattr), auth_root);
  }

  void check_finish()
  {
    if (outstanding_reads == 0 && outstanding_writes == 0) {
      if (!errors && next_read == size) {
	commit3args arg;
	arg.file = dst;
	arg.offset = 0;
	arg.count = size;
        c->call (NFSPROC3_COMMIT, &arg, &cres,
	         wrap(this, &copy_obj::gotcommit), auth_root);
      }
      else 
	delete this;
    }
  }

  void gotwrite (u_int64_t pos, u_int32_t count, read3res *rres, 
                 write3res *wres, clnt_stat stat) 
  {
    if (stat || wres->status || errors > 0) {
      if (errors == 0) 
	(*cb) (NULL, NULL, stat2str (wres->status, stat));
      delete rres;
      delete wres;
      outstanding_writes--;
      errors++;
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
	         wrap(this, &copy_obj::gotwrite, 
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
    if (stat || res->status || errors) {
      if (errors == 0)
	(*cb) (NULL, NULL, stat2str (res->status, stat));
      delete res;
      outstanding_reads--;
      errors++;
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
      read_cb(reinterpret_cast<unsigned char *>(res->resok->data.base()), 
	      count, pos);
      write3res *wres = new write3res;
      c->call (NFSPROC3_WRITE, &arg, wres,
	       wrap(this, &copy_obj::gotwrite, 
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
	     wrap (this, &copy_obj::gotread, arg.offset, arg.count, rres), 
	     auth_root);
    outstanding_reads++;
  }

  void gotattr (clnt_stat stat) {
    if (stat || ares.status) {
      (*cb) (NULL, NULL, stat2str(ares.status, stat));
      delete this;
    }
    else {
      FATTR3 * attr = ares.attributes.addr();
      size = attr->size;
      next_read = 0;
      for(int i=0; i<10 && next_read < size; i++) {
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
	     wrap (this, &copy_obj::gotattr), auth_root);
  }

  copy_obj (ref<aclnt> c, const nfs_fh3 &s, const nfs_fh3 &d, 
            read_cb_t rcb, cb_t cb)
    : read_cb(rcb), cb(cb), c(c), src(s), dst(d)
  {
    errors = outstanding_reads = outstanding_writes = 0;
    do_getattr();
  }
};

// cb may be called more than once
void
nfs3_copy (ref<aclnt> c, const nfs_fh3 &src, const nfs_fh3 &dst,
           copy_obj::read_cb_t rcb, copy_obj::cb_t cb)
{
  vNew copy_obj (c, src, dst, rcb, cb);
}

struct write_obj {
  typedef callback<void, write3res *, str>::ref cb_t;
  ref<aclnt> c;
  cb_t cb;

  const nfs_fh3 fh;
  unsigned char *data;
  off_t pos; 
  uint32 count;
  uint32 total;
  stable_how stable;
  
  unsigned outstanding_writes;
  unsigned errors;

  void check_finish() {
    if (outstanding_writes == 0) {
      delete[] data;
      delete this;
    }
  }
    
  void done_write (write3res *res, clnt_stat stat) {
    if (stat || res->status || errors > 0) {
      if (errors == 0)
	(*cb) (res, stat2str (res->status, stat));
      delete res;
      outstanding_writes--;
      errors++;
      check_finish();
    }
    else {
      if (count < total) {
	do_write();
        outstanding_writes--;
      }
      else {
        outstanding_writes--;
	if (outstanding_writes == 0)
	  (*cb) (res, NULL);
        delete res;
        check_finish();
      }
    }
  }
  
  void do_write() {
    for(int i=0; i<10 && count<total; i++) {
      int cnt = total - count;
      if (cnt > 8192) cnt = 8192;
      write3args arg;
      arg.file = fh;
      arg.offset = pos;
      arg.count = cnt;
      arg.stable = stable;
      arg.data.set(reinterpret_cast<char*>(data+count), cnt, freemode::NOFREE);
      write3res *res = new write3res;
      c->call (NFSPROC3_WRITE, &arg, res,
	       wrap (this, &write_obj::done_write, res), auth_root);
      pos += cnt;
      count += cnt;
      outstanding_writes++;
    }
  }
  
  write_obj (ref<aclnt> c, const nfs_fh3 &f, 
             unsigned char *data, off_t p, uint32 cnt, stable_how s, cb_t cb)
    : c(c), cb(cb), fh(f), data(data), pos(p), total(cnt), stable(s)
  {
    count = 0;
    outstanding_writes = 0;
    errors = 0;
    do_write();
  }
};

// cb may be called multiple times
void
nfs3_write (ref<aclnt> c, const nfs_fh3 &fh, 
            write_obj::cb_t cb, 
	    unsigned char *data, off_t pos, uint32 count, stable_how s)
{
  vNew write_obj (c, fh, data, pos, count, s, cb);
}



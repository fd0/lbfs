/* $Id$ */

/*
 *
 * Copyright (C) 1998 David Mazieres (dm@uun.org)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; either version 2, or (at
 * your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
 * USA
 *
 */

#include "sfslbsd.h"

#define NFS3_BLOCK_SIZE 8192
#define CONCURRENT_READS 4

AUTH *auth_root = authunix_create ("localhost", 0, 0, 0, NULL);
AUTH *auth_default = 
  authunix_create ("localhost", (uid_t) 32767, (gid_t) 9999, 0, NULL);

const strbuf &
strbuf_cat (const strbuf &sb, mountstat3 stat)
{
  switch (stat) {
  case MNT3_OK:
    return strbuf_cat (sb, "no error", false);
  case MNT3ERR_PERM:
    return strbuf_cat (sb, "Not owner", false);
  case MNT3ERR_NOENT:
    return strbuf_cat (sb, "No such file or directory", false);
  case MNT3ERR_IO:
    return strbuf_cat (sb, "I/O error", false);
  case MNT3ERR_ACCES:
    return strbuf_cat (sb, "Permission denied", false);
  case MNT3ERR_NOTDIR:
    return strbuf_cat (sb, "Not a directory", false);
  case MNT3ERR_INVAL:
    return strbuf_cat (sb, "Invalid argument", false);
  case MNT3ERR_NAMETOOLONG:
    return strbuf_cat (sb, "Filename too long", false);
  case MNT3ERR_NOTSUPP:
    return strbuf_cat (sb, "Operation not supported", false);
  case MNT3ERR_SERVERFAULT:
    return strbuf_cat (sb, "Server failure", false);
  }
  return sb << "Unknown error " << int (stat);
}

struct getfh3obj {
  typedef callback<void, const nfs_fh3 *, str>::ref cb_t;
  cb_t cb;

  mountres3 res;

  void gotfh3 (clnt_stat stat) {
    if (stat || res.fhs_status)
      (*cb) (NULL, stat2str (res.fhs_status, stat));
    else {
      nfs_fh3 fh;
      fh.data = res.mountinfo->fhandle;
      (*cb) (&fh, NULL);
    }
    delete this;
  }
  getfh3obj (const char *host, dirpath path, cb_t cb) : cb (cb) {
    acallrpc (host, mount_program_3, MOUNTPROC3_MNT, &path, &res,
	      wrap (this, &getfh3obj::gotfh3), 0, auth_root);
  }
  getfh3obj (ref<aclnt> c, dirpath path, cb_t cb) : cb (cb) {
    c->call (MOUNTPROC3_MNT, &path, &res,
	     wrap (this, &getfh3obj::gotfh3), auth_root);
  }
};

void
getfh3 (const char *host, const str path, getfh3obj::cb_t cb)
{
  vNew getfh3obj (host, path, cb);
}

void
getfh3 (ref<aclnt> c, const str path, getfh3obj::cb_t cb)
{
  vNew getfh3obj (c, path, cb);
}

static void
splitpath (vec<str> &out, str in)
{
  const char *p = in.cstr ();
  const char *e = p + in.len ();
  const char *n;

  for (;;) {
    while (*p == '/')
      p++;
    for (n = p; n < e && *n != '/'; n++)
      ;
    if (n == p)
      return;
    out.push_back (str (p, n - p));
    p = n;
  }
    
}

struct lookup3obj {
  typedef callback<void, const nfs_fh3 *, const FATTR3 *, str>::ref cb_t;

  ref<aclnt> c;
  vec<str> cns;
  cb_t cb;
  lookup3res res;
  getattr3res ares;

  void getattr (clnt_stat stat) {
    if (stat || ares.status)
      (*cb) (NULL, NULL, stat2str (ares.status, stat));
    else
      (*cb) (&res.resok->object, ares.attributes.addr (), NULL);
    delete this;
  }

  void nextcn (const nfs_fh3 &fh, const FATTR3 *attrp) {
    if (!cns.size ()) {
      if (attrp) {
	(*cb) (&fh, attrp, NULL);
	delete this;
      }
      else
	c->call (NFSPROC3_GETATTR, &fh, &ares,
		 wrap (this, &lookup3obj::getattr), auth_default);
      return;
    }
      
    diropargs3 arg;
    arg.dir = fh;
    arg.name = cns.pop_front ();
    c->call (NFSPROC3_LOOKUP, &arg, &res,
	     wrap (this, &lookup3obj::getres), auth_default);
  }

  void getres (clnt_stat stat) {
    if (stat || res.status) {
      (*cb) (NULL, NULL, stat2str (res.status, stat));
      delete this;
    }
    else
      nextcn (res.resok->object,
	      res.resok->obj_attributes.present
	      ? res.resok->obj_attributes.attributes : NULL);
  }

  lookup3obj (ref<aclnt> c, const nfs_fh3 &start,
	      str path, cb_t cb)
    : c (c), cb (cb) {
    splitpath (cns, path);
    res.resok->object = start;
    nextcn (start, NULL);
  }
};

void
lookupfh3 (ref<aclnt> c, const nfs_fh3 &start, str path, lookup3obj::cb_t cb)
{
  vNew lookup3obj (c, start, path, cb);
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


struct read_obj {
  typedef callback<void, const unsigned char *, size_t, off_t>::ref read_cb_t;
  typedef callback<void, size_t, read3res *, str>::ref cb_t;
  read_cb_t read_cb;
  cb_t cb;
  ref<aclnt> c;
  bool cb_called;

  const nfs_fh3 fh;
  off_t pos; 
  uint32 count;
  uint32 want;
    
  void gotdata (read3res *res, clnt_stat stat) {
    
    if (stat || res->status) {
      (*cb) (count, res, stat2str (res->status, stat));
      cb_called = true;
      delete res;
      delete this;
    }
    else {
      off_t oldpos = pos;
      count += res->resok->count;
      if (want > res->resok->count) {
	want -= res->resok->count;
	pos += res->resok->count;
      }
      else
	want = 0;
      if (want == 0 || res->resok->eof) {
        read_cb(reinterpret_cast<unsigned char*>(res->resok->data.base()), 
	        res->resok->count, oldpos);
        (*cb) (count, res, NULL);
        cb_called = true;
	delete res;
        delete this;
      }
      else {
	do_read();
	read_cb(reinterpret_cast<unsigned char*>(res->resok->data.base()),
		res->resok->count, oldpos);
	delete res;
      }
    }
  }
  
  void do_read() {
    read3args arg;
    arg.file = fh;
    arg.offset = pos;
    arg.count = (want <= NFS3_BLOCK_SIZE ? want : NFS3_BLOCK_SIZE);
    read3res *res = New read3res;
    c->call (NFSPROC3_READ, &arg, res,
	     wrap (this, &read_obj::gotdata, res), auth_default);
  }
  
  read_obj (ref<aclnt> c, const nfs_fh3 &f, off_t p, uint32 cnt, 
            read_cb_t rcb, cb_t cb)
    : read_cb(rcb), cb(cb), c(c), fh(f)
  {
    count = 0;
    pos = p;
    want = cnt;
    cb_called = false;
    do_read();
  }
};

void
nfs3_read (ref<aclnt> c, const nfs_fh3 &fh, off_t pos, uint32 count, 
           read_obj::read_cb_t rcb, read_obj::cb_t cb)
{
  vNew read_obj (c, fh, pos, count, rcb, cb);
}


struct copy_obj {
  typedef callback<void, unsigned const char *, size_t, off_t>::ref read_cb_t;
  typedef callback<void, commit3res *, str>::ref cb_t;
  read_cb_t read_cb;
  cb_t cb;
  ref<aclnt> c;

  const nfs_fh3 src;
  const nfs_fh3 dst;

  getattr3res ares;
  commit3res cres;

  bool cb_called;
  int concurrent_reads;
  int outstanding_reads;
  int outstanding_writes;
  int errors;
  u_int64_t size;
  u_int64_t next_read;
  
  void gotcommit(clnt_stat stat)
  { 
    if (!cb_called) {
      if (stat || cres.status)
        (*cb) (NULL, stat2str (cres.status, stat));
      else
        (*cb) (&cres, NULL);
      cb_called = true;
    }
    delete this;
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
	         wrap(this, &copy_obj::gotcommit), auth_default);
      }
      else 
	delete this;
    }
  }

  void gotwrite (u_int64_t pos, u_int32_t count, read3res *rres, 
                 write3res *wres, clnt_stat stat) 
  {
    if (stat || wres->status || errors > 0) {
      if (errors == 0 && !cb_called) {
	(*cb) (NULL, stat2str (wres->status, stat));
        cb_called = true;
      }
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
        write3res *wres2 = New write3res;
        c->call (NFSPROC3_WRITE, &arg, wres2,
	         wrap(this, &copy_obj::gotwrite, 
		      arg.offset, arg.count, rres, wres2), auth_default);
        outstanding_writes++;
      }
      else 
	delete rres;
    }
    delete wres;

    outstanding_writes--;
    check_finish();
  }

  void gotread (u_int64_t pos, u_int32_t count, read3res *res, clnt_stat stat) 
  {
    if (stat || res->status || errors) {
      if (errors == 0 && !cb_called) {
	(*cb) (NULL, stat2str (res->status, stat));
	cb_called = true;
      }
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
      else if (next_read < size) {
        size_t c = size > (next_read+NFS3_BLOCK_SIZE) 
	           ? NFS3_BLOCK_SIZE : (size-next_read);
        do_read(next_read, c);
        next_read += c;
      }
      write3args arg;
      arg.file = dst;
      arg.offset = pos;
      arg.count = count;
      arg.stable = UNSTABLE;
      arg.data.set(res->resok->data.base(), count, freemode::NOFREE);
      write3res *wres = New write3res;
      c->call (NFSPROC3_WRITE, &arg, wres,
	       wrap(this, &copy_obj::gotwrite, 
		    arg.offset, arg.count, res, wres), auth_default);
      outstanding_writes++;
      outstanding_reads--;
      read_cb(reinterpret_cast<unsigned char *>(res->resok->data.base()), 
	      count, pos);
    } 
  }
 
  void do_read(u_int64_t pos, u_int32_t count)
  {
    read3res *rres = New read3res;
    read3args arg;
    arg.file = src;
    arg.offset = pos;
    arg.count = count;
    c->call (NFSPROC3_READ, &arg, rres,
	     wrap (this, &copy_obj::gotread, arg.offset, arg.count, rres), 
	     auth_default);
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
      if (size == 0) {
        check_finish();
	return;
      }
      for(int i=0; i<concurrent_reads && next_read < size; i++) { 
	int count = size > (next_read+NFS3_BLOCK_SIZE) 
	            ? NFS3_BLOCK_SIZE : (size-next_read);
        do_read(next_read, count);
        next_read += count;
      }
    }
  }
  
  void do_getattr()
  {
    c->call (NFSPROC3_GETATTR, &src, &ares,
	     wrap (this, &copy_obj::gotattr), auth_default);
  }

  copy_obj (ref<aclnt> c, const nfs_fh3 &s, const nfs_fh3 &d, 
            read_cb_t rcb, cb_t cb, bool in_order)
    : read_cb(rcb), cb(cb), c(c), src(s), dst(d), cb_called(false)
  {
    errors = outstanding_reads = outstanding_writes = 0;
    if (in_order)
      concurrent_reads = 1;
    else 
      concurrent_reads = CONCURRENT_READS;
    do_getattr();
  }
};

void
nfs3_copy (ref<aclnt> c, const nfs_fh3 &src, const nfs_fh3 &dst,
           copy_obj::read_cb_t rcb, copy_obj::cb_t cb, bool in_order)
{
  vNew copy_obj (c, src, dst, rcb, cb, in_order);
}

struct write_obj {
  typedef callback<void, write3res *, str>::ref cb_t;
  ref<aclnt> c;
  cb_t cb;
  bool cb_called;

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
      if (errors == 0 && !cb_called) {
	(*cb) (res, stat2str (res->status, stat));
        cb_called = true;
      }
      delete res;
      outstanding_writes--;
      errors++;
      check_finish();
    }
    else {
      if (count < total) {
        delete res;
	do_write();
        outstanding_writes--;
      }
      else {
        outstanding_writes--;
	if (outstanding_writes == 0 && !cb_called) {
	  (*cb) (res, NULL);
	  cb_called = true;
        }
        delete res;
        check_finish();
      }
    }
  }
  
  void do_write() {
    for(int i=0; i<CONCURRENT_READS && count<total; i++) {
      int cnt = total - count;
      if (cnt > NFS3_BLOCK_SIZE) cnt = NFS3_BLOCK_SIZE;
      write3args arg;
      arg.file = fh;
      arg.offset = pos;
      arg.count = cnt;
      arg.stable = stable;
      arg.data.set(reinterpret_cast<char*>(data+count), cnt, freemode::NOFREE);
      write3res *res = New write3res;
      c->call (NFSPROC3_WRITE, &arg, res,
	       wrap (this, &write_obj::done_write, res), auth_default);
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
    cb_called = false;
    do_write();
  }
};

// data will be deleted at the end when write is finished.
void
nfs3_write (ref<aclnt> c, const nfs_fh3 &fh, 
            write_obj::cb_t cb, 
	    unsigned char *data, off_t pos, uint32 count, stable_how s)
{
  vNew write_obj (c, fh, data, pos, count, s, cb);
}



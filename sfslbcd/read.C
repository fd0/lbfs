/*
 *
 * Copyright (C) 2002 Benjie Chen (benjie@lcs.mit.edu)
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

#include "sfslbcd.h"
#include "lbfs_prot.h"

struct read_obj {
  static const int PARALLEL_READS = 8;
  static const int READ_SIZE = 4096;
  typedef callback<void,bool>::ref cb_t;

  cb_t cb;
  ref<server> srv;
  nfs_fh3 fh;
  AUTH *auth;
  int fd;
  size_t size;
  size_t requested;
  unsigned int outstanding_reads;
  bool callback;

  void
  read_reply(time_t rqtime, uint64 off, size_t size,
             ref<read3args> arg, ref<ex_read3res> res, clnt_stat err) 
  {
    outstanding_reads--;
    if (!err)
      srv->getattr (rqtime, NFSPROC3_READ, 0, arg, res);
    if (!callback && !err && res->status == NFS3_OK) {
      if (lseek(fd, off, SEEK_SET) < 0) {
	fail();
	return;
      }
      if (write(fd, res->resok->data.base (), res->resok->count) < (int)size) {
	fail();
	return;
      }
      do_read();
      ok();
      return;
    }
    warn << "READ RPC failed: " << err << ":" << res->status << "\n";
    fail();
  }

  void nfs3_read (uint64 off, uint32 size) 
  {
    ref<read3args> a = New refcounted<read3args>;
    a->file = fh;
    a->offset = off;
    a->count = size;
    outstanding_reads++;
    warn << "NFS READ " << off << ":" << size << "\n";
    ref<ex_read3res> res = New refcounted <ex_read3res>;
    srv->nfsc->call (lbfs_NFSPROC3_READ, a, res,
	             wrap (this, &read_obj::read_reply,
		           timenow, off, size, a, res),
		     auth);
  }

  bool do_read() {
    if (requested < size) {
      int s = size-requested;
      s = s > READ_SIZE ? READ_SIZE : s;
      nfs3_read (requested, s);
      requested += s;
      if (requested < size)
	return false;
    }
    return true;
  }

  void fail() {
    if (!callback) {
      ftruncate(fd, 0);
      close(fd);
      callback = true;
      cb(false);
    }
    if (outstanding_reads == 0)
      delete this;
  }

  void ok() {
    if (outstanding_reads == 0) {
      if (!callback) {
	close(fd);
        callback = true;
	cb(true);
      }
      delete this;
    }
  }

  read_obj (str fn, nfs_fh3 fh, size_t size, ref<server> srv,
            AUTH *a, read_obj::cb_t cb)
    : cb(cb), srv(srv), fh(fh), auth(a), size(size), requested(0),
      outstanding_reads(0), callback(false)
  {
    fd = open (fn, O_CREAT | O_TRUNC | O_WRONLY, 0666);
    if (fd < 0) {
      perror ("update cache file\n");
      fail();
    }
    else {
      bool eof = false;
      for (int i=0; i<PARALLEL_READS && !eof; i++)
        eof = do_read();
    }
    if (!outstanding_reads) // nothing to do
      ok();
  }

  ~read_obj() {}
};

void
lbfs_read (str fn, nfs_fh3 fh, size_t size, ref<server> srv,
           AUTH *a, read_obj::cb_t cb)
{
  vNew read_obj (fn, fh, size, srv, a, cb);
}


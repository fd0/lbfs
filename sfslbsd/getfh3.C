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

#include "sfsrwsd.h"

AUTH *auth_root = authunix_create ("localhost", 0, 0, 0, NULL);
AUTH *auth_default = authunix_create_default ();

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

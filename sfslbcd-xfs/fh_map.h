/*
 *
 * Copyright (C) 2000 Athicha Muthitacharoen (athicha@mit.edu)
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

#ifndef _CACHE_H_
#define _CACHE_H_

#include <xfs/xfs_message.h>
#include "nfs3exp_prot.h"
#include "xfs-nfs.h"
#include "ihash.h"
#include "nfstrans.h"

#define MAX_FH 65535 //4000
//#define MAX_FH MAXHANDLE //xfs constant for max file handles opened at any time
//#define MAXPATHLEN (1024+4) //This is what arla uses...

bool xfs_fheq(xfs_handle, xfs_handle);
bool nfs_fheq(nfs_fh3, nfs_fh3);
str setcache_name(uint32 index);
nfstime3 max(nfstime3 mtime, nfstime3 ctime);

template<>
struct hashfn<xfs_handle> {
  hash_t operator() (const xfs_handle &xh) const
    { return xh.a; }
};

template<>
struct equals<xfs_handle> {
  bool operator() (const xfs_handle &a, const xfs_handle &b) const {
    return xfs_handle_eq (&a, &b);
  }
};

typedef struct cache_entry{
  static u_int64_t nextxh;

  xfs_handle xh;
  nfs_fh3 nh;
  ex_fattr3 nfs_attr;
  nfstime3 ltime; //mtime of local cache
  str cache_name;
  bool incache;
  uint32 writers;
  ihash_entry<cache_entry> nlink;
  ihash_entry<cache_entry> xlink;

  cache_entry (nfs_fh3 &n, ex_fattr3 &na);
  ~cache_entry ();
  set_exp (ex_fattr3 &na, time_t rqtime, bool update_dir_expire);
} cache_entry;

extern ihash<nfs_fh3, cache_entry, &cache_entry::nh,
  &cache_entry::nlink> nfsindex;
extern ihash<xfs_handle, cache_entry, &cache_entry::xh,
  &cache_entry::xlink> xfsindex;

inline
cache_entry::cache_entry (nfs_fh3 &n, ex_fattr3 &na)
  : nh (n), nfs_attr(na), incache(false), writers(0)
{
  bzero (&xh, sizeof (xh));
  xh.a = ++nextxh;
  xh.b = nextxh >> 32;
  cache_name = setcache_name(xh.a); //check if NULL??
  ltime = max(nfs_attr.mtime, nfs_attr.ctime);
  nfsindex.insert (this);
  xfsindex.insert (this);
}

inline
cache_entry::~cache_entry ()
{
  nfsindex.remove (this);
  xfsindex.remove (this);
}

inline
cache_entry::set_exp (time_t rqtime, bool update_dir_expire) 
{
  // change expire to rpc_time + expire
  if (nfs_attr.type != NF3DIR || update_dir_expire)
    nfs_attr.expire += rqtime;
}
#endif /* _CACHE_H_ */












/* $Id$ */

/*
 *
 * Copyright (C) 1999 David Mazieres (dm@uun.org)
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
#include "itree.h"
#include "list.h"
#include "lbfs.h"

enum { num_leases_max = 2048 };
static u_int num_leases;

struct fhsync;

struct lease {
  const u_int64_t cgen;
  const u_int32_t fsno;
  u_int32_t expire;
  fhsync *const fhs;

  ihash_entry<lease> synclink;
  itree_entry<lease> explink;

  lease (fhsync *f, u_int64_t cg, u_int32_t fsn, u_int32_t ex);
  ~lease ();
  void renew (u_int32_t);

  static timecb_t *tmocb;
  static void sched (bool timedout = false);
};

struct lease_compare {
  lease_compare () {}
  bool operator() (const lease &a, const lease &b) const 
    { return a.expire < b.expire ? -1 : b.expire != a.expire; }
};

struct fhsync {
  const nfs_fh3 fh;
  nfstime3 mtime;
  nfstime3 ctime;
  filesrv *const fsrv;
  ihash2<const u_int64_t, const u_int32_t, lease,
         &lease::cgen, &lease::fsno, &lease::synclink> leases;
  ihash_entry<fhsync> hlink;
  
  fhsync (filesrv *fs, const nfs_fh3 &f, const ex_fattr3 *a);
  ~fhsync ();
  void update (u_int64_t cgen, u_int32_t fsno, const ex_fattr3 *a);
};

struct synctab {
  ihash<const nfs_fh3, fhsync, &fhsync::fh, &fhsync::hlink> fhtab;
  void update (xattrvec *xvp);
};

timecb_t *lease::tmocb;

static itree_core<lease, &lease::explink, lease_compare> leasetree;

lease::lease (fhsync *fs, u_int64_t cg, u_int32_t fsn, u_int32_t ex)
  : cgen (cg), fsno (fsn), expire (timenow + ex), fhs (fs)
{
  num_leases++;
  leasetree.insert (this);
  fhs->leases.insert (this);
  sched ();
}

lease::~lease ()
{
  num_leases--;
  leasetree.remove (this);
  fhs->leases.remove (this);
  if (!fhs->leases.size ())
    delete fhs;
}

void
lease::renew (u_int32_t duration)
{
  leasetree.remove (this);
  expire = timenow + duration;
  leasetree.insert (this);
}

void
lease::sched (bool timedout)
{
  if (timedout)
    tmocb = NULL;
  if (!tmocb) {
    lease *l = leasetree.first ();
    while (l && (time_t) l->expire < timenow) {
      lease *ll = l;
      l = leasetree.next (l);
      delete ll;
    }
    if (l && !tmocb)
      tmocb = timecb (l->expire, wrap (sched, true));
  }
}

void
fhsync::update (u_int64_t cgen, u_int32_t fsno, const ex_fattr3 *a)
{
  lease *l = leases (cgen, fsno);

  if (a) {
    if (l)
      l->renew (a->expire);
    if (!l && a->expire)
      vNew lease (this, cgen, fsno, a->expire);
    if (a && a->mtime == mtime && a->ctime == ctime)
      return;
    mtime = a->mtime;
    ctime = a->ctime;
  }
  else
    ctime.seconds = ctime.nseconds = mtime.seconds = mtime.nseconds = 0;

  ex_invalidate3args arg;
  if (a) {
    arg.attributes.set_present (true);
    *arg.attributes.attributes = *a;
    arg.attributes.attributes->expire = 0;
  }

  fh3trans fht (fh3trans::ENCODE, fsrv->fhkey, 0, NULL);
  lease *ll;
  for (l = leases.first (); l; l = ll) {
    ll = leases.next (l);
    if (l->cgen != cgen || l->fsno != fsno || !a) {
      if (client *c = clienttab[l->cgen]) {
	arg.handle = fh;
	fht.srvno = l->fsno;
	if (rpc_traverse (fht, arg.handle))
	  c->nfscbc->call (ex_NFSCBPROC3_INVALIDATE,
			   &arg, NULL, aclnt_cb_null);
      }
      delete l;
    }
  }
}

fhsync::fhsync (filesrv *fs, const nfs_fh3 &f, const ex_fattr3 *a)
  : fh (f), mtime (a->mtime), ctime (a->ctime), fsrv (fs)
{
  fsrv->st->fhtab.insert (this);
}
fhsync::~fhsync ()
{
  fsrv->st->fhtab.remove (this);
}


synctab *
synctab_alloc ()
{
  return New synctab;
}
void
synctab_free (synctab *st)
{
  delete st;
}

void
dolease (filesrv *fsrv, u_int64_t cgen, u_int32_t fsno, xattr *xp)
{
  if (xp->fattr)
    xp->fattr->expire = fsrv->leasetime;
  fhsync *fsy = fsrv->st->fhtab[*xp->fh];
  if (!fsy && xp->fattr)
    fsy = New fhsync (fsrv, *xp->fh, xp->fattr);
  if (fsy)
    fsy->update (cgen, fsno, xp->fattr);
}

void
doleases (filesrv *fsrv, u_int64_t cgen, u_int32_t fsno, svccb *sbp, void *res)
{
  lbfs_exp_enable (sbp->proc(), res);
  xattrvec xv;
#if 0
  nfs3_getxattr (&xv, LBFS_PROC_RES_TRANS(sbp->proc()), 
                 sbp->getvoidarg (), res);
#else
  lbfs_getxattr (&xv, sbp->proc(), sbp->getvoidarg (), res);
#endif

  for (xattr *xp = xv.base (); xp < xv.lim (); xp++)
    dolease (fsrv, cgen, fsno, xp);
}

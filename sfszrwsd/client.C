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
#include <grp.h>

ihash<const u_int64_t, client, &client::generation, &client::glink> clienttab;

void
client::fail ()
{
  nfssrv = NULL;
  nfscbc = NULL;
}

void
client::nfs3reply (svccb *sbp, void *res, filesrv::reqstate rqs, clnt_stat err)
{
  xdrproc_t xdr = nfs_program_3.tbl[sbp->proc ()].xdr_res;
  if (err) {
    xdr_delete (xdr, res);
    sbp->reject (SYSTEM_ERR);
    return;
  }
  doleases (fsrv, generation, rqs.fsno, sbp, res);
  if (fsrv->fixres (sbp, res, &rqs)) {
    nfs3_exp_enable (sbp->proc (), res);
    sbp->reply (res);
    xdr_delete (xdr, res);
  }
  else
    xdr_delete (xdr, res);
}

void
client::removecb_3 (svccb *sbp, getattr3res *gres, filesrv::reqstate rqs,
                    nfs_fh3 fh, void *res, clnt_stat err)
{
  if (!err && !gres->status) {
    xattr xa;
    xa.fh = &fh;
    xa.fattr = reinterpret_cast<ex_fattr3 *> (gres->attributes.addr ());
    dolease (fsrv, 0, static_cast<u_int32_t> (-1), &xa);
  }
  nfs3reply (sbp, res, rqs, RPC_SUCCESS);
  delete gres;
}

void
client::removecb_2 (svccb *sbp, void *_res, filesrv::reqstate rqs,
		    lookup3res *ares, clnt_stat err)
{
  wccstat3 *res = static_cast<wccstat3 *> (_res);
  AUTH *auth;
  if (err || res->status || !(auth = authtab[sbp->getaui ()]) || !ares) {
    if (ares)
      delete ares;
    nfs3reply (sbp, res, rqs, err);
    return;
  }

  getattr3res *gres = New getattr3res;
  rqs.c->call (NFSPROC3_GETATTR, &ares->resok->object, gres,
               wrap (mkref (this), &client::removecb_3, sbp, gres, rqs,
		     ares->resok->object, _res), auth);
  delete ares;
}

void
client::removecb_1 (svccb *sbp, lookup3res *ares, filesrv::reqstate rqs,
		    clnt_stat err)
{
  AUTH *auth = authtab[sbp->getaui ()];
  if (err || ares->status || !auth) {
    delete ares;
    ares = 0;
  }
    
  void *res = nfs_program_3.tbl[sbp->proc ()].alloc_res ();
  rqs.c->call (sbp->proc (), sbp->template getarg<void> (), res,
               wrap (mkref (this), &client::removecb_2, sbp, res, rqs, ares),
	       auth);
}

void
client::renamecb_3 (svccb *sbp, getattr3res *gres, filesrv::reqstate rqs,
                    nfs_fh3 fh, void *res, clnt_stat err)
{
  if (!err && !gres->status) {
    xattr xa;
    xa.fh = &fh;
    xa.fattr = reinterpret_cast<ex_fattr3 *> (gres->attributes.addr ());
    dolease (fsrv, 0, static_cast<u_int32_t> (-1), &xa);
  }
  nfs3reply (sbp, res, rqs, RPC_SUCCESS);
  delete gres;
}

void
client::renamecb_2 (svccb *sbp, rename3res *rres, filesrv::reqstate rqs,
                    lookup3res *ares_old, lookup3res *ares, clnt_stat err)
{
  if (!err && !ares->status) {
    xattr xa;
    xa.fh = &ares->resok->object;
    if (ares->resok->obj_attributes.present)
      xa.fattr = reinterpret_cast<ex_fattr3 *>
        (ares->resok->obj_attributes.attributes.addr ());
    dolease (fsrv, 0, static_cast<u_int32_t> (-1), &xa);
  }
  delete ares;

  if (ares_old) {
    AUTH *auth = authtab[sbp->getaui ()];
    getattr3res *gres = New getattr3res;
    rqs.c->call (NFSPROC3_GETATTR, &ares_old->resok->object, gres,
                 wrap (mkref (this), &client::renamecb_3, sbp, gres, rqs,
                       ares_old->resok->object, rres), auth);
    delete ares_old;
  }
  else
    nfs3reply (sbp, rres, rqs, RPC_SUCCESS);
}

void
client::renamecb_1 (svccb *sbp, void *_res, filesrv::reqstate rqs,
                    lookup3res *ares_old, clnt_stat err)
{
  rename3res *res = static_cast<rename3res *> (_res);
  AUTH *auth;
  if (err || res->status || !(auth = authtab[sbp->getaui ()])) {
    if (ares_old) {
      delete ares_old;
      ares_old = 0;
    }
    nfs3reply (sbp, res, rqs, err);
    return;
  }

  lookup3res *ares = New lookup3res;
  rqs.c->call (NFSPROC3_LOOKUP, &sbp->template getarg<rename3args> ()->to,
               ares, wrap (mkref (this), &client::renamecb_2,
                           sbp, res, rqs, ares_old, ares), auth);
}

void
client::renamecb_0 (svccb *sbp, lookup3res *ares, filesrv::reqstate rqs,
                    clnt_stat err)
{
  AUTH *auth = authtab[sbp->getaui ()];
  if (err || ares->status || !auth) {
    delete ares;
    ares = 0;
  }

  void *res = nfs_program_3.tbl[sbp->proc ()].alloc_res ();
  rqs.c->call (sbp->proc (), sbp->template getarg<void> (), res,
               wrap (mkref (this), &client::renamecb_1, sbp, res, rqs, ares),
               authtab[sbp->getaui ()]);
}

void
client::nfs3dispatch (svccb *sbp)
{
  if (!sbp) {
    fail ();
    return;
  }
  if (sbp->proc () == NFSPROC3_NULL) {
    sbp->reply (NULL);
    return;
  }

  u_int32_t authno = sbp->getaui ();
  if (authno >= authtab.size () || !authtab[authno]) {
    sbp->reject (AUTH_REJECTEDCRED);
    return;
  }
  if (!fsrv) {
    nfs3exp_err (sbp, NFS3ERR_BADHANDLE);
    return;
  }

  filesrv::reqstate rqs;
  if (!fsrv->fixarg (sbp, &rqs))
    return;

  if (sbp->proc () == NFSPROC3_REMOVE) {
    // change REMOVE to LOOKUP,REMOVE,GETATTR, so we can invalidate
    // the fh whose linkcount changed
    lookup3res *ares = New lookup3res;
    rqs.c->call (NFSPROC3_LOOKUP, sbp->template getarg<void> (), ares,
                 wrap (mkref (this), &client::removecb_1, sbp, ares, rqs),
                 authtab[authno]);
  }
  else if (sbp->proc () == NFSPROC3_RENAME) {
    // change RENAME to LOOKUP,RENAME,LOOKUP,GETATTR, so we can
    // invalidate both the old fh for the destination file and the new
    // fh for the destination file, if needed
    lookup3res *ares = New lookup3res;
    rqs.c->call (NFSPROC3_LOOKUP,
                 &sbp->template getarg<rename3args> ()->to, ares,
                 wrap (mkref (this), &client::renamecb_0, sbp, ares, rqs),
                 authtab[authno]);
  }
  else {
    void *res = nfs_program_3.tbl[sbp->proc ()].alloc_res ();
    rqs.c->call (sbp->proc (), sbp->template getarg<void> (), res,
                 wrap (mkref (this), &client::nfs3reply, sbp, res, rqs),
                 authtab[authno]);
  }
}

u_int64_t
client::nextgen ()
{
  static u_int64_t g;
  while (clienttab[++g] || !g)
    ;
  return g;
}

client::client (ref<axprt_zcrypt> xx)
  : sfsserv (xx), fsrv (NULL), generation (nextgen ())
{
  nfssrv = asrv::alloc (x, ex_nfs_program_3,
			wrap (mkref (this), &client::nfs3dispatch));
  nfscbc = aclnt::alloc (x, ex_nfscb_program_3);
  authtab[0] = authunix_create ("localhost", nobody_uid, nobody_gid,
				0, NULL);
  clienttab.insert (this);
  try_compress = true;
}

client::~client ()
{
  clienttab.remove (this);
}

void
client::sfs_getfsinfo (svccb *sbp)
{
  if (fsrv) {
    sbp->replyref (fsrv->fsinfo);
    if (try_compress) {
      static_cast<axprt_zcrypt *> (x.get ())->compress();
      try_compress = false;
    }
  }
  else
    sbp->reject (PROC_UNAVAIL);
}

ptr<sfspriv>
client::doconnect (const sfs_connectarg *ci, sfs_servinfo *si)
{
  fsrv = defsrv;
  *si = fsrv->servinfo;
  return fsrv->privkey;
}

void
client::launch (ref<axprt_zcrypt> xc)
{
  vNew refcounted<client> (xc);
}

ptr<axprt_crypt>
client_accept (ptr<axprt_crypt> x)
{
  if (!x)
    fatal ("EOF from sfssd\n");
  int fd = x->reclaim();
  ptr<axprt_zcrypt> xx = New refcounted<axprt_zcrypt>(fd, axprt_zcrypt::ps());
  client::launch (xx);
  return xx;
}


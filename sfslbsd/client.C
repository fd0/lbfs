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
#include "lbfsdb.h"
#include "fingerprint.h"
#include "lbfs.h"
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
client::renamecb_2 (svccb *sbp, rename3res *rres, filesrv::reqstate rqs,
		    lookup3res *ares, clnt_stat err)
{
  if (!err && !ares->status) {
    xattr xa;
    xa.fh = &ares->resok->object;
    if (ares->resok->obj_attributes.present)
      xa.fattr = reinterpret_cast<ex_fattr3 *>
	(ares->resok->obj_attributes.attributes.addr ());
    dolease (fsrv, 0, static_cast<u_int32_t> (-1), &xa);
  }
  nfs3reply (sbp, rres, rqs, RPC_SUCCESS);
  delete ares;
}

void
client::renamecb_1 (svccb *sbp, void *_res, filesrv::reqstate rqs,
		    clnt_stat err)
{
  rename3res *res = static_cast<rename3res *> (_res);
  AUTH *auth;
  if (err || res->status || !(auth = authtab[sbp->getaui ()])) {
    nfs3reply (sbp, res, rqs, err);
    return;
  }

  lookup3res *ares = New lookup3res;
  fsrv->c->call (NFSPROC3_LOOKUP, &sbp->template getarg<rename3args> ()->to,
		 ares, wrap (mkref (this), &client::renamecb_2,
			     sbp, res, rqs, ares), auth);
}

void
client::condwrite_read_cb (svccb *sbp, void *_res, filesrv::reqstate rqs,
                           lbfs_db::chunk_iterator *iter, 
			   unsigned char *data, size_t count, str err)
{
  lbfs_condwrite3args *cwa = sbp->template getarg<lbfs_condwrite3args> ();
  lbfs_chunk_loc c;
  iter->get(&c);

  if (err || count != cwa->count ||
      fingerprint(data, count) != cwa->fingerprint) {
    delete data;
    // only remove record if it is not an error, so transient 
    // failures won't cause db to be incorrected deleted.
    if (!err)
      iter->del(); 
    if (!iter->next(&c)) { 
      nfs_fh3 fh; 
      c.get_fh(fh); 
      readfh3(fsrv->c, fh,
	      wrap(mkref(this), &client::condwrite_read_cb, 
		   sbp, _res, rqs, iter), c.pos(), c.count());
      return; 
    }
  }
 
  else {
    // fingerprint matches, do write
    u_int32_t authno = sbp->getaui ();
    write3args w3arg;
    w3arg.file = cwa->file;
    w3arg.offset = cwa->offset;
    w3arg.count = cwa->count;
    w3arg.stable = cwa->stable;
    w3arg.data.set(reinterpret_cast<char*>(data), count, freemode::DELETE);
    fsrv->c->call (NFSPROC3_WRITE, &w3arg, _res,
		   wrap (mkref (this), &client::nfs3reply, sbp, _res, rqs),
		   authtab[authno]);
    delete iter;
  }

  nfs3reply (sbp, _res, rqs, RPC_FAILED);
  delete iter;
}

void
client::condwrite (svccb *sbp, void *_res, filesrv::reqstate rqs)
{
  lbfs_condwrite3args *cwa = sbp->template getarg<lbfs_condwrite3args> ();
  lbfs_db::chunk_iterator *iter = 0;
  if (lbfsdb.get_chunk_iterator(cwa->fingerprint, &iter) == 0) {
    if (iter) { 
      lbfs_chunk_loc c; 
      if (!iter->get(&c)) { 
	nfs_fh3 fh; 
	c.get_fh(fh); 
	readfh3(fsrv->c, fh,
	        wrap(mkref(this), &client::condwrite_read_cb, 
		     sbp, _res, rqs, iter), c.pos(), c.count());
	return;
      } 
      delete iter; 
    }
  }
  nfs3reply (sbp, _res, rqs, RPC_FAILED);
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

  void *res = nfs_program_3.tbl[sbp->proc ()].alloc_res ();
  if (sbp->proc () == NFSPROC3_RENAME)
    fsrv->c->call (sbp->proc (), sbp->template getarg<void> (), res,
		   wrap (mkref (this), &client::renamecb_1, sbp, res, rqs),
		   authtab[authno]);

  else if (sbp->proc () == lbfs_NFSPROC3_CONDWRITE)
    condwrite(sbp, res, rqs);

  else
    fsrv->c->call (sbp->proc (), sbp->template getarg<void> (), res,
		   wrap (mkref (this), &client::nfs3reply, sbp, res, rqs),
		   authtab[authno]);
}

u_int64_t
client::nextgen ()
{
  static u_int64_t g;
  while (clienttab[++g] || !g)
    ;
  return g;
}

client::client (ref<axprt_crypt> x)
  : sfsserv (x), fsrv (NULL), generation (nextgen ())
{
  nfssrv = asrv::alloc (x, lbfs_program_3,
			wrap (mkref (this), &client::nfs3dispatch));
  nfscbc = aclnt::alloc (x, lbfscb_program_3);
  authtab[0] = authunix_create ("localhost", (uid_t) -1,
				(gid_t) -1, 0, NULL);
  clienttab.insert (this);

  lbfsdb.open();
}

client::~client ()
{
  clienttab.remove (this);
}

void
client::sfs_getfsinfo (svccb *sbp)
{
  if (fsrv)
    sbp->replyref (fsrv->fsinfo);
  else
    sbp->reject (PROC_UNAVAIL);
}

ptr<rabin_priv>
client::doconnect (const sfs_connectarg *ci, sfs_servinfo *si)
{
  fsrv = defsrv;
  *si = fsrv->servinfo;
  return fsrv->sk;
}

void
client_accept (ptr<axprt_crypt> x)
{
  if (!x)
    fatal ("EOF from sfssd\n");
  client::launch (x);
}


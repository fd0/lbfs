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

#include <stdlib.h>

#include "sha1.h"
#include "serial.h"
#include "sfsrwsd.h"
#include <grp.h>
#include "arpc.h"

#include "lbfsdb.h"
#include "fingerprint.h"
#include "lbfs.h"

#define DEBUG 3


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
  xdrproc_t xdr = lbfs_program_3.tbl[sbp->proc()].xdr_res;

  if (err) {
    xdr_delete (xdr, res);
    sbp->reject (SYSTEM_ERR);
    return;
  }
  doleases (fsrv, generation, rqs.fsno, sbp, res);

  if (fsrv->fixres (sbp, res, &rqs)) {
    lbfs_exp_enable (sbp->proc(), res);
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

// returns 0 if sha1 hash of data is equals to the given hash
static inline int
compare_sha1_hash(unsigned char *data, size_t count, sfs_hash &hash)
{
  char h[sha1::hashsize];
  sha1_hash(h, data, count);
  return strncmp(h, hash.base(), sha1::hashsize);
}

void
client::condwrite_got_chunk (svccb *sbp, filesrv::reqstate rqs,
                             fp_db::iterator *iter,
			     unsigned char *data, 
			     size_t count, read3res *, str err)
{
  lbfs_condwrite3args *cwa = sbp->template getarg<lbfs_condwrite3args> ();
  lbfs_chunk_loc c;
  iter->get(&c);

  if (err || count != cwa->count ||
      fingerprint(data, count) != cwa->fingerprint ||
      compare_sha1_hash(data, count, cwa->hash)) {
#if DEBUG > 0
    if (err) 
      warn << "CONDWRITE: error reading file\n";
    else if (count != cwa->count)
      warn << "CONDWRITE: db corrupted, size does not match\n";
    else if (fingerprint(data,count) != cwa->fingerprint)
      warn << "CONDWRITE: fingerprint mismatch\n";
    else 
      warn << "CONDWRITE: sha1 hash mismatch\n";
#endif
    delete[] data;
    iter->del(); 
    if (!iter->next(&c)) { 
      nfs_fh3 fh; 
      c.get_fh(fh); 
      unsigned char *buf = New unsigned char[c.count()];
      nfs3_read
	(fsrv->c, fh, 
	 wrap(mkref(this), &client::condwrite_read_cb, buf, c.pos()), 
	 wrap(mkref(this), &client::condwrite_got_chunk, sbp, rqs, iter, buf), 
	 c.pos(), c.count());
      return; 
    }
  }
 
  else {
#if DEBUG > 2
    // fingerprint matches, do write
    warn << "CONDWRITE: bingo, found a condwrite candidate\n";
#endif
    nfs3_write(fsrv->c, cwa->file, 
	       wrap(mkref(this), &client::condwrite_write_cb, 
		    sbp, rqs, cwa->count),
	       data, cwa->offset, cwa->count, UNSTABLE);
    delete iter;
    fpdb.sync();
    return;
  }

  delete iter;
#if DEBUG > 0
  warn << "CONDWRITE: ran out of files to try\n";
#endif
  fpdb.sync();
  lbfs_nfs3exp_err (sbp, NFS3ERR_FPRINTNOTFOUND);
}
  
void 
client::condwrite_write_cb (svccb *sbp, filesrv::reqstate rqs, size_t count,
                            write3res *res, str err)
{
  write3res *wres = New write3res;
  *wres = *res;
  if (!err || res->status) {
    if (!res->status)
      wres->resok->count = count;
    nfs3reply(sbp, wres, rqs, RPC_SUCCESS);
  }
  else
    nfs3reply(sbp, wres, rqs, RPC_FAILED);
}

void
client::condwrite_read_cb(unsigned char *buf, off_t pos0,
                          const unsigned char *data, size_t count, off_t pos)
{
  memmove(buf+(pos-pos0), data, count);
}

void
client::condwrite (svccb *sbp, filesrv::reqstate rqs)
{
  lbfs_condwrite3args *cwa = sbp->template getarg<lbfs_condwrite3args> ();
  fp_db::iterator *iter = 0;
  if (fpdb.get_iterator(cwa->fingerprint, &iter) == 0) {
    if (iter) { 
      lbfs_chunk_loc c; 
      if (!iter->get(&c)) { 
	nfs_fh3 fh; 
	c.get_fh(fh);
	unsigned char *buf = New unsigned char[c.count()];
	nfs3_read
	  (fsrv->c, fh,
	   wrap(mkref(this), &client::condwrite_read_cb, buf, c.pos()), 
	   wrap(mkref(this), &client::condwrite_got_chunk, sbp, rqs, iter, buf),
	   c.pos(), c.count());
	return;
      } 
      delete iter; 
    }
  }
#if DEBUG > 2
  warn << "CONDWRITE: " << cwa->fingerprint << " not in DB\n";
#endif
  lbfs_nfs3exp_err (sbp, NFS3ERR_FPRINTNOTFOUND);
}

void
client::mktmpfile_cb (svccb *sbp, filesrv::reqstate rqs, char *path,
                      void *_cres, clnt_stat err)
{
  diropres3 *cres = static_cast<diropres3 *>(_cres);
  if (err) {
    delete[] path;
    nfs3reply (sbp, _cres, rqs, err);
  }
  else {
    switch(cres->status) {
      case NFS3ERR_EXIST:
	delete cres;
        delete[] path;
	mktmpfile(sbp, rqs);
	break;
      default:
	fhtab.tab.insert
	  (New tmpfh_record(*(cres->resok->obj.handle),path,strlen(path)));
	delete[] path;
	nfs3reply (sbp, _cres, rqs, RPC_SUCCESS);
    }
  }
}

void
client::mktmpfile (svccb *sbp, filesrv::reqstate rqs)
{
  lbfs_mktmpfile3args *mta = sbp->template getarg<lbfs_mktmpfile3args> ();

  str fhstr = armor32(mta->commit_to.data.base(), mta->commit_to.data.size());
  int r = rand();
  str rstr = armor32((void*)&r, sizeof(int));
  char *tmpfile = New char[5+fhstr.len()+1+rstr.len()+1];
  sprintf(tmpfile, "sfs.%s.%s", fhstr.cstr(), rstr.cstr());
#if DEBUG > 1
  warn << "MKTMPFILE: " << tmpfile << "\n";
#endif
  
  u_int32_t authno = sbp->getaui ();
  create3args c3arg;
  c3arg.where.dir = fsrv->sfs_trash_fhs[rqs.fsno];
  c3arg.where.name = tmpfile;
  c3arg.how.set_mode(GUARDED);
  *(c3arg.how.obj_attributes) = mta->obj_attributes;

  void *cres = nfs_program_3.tbl[NFSPROC3_CREATE].alloc_res ();
  fsrv->c->call (NFSPROC3_CREATE, &c3arg, cres,
		 wrap (mkref (this),
		       &client::mktmpfile_cb, sbp, rqs, tmpfile, cres),
		 authtab[authno]);
}

void
client::committmp_cb (svccb *sbp, filesrv::reqstate rqs, Chunker *chunker,
                      const FATTR3 *attr, commit3res *res, str err)
{
  lbfs_committmp3args *cta = sbp->template getarg<lbfs_committmp3args> ();
  nfs_fh3 tmpfh = cta->commit_from;
  nfs_fh3 fh = cta->commit_to;
  u_int32_t authno = sbp->getaui ();
  unsigned fsno = rqs.fsno;

  commit3res *cres = New commit3res;
  *cres = *res;
  if (!err || cres->status)
    nfs3reply (sbp, cres, rqs, RPC_SUCCESS);
  else
    nfs3reply (sbp, cres, rqs, RPC_FAILED);

  chunker->stop();
  const vec<lbfs_chunk *>& cv = chunker->chunk_vector();
  if (!err) {
    for (unsigned i=0; i<cv.size(); i++) {
      cv[i]->loc.set_fh(fh);
      fpdb.add_entry(cv[i]->fingerprint, &(cv[i]->loc)); 
#if DEBUG > 2
      warn << "COMMITTMP: adding " << cv[i]->fingerprint << " to database\n";
#endif
    }
    fpdb.sync();
  }
  delete chunker;

  tmpfh_record *tfh_rec = fhtab.tab[tmpfh];
  if (tfh_rec) {
    tfh_rec->name[tfh_rec->len] = '\0';
#if DEBUG > 1
    warn ("COMITTMP: remove %s\n", tfh_rec->name);
#endif
    wccstat3 *rres = New wccstat3;
    diropargs3 rarg;
    rarg.dir = fsrv->sfs_trash_fhs[fsno];
    rarg.name = tfh_rec->name;
    fsrv->c->call (NFSPROC3_REMOVE, &rarg, rres,
	           wrap (mkref (this), &client::removetmp_cb, rres),
		   authtab[authno]);
    fhtab.tab.remove(tfh_rec);
    delete tfh_rec;
  }
}

void
client::removetmp_cb (wccstat3 *res, clnt_stat err)
{
  delete res;
}

void
client::chunk_data 
  (Chunker *chunker, const unsigned char *data, size_t count, off_t)
{
  chunker->chunk(data, count);
}

void
client::committmp (svccb *sbp, filesrv::reqstate rqs)
{
  lbfs_committmp3args *cta = sbp->template getarg<lbfs_committmp3args> ();
  Chunker *chunker = New Chunker(CHUNK_SIZES(0));
  nfs3_copy (fsrv->c, cta->commit_from, cta->commit_to,
             wrap(mkref(this), &client::chunk_data, chunker),
             wrap(mkref(this), &client::committmp_cb, sbp, rqs, chunker));
}

void 
client::getfp_cb (svccb *sbp, filesrv::reqstate rqs, Chunker *chunker,
                  size_t count, read3res *rres, str err)
{
  lbfs_getfp3args *arg = sbp->template getarg<lbfs_getfp3args> ();
  if (!err && rres->resok->eof) 
    chunker->stop();
  const vec<lbfs_chunk *>& cv = chunker->chunk_vector();
  lbfs_getfp3res *res = New lbfs_getfp3res;
  if (!err) {
    unsigned i = 0;
    unsigned n = cv.size() < 1024 ? cv.size() : 1024;
    size_t off = cv[0]->loc.pos();
    res->resok->fprints.setsize(n);
    for (; i<n; i++) {
      struct lbfs_fp3 x;
      x.count = cv[i]->loc.count();
      x.fingerprint = cv[i]->fingerprint;
      cv[i]->get_hash(x.hash);
      res->resok->fprints[i] = x;
#if DEBUG > 2
      warn << "GETFP: " << off+arg->offset << " " << cv[i]->fingerprint 
	   << " " << armor32(x.hash.base(), sha1::hashsize) << "\n";
#endif
      off += x.count;
    }
    res->resok->eof=rres->resok->eof;
    res->resok->file_attributes = 
      *(reinterpret_cast<ex_post_op_attr*>(&(rres->resok->file_attributes)));
#if DEBUG > 1
    warn << "GETFP: " << arg->offset << " returned " << n 
         << " eof " << res->resok->eof << "\n";
#endif
    nfs3reply (sbp, res, rqs, RPC_SUCCESS);
  }
  else {
#if DEBUG > 0
    warn << "GETFP: failed " << err << "\n";
#endif
    if (rres->status) {
      res->set_status(rres->status);
      nfs3_exp_enable (NFSPROC3_READ, rres);
      *(res->resfail) = *((reinterpret_cast<ex_read3res*>(rres))->resfail);
      nfs3reply (sbp, res, rqs, RPC_SUCCESS);
    }
    else
      nfs3reply (sbp, res, rqs, RPC_FAILED);
  }
  delete chunker;
}

void
client::getfp (svccb *sbp, filesrv::reqstate rqs)
{
  lbfs_getfp3args *arg = sbp->template getarg<lbfs_getfp3args> ();
#if DEBUG > 1
  warn << "GETFP: ask for " << arg->offset << " and " << arg->count << "\n"; 
#endif
  Chunker *chunker = New Chunker(CHUNK_SIZES(0), true);
  nfs3_read 
    (fsrv->c, arg->file, 
     wrap(mkref(this), &client::chunk_data, chunker),
     wrap(mkref(this), &client::getfp_cb, sbp, rqs, chunker),
     arg->offset, arg->count);
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
    lbfs_nfs3exp_err (sbp, NFS3ERR_BADHANDLE);
    return;
  }

  filesrv::reqstate rqs;
  if (!fsrv->fixarg (sbp, &rqs))
    return;

  if (sbp->proc () == lbfs_MKTMPFILE)
    mktmpfile(sbp, rqs);
  else if (sbp->proc () == lbfs_COMMITTMP)
    committmp(sbp, rqs);
  else if (sbp->proc () == lbfs_CONDWRITE)
    condwrite(sbp, rqs);
  else if (sbp->proc () == lbfs_GETFP)
    getfp(sbp, rqs);
  else {
    if (sbp->proc () == NFSPROC3_LOOKUP) 
      warn ("server: %lu %lu\n", xc->bytes_sent, xc->bytes_recv);
    void *res = nfs_program_3.tbl[sbp->proc ()].alloc_res ();
    if (sbp->proc () == NFSPROC3_RENAME)
      fsrv->c->call (sbp->proc (), sbp->template getarg<void> (), res,
		     wrap (mkref (this), &client::renamecb_1, sbp, res, rqs),
		     authtab[authno]);
    else
      fsrv->c->call (sbp->proc (), sbp->template getarg<void> (), res,
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

client::client (ref<axprt_crypt> x)
  : sfsserv (x), fsrv (NULL), generation (nextgen ())
{
  nfssrv = asrv::alloc (x, lbfs_program_3,
			wrap (mkref (this), &client::nfs3dispatch));
  nfscbc = aclnt::alloc (x, lbfscb_program_3);
  authtab[0] = authunix_create ("localhost", (uid_t) -1,
				(gid_t) -1, 0, NULL);
  clienttab.insert (this);

  fpdb.open (FP_DB);
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
#if 0
    if (typeid (*x) != typeid (axprt_compress))
      panic ("client::sfs_getfsinfo %s != %s\n",
	     typeid (*x).name (), typeid (axprt_compress).name ());
    static_cast<axprt_compress *> (x.get ())->compress ();
#endif
  }
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


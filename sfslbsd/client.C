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
#include "crypt.h"
#include "serial.h"
#include "sfsrwsd.h"
#include <grp.h>
#include "arpc.h"

#include "lbfsdb.h"
#include "fingerprint.h"
#include "lbfs.h"

#define DEBUG 1
#define KEEP_TMP_VERSIONS 1

struct timeval t0;
struct timeval t1;
inline unsigned timediff() {
  return (t1.tv_sec*1000000+t1.tv_usec)-(t0.tv_sec*1000000+t0.tv_usec);
}

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
                             fp_db::iterator *iter, Chunker *chunker0,
			     unsigned char *data, 
			     size_t count, read3res *, str err)
{
  lbfs_condwrite3args *cwa = sbp->template getarg<lbfs_condwrite3args> ();
  chunker0->stop();
  const vec<chunk *>& cv = chunker0->chunk_vector();

  if (err || count != cwa->count || cv.size() != 1 || 
      cv[0]->fingerprint() != cwa->fingerprint || !cv[0]->hash_eq(cwa->hash)) {
#if DEBUG > 0
    if (err) 
      warn << "CONDWRITE: error reading file: " << err << "\n";
    else if (count != cwa->count)
      warn << "CONDWRITE: size does not match, old chunk? " 
	   << "want " << cwa->count << " got " << count << "\n";
    else 
    if (cv.size() != 1 || cv[0]->fingerprint() != cwa->fingerprint)
      warn << "CONDWRITE: fingerprint mismatch\n";
    else 
      warn << "CONDWRITE: sha1 hash mismatch\n";
#endif
    delete[] data;
    delete chunker0;
    iter->del(); 
    chunk_location c;
    if (!iter->next(&c)) { 
      nfs_fh3 fh; 
      c.get_fh(fh); 
      Chunker *chunker = New Chunker(true);
      unsigned char *buf = New unsigned char[c.count()];
      nfs3_read
	(fsrv->c, fh, 
	 c.pos(), c.count(),
	 wrap(mkref(this), &client::condwrite_read_cb, buf, c.pos(), chunker), 
	 wrap(mkref(this), &client::condwrite_got_chunk, 
	      sbp, rqs, iter, chunker, buf));
      return; 
    }
  }
 
  else {
#if DEBUG > 1
    // fingerprint matches, do write
    warn << "CONDWRITE: bingo, found a condwrite candidate\n";
#endif
    nfs3_write(fsrv->c, cwa->file, 
	       wrap(mkref(this), &client::condwrite_write_cb, 
		    sbp, rqs, cwa->count),
	       data, cwa->offset, cwa->count, UNSTABLE);
    
    delete chunker0;
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
  if (!err || res->status) {
    *wres = *res;
    if (!res->status)
      wres->resok->count = count;
    nfs3reply(sbp, wres, rqs, RPC_SUCCESS);
  }
  else
    nfs3reply(sbp, wres, rqs, RPC_FAILED);
}

void
client::condwrite_read_cb(unsigned char *buf, off_t pos0, Chunker *chunker,
                          const unsigned char *data, size_t count, off_t pos)
{
  memmove(buf+(pos-pos0), data, count);
  chunker->chunk_data(data, count);
}

void
client::condwrite (svccb *sbp, filesrv::reqstate rqs)
{
  lbfs_condwrite3args *cwa = sbp->template getarg<lbfs_condwrite3args> ();
    
  tmpfh_record *tfh_rec = fhtab.tab[cwa->file]; 
  if (tfh_rec) 
    tfh_rec->chunks.push_back 
      (New chunk(cwa->offset, cwa->count, cwa->fingerprint));

  fp_db::iterator *iter = 0;
  if (fpdb.get_iterator(cwa->fingerprint, &iter) == 0) {
    if (iter) { 
      chunk_location c; 
      if (!iter->get(&c)) { 
	nfs_fh3 fh; 
	c.get_fh(fh);
        Chunker *chunker = New Chunker(true);
	unsigned char *buf = New unsigned char[c.count()];
	nfs3_read
	  (fsrv->c, fh,
	   c.pos(), c.count(),
	   wrap(mkref(this), &client::condwrite_read_cb, buf, c.pos(),chunker),
	   wrap(mkref(this), &client::condwrite_got_chunk,
	        sbp, rqs, iter, chunker, buf));
	return;
      } 
      delete iter; 
    }
  }
#if DEBUG > 0
  warn << "CONDWRITE: " << cwa->fingerprint << " not in DB\n";
#endif
  lbfs_nfs3exp_err (sbp, NFS3ERR_FPRINTNOTFOUND);
}

void
client::mktmpfile_cb (svccb *sbp, filesrv::reqstate rqs, 
                      nfs_fh3 dir, char *path,
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
        delete[] path;
	delete cres;
	mktmpfile(sbp, rqs);
	break;
      default:
	if (!cres->status) 
	  fhtab.tab.insert 
	    (New tmpfh_record
	     (*(cres->resok->obj.handle),dir,path,strlen(path)));
	delete[] path;
	nfs3reply (sbp, _cres, rqs, RPC_SUCCESS);
    }
  }
}

void
client::mktmpfile (svccb *sbp, filesrv::reqstate rqs)
{
  lbfs_mktmpfile3args *mta = sbp->template getarg<lbfs_mktmpfile3args> ();

  unsigned r = fsrv->get_oscar(rqs.fsno);
  str rstr = armor32((void*)&r, sizeof(r));
  char *tmpfile = New char[7+rstr.len()+1];
  sprintf(tmpfile, "oscar.%s", rstr.cstr());
#if DEBUG > 1
  warn << "MKTMPFILE: " << tmpfile << "\n";
#endif
  
  u_int32_t authno = sbp->getaui ();
  create3args c3arg;
  c3arg.where.dir = fsrv->sfs_trash[rqs.fsno].subdirs[r%SFS_TRASH_DIR_BUCKETS];
  c3arg.where.name = tmpfile;
  c3arg.how.set_mode(GUARDED);
  *(c3arg.how.obj_attributes) = mta->obj_attributes;
  (c3arg.how.obj_attributes)->uid.set_set(false);
  (c3arg.how.obj_attributes)->gid.set_set(false);

  void *cres = nfs_program_3.tbl[NFSPROC3_CREATE].alloc_res ();
  fsrv->c->call (NFSPROC3_CREATE, &c3arg, cres,
		 wrap (mkref (this), &client::mktmpfile_cb, 
		       sbp, rqs, c3arg.where.dir, tmpfile, cres),
		 authtab[authno]);
  fsrv->update_oscar(rqs.fsno);
}

void
client::committmp_cb (svccb *sbp, filesrv::reqstate rqs,
                      commit3res *res, str err)
{
  lbfs_committmp3args *cta = sbp->template getarg<lbfs_committmp3args> ();
  nfs_fh3 tmpfh = cta->commit_from;
  nfs_fh3 fh = cta->commit_to;
  u_int32_t authno = sbp->getaui ();
  unsigned fsno = rqs.fsno;
 
#if DEBUG > 1
  gettimeofday(&t1, 0L);
  warn << "committmp: " << timediff() << " usecs\n";
#endif

  commit3res *cres = New commit3res;
  if (!res)
    nfs3reply (sbp, cres, rqs, RPC_FAILED);
  else if (!err || res->status) {
    *cres = *res;
    nfs3reply (sbp, cres, rqs, RPC_SUCCESS);
  }
  else {
    *cres = *res;
    nfs3reply (sbp, cres, rqs, RPC_FAILED);
  }

  tmpfh_record *tfh_rec = fhtab.tab[tmpfh];
  if (tfh_rec) {
    for (unsigned i=0; i<tfh_rec->chunks.size(); i++) {
      chunk *c = tfh_rec->chunks[i];
#if KEEP_TMP_VERSIONS
      c->location().set_fh(tmpfh);
      fpdb.add_entry(c->fingerprint(), &(c->location()));
#endif
      c->location().set_fh(fh);
      fpdb.add_entry(c->fingerprint(), &(c->location()));
#if DEBUG > 1
      warn << "COMMITTMP: adding " << c->fingerprint() << " @"
	   << c->location().pos() << " " 
	   << c->location().count() << " to database\n";
#endif
    }
    fpdb.sync();
    tfh_rec->name[tfh_rec->len] = '\0';

#if KEEP_TMP_VERSIONS == 0
    warn ("COMMITTMP: remove %s\n", tfh_rec->name);
    wccstat3 *rres = New wccstat3;
    diropargs3 rarg;
    rarg.dir = tfh_rec->dir;
    rarg.name = tfh_rec->name;
    fsrv->c->call (NFSPROC3_REMOVE, &rarg, rres,
	           wrap (mkref (this), &client::removetmp_cb, rres),
		   authtab[authno]);
#endif

    fhtab.tab.remove(tfh_rec);
    delete tfh_rec;
  }
}

void
client::movetmp_cb (rename3res *res, clnt_stat err)
{
  if (err || res->status)
    warn << "movetmp_cb error\n";
  delete res;
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
  chunker->chunk_data(data, count);
}

void
read_cb_nop (const unsigned char *data, size_t count, off_t)
{
}

void
client::committmp (svccb *sbp, filesrv::reqstate rqs)
{
  lbfs_committmp3args *cta = sbp->template getarg<lbfs_committmp3args> ();
#if DEBUG > 1
  gettimeofday(&t0, 0L);
#endif
  nfs3_copy (fsrv->c, cta->commit_from, cta->commit_to,
             wrap(read_cb_nop),
             wrap(mkref(this), &client::committmp_cb, sbp, rqs));
}

void 
client::getfp_cb (svccb *sbp, filesrv::reqstate rqs, Chunker *chunker,
                  size_t count, read3res *rres, str err)
{
#if DEBUG > 1
  lbfs_getfp3args *arg = sbp->template getarg<lbfs_getfp3args> ();
#endif
  if (!err && !rres->status && rres->resok->eof) 
    chunker->stop();
  const vec<chunk *>& cv = chunker->chunk_vector();
  lbfs_getfp3res *res = New lbfs_getfp3res;
  if (!err && !rres->status) {
    unsigned i = 0;
    unsigned n = cv.size() < 1024 ? cv.size() : 1024;
    res->resok->fprints.setsize(n);
    for (; i<n; i++) {
      struct lbfs_fp3 x;
      x.fingerprint = cv[i]->fingerprint();
      x.hash = cv[i]->hash();
      x.count = cv[i]->location().count();
      res->resok->fprints[i] = x;
#if DEBUG > 2
      warn << "GETFP: " << cv[i]->fingerprint() << " " 
	   << armor32(x.hash.base(), sha1::hashsize) << "\n";
#endif
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
#if DEBUG > 1
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

#if DEBUG > 2
  gettimeofday(&t1, NULL);
  warn << "GETFP in " << timediff() << " usecs\n";
  fflush(stdout);
  fflush(stderr);
#endif
  delete chunker;
}

void
client::getfp (svccb *sbp, filesrv::reqstate rqs)
{
  lbfs_getfp3args *arg = sbp->template getarg<lbfs_getfp3args> ();
#if DEBUG > 1
  warn << "GETFP: ask @" << arg->offset << " +" << arg->count << "\n"; 
#endif
  Chunker *chunker = New Chunker(true);
  nfs3_read 
    (fsrv->c, arg->file, 
     arg->offset, arg->count,
     wrap(mkref(this), &client::chunk_data, chunker),
     wrap(mkref(this), &client::getfp_cb, sbp, rqs, chunker));
}

void 
client::oscar_add_cb (svccb *sbp, filesrv::reqstate rqs, 
                      link3res *lnres, clnt_stat err)
{
#if DEBUG > 0
  if (err) 
    warn << "oscar_add_cb: failed\n";
#endif
  normal_dispatch(sbp, rqs);
  delete lnres;
  fsrv->update_oscar(rqs.fsno);
}

void
client::oscar_lookup_cb (svccb *sbp, filesrv::reqstate rqs, 
                         lookup3res *lres, clnt_stat err)
{
  if (!err && !lres->status && lres->resok->obj_attributes.present)
    oscar_add(sbp, rqs, lres->resok->object);
  else
    normal_dispatch(sbp, rqs);
  delete lres;
}

void
client::oscar_add (svccb *sbp, filesrv::reqstate rqs, nfs_fh3 fh)
{
  u_int32_t authno = sbp->getaui ();
  link3args lnarg;
  lnarg.file = fh;
  unsigned r = fsrv->get_oscar(rqs.fsno);
  str rstr = armor32((void*)&r, sizeof(r));
  char tmpfile[7+rstr.len()+1];
  sprintf(tmpfile, "oscar.%s", rstr.cstr());
  lnarg.link.name = tmpfile;
  lnarg.link.dir = fsrv->sfs_trash[rqs.fsno].subdirs[r%SFS_TRASH_DIR_BUCKETS];
  link3res *lnres = New link3res;
  fsrv->c->call (NFSPROC3_LINK, &lnarg, lnres,
	         wrap (mkref (this), &client::oscar_add_cb, sbp, rqs, lnres),
		 authtab[authno]);
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
  else if (sbp->proc () == lbfs_GETFP) {
#if DEBUG > 2
    gettimeofday(&t0, NULL);
#endif
    getfp(sbp, rqs);
  }
#if KEEP_TMP_VERSIONS == 0
  // keep removed files, so we can use their chunks
  else if (sbp->proc () == NFSPROC3_REMOVE) {
    diropargs3 *arg = sbp->template getarg<diropargs3> ();
    diropargs3 larg;
    larg.dir = arg->dir;
    larg.name = arg->name;
    lookup3res *res = New lookup3res;
    fsrv->c->call (NFSPROC3_LOOKUP, &larg, res,
	           wrap (mkref(this), &client::oscar_lookup_cb, sbp, rqs, res),
	           authtab[authno]);
  }
  // why do we do this again?
  else if (sbp->proc () == NFSPROC3_RENAME) {
    rename3args *arg = sbp->template getarg<rename3args> ();
    diropargs3 larg;
    larg.dir = arg->to.dir;
    larg.name = arg->to.name;
    lookup3res *res = New lookup3res;
    fsrv->c->call (NFSPROC3_LOOKUP, &larg, res,
	           wrap (mkref(this), &client::oscar_lookup_cb, sbp, rqs, res),
	           authtab[authno]);
  }
#endif
  else {
#if DEBUG > 2
    if (sbp->proc () == NFSPROC3_LOOKUP) 
      warn ("server: %lu %lu\n", xc->bytes_sent, xc->bytes_recv);
#endif
    normal_dispatch(sbp, rqs);
  }
}
    
void 
client::normal_dispatch (svccb *sbp, filesrv::reqstate rqs)
{
  u_int32_t authno = sbp->getaui ();
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

u_int64_t
client::nextgen ()
{
  static u_int64_t g;
  while (clienttab[++g] || !g)
    ;
  return g;
}

client::client (ref<axprt_crypt> xx)
  : sfsserv (xx, axprt_compress::alloc (xx)), fsrv (NULL),
    generation (nextgen ())
{
  nfssrv = asrv::alloc (x, lbfs_program_3,
			wrap (mkref (this), &client::nfs3dispatch));
  nfscbc = aclnt::alloc (x, lbfscb_program_3);
  authtab[0] = authunix_create ("localhost", (uid_t) 32767,
				(gid_t) 9999, 0, NULL);
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
#if 1
    if (typeid (*x) != typeid (refcounted<axprt_compress>))
      panic ("client::sfs_getfsinfo %s != %s\n",
	     typeid (*x).name (), typeid (refcounted<axprt_compress>).name ());
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
client::launch (ref<axprt_crypt> xc)
{
  vNew refcounted<client> (xc);
}

void
client_accept (ptr<axprt_crypt> x)
{
  if (!x)
    fatal ("EOF from sfssd\n");
  client::launch (x);
}


/*
 *
 * Copyright (C) 2001 Benjie Chen (benjie@lcs.mit.edu)
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
#include "sfslbsd.h"
#include <grp.h>
#include "arpc.h"

#include "lbfsdb.h"
#include "fingerprint.h"
#include "lbfs.h"

int lbsd_trace = (getenv("LBSD_TRACE") ? atoi (getenv ("LBSD_TRACE")) : 0);

static struct timeval t0;
static struct timeval t1;
static inline unsigned timediff() 
{
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
  rqs.c->call (NFSPROC3_LOOKUP, &sbp->template getarg<rename3args> ()->to,
               ares, wrap (mkref (this), &client::renamecb_2,
		           sbp, res, rqs, ares), auth);
}

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
      !cv[0]->hash_eq(cwa->hash)) {
    if (lbsd_trace > 1) {
      if (err) 
        warn << "CONDWRITE: error reading file: " << err << "\n";
      else if (count != cwa->count)
        warn << "CONDWRITE: size does not match, old chunk? " 
	     << "want " << cwa->count << " got " << count << "\n";
      else 
        warn << "CONDWRITE: sha1 hash mismatch\n";
    }
    delete[] data;
    delete chunker0;
    iter->del(); 
    chunk_location c;
    if (!iter->next(&c)) {
      nfs_fh3 fh; 
      c.get_fh(fh); 
      Chunker *chunker = New Chunker;
      unsigned char *buf = New unsigned char[c.count()];
      nfs3_read
	(rqs.c, fh, 
	 c.pos(), c.count(),
	 wrap(mkref(this), &client::condwrite_read_cb, buf, c.pos(), chunker), 
	 wrap(mkref(this), &client::condwrite_got_chunk, 
	      sbp, rqs, iter, chunker, buf));
      return; 
    }
  }
 
  else {
    if (lbsd_trace > 1)
      warn << "CONDWRITE: bingo, found a condwrite candidate\n";

    ufd_rec *u = ufdtab.tab[cwa->fd];
    if (!u) {
      lbfs_nfs3exp_err (sbp, NFS3ERR_NOENT);
      return;
    }
    nfs_fh3 fh = u->fh;
    nfs3_write(rqs.c, fh,
	       wrap(mkref(this), &client::condwrite_write_cb, 
		    sbp, rqs, cwa->count),
	       data, cwa->offset, cwa->count, UNSTABLE);
    
    delete chunker0;
    delete iter;
    fsrv->db_dirty();
    return;
  }

  delete iter;
  if (lbsd_trace > 0)
    warn << "CONDWRITE: ran out of files to try\n";
  lbfs_nfs3exp_err (sbp, NFS3ERR_FPRINTNOTFOUND);
  fsrv->db_dirty();
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
  else {
    lbfs_condwrite3args *cwa = sbp->template getarg<lbfs_condwrite3args> ();
    ufd_rec *u = ufdtab.tab[cwa->fd];
    if (u)
      u->error = true;
    nfs3reply(sbp, wres, rqs, RPC_FAILED);
  }
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
    
  ufd_rec *u = ufdtab.tab[cwa->fd]; 
  if (u) {
    if (u->inuse)
      u->chunks.push_back
        (New chunk(cwa->offset, cwa->count, cwa->hash));
    else {
      u->sbps.push_back(sbp);
      return;
    }
  }
  else {
    lbfs_nfs3exp_err (sbp, NFS3ERR_NOENT);
    return;
  }

  fp_db::iterator *iter = 0;
  u_int64_t index;
  memmove(&index, cwa->hash.base(), sizeof(index));
  if (fsrv->fpdb.get_iterator(index, &iter) == 0) {
    if (iter) { 
      chunk_location c; 
      if (!iter->get(&c)) { 
	nfs_fh3 fh; 
	c.get_fh(fh);
        Chunker *chunker = New Chunker;
	unsigned char *buf = New unsigned char[c.count()];
	nfs3_read
	  (rqs.c, fh,
	   c.pos(), c.count(),
	   wrap(mkref(this), &client::condwrite_read_cb, buf, c.pos(),chunker),
	   wrap(mkref(this), &client::condwrite_got_chunk,
	        sbp, rqs, iter, chunker, buf));
	return;
      } 
      delete iter; 
    }
  }
  if (lbsd_trace)
    warn << "CONDWRITE: " << index << " not in DB\n";
  lbfs_nfs3exp_err (sbp, NFS3ERR_FPRINTNOTFOUND);
}

void
client::tmpwrite_cb (svccb *sbp, filesrv::reqstate rqs, 
                     write3res *wres, clnt_stat err)
{
  lbfs_tmpwrite3args *twa = sbp->template getarg<lbfs_tmpwrite3args> ();
  ufd_rec *u = ufdtab.tab[twa->fd];
  if (!u) {
    lbfs_nfs3exp_err (sbp, NFS3ERR_NOENT);
    return;
  }
  u->writes--;
  if (!err && !wres->status)
    nfs3reply(sbp, wres, rqs, RPC_SUCCESS);
  else {
    u->error = true;
    nfs3reply(sbp, wres, rqs, err ? RPC_FAILED : RPC_SUCCESS);
  }

  // if no more writes, finish off by doing the commit. in absolutely horrible
  // case there might be some more writes left to be done as well.
  if (u->writes == 0) {
    for (size_t i=0; i<u->sbps.size(); i++)
      demux(u->sbps[i],rqs);
    u->sbps.setsize(0);
  }
}

void
client::tmpwrite (svccb *sbp, filesrv::reqstate rqs)
{
  lbfs_tmpwrite3args *fwa = sbp->template getarg<lbfs_tmpwrite3args> ();
  ufd_rec *u = ufdtab.tab[fwa->fd];
  if (u) {
    if (u->inuse && u->writes < WRITES_MAX) {
      u_int32_t authno = sbp->getaui ();
      write3args warg;
      warg.file = u->fh;
      warg.offset = fwa->offset;
      warg.count = fwa->count;
      warg.stable = fwa->stable;
      warg.data = fwa->data;
      u->writes++;
      write3res *res = New write3res;
      rqs.c->call(NFSPROC3_WRITE, &warg, res,
		  wrap (mkref (this), &client::tmpwrite_cb, sbp, rqs, res),
	          authtab[authno]);
    }
    else
      u->sbps.push_back(sbp);
  }
  else
    lbfs_nfs3exp_err (sbp, NFS3ERR_NOENT);
}

void
client::mktmpfile_cb (svccb *sbp, filesrv::reqstate rqs, 
                      nfs_fh3 dir, int sfd, void *_cres, clnt_stat err)
{
  diropres3 *cres = static_cast<diropres3 *>(_cres);
  if (err)
    nfs3reply (sbp, _cres, rqs, err);
  else {
    switch(cres->status) {
      case NFS3ERR_EXIST:
	delete cres;
	mktmpfile(sbp, rqs);
	break;
      default:
	if (!cres->status) {
          lbfs_mktmpfile3args *mta =
	    sbp->template getarg<lbfs_mktmpfile3args> ();
	  ufd_rec *u = ufdtab.tab[mta->fd];
	  assert(u);
	  str rstr = armor32((void*)&sfd, sizeof(sfd));
          char tmpfile[7+rstr.len()+1];
          sprintf(tmpfile, "oscar.%s", rstr.cstr());
	  u->use(*(cres->resok->obj.handle),dir,tmpfile,strlen(tmpfile),sfd);
	  for (size_t i=0; i<u->sbps.size(); i++)
	    demux(u->sbps[i],rqs);
	  u->sbps.setsize(0);
	}
	nfs3reply (sbp, _cres, rqs, RPC_SUCCESS);
    }
  }
}

void
client::mktmpfile (svccb *sbp, filesrv::reqstate rqs)
{
  lbfs_mktmpfile3args *mta = sbp->template getarg<lbfs_mktmpfile3args> ();
  ufd_rec *u = ufdtab.tab[mta->fd];

  if (!u)
    ufdtab.tab.insert(New ufd_rec (mta->fd));

  int r = fsrv->get_trashent(rqs.fsno);
  if (r < 0) {
    warn << "sfslbsd: cannot create tmp file!\n";
    fsrv->update_trashent(rqs.fsno);
    return;
  }
  str rstr = armor32((void*)&r, sizeof(r));
  char tmpfile[7+rstr.len()+1];
  sprintf(tmpfile, "oscar.%s", rstr.cstr());
  if (lbsd_trace > 2)
    warn << "MKTMPFILE: " << tmpfile << "\n";
  
  u_int32_t authno = sbp->getaui ();
  create3args c3arg;
  c3arg.where.dir = fsrv->sfs_trash[rqs.fsno].subdirs[r%SFS_TRASH_DIR_BUCKETS];
  c3arg.where.name = tmpfile;
  c3arg.how.set_mode(GUARDED);
  *(c3arg.how.obj_attributes) = mta->obj_attributes;
  (c3arg.how.obj_attributes)->uid.set_set(false);
  (c3arg.how.obj_attributes)->gid.set_set(false);

  void *cres = nfs_program_3.tbl[NFSPROC3_CREATE].alloc_res ();
  rqs.c->call (NFSPROC3_CREATE, &c3arg, cres,
               wrap (mkref (this), &client::mktmpfile_cb, 
		     sbp, rqs, c3arg.where.dir, r, cres), authtab[authno]);
  fsrv->update_trashent(rqs.fsno);
}

void
client::committmp_cb (svccb *sbp, filesrv::reqstate rqs,
                      commit3res *res, str err)
{
  lbfs_committmp3args *cta = sbp->template getarg<lbfs_committmp3args> ();
  nfs_fh3 fh = cta->commit_to;
 
  if (lbsd_trace > 2) {
    gettimeofday(&t1, 0L);
    warn << "COMMITTMP: " << timediff() << " usecs\n";
  }

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

  ufd_rec *u = ufdtab.tab[cta->fd];
  if (u) {
    for (unsigned i=0; i<u->chunks.size(); i++) {
      chunk *c = u->chunks[i];
      c->location().set_fh(u->fh);
      fsrv->fpdb.add_entry(c->hashidx(),
	                   &(c->location()), c->location().size());
      c->location().set_fh(fh);
      fsrv->fpdb.add_entry(c->hashidx(), 
	                   &(c->location()), c->location().size());
      if (lbsd_trace > 2) 
      {
        warn << "COMMITTMP: adding " << c->hashidx() << " @"
	     << c->location().pos() << " " 
	     << c->location().count() << " to database "
	     << c->location().size() << " bytes\n";
      }
    }
    u->name[u->len] = '\0';
    fsrv->clear_trashent(rqs.fsno, u->srv_fd);
    ufdtab.tab.remove(u);
    delete u;
  }
  fsrv->db_dirty();
}

void
client::committmp (svccb *sbp, filesrv::reqstate rqs)
{
  lbfs_committmp3args *cta = sbp->template getarg<lbfs_committmp3args> ();
  
  ufd_rec *u = ufdtab.tab[cta->fd]; 
  if (u) {
    if (!u->inuse || u->writes > 0)
      u->sbps.push_back(sbp);
    else if (u->error)
      lbfs_nfs3exp_err (sbp, NFS3ERR_IO); // correct errno to use?
    else {
      if (lbsd_trace > 2)
        gettimeofday(&t0, 0L);
      nfs3_copy (rqs.c, u->fh, cta->commit_to,
                 wrap(read_cb_nop),
                 wrap(mkref(this), &client::committmp_cb, sbp, rqs));
    }
  }
  else
    lbfs_nfs3exp_err (sbp, NFS3ERR_NOENT);
}

void
client::aborttmp (svccb *sbp, filesrv::reqstate rqs)
{
  lbfs_committmp3args *cta = sbp->template getarg<lbfs_committmp3args> ();
  
  ufd_rec *u = ufdtab.tab[cta->fd]; 
  if (u) {
    if (u->inuse) {
      for (unsigned i=0; i<u->chunks.size(); i++) {
        chunk *c = u->chunks[i];
        c->location().set_fh(u->fh);
        fsrv->fpdb.add_entry
	  (c->hashidx(), &(c->location()), c->location().size());
        if (lbsd_trace > 2) {
          warn << "ABORTTMP: adding " << c->hashidx() << " @"
	       << c->location().pos() << " " 
	       << c->location().count() << " to database\n";
        }
      }
    }
    else {
      for (size_t i=0; i<u->sbps.size(); i++)
        lbfs_nfs3exp_err (u->sbps[i], NFS3ERR_ABORTED);
      u->sbps.setsize(0);
    }
    fsrv->clear_trashent(rqs.fsno, u->srv_fd);
    ufdtab.tab.remove(u);
    delete u;
    sbp->reply (NULL);
  }
  else
    lbfs_nfs3exp_err (sbp, NFS3ERR_NOENT);
  fsrv->db_dirty();
}

void 
client::getfp_cb (svccb *sbp, filesrv::reqstate rqs, Chunker *chunker,
                  size_t count, read3res *rres, str err)
{
  lbfs_getfp3args *arg = 0;
  if (lbsd_trace > 2) 
    arg = sbp->template getarg<lbfs_getfp3args> ();
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
      x.hash = cv[i]->hash();
      x.count = cv[i]->location().count();
      res->resok->fprints[i] = x;
      if (lbsd_trace > 3)
        warn << "GETFP: " << cv[i]->hashidx() << " " 
	     << armor32(x.hash.base(), sha1::hashsize) << "\n";
    }
    res->resok->eof=rres->resok->eof;
    res->resok->file_attributes = 
      *(reinterpret_cast<ex_post_op_attr*>(&(rres->resok->file_attributes)));
    if (lbsd_trace > 2)
      warn << "GETFP: " << arg->offset << " returned " << n 
           << " eof " << res->resok->eof << "\n";
    nfs3reply (sbp, res, rqs, RPC_SUCCESS);
  }
  else {
    if (lbsd_trace > 1)
      warn << "GETFP: failed " << err << "\n";
    if (rres->status) {
      res->set_status(rres->status);
      nfs3_exp_enable (NFSPROC3_READ, rres);
      *(res->resfail) = *((reinterpret_cast<ex_read3res*>(rres))->resfail);
      nfs3reply (sbp, res, rqs, RPC_SUCCESS);
    }
    else
      nfs3reply (sbp, res, rqs, RPC_FAILED);
  }

  if (lbsd_trace > 2) {
    gettimeofday(&t1, NULL);
    warn << "GETFP in " << timediff() << " usecs\n";
    fflush(stdout);
    fflush(stderr);
  }
  delete chunker;
}

void
client::getfp (svccb *sbp, filesrv::reqstate rqs)
{
  lbfs_getfp3args *arg = sbp->template getarg<lbfs_getfp3args> ();
  if (lbsd_trace > 1)
    warn << "GETFP: ask @" << arg->offset << " +" << arg->count << "\n"; 
  if (lbsd_trace > 2)
    gettimeofday(&t0, NULL);
  Chunker *chunker = New Chunker;
  nfs3_read 
    (rqs.c, arg->file, 
     arg->offset, arg->count,
     wrap(mkref(this), &client::chunk_data, chunker),
     wrap(mkref(this), &client::getfp_cb, sbp, rqs, chunker));
}

void 
client::trashent_link_cb (svccb *sbp, filesrv::reqstate rqs, 
                          link3res *lnres, clnt_stat err)
{
  if (lbsd_trace > 1 && err)
    warn << "trashent_link_cb: failed\n";
  normal_demux(sbp, rqs);
  delete lnres;
}

void
client::trashent_lookup_cb (svccb *sbp, filesrv::reqstate rqs, 
                            lookup3res *lres, clnt_stat err)
{
  if (!err && !lres->status && lres->resok->obj_attributes.present)
    trashent_link(sbp, rqs, lres->resok->object);
  else
    normal_demux(sbp, rqs);
  delete lres;
}

void
client::trashent_link (svccb *sbp, filesrv::reqstate rqs, nfs_fh3 fh)
{
  u_int32_t authno = sbp->getaui ();
  link3args lnarg;
  lnarg.file = fh;
  int r = fsrv->get_trashent(rqs.fsno);
  if (r < 0) {
    warn << "sfslbsd: cannot link into trash directory!\n";
    fsrv->update_trashent(rqs.fsno);
    return;
  }
  str rstr = armor32((void*)&r, sizeof(r));
  char tmpfile[7+rstr.len()+1];
  sprintf(tmpfile, "oscar.%s", rstr.cstr());
  lnarg.link.name = tmpfile;
  lnarg.link.dir = fsrv->sfs_trash[rqs.fsno].subdirs[r%SFS_TRASH_DIR_BUCKETS];
  link3res *lnres = New link3res;
  rqs.c->call (NFSPROC3_LINK, &lnarg, lnres,
	       wrap(mkref(this), &client::trashent_link_cb, sbp, rqs, lnres),
	       authtab[authno]);
  fsrv->update_trashent(rqs.fsno);
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

  demux(sbp, rqs);
}

void
client::demux (svccb *sbp, filesrv::reqstate rqs)
{
  if (sbp->proc () == lbfs_MKTMPFILE)
    mktmpfile(sbp, rqs);
  else if (sbp->proc () == lbfs_COMMITTMP)
    committmp(sbp, rqs);
  else if (sbp->proc () == lbfs_TMPWRITE)
    tmpwrite(sbp, rqs);
  else if (sbp->proc () == lbfs_CONDWRITE)
    condwrite(sbp, rqs);
  else if (sbp->proc () == lbfs_GETFP)
    getfp(sbp, rqs);
  else if (sbp->proc () == lbfs_ABORTTMP)
    aborttmp(sbp, rqs);
  else {
    if (lbsd_trace > 2 && sbp->proc () == NFSPROC3_LOOKUP)
      warn ("server: %lu %lu\n", xc->bytes_sent, xc->bytes_recv);
    normal_demux(sbp, rqs);
  }
}
    
void 
client::normal_demux (svccb *sbp, filesrv::reqstate rqs)
{
  u_int32_t authno = sbp->getaui ();
  void *res = nfs_program_3.tbl[sbp->proc ()].alloc_res ();
  if (sbp->proc () == NFSPROC3_RENAME)
    rqs.c->call (sbp->proc (), sbp->template getarg<void> (), res,
		 wrap (mkref (this), &client::renamecb_1, sbp, res, rqs),
		 authtab[authno]);
  else
    rqs.c->call (sbp->proc (), sbp->template getarg<void> (), res,
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

client::client (ref<axprt_zcrypt> xx)
  : sfsserv (xx), fsrv (NULL),
    generation (nextgen ())
{
  nfssrv = asrv::alloc (x, lbfs_program_3,
			wrap (mkref (this), &client::nfs3dispatch));
  nfscbc = aclnt::alloc (x, lbfscb_program_3);
  authtab[0] = authunix_create ("localhost", (uid_t) 32767,
				(gid_t) 9999, 0, NULL);
  clienttab.insert (this);
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
    static_cast<axprt_zcrypt *> (x.get ())->compress ();
    warn << "turning compress on\n";
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


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

AUTH *auth_default = authunix_create_realids ();

class erraccum : public virtual refcount {
  const cbb cb;
  bool ok;
public:
  erraccum (cbb c) : cb (c), ok (true) {}
  ~erraccum () { (*cb) (ok); }
  void seterr () { ok = false; }
};

#define FSID_SHIFT 20

static inline u_int64_t
fsidino2ino (u_int64_t fsid, u_int64_t fileid)
{
  return fileid ^ (fsid << FSID_SHIFT);
}

inline bool
resok (erraccum *ea, str path, str err)
{
  if (!err)
    return true;
  warn << path << ": " << err << "\n";
  ea->seterr ();
  return false;
}

int
filesrv::path2fsidx (str path, size_t nfs)
{
  int best = 0;
  for (size_t i = 1; i < nfs; i++) {
    const str &iname = fstab[i].path_mntpt;
    if (iname.len () > fstab[best].path_mntpt.len ()
	&& iname.len () < path.len ()
	&& path[iname.len ()] == '/'
	&& !memcmp (iname, path, iname.len ()))
      best = i;
  }
  return best;
}

filesrv::filesrv ()
  : leasetime (60), st (synctab_alloc ())
{
}

void
filesrv::init (cb_t c)
{
  assert (!cb);
  cb = c;

  for (size_t i = 0; i < fstab.size (); i++)
    fstab[i].parent = &fstab[path2fsidx (fstab[i].path_mntpt, i)];
  ref<erraccum> ea (New refcounted<erraccum> (wrap (this,
						    &filesrv::gotroots)));
  for (size_t i = 0; i < fstab.size (); i++)
    findfs (NULL, fstab[i].path_root,
	    wrap (this, &filesrv::gotroot, ea, i), FINDFS_NOSFS);
}


void
filesrv::gotroot (ref<erraccum> ea, int i, ptr<nfsinfo> ni, str err)
{
  if (resok (ea, "findfs", err)) {
    fstab[i].host = ni->hostname;
    fstab[i].fh_root = ni->fh;
    fstab[i].c = ni->c;
    /* XXX - the fsid will vary accross server reboots if path_root is
     * an NFS mount point (particularly an automounted one). */
    fstab[i].fsid = ni->rdev;
  }
}

void
filesrv::gotroots (bool ok)
{
  if (!ok) {
    (*cb) (false);
    return;
  }

  ref<erraccum> ea (New refcounted<erraccum> (wrap (this, &filesrv::gotmps)));
  for (size_t i = 0; i < fstab.size (); i++) {
    lookupfh3 (fstab[i].c, fstab[i].fh_root, "",
	       wrap (this, &filesrv::gotrootattr, ea, i));
    lookupfh3 (fstab[i].parent->c, fstab[i].parent->fh_root,
	       substr (fstab[i].path_mntpt,
		       fstab[i].parent->path_mntpt.len ()),
	       wrap (this, &filesrv::gotmp, ea, i));
  }
}

void
filesrv::gotrootattr (ref<erraccum> ea, int i,
		      const nfs_fh3 *fhp, const FATTR3 *attr, str err)
{
  if (resok (ea, strbuf ("getattr: ") << fstab[i].path_root, err)) {
    // fstab[i].fsid = attr->fsid;
    fstab[i].fileid_root = attr->fileid;
  }
}

void
filesrv::gotmp (ref<erraccum> ea, int i,
		const nfs_fh3 *fhp, const FATTR3 *attr, str err)
{
  if (resok (ea, fstab[i].parent->path_root << fstab[i].path_mntpt, err)) {
    fstab[i].fh_mntpt = *fhp;
    fstab[i].fileid_mntpt = attr->fileid;
    fstab[i].parent->mp3tab.insert (&fstab[i]);
  }
}

void
filesrv::gotmps (bool ok)
{
  if (!ok) {
    (*cb) (false);
    return;
  }

  ref<erraccum> ea (New refcounted<erraccum> (wrap (this, &filesrv::gotdds)));
  for (size_t i = 0; i < fstab.size (); i++)
    for (int mp = 0; mp < 2; mp++) {
      ref<readdir3res> res (New refcounted<readdir3res>);
      rpc_clear (*res);
      res->resok->reply.entries.alloc ();
      rpc_clear (*res->resok->reply.entries);
      gotrdres (ea, res, i, mp, RPC_SUCCESS);
    }
}

void
filesrv::gotrdres (ref<erraccum> ea, ref<readdir3res> res,
		   int i, bool mp, clnt_stat stat)
{
  str path (strbuf ("readdir: ")
	    << (mp ? fstab[i].path_mntpt : fstab[i].path_root));
  if (!resok (ea, path, stat2str (res->status, stat)))
    return;

  entry3 *ep = res->resok->reply.entries;
  if (ep)
    for (;;) {
      if (ep->name == "..") {
	if (mp)
	  fstab[i].fileid_mntpt_dd = ep->fileid;
	else
	  fstab[i].fileid_root_dd = ep->fileid;
	return;
      }
      entry3 *nep = ep->nextentry;
      if (!nep)
	break;
      ep = nep;
    }

  if (res->resok->reply.eof) {
    warn << path << ": could not find \"..\"\n";
    ea->seterr ();
    return;
  }
  else if (!ep) {
    warn << path << ": zero entry non-EOF reply from kernel\n";
    ea->seterr ();
    return;
  }

  readdir3args arg;
  arg.dir = mp ? fstab[i].fh_mntpt : fstab[i].fh_root;
  arg.cookie = ep->cookie;
  arg.cookieverf = res->resok->cookieverf;
  arg.count = 8192;

  aclnt *c = mp ? fstab[i].parent->c : fstab[i].c;
  c->call (NFSPROC3_READDIR, &arg, res,
	   wrap (this, &filesrv::gotrdres, ea, res, i, mp),
	   auth_default);
}

#if 0
static void
dumpsubst (const u_int64_t &l, u_int64_t *r)
{
  warnx ("%14qx -> %qx\n", l, *r);
}
#endif

void
filesrv::gotdds (bool ok)
{
  if (!ok) {
    (*cb) (false);
    return;
  }

  u_int8_t key[sha1::hashsize];
  privkey->get_privkey_hash (key, hostid);
  fhkey.setkey (key, sizeof (key));
  bzero (key, sizeof (key));

  for (filesys *fsp = fstab.base (); fsp < fstab.lim (); fsp++) {
    if (fsp == fsp->parent)
      /* Make sure readdir returns unique fileid for /.., since an
       * accessible copy of /.. may reside somewhere else in the file
       * system and otherwise break pwd. */
      fsp->inotab->insert (fsp->fileid_root_dd, 1);
    else {
      fsp->parent->inotab->insert (fsp->fileid_mntpt,
				   fsidino2ino (fsp->fsid, fsp->fileid_root));
      fsp->inotab->insert (fsp->fileid_root_dd,
			   fsidino2ino (fsp->parent->fsid,
					fsp->fileid_mntpt_dd));
    }
  }

#if 0
  for (filesys *fsp = fstab.base (); fsp < fstab.lim (); fsp++) {
    warnx << fsp->path_mntpt << ":\n";
    warnx ("fsid 0x%qx, / 0x%qx, /.. 0x%qx, mp 0x%qx, mp/.. 0x%qx\n",
	   fsp->fsid, fsp->fileid_root, fsp->fileid_root_dd,
	   fsp->fileid_mntpt, fsp->fileid_mntpt_dd);
    fsp->inotab->traverse (wrap (dumpsubst));
  }
#endif

  fh3trans fht (fh3trans::ENCODE, fhkey, 0);

  fsinfo.set_prog (ex_NFS_PROGRAM);
  fsinfo.nfs->set_vers (ex_NFS_V3);
  fsinfo.nfs->v3->root = fstab[0].fh_root;
  if (!rpc_traverse (fht, fsinfo.nfs->v3->root))
    fatal ("filesrv::finish: nfs3_transres encode failed (err %d)\n", fht.err);

  fsinfo.nfs->v3->subfs.setsize (fstab.size () - 1);
  for (size_t i = 1; i < fstab.size (); i++) {
    fsinfo.nfs->v3->subfs[i-1].path = fstab[i].path_mntpt;
    fsinfo.nfs->v3->subfs[i-1].fh = fstab[i].fh_root;
    fht.srvno = i;
    if (!rpc_traverse (fht, fsinfo.nfs->v3->subfs[i-1].fh))
      fatal ("filesrv::finish: nfs3_transres encode failed (err %d)\n",
	     fht.err);
  }

  cb_t c = cb;
  cb = NULL;
  (*c) (true);
}

bool
filesrv::getauthclnt ()
{
  if (authxprt && !authxprt->ateof ())
    return true;
  int fd = suidgetfd ("sfsauthd");
  if (fd < 0) {
    authxprt = NULL;
    authclnt = NULL;
    return false;
  }
  authclnt = aclnt::alloc (authxprt = axprt_stream::alloc (fd),
			   sfsauth_program_1);
  return true;
}

int
filesrv::fhsubst (bool *substp, filesys *pfsp, nfs_fh3 *fhp, u_int32_t *fhnop)
{
  filesys *fsp;
  for (fsp = pfsp->mp3tab[*fhp]; fsp; fsp = pfsp->mp3tab.nextkeq (fsp))
    if (fsp != fstab.base () && getfsno (fsp->parent) == *fhnop) {
      *fhp = fsp->fh_root;
      *fhnop = fsp - fstab.base ();
      *substp = true;
      break;
    }
  return 0;
}

static inline bool
anon_checkperm (svccb *sbp, u_int options, bool isroot)
{
  switch (sbp->proc ()) {
  case NFSPROC3_GETATTR:
  case NFSPROC3_ACCESS:
  case NFSPROC3_FSINFO:
  case NFSPROC3_PATHCONF:
    if (isroot || (options & filesys::ANON_READ) == filesys::ANON_READ)
      return true;
    break;

  case NFSPROC3_FSSTAT:
    {
      if (isroot || (options & filesys::ANON_READ) == filesys::ANON_READ)
	return true;
      ex_fsstat3res res (NFS3_OK);
      res.resok->tbytes = 0;
      res.resok->fbytes = 0;
      res.resok->abytes = 0;
      res.resok->tfiles = 0;
      res.resok->ffiles = 0;
      res.resok->afiles = 0;
      res.resok->invarsec = 0;
      sbp->reply (&res);
      return false;
    }

  case NFSPROC3_LOOKUP:
  case NFSPROC3_READLINK:
  case NFSPROC3_READ:
  case NFSPROC3_READDIR:
  case NFSPROC3_READDIRPLUS:
    if (options & filesys::ANON_READ)
      return true;
    break;

  default:
    if ((options & filesys::ANON_READWRITE) == filesys::ANON_READWRITE)
      return true;
    break;
  }

  nfs3exp_err (sbp, NFS3ERR_PERM);
  return false;
}

bool
filesrv::fixarg (svccb *sbp, reqstate *rqsp)
{
  fh3trans fht (fh3trans::DECODE, fhkey);
  if (!nfs3_transarg (fht, sbp->template getarg<void> (), sbp->proc ())) {
    nfs3exp_err (sbp, nfsstat3 (fht.err));
    return false;
  }
  if (fht.srvno >= fstab.size ()) {
    nfs3exp_err (sbp, NFS3ERR_BADHANDLE);
    return false;
  }
  rqsp->fsno = fht.srvno;
  filesys *fsp = &fstab[rqsp->fsno];
  rqsp->rootfh = false;
  rqsp->c = fsp->c;

#if 1
  /* We let anonymous users GETATTR any root file handle, not just the
   * root of all exported files.  This is to help client
   * implementations that want to avoid (st_ino, st_dev) conflicts by
   * creating multiple mount points for each server.  Is this bad? */
  if (!sbp->getaui ()
      && !anon_checkperm (sbp, fsp->options,
			  *sbp->template getarg<nfs_fh3> () == fsp->fh_root))
    return false;
#else
  /* The other option is to disallow this.  Then commands like "ls
   * -al" will return a bunch of permission denied errors.  */
  if (!sbp->getaui ()
      && !anon_checkperm (sbp, fsp->options,
			  (fsp == fstab.base ()
			   && (*sbp->template getarg<nfs_fh3> ()
			       == fsp->fh_root))))
    return false;
#endif

  switch (sbp->proc ()) {
  case NFSPROC3_LOOKUP:
    {
      diropargs3 *doa = sbp->template getarg<diropargs3> ();
      if (doa->name == ".." && doa->dir == fsp->fh_root) {
	if (!getfsno (fsp)) {
	  nfs3exp_err (sbp, NFS3ERR_ACCES);
	  return false;
	}
	doa->dir = fsp->fh_mntpt;
	rqsp->fsno = getfsno (fsp->parent);
	fsp = &fstab[rqsp->fsno];
	rqsp->rootfh = true;
	rqsp->c = fsp->c;
      }
      break;
    }
  case NFSPROC3_READDIR:
  case NFSPROC3_READDIRPLUS:
    {
      nfs_fh3 *rpa = sbp->template getarg<nfs_fh3> ();
      if (*rpa == fsp->fh_root)
	rqsp->rootfh = true;
      break;
    }
  }
  return true;
}

void
filesrv::fixrdres (void *_res, filesys *fsp, bool rootfh)
{
  ex_readdir3res *res = static_cast<ex_readdir3res *> (_res);
  if (res->status)
    return;
  for (entry3 *e = res->resok->reply.entries; e; e = e->nextentry) {
    u_int64_t *idp = (*fsp->inotab)[e->fileid];
    if (idp && e->name != "." && (rootfh || e->name != ".."))
      e->fileid = *idp;
    else
      e->fileid = fsidino2ino (fsp->fsid, e->fileid);
  }
}

void
filesrv::fixrdplusres (void *_res, filesys *fsp, bool rootfh)
{
  ex_readdirplus3res *res = static_cast<ex_readdirplus3res *> (_res);
  if (res->status)
    return;
  for (ex_entryplus3 *e = res->resok->reply.entries; e; e = e->nextentry) {
    u_int64_t *idp = (*fsp->inotab)[e->fileid];
    if (idp && e->name != "." && (rootfh || e->name != ".."))
      e->fileid = *idp;
    else
      e->fileid = fsidino2ino (fsp->fsid, e->fileid);
    if (!e->name_handle.present)
      e->name_attributes.set_present (false);
    else if (rootfh && e->name == "..") {
      e->name_attributes.set_present (false);
      e->name_handle.set_present (false);
    }
    else {
      filesys *nfsp;
      for (nfsp = fsp->mp3tab[*e->name_handle.handle];
	   nfsp && nfsp->parent != fsp; nfsp = fsp->mp3tab.nextkeq (nfsp))
	;
      if (nfsp)
	e->name_attributes.set_present (false);
    }
  }
}

static int
xor_ino (filesrv *fsrv, fattr3 *fp, u_int32_t srvno)
{
  if (srvno >= fsrv->fstab.size ())
    return NFS3ERR_BADHANDLE;
  fp->fileid = fsidino2ino (fsrv->fstab[srvno].fsid, fp->fileid);
  return 0;
}

bool
filesrv::fixres (svccb *sbp, void *res, reqstate *rqsp)
{
  filesys *fsp = &fstab[rqsp->fsno];

  if (sbp->proc () == NFSPROC3_READDIR)
    fixrdres (res, fsp, rqsp->rootfh);
  else if (sbp->proc () == NFSPROC3_READDIRPLUS)
    fixrdplusres (res, fsp, rqsp->rootfh);

  bool subst = false;
  fh3trans fht (fh3trans::ENCODE, fhkey, rqsp->fsno,
		wrap (this, &filesrv::fhsubst, &subst, fsp));

  fht.fattr_hook = wrap (xor_ino, this);
  if (!nfs3exp_transres (fht, res, sbp->proc ())) {
    warn ("nfs3reply: nfs3_transres encode failed (err %d)\n", fht.err);
    nfs3exp_err (sbp, nfsstat3 (fht.err));
    return false;
  }

  switch (sbp->proc ()) {
  case NFSPROC3_LOOKUP:
    {
      ex_lookup3res *dor = static_cast<ex_lookup3res *> (res);
      if (rqsp->rootfh) {
	if (dor->status)
	  dor->resfail->set_present (false);
	else
	  dor->resok->dir_attributes.set_present (false);
      }
      if (subst && !dor->status)
	dor->resok->obj_attributes.set_present (false);
      break;
    }
  case NFSPROC3_ACCESS:
    if (!sbp->getaui () && fsp->options != filesys::ANON_READWRITE) {
      ex_access3res *ar = static_cast<ex_access3res *> (res);
      if ((fsp->options & filesys::ANON_READ) != filesys::ANON_READ) {
	if (ar->status)
	  ar->resfail->set_present (false);
	else {
	  ar->resok->access = 0;
	  ar->resok->obj_attributes.set_present (false);
	}
      }
      else if (!ar->status)
	ar->resok->access &= ACCESS3_READ|ACCESS3_LOOKUP|ACCESS3_EXECUTE;
    }
    break;
  }
  return true;
}

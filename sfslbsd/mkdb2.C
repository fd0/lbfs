/* $Id$ */

#include "fingerprint.h"
#include "lbfsdb.h"
#include "getfh3.h"
#include "sfsmisc.h"
#include "qhash.h"

static void chunkify (str path, const nfs_fh3 &fh, int fd);

static AUTH *auth;
static ptr<aclnt> nfsc;
static bhash<u_int32_t> inotab;
static fp_db db;

struct diriter {
  const cbv cb;
  const str name;
  readdirplus3args rdarg;
  readdirplus3res rdres;
  entryplus3 *e;
  const cbv ncb;

  diriter (const nfs_fh3 &fh, str name, cbv cb);
  void getrdres (clnt_stat stat);
  void dofile ();
  void gotfh (ref<lookup3res> resp, clnt_stat stat);
  void gotattr (ref<getattr3res> resp, clnt_stat stat);
  void process ();
  void nextfile ();
};

diriter::diriter (const nfs_fh3 &fh, str n, cbv c)
  : cb (c), name (n), ncb (wrap (this, &diriter::nextfile))
{
  rdarg.dir = fh;
  rdarg.cookie = 0;
  rdarg.dircount = rdarg.maxcount = 8192;
  nfsc->call (NFSPROC3_READDIRPLUS, &rdarg, &rdres,
	      wrap (this, &diriter::getrdres), auth);
  
}

void
diriter::getrdres (clnt_stat stat)
{
  if (stat)
    fatal << name << ": " << stat << "\n";
  if (rdres.status)
    fatal << name << ": " << rdres.status << "\n";
  e = rdres.resok->reply.entries;
  dofile ();
}

void
diriter::dofile ()
{
  if (!e) {
    if (rdres.resok->reply.eof) {
      (*cb) ();
      delete this;
    }
    else
      nfsc->call (NFSPROC3_READDIRPLUS, &rdarg, &rdres,
		  wrap (this, &diriter::getrdres), auth);
  }
  else if (e->name == "." || e->name == "..")
    nextfile ();
  else if (!e->name_handle.present) {
    diropargs3 arg;
    arg.dir = rdarg.dir;
    arg.name = e->name;
    ref<lookup3res> resp (New refcounted<lookup3res>);
    nfsc->call (NFSPROC3_LOOKUP, &arg, resp,
		wrap (this, &diriter::gotfh, resp), auth);
  }
  else if (!e->name_attributes.present) {
    ref<getattr3res> resp (New refcounted<getattr3res>);
    nfsc->call (NFSPROC3_GETATTR, e->name_handle.handle.addr (), resp,
		wrap (this, &diriter::gotattr, resp), auth);
  }
  else
    process ();
}

void
diriter::gotfh (ref<lookup3res> resp, clnt_stat stat)
{
  if (stat)
    fatal << name << "/" << e->name << ": " << stat << "\n";
  if (resp->status)
    fatal << name << "/" << e->name << ": " << resp->status << "\n";
  e->name_handle.set_present (true);
  *e->name_handle.handle = resp->resok->object;
  if (resp->resok->obj_attributes.present)
    e->name_attributes = resp->resok->obj_attributes;
  if (e->name_attributes.present)
    process ();
  else {
    ref<getattr3res> resp (New refcounted<getattr3res>);
    nfsc->call (NFSPROC3_GETATTR, e->name_handle.handle.addr (), resp,
		wrap (this, &diriter::gotattr, resp), auth);
  }
}

void
diriter::gotattr (ref<getattr3res> resp, clnt_stat stat)
{
  if (stat)
    fatal << name << "/" << e->name << ": " << stat << "\n";
  if (resp->status)
    fatal << name << "/" << e->name << ": " << resp->status << "\n";
  e->name_attributes.set_present (true);
  *e->name_attributes.attributes = *resp->attributes;
  process ();
}

void
diriter::process ()
{
  str path (name << "/" << e->name);
  // warn << "processing " << path << "/" << e->name << "\n";
  if (e->name_attributes.attributes->type == NF3DIR) {
    vNew diriter (*e->name_handle.handle, path, ncb);
    return;
  }
  if (e->name_attributes.attributes->type == NF3LNK
      || inotab[e->name_attributes.attributes->fileid]) {
    // warn << "skipping link " << path << "\n";
    delaycb (0, ncb);
    return;
  }
  inotab.insert (e->name_attributes.attributes->fileid);

  int fd = open (path, O_RDONLY);
  if (fd < 0)
    warn ("%s: %m\n", path.cstr ());
  else {
    chunkify (path, *e->name_handle.handle, fd);
    close (fd);
  }

  delaycb (0, ncb);
}

void
diriter::nextfile ()
{
  entryplus3 *n = e->nextentry;
  if (!n)
    rdarg.cookie = e->cookie;
  e = n;
  dofile ();
}

static int opt_count_dups;
static u_int num_files;
static u_int64_t num_chunks;
static u_int64_t num_bytes;
static u_int64_t num_dup_chunks;
static u_int64_t num_dup_bytes;
// static u_int64_t num_collisions;

static void
chunkify (str path, const nfs_fh3 &fh, int fd)
{
  u_char buf[4096];
  int n;
  Chunker chunker;

  num_files++;
  while ((n = read (fd, buf, sizeof (buf))) > 0) {
    num_bytes += n;
    chunker.chunk_data (buf, n);
  }
  chunker.stop ();
  num_chunks += chunker.chunk_vector ().size ();
  for (u_int i = 0; i < chunker.chunk_vector ().size (); i++) {
    chunk *c = chunker.chunk_vector ()[i];
    c->location ().set_fh (fh);

    /* warnx ("%s %5d bytes @%" U64F "d\n", path.cstr (),
              c->location ().count (), c->location ().pos ()); */
    if (opt_count_dups) {
      fp_db::iterator *iterp;
      if (!db.get_iterator (c->index(), &iterp)) {
	// XXX - need to check for collisions!
	num_dup_chunks++;
	num_dup_bytes += c->location ().count ();
	if (opt_count_dups > 1)
	  warnx ("DUP: %s %5d bytes @%" U64F "d\n", path.cstr (),
		 c->location ().count (), c->location ().pos ());
	delete iterp;
      }
    }

    db.add_entry (c->index(), &(c->location ()));
  }
}

static void
done ()
{
  warnx << "     Total files: " << num_files << "\n"
	<< "    Total chunks: " << num_chunks << "\n"
	<< "     Total bytes: " << num_bytes << "\n";
  if (opt_count_dups)
    warnx // << " Hash collisions: " << num_collisions << "\n"
	  << "Duplicate chunks: " << num_dup_chunks << "\n"
	  << " Duplicate bytes: " << num_dup_bytes << "\n"
	  << "    Unique bytes: " << num_bytes - num_dup_bytes << "\n";
  db.sync ();
  exit (0);
}

static void
foundfs (str name, ptr<nfsinfo> nfsi, str err)
{
  if (!nfsi)
    fatal << err << "\n";
  nfsc = nfsi->c;
  vNew diriter (nfsi->fh, name, wrap (done));
}

static void
usage ()
{
  warnx ("usage: %s [-d] db dir\n", progname.cstr ());
  exit (1);
}

int
main (int argc, char **argv)
{
  sfsconst_init ();
  setprogname (argv[0]);

  int ch;
  while ((ch = getopt (argc, argv, "d")) != -1)
    switch (ch) {
    case 'd':
      opt_count_dups++;
      break;
    default:
      usage ();
    }
  argv += optind;
  argc -= optind;
  if (argc != 2)
    usage ();

  db.open (argv[0]);		// XXX - no error return

  auth = authunix_create_realids ();
  findfs (NULL, argv[1], wrap (foundfs, argv[1]));
  amain ();
}

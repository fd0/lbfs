
#include "sfslbcd.h"
#include "aiod.h"
#include "callback.h"
#include "arpc.h"

typedef callback<void, ptr<aiobuf>, ssize_t, int>::ref aiofh_cbrw;

aiod* a;
ptr<aiofh> afh;

void aiod_read (uint64 off, uint32 cnt, aiofh_cbrw cb)
{
  ptr<aiobuf> buf = a->bufalloc (cnt);
  if (!buf) {
    a->bufwait (wrap (aiod_read, off, cnt, cb));
    return;
  }
  afh->read (off, buf, cb);
}

void
closed (int err)
{
  exit (0);
}

void read_done (uint32 cnt, ptr<aiobuf> buf, ssize_t sz, int err)
{
  if (err) {
    warn << "read failed: " << err << "\n";
    return;
  }

  if ((unsigned)sz != cnt) {
    warn << "short read: got " << sz << " wanted " << cnt << "\n";
  }

  afh->close (wrap(closed));
}

void do_read ()
{
  aiod_read (0, 3, wrap (read_done, 3));
}

void truncate_three (int err)
{
  if (err)
    warn << "truncate_one " << err << "\n";
  do_read ();
}

void truncate_one (int err)
{
  if (err)
    warn << "truncate_one " << err << "\n";
  warn << "truncate file size to 3\n";
  afh->ftrunc (3, wrap (truncate_three));
}

void write_done (ptr<aiobuf> buf, ssize_t sz, int err)
{
  warn << "truncate file size to 1\n";
  afh->ftrunc (1, wrap (truncate_one));
}

void do_write ()
{
  ptr<aiobuf> buf = a->bufalloc (2);
  if (!buf) {
    a->bufwait (wrap (do_write));
    return;
  }

  str x = "xx";
  memmove (buf->base (), x.cstr (), 2);
  afh->write (0, buf, wrap (write_done));
}

void file_open (ptr<aiofh> fh, int err)
{
  afh = fh;
  do_write ();
}

int
main ()
{
  a = New aiod (1);
  a->open ("blah", O_CREAT | O_TRUNC | O_RDWR, 0666, wrap (file_open));
  amain ();
}



/*
 *
 * Copyright (C) 2000 Frans Kaashoek (kaashoek@mit.edu)
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

#include "lbfsusrv.h"
#include "parseopt.h"
#include "rxx.h"

filesrv *defsrv;
static str configfile;

void 
client_accept (ptr<axprt_crypt> x)
{
  if (!x)
    fatal ("EOF from sfssd\n");
  vNew client (x);
}

static void
start_server (filesrv *fsrv)
{
  setgid (sfs_gid);
  // setgroups (0, NULL);

  warn ("version %s, pid %d\n", VERSION, getpid ());
  warn << "serving " << sfsroot << "/" 
       << sfs_hostinfo2path (fsrv->servinfo.host) << "\n";

  defsrv = fsrv;

  sfssd_slave (wrap (client_accept));
}


static filesrv *
parseconfig (str cf)
{
  parseargs pa (cf);
  bool errors = false;

  filesrv *fsrv = New filesrv;

  int line;
  vec<str> av;
  while (pa.getline (&av, &line)) {
    if (!strcasecmp (av[0], "export")) {
       str root (av[1]);
       if ((fsrv->root = fsrv->lookup_add (root)) == NULL) {
 	 errors = true;
	 warn << cf << ":" << line << ": non-existing root\n";
       }
    }
    else if (!strcasecmp (av[0], "hostname")) {
      if (av.size () != 2) {
	errors = true;
	warn << cf << ":" << line << ": usage: hostname name\n";
      }
      else if (fsrv->servinfo.host.hostname) {
	errors = true;
	warn << cf << ":" << line << ": hostname already specified\n";
      }
      else
	fsrv->servinfo.host.hostname = av[1];
    }
    else if (!strcasecmp (av[0], "keyfile")) {
      if (fsrv->sk) {
	  errors = true;
	  warn << cf << ":" << line << ": keyfile already specified\n";
      }
      else if (av.size () == 2) {
	str key = file2wstr (av[1]);
	if (!key) {
	  errors = true;
	  warn << av[1] << ": " << strerror (errno) << "\n";
	  warn << cf << ":" << line << ": could not read keyfile\n";
	}
	else if (!(fsrv->sk = import_rabin_priv (key, NULL))) {
	  errors = true;
	  warn << cf << ":" << line << ": could not decode keyfile\n";
	}
      }
      else {
	errors = true;
	warn << cf << ":" << line << ": usage: keyfile path\n";
      }
    }
  }

  fsrv->servinfo.release = sfs_release;
  fsrv->servinfo.host.type = SFS_HOSTINFO;
  if (!fsrv->servinfo.host.hostname
      && !(fsrv->servinfo.host.hostname = myname ()))
    fatal ("could not figure out my host name\n");
  if (!fsrv->sk) 
    fatal ("no Keyfile specified\n");
  fsrv->servinfo.host.pubkey = fsrv->sk->n;
  if (!sfs_mkhostid (&fsrv->hostid, fsrv->servinfo.host))
    fatal ("could not marshal my own hostinfo\n");
  fsrv->servinfo.prog = ex_NFS_PROGRAM;
  fsrv->servinfo.vers = ex_NFS_V3;

  return fsrv;
}


static void
usage ()
{
  warnx << "usage: " << progname << " -f configfile\n";
  exit (1);
}

int
main (int argc, char **argv)
{
  filesrv *fsrv;

  setprogname (argv[0]);

  int ch;
  while ((ch = getopt (argc, argv, "f:")) != -1)
    switch (ch) {
    case 'f':
      configfile = optarg;
      break;
    case '?':
    default:
      usage ();
    }
  argc -= optind;
  argv += optind;

  if ( (argc > 0) || (!configfile) )
    usage ();

  sfsconst_init ();

  fsrv = parseconfig (configfile);

  start_server (fsrv);
  amain ();
}

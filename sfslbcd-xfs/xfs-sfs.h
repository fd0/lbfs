/*
 *
 * Copyright (C) 2000 Athicha Muthitacharoen (athicha@mit.edu)
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

#ifndef _XFS_SFS_H_
#define _XFS_SFS_H_ 1
#define LBFS_READ 1

#include "async.h"
#include "arpc.h"
#include "sfsmisc.h"
#include "lbfs_prot.h"
#include "axprt_crypt.h"
#include "axprt_compress.h"

#if LBFS_READ
#include "lbfsdb.h"
extern fp_db lbfsdb;
#endif

extern int server_fd;
extern ptr<aclnt> sfsc;
extern ptr<aclnt> nfsc;
extern ptr<asrv> nfscbs;
extern ptr<axprt_compress> x;
extern ptr<axprt_crypt> xc;


void sfsInit(const char* hostpath);
#endif

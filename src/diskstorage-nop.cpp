//
// diskstorage-nop.cpp
//
// Disk storage of server objects. This implementation does not actually
// store anything on disk. This is to be used when disk storage is disabled
// on Yesquel.
//

/*
  Original code: Copyright (c) 2014 Microsoft Corporation
  Modified code: Copyright (c) 2015-2016 VMware, Inc
  All rights reserved. 

  Written by Marcos K. Aguilera

  MIT License

  Permission is hereby granted, free of charge, to any person
  obtaining a copy of this software and associated documentation files
  (the "Software"), to deal in the Software without restriction,
  including without limitation the rights to use, copy, modify, merge,
  publish, distribute, sublicense, and/or sell copies of the Software,
  and to permit persons to whom the Software is furnished to do so,
  subject to the following conditions:

  The above copyright notice and this permission notice shall be
  included in all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND,
  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
  BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
  ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
  CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  SOFTWARE.
*/


#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <assert.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <malloc.h>

#include <list>
#include <map>
#include <set>

#ifndef LOCALSTORAGE
#define LOCALSTORAGE
#endif

#include "tmalloc.h"
#include "debug.h"
#include "diskstorage.h"
#include "pendingtx.h"

DiskStorage::DiskStorage(char *diskstoragepath){}
char *DiskStorage::getFilename(const COid& coid){ return 0; }
char *DiskStorage::searchseparator(char *name){ return 0; }
int DiskStorage::Makepath(char *dirname){return 0; }
COid DiskStorage::FilenameToCOid(char *filename){
  COid d;
  d.cid=d.oid=0;
  return d;
}
int DiskStorage::readCOidFromFile(FILE *f, const COid &coid,
                                  Ptr<TxUpdateCoid> &tucoid){ return 0; }
int DiskStorage::writeCOidToFile(FILE *f, Ptr<TxUpdateCoid> tucoid){ return 0; }
int DiskStorage::readCOid(const COid& coid, int len,
               Ptr<TxUpdateCoid> &tucoid, Timestamp& version){ return -1; }
int DiskStorage::writeCOid(const COid& coid, Ptr<TxUpdateCoid> tucoid,
                           Timestamp version){ return 0; }
int DiskStorage::getCOidSize(const COid& coid){ return -1; }

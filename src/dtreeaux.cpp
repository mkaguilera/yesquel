//
// dtreeaux.cpp
//
// Auxiliary definitions of distributed b-tree
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


#include <time.h>
#include <stdio.h>
#include <assert.h>
#include <stdarg.h>

#include "options.h"
#include "os.h"
#include "dtreeaux.h"
#include "supervalue.h"
#include "util.h"

// initializes a supervalue used to used a DTreeNode
void DTreeNode::InitSuperValue(SuperValue *sv, u8 celltype){
  sv->Nattrs = DTREENODE_NATTRIBS;
  sv->CellType = celltype;
  sv->Ncells = 0;
  sv->Attrs = new u64[sv->Nattrs];
  sv->Cells = 0;
  sv->CellsSize = 0;
  memset(sv->Attrs, 0, sizeof(u64)*sv->Nattrs);
}

// Reads a real node given its oid, obtaining a private copy to the transaction.
// Real is defined by the cursor transaction's start timestamp.
// First try nodes stored in the TxWriteCache (the transaction write set)
// and in TxReadCache.
// We can return data from either cache without making a copy, since that
// data is private to this transaction.
// Then try nodes in the NodeCache, but only if it would bring real data
//          (NOT IMPLEMENTED).
// If reading from NodeCache, make a private copy to return.
// If neither worked, finally read from the TKVS.
// Puts a copy in NodeCache if read node is not leaf and it is more recent than
//      the one there.
// Puts the read node (not copy) in TxReadCache if it is not in TxWriteCache.
// Returns a status: 0 if ok, != 0 if problem.
// The read node is returned in variable outptr.
int auxReadReal(KVTransaction *tx, COid coid, DTreeNode &outptr,
                ListCell *cell, Ptr<RcKeyInfo> prki){
  DTreeNode dtn;
  int res;

  res = KVreadSuperValue(tx, coid, dtn.raw, cell, prki);
  if (res) return res;

  if (dtn.raw->type == 1 && dtn.isInner()){ // inner supernode
    // try to refresh cache if newer
    GCache.refresh(dtn.raw);
  }

  outptr = dtn;
  return 0;
}

// Read a node from the global cache. The data will be immutable
//   (caller should not modify it).
// Returns a status: 0 if found, non-zero if not found
// The read node is returned in variable outptr.
int auxReadCache(COid coid, DTreeNode &outptr){
  return GCache.lookup(coid, outptr.raw);
}

void auxRemoveCache(COid coid){
  GCache.remove(coid);
}

// read data from cache or, if it is not there, from the TKVS.
// If the returned node is a leaf node, it will be real (cache should not
//   have it).
// Returns three things:
//    real=1 if value came from TKVS, real=0 if value came from cache
//    outptr has the read node
//    return value is 0 iff there were no errors
// If the data returned is from the cache (non-real), caller is not allowed to
// change it since the value may be shared with other threads.
// Otherwise, the data is private and the caller can change it.
int auxReadCacheOrReal(KVTransaction *tx, COid coid, DTreeNode &outptr,
                       int &real, ListCell *cell, Ptr<RcKeyInfo> prki){
  int res;
  res = auxReadCache(coid, outptr);
  if (res==0){ 
    assert(outptr.isInner()); // cannot read leaf nodes from cache
    real=0; 
    return 0; 
  } // found in cache
  res = auxReadReal(tx, coid, outptr, cell, prki);
  if (res) return res; // error
  real = 1;
  return 0;
}


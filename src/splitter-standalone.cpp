//
// splitter-standalone.cpp
//
// Functions for a standalone splitter
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
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <malloc.h>
#include <signal.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdarg.h>
#include <float.h>

#include <map>
#include <list>

#include "tmalloc.h"
#include "splitter-standalone.h"
#include "record.h"
#include "dtreeaux.h"

// these functions are already defined in dtree.c
// Here, they are intended for the standalone splitter. This version
// uses the myVdbeRecordCompare() and myVdbeDeleteUnpackedRecord() instead of
// sqlite3VdbeRecordCompare() and sqlite3VdbeDeleteUnpackedRecord() (which are
// part of sqlite).

// compares a cell key against intKey2/pIdxKey2. Use intKey2 if pIdxKey2==0
// otherwise use pIdxKey2
int compareNpKeyWithKey(i64 nKey1, char *pKey1, i64 nKey2,
                        UnpackedRecord *pIdxKey2) {
  if (pIdxKey2) return myVdbeRecordCompare((int)nKey1, pKey1, pIdxKey2);
  else if (nKey1==nKey2) return 0;
  else return (nKey1 < nKey2) ? -1 : +1;
}

// Searches the cells of a node for a given key, using binary search.
// Returns the child pointer that needs to be followed for that key.
// If biasRight!=0 then optimize for the case the key is larger than any
// entries in node.
// Assumes that the path at the given level has some node (real or approx).
// Guaranteed to return an index between 0 and N where N is the number of cells
// in that node (N+1 is the number of pointers).
// Returns *matches!=0 if found key, *matches==0 if did not find key.
int CellSearchNodeUnpacked(DTreeNode &node, UnpackedRecord *pIdxKey, i64 nkey,
                           int biasRight, int *matches){
  int cmp;
  int bottom, top, mid;
  ListCell *cell;

  bottom=0;
  top=node.Ncells()-1; /* number of keys on node minus 1 */
  if (top<0){
    // there are no keys in node, so return index of only pointer there
    // (index 0)    
    if (matches) *matches=0;
    return 0;
  }
  do {
    if (biasRight){ mid = top; biasRight=0; } /* bias first search only */
    else mid=(bottom+top)/2;
    cell = &node.Cells()[mid];
    cmp = compareNpKeyWithKey(cell->nKey, cell->pKey, nkey, pIdxKey);

    if (cmp==0) break; /* found key */
    if (cmp < 0) bottom=mid+1; /* mid < target */
    else top=mid-1; /* mid > target */
  } while (bottom <= top);
  // if key was found, then mid points to its index and cmp==0
  // if key was not found, then mid points to entry immediately before key
  //    (cmp<0) or after key (cmp>0)

  if (cmp<0) ++mid; // now mid points to entry immediately after key or to one
                    // past the last entry if key is greater than all entries
  // note: if key matches a cell (cmp==0), we follow the pointer to the left
  // of the cell, which has the same index as the cell

  if (matches) *matches = cmp == 0 ? 1 : 0;
  assert(0 <= mid && mid <= node.Ncells());
  return mid;
}
int GCellSearchNode(DTreeNode &node, i64 nkey, void *pkey, Ptr<RcKeyInfo> prki,
                    int biasRight){
  UnpackedRecord *pIdxKey;   /* Unpacked index key */
  char aSpace[150];          /* Temp space for pIdxKey - to avoid a malloc */
  int res;

  if (pkey){
    pIdxKey = myVdbeRecordUnpack(&*prki, (int)nkey, pkey, aSpace,
                                 sizeof(aSpace));
    if (pIdxKey == 0) return SQLITE_NOMEM;
  } else pIdxKey = 0;
  res = CellSearchNodeUnpacked(node, pIdxKey, nkey, biasRight);
  if (pkey) myVdbeDeleteUnpackedRecord(pIdxKey);
  return res;
}

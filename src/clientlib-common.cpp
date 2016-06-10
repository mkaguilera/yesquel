//
// clientlib-common.cpp
//
// Parts of clientlib that are common to both clientlib.cpp and
// clientlib-local.cpp
//

/*
  Copyright (c) 2015-2016 VMware, Inc
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
#include <assert.h>

#include "tmalloc.h"
#include "options.h"
#include "debug.h"
#include "clientlib-common.h"

void TxCacheEntry::set(int level, Ptr<Valbuf> &vbuf){
  TxCacheEntryList *curr;
  for (curr = levelList.getFirst(); curr != levelList.getLast();
       curr = levelList.getNext(curr)){
    if (curr->level <= level) break; // went past insertion point
  }
  if (curr != levelList.getLast() && curr->level == level){
    // level exists, replace entry
    curr->vbuf = vbuf;
  } else { // level does not exist, add it
    TxCacheEntryList *toadd = new TxCacheEntryList;
    toadd->level = level;
    toadd->vbuf = vbuf;
    levelList.addBefore(toadd, curr);
  }
}

// remove the value above given level
// returns 0 if list is not empty, != 0 if list has become empty
int TxCacheEntry::abort(int level){
  TxCacheEntryList *curr, *next;
  for (curr = levelList.getFirst(); curr != levelList.getLast();
       curr = next){
    next = levelList.getNext(curr);
    if (curr->level > level){ // delete entry
      levelList.remove(curr);
      delete curr;
    } else break; // we are done since list is in decreasing order
  }
  return levelList.empty();
}

// change level of value above given level to given level
// Returns 0 if list is not empty, != 0 if list has become empty
void TxCacheEntry::release(int level){
  assert(level >= 0);
  TxCacheEntryList *curr, *next;
  Ptr<Valbuf> vbuf;
  for (curr = levelList.getFirst(); curr != levelList.getLast();
       curr = next){
    next = levelList.getNext(curr);
    if (curr->level > level){ // delete entry
      if (!vbuf.isset()) vbuf = curr->vbuf; // remember first (highest level)
      levelList.remove(curr);
      delete curr;
    } else break; // found insertion point
  }
  if (vbuf.isset()) set(level, vbuf); // put back remembered item
}

void TxCacheEntry::print(){ // for debugging purposes only
  TxCacheEntryList *curr;
  for (curr = levelList.getFirst(); curr != levelList.getLast();
       curr = levelList.getNext(curr)){
    printf("(%d %p) ", curr->level, (void*) &*curr->vbuf);
  }
  putchar('\n');
}

void TxCache::clear(){
  cache.clear(0, TxCacheEntry::delEntry);
  clearallPendingOps();    
}

// returns 0 if found, non-zero if not found. If found, makes
// tcel point to entry found
int TxCache::lookupCache(COid &coid, TxCacheEntryList *&tcel){
  int res;
  TxCacheEntry **tcepp, *tce;
  res = cache.lookup(coid, tcepp);
  if (res) return res; // not found
  tce = *tcepp; assert(tce);
  tcel = tce->get(); assert(tcel);
  return 0;
}

void TxCache::setCache(COid &coid, int level, Ptr<Valbuf> &vbuf){
  int res;
  TxCacheEntry **tcepp, *tce;
  res = cache.lookupInsert(coid, tcepp);
  if (res) *tcepp = new TxCacheEntry;
  tce = *tcepp; assert(tce);
  tce->set(level, vbuf);
}

void TxCache::abortLevel(int level){
  SkipListNode<COid,TxCacheEntry*> *curr, *next;
  TxCacheEntry *tce;
  int res;
  // iterate over all keys in cache
  for (curr = cache.getFirst(); curr != cache.getLast();
       curr = next){
    next = cache.getNext(curr);
    tce = curr->value; assert(tce);
    res = tce->abort(level);
    if (res){ // list has become empty, remove list from cache
      res = cache.lookupRemove(curr->key, 0, tce); assert(!res);
    }
  }

  // iterate over all keys in pendingops
  SkipListNode<COid,PendingOpsList*> *currpops, *nextpops;
  PendingOpsList *pol;
  for (currpops = pendingOps.getFirst(); currpops != pendingOps.getLast();
       currpops = nextpops){
    nextpops = pendingOps.getNext(currpops);
    pol = currpops->value; assert(pol);
    res = pol->abort(level);
    if (res){ // list has become empty, remove list from cache
      res = pendingOps.lookupRemove(curr->key, 0, pol); assert(!res);
    }
  }
}

void TxCache::releaseLevel(int level){
  SkipListNode<COid,TxCacheEntry*> *curr, *next;
  TxCacheEntry *tce;
  assert(level >= 0);
  // iterate over all keys in cache
  for (curr = cache.getFirst(); curr != cache.getLast();
       curr = next){
    next = cache.getNext(curr);
    tce = curr->value; assert(tce);
    tce->release(level);
  }

  // iterate over all keys in pendingops
  SkipListNode<COid,PendingOpsList*> *currpops, *nextpops;
  PendingOpsList *pol;
  for (currpops = pendingOps.getFirst(); currpops != pendingOps.getLast();
       currpops = nextpops){
    nextpops = pendingOps.getNext(currpops);
    pol = currpops->value; assert(pol);
    pol->release(level);
  }
}

PendingOpsEntry::~PendingOpsEntry(){
  switch(type){
  case 0: // add
    u.add.cell.Free(); // free cell
    break;
  case 1: // delrange
    u.delrange.cell1.Free(); // free cell1
    u.delrange.cell2.Free(); // free cell2
    break;
  case 2:
    break;
  default:
    assert(0);
  }
}

void PendingOpsList::del(PendingOpsList *pol){
  PendingOpsEntry *poe;
  if (!pol) return;
  while (!pol->list.empty()){
    poe = pol->list.popHead();
    delete poe;
  }
  delete pol;
}

static void delpoe(PendingOpsEntry *poe){ delete poe; }

// remove the value above given level
// returns 0 if list is not empty, != 0 if list has become empty
int PendingOpsList::abort(int level){
  PendingOpsEntry *poe, *prevpoe;
  // find first item with >= level
  prevpoe = 0;
  poe = list.getFirst();
  while (poe){
    if (poe->level > level) break; // found it
    prevpoe = poe;
    poe = list.getNext(poe);
  }
  if (poe){ // found something before getting to end
    list.removeRest(prevpoe, delpoe);
  }
  return list.empty();
}
  
// change level of value above given level to given level
void PendingOpsList::release(int level){
  PendingOpsEntry *poe;
  assert(level >= 0);
  poe = list.getFirst();
  while (poe){
    if (poe->level > level) poe->level = level; // change level
    poe = list.getNext(poe);
  }
}
  
void TxCache::clearallPendingOps(void){
  pendingOps.clear(0, PendingOpsList::del);
}

// Applies a pending ops entry to a valbuf
// Returns 0 if ok, non-0 if error
int TxCache::auxApplyOp(Ptr<Valbuf> vbuf, PendingOpsEntry *poe){
  assert(vbuf->type == 1); // only supervalue
  if (poe->type == 0){ // add
    ListCell &cell = poe->u.add.cell;
    int index, matches=0;

    if (vbuf->u.raw->Ncells >= 1){
      index = myCellSearchNode(vbuf, cell.nKey, cell.pKey, 1, poe->prki,
                               &matches);
      if (index < 0) return index;
    }
    else index = 0; // no cells, so insert at position 0
    assert(0 <= index && index <= vbuf->u.raw->Ncells);
    //ListCell *newcell = new ListCell(*cell);
    if (!matches) vbuf->u.raw->InsertCell(index); // item not yet in list
    else {
      vbuf->u.raw->CellsSize -= vbuf->u.raw->Cells[index].size();
      vbuf->u.raw->Cells[index].Free(); // free old item to be replaced
    }
    new(&vbuf->u.raw->Cells[index]) ListCell(cell); // placement constructor
    vbuf->u.raw->CellsSize += cell.size();
  } else if (poe->type == 1){ // delrange
    ListCell &cell1 = poe->u.delrange.cell1;
    ListCell &cell2 = poe->u.delrange.cell2;
    int index1, index2;
    int matches1, matches2;
    int intervtype = poe->u.delrange.intervtype;

    if (intervtype < 6){
      index1 = myCellSearchNode(vbuf, cell1.nKey, cell1.pKey, 0, poe->prki,
                                &matches1);
      if (index1 < 0){ return index1; }
      assert(0 <= index1 && index1 <= vbuf->u.raw->Ncells);
      if (matches1 && intervtype < 3) ++index1; // open interval,
                                                // do not del cell1
    }
    else index1 = 0; // delete from -infinity

    if (index1 < vbuf->u.raw->Ncells){
      if (intervtype % 3 < 2){
        index2 = myCellSearchNode(vbuf, cell2.nKey, cell2.pKey, 0, poe->prki,
                                  &matches2);
        if (index2 < 0){ return index2; }
        // must find value in cell
        assert(0 <= index2 && index2 <= vbuf->u.raw->Ncells);
        if (matches2 && intervtype % 3 == 0) --index2; // open interval,
                                                       // do not del cell2
        if (!matches2) --index2; // if does not match, back 1
        
      } else index2 = vbuf->u.raw->Ncells; // delete til +infinity

      if (index2 == vbuf->u.raw->Ncells) --index2;

      if (index1 <= index2){
        vbuf->u.raw->DeleteCellRange(index1, index2+1);
      }
    }
  } else if (poe->type == 2){ // attrset
    u32 attrid = poe->u.attrset.attrid;
    u64 attrvalue = poe->u.attrset.attrvalue;
    if (attrid >= (unsigned)vbuf->u.raw->Nattrs){ return GAIAERR_ATTR_OUTRANGE; }
    vbuf->u.raw->Attrs[attrid] = attrvalue;
  } else { // bad type
    assert(0);
    printf("clientlib.cpp: bad type in PendingOps\n");
    exit(1);
  } // else
  return 0;
}

// applies one operation to the current version in the cache.
// Requires that cache contains version and that poe's level
// be the largest level in cache.
void TxCache::applyOneOp(COid &coid, PendingOpsEntry *poe){
  int res;
  TxCacheEntryList *tcel;

  res = lookupCache(coid, tcel); assert(res==0);
  assert(tcel->level <= poe->level);
  if (tcel->level == poe->level){
    // same level, can modify it directly
    if (tcel->vbuf.refcount()>2){ // 2 means private: 1 ref in cache, 1 in tcel
      // shared buffer, copy it
      tcel->vbuf = new Valbuf(*tcel->vbuf);
    }
    auxApplyOp(tcel->vbuf, poe);
  } else { // level has changed
    Ptr<Valbuf> vbuf;
    vbuf = new Valbuf(*tcel->vbuf);
    auxApplyOp(vbuf, poe);
    setCache(coid, poe->level, vbuf);
  }
}


// If there are pending ops, apply them, clear them, and set the cache
// at all the relevant levels. vbuf is the value read from the storageserver
// from the transaction's snapshot, without reflecting any updates
// done by the transaction.
// If forcezero if set, then set the cache at level 0 with vbuf.
// Return <0 if error, 0 if no pending ops, >0 if some pending ops
int TxCache::applyPendingOps(COid &coid, Ptr<Valbuf> &vbuf, bool forcezero){
  int res, retval, clevel;
  bool saveclevel;
  PendingOpsList *pol;
  PendingOpsEntry *poe;

  res = pendingOps.lookupRemove(coid, 0, pol);
  if (res){
    if (forcezero) setCache(coid, 0, vbuf); // set level 0
    return 0;
  }

  assert(pol);
  retval = 1;
  clevel = 0;
  saveclevel = forcezero; // whether to save level 0 with no changes
  
  if (vbuf->type != 1) // not a supervalue, error
    return GAIAERR_WRONG_TYPE;
  for (poe = pol->getFirst(); poe; poe = pol->getNext(poe)){
    assert(poe->level >= clevel);
    if (poe->level > clevel){ // moving to another level
      if (saveclevel){ // save this level
        setCache(coid, clevel, vbuf);
        vbuf = new Valbuf(*vbuf); // make a copy
      }
      clevel = poe->level;
    }
    saveclevel = true; // from now on, save level since it will have changes

    res = auxApplyOp(vbuf, poe);
    if (res){ retval = res;  break; }
  } // for
  if (saveclevel){ // save last updated level
    setCache(coid, clevel, vbuf);
  }
  PendingOpsList::del(pol);
  return retval;
}

// check if transaction has pending updates on coid
bool TxCache::hasPendingOps(COid &coid){
  int res;
  PendingOpsList **polptr;
  
  res = pendingOps.lookup(coid, polptr);
  if (res==0) return true;
  else return false;
}

 // check if key was added or removed by pending operations.
// Returns 1 if added, 0 if removed, -1 if neither
int TxCache::checkPendingOps(COid &coid, int nKey, char *pKey,
                             Ptr<RcKeyInfo> prki){
  int res;
  PendingOpsList **polptr, *pol;
  
  res = pendingOps.lookup(coid, polptr);
  if (res==0){ // found something
    pol = *polptr;
    UnpackedRecord *pIdxKey=0;
    PendingOpsEntry *poe;
    char aSpace[150];          /* Temp space for pIdxKey - to avoid a malloc */
    
    int got = 0; // whether key was found or not in pending ops
    assert(pol);
    
    if (pKey){
      pIdxKey = myVdbeRecordUnpack(&*prki, (int) nKey, pKey, aSpace,
                                   sizeof(aSpace));
      if (pIdxKey == 0) return GAIAERR_NO_MEMORY;
    }
    
    for (poe = pol->getFirst(); poe; poe = pol->getNext(poe)){
      if (poe->type == 0){ // add
	ListCell &cell = poe->u.add.cell;
        
        int cmp = mycompareNpKeyWithKey(cell.nKey, cell.pKey, nKey, pIdxKey);
        if (cmp == 0) got = 1; // found it
      } else if (poe->type == 1){ // delrange
	ListCell &cell1 = poe->u.delrange.cell1;
	ListCell &cell2 = poe->u.delrange.cell2;
        int intervtype = poe->u.delrange.intervtype;

        int cmp1 = mycompareNpKeyWithKey(cell1.nKey, cell1.pKey, nKey, pIdxKey);
        int cmp2 = mycompareNpKeyWithKey(cell2.nKey, cell2.pKey, nKey, pIdxKey);
        int match1=0, match2=0;

        switch(intervtype/3){
        case 0: match1 = cmp1 < 0; break;
        case 1: match1 = cmp1 <= 0; break;
        case 2: match1 = 1; break;
        default: assert(0);
        }

        switch(intervtype%3){
        case 0: match2 = cmp2 > 0; break;
        case 1: match2 = cmp2 >= 0; break;
        case 2: match2 = 1; break;
        default: assert(0);
        }

        if (match1 && match2) got = 0; // deleted key
      }
    } // for
    if (pIdxKey) myVdbeDeleteUnpackedRecord(pIdxKey);
    if (got) return 1; // key was added
    else return 0; // key was removed
  } // if (res==0)
  return -1; // unknown
}

  // remove pending ops for coid
void TxCache::removePendingOps(COid &coid){
  int res;
  PendingOpsList *pol=0;
  res = pendingOps.lookupRemove(coid, 0, pol);
  if (res==0){ // found something to delete
    assert(pol);
    PendingOpsList::del(pol);
  }
}

// add to coid's pendingops
void TxCache::addPendingOps(COid &coid, PendingOpsEntry *poe){
  int res;
  PendingOpsList **polptr, *pol;
  res = pendingOps.lookupInsert(coid, polptr);
  if (res) *polptr = new PendingOpsList; // not found, so create new item
  pol = *polptr;

  // insert add entry into pending ops
  pol->add(poe);
}

// Searches the cells of a node for a given key, using binary search.
// Returns the child pointer that needs to be followed for that key.
// If biasRight!=0 then optimize for the case the key is larger than any
// entries in node.
// Assumes that the path at the given level has some node (real or approx).
// Guaranteed to return an index between 0 and N where N is the number of cells
// in that node (N+1 is the number of pointers).
// Returns *matches!=0 if found key, *matches==0 if did not find key.
int myCellSearchUnpacked(Ptr<Valbuf> &vbuf, UnpackedRecord *pIdxKey,
                              i64 nkey, int biasRight, int *matches){
  int cmp;
  int bottom, top, mid;
  ListCell *cell;
  assert(vbuf->type==1); // must be supernode

  bottom=0;
  top=vbuf->u.raw->Ncells-1; /* number of keys on node minus 1 */
  if (top<0){ matches=0; return 0; } // there are no keys in node, so return
                                     // index of only pointer there (index 0)
  do {
    if (biasRight){ mid = top; biasRight=0; } /* bias first search only */
    else mid=(bottom+top)/2;
    cell = &vbuf->u.raw->Cells[mid];
    cmp = mycompareNpKeyWithKey(cell->nKey, cell->pKey, nkey, pIdxKey);

    if (cmp==0) break; /* found key */
    if (cmp < 0) bottom=mid+1; /* mid < target */
    else top=mid-1; /* mid > target */
  } while (bottom <= top);
  // if key was found, then mid points to its index and cmp==0
  // if key was not found, then mid points to entry immediately before key
  //    (cmp<0) or after key (cmp>0)

  if (cmp<0) ++mid; // now mid points to entry immediately after key or to
                    // one past the last entry
                    // if key is greater than all entries
  // note: if key matches a cell (cmp==0), we follow the pointer to the left of
  // the cell, which has the same index as the cell

  if (matches) *matches = cmp == 0 ? 1 : 0;
  assert(0 <= mid && mid <= vbuf->u.raw->Ncells);
  return mid;
}

int myCellSearchNode(Ptr<Valbuf> &vbuf, i64 nkey, void *pkey, int biasRight, 
                     Ptr<RcKeyInfo> prki, int *matches){
  UnpackedRecord *pIdxKey=0; /* Unpacked index key */
  char aSpace[150];          /* Temp space for pIdxKey - to avoid a malloc */
  int res;

  if (pkey){
    pIdxKey = myVdbeRecordUnpack(&*prki, (int) nkey, pkey, aSpace,
                                 sizeof(aSpace));
    if (pIdxKey == 0) return GAIAERR_NO_MEMORY;
  }
  res = myCellSearchUnpacked(vbuf, pIdxKey, nkey, biasRight, matches);
  if (pIdxKey) myVdbeDeleteUnpackedRecord(pIdxKey);
  return res;
}

// compares a cell key against intKey2/pIdxKey2. Use intKey2 if pIdxKey2==0
// otherwise use pIdxKey2
int mycompareNpKeyWithKey(i64 nKey1, char *pKey1, i64 nKey2,
                          UnpackedRecord *pIdxKey2){
  if (pIdxKey2) return myVdbeRecordCompare((int) nKey1, pKey1, pIdxKey2);
  else if (nKey1==nKey2) return 0;
  else return (nKey1 < nKey2) ? -1 : +1;
}

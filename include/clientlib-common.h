//
// clientlib-common.h
//
// Parts of clientlib common to clientlib.h and clientlib-local.h
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

#ifndef _CLIENTLIB_COMMON_H
#define _CLIENTLIB_COMMON_H

#include <set>
#include <list>
#include <signal.h>
#include <sys/uio.h>

#include "debug.h"
#include "util.h"

#include "ipmisc.h"

#include "clientdir.h"
#include "gaiarpcaux.h"
#include "datastruct.h"
#include "supervalue.h"


// cache only the first MAX_READS_TO_TXCACHE reads of a transaction
#define MAX_READS_TO_TXCACHE 1000

// ----------------------------- TxCache -----------------------------

// list of levels and vbufs
struct TxCacheEntryList {
  TxCacheEntryList *prev, *next;
  int level;
  Ptr<Valbuf> vbuf;
};

class TxCacheEntry {
private:
  LinkList<TxCacheEntryList> levelList; // list with levels and values,
                                        // ordered by decreasing level 
public:
  //Ptr<Valbuf> vbuf;

  static void delEntry(TxCacheEntry *tce){ delete tce; }

  TxCacheEntry() : levelList(true){ }
  ~TxCacheEntry(){}
  
  TxCacheEntryList *get(){
    if (levelList.empty()) return 0;
    else return levelList.getFirst();
  }
  
  void set(int level, Ptr<Valbuf> &vbuf); // set the value at a given level

  // remove the value above given level.
  // returns 0 if list is not empty, != 0 if list has become empty
  int abort(int level);

  // change level of value above given level to given level
  void release(int level);

  void print(); // for debugging purposes only
};

struct PendingOpsEntry {
  ~PendingOpsEntry();
  PendingOpsEntry *next;
  int type;   // 0 = add, 1 = delrange, 2 = attrset
  int level;
  Ptr<RcKeyInfo> prki; // only valid if type==0 or 1. We put this here,
                       // instead of inside union, to avoid dealing with
                       // destructors inside unions, which are a pain
  union dummy {
    dummy(){}
    ~dummy(){}
    struct { ListCell cell; } add;
    struct { ListCell cell1, cell2; int intervtype; }
      delrange;
    struct { u32 attrid; u64 attrvalue; } attrset;
  } u;
};

class PendingOpsList {
private:
  SLinkList<PendingOpsEntry> list;
public:
  static void del(PendingOpsList *pol); // delete list
  
  // remove the value above given level.
  // returns 0 if list is not empty, != 0 if list has become empty
  int abort(int level);
  
  // change level of values above given level to given level
  void release(int level);

  void add(PendingOpsEntry *poe){ list.pushTail(poe); }
  PendingOpsEntry *getFirst(){ return list.getFirst(); }
  PendingOpsEntry *getNext(PendingOpsEntry *poe){ return list.getNext(poe); }
};


class TxCache {
private:
  SkipList<COid,TxCacheEntry*> cache;
  SkipList<COid,PendingOpsList*> pendingOps; // contains pending listadd and
  // listdelrange operations that should be applied to supervalue in TxCache
  // as soon as it is read from the server. We do this so that
  // clients can execute listadd and listdelrange without having to populate
  // the txcache (which would require reading the supervalue from the server).
  // The invariant is that, for a given coid, if TxCache[coid] is set then
  // PendingOps[coid] is empty. When we read the supervalue (when the
  // client really needs the full supervalue), we set TxCache[coid] and
  // then apply all the operations in PendingOps[coid].

  int auxApplyOp(Ptr<Valbuf> vbuf, PendingOpsEntry *poe);
  

public:
  // ----- Operations that affect both cache and pendingOps ----------------
  void clear(); // clear entire cache and pendingops
  void abortLevel(int level); // remove entries with > level
  void releaseLevel(int level); // change entries with > level to level
  
  // If there are pending ops, apply them, clear them, and set the cache
  // at all the relevant levels. vbuf is the value read from the storageserver
  // from the transaction's snapshot, without reflecting any updates
  // done by the transaction.
  // If forcezero if set, then set the cache at level 0 with vbuf.
  // The given vbuf may be modified by this function, so caller
  // should not pass a value that is shared with others (unless
  // the intent is that the shared values be updated as well)
  // Return <0 if error, 0 if no pending ops, >0 if some pending ops
  int applyPendingOps(COid &coid, Ptr<Valbuf> &vbufref, bool forcezero);

  // applies one operation to the current version in the cache.
  // Requires that cache contains version and that poe's level
  // be the largest level in cache.
  void applyOneOp(COid &coid, PendingOpsEntry *poe);

  // -------------- Operations that affect just cache ----------------
  // returns 0 if found, non-zero if not found. If found, makes
  // tcel point to entry found
  int lookupCache(COid &coid, TxCacheEntryList *&tcel);
  void setCache(COid &coid, int level, Ptr<Valbuf> &vbuf); // set coid at level

  // -------------- Operations that affect just pending ops ----------------
  void clearallPendingOps(); // clears all pending ops
  bool hasPendingOps(COid &coid); // checks of tx nas pending updates on coid
  int checkPendingOps(COid &coid, int nKey, char *pKey, Ptr<RcKeyInfo> prki);
     // check if key was added or removed by pending operations. Returns 1 if
     // added, 0 if removed, -1 if neither
  void removePendingOps(COid &coid); // remove pending ops for coid
  void addPendingOps(COid &coid, PendingOpsEntry *poe); // add to coid's
                                                        // pendingops
};

int myCellSearchUnpacked(Ptr<Valbuf> &vbuf, UnpackedRecord *pIdxKey,
                                i64 nkey, int biasRight, int *matches=0);

int myCellSearchNode(Ptr<Valbuf> &vbuf, i64 nkey, void *pkey, int biasRight, 
                     Ptr<RcKeyInfo> prki, int *matches=0);

int mycompareNpKeyWithKey(i64 nKey1, char *pKey1, i64 nKey2,
                          UnpackedRecord *pIdxKey2);

#endif

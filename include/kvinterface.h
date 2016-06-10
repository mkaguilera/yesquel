//
// kvinterface.h
//
// Client interface to key-value storage system
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

#ifndef _KVINTERFACE_H
#define _KVINTERFACE_H

#include "options.h"
#include "record.h"
#include "coid.h"

#define MEMKVSTORE_HASHTABLE_SIZE 50000

#if defined(DEBUGKVLOG)
#include "debug.h"
#define KVLOG(format,...) dprintf(3, "%s:%d:" format "\n", __FUNCTION__, __LINE__, ##__VA_ARGS__)
#else
#define KVLOG(format,...)
#endif

class Transaction;
class LocalTransaction;

#include "gaiatypes.h"
#include "supervalue.h"
#include "datastructmt.h"

// This function must be called exactly once before using the functions below
void KVInterfaceInit(void);

#define GLOBALCACHE_HASHTABLE_SIZE 200

struct GlobalCacheEntry {
  Ptr<Valbuf> vbuf;
};

class GlobalCache {
private:
  static int auxExtractVbuf(COid &coid, GlobalCacheEntry *gce, int status, SkipList<COid,GlobalCacheEntry> *b, u64);
  static int auxReplaceVbuf(COid &coid, GlobalCacheEntry *gce, int status, SkipList<COid,GlobalCacheEntry> *b, u64);
  HashTableMT<COid,GlobalCacheEntry> Cache;
public:
  GlobalCache();
  ~GlobalCache();

  // looks up a coid. If there is an entry, sets vbuf to it.
  // Does not copy buffer in vbuf, so caller should give an immutable buffer.
  // Returns 0 if item was found, non-zero if not found.
  int lookup(COid &coid, Ptr<Valbuf> &vbuf);

  // Remove a coid from the cache.
  // Returns 0 if item was found and removed, non-zero if not found
  int remove(COid &coid);

  // Refresh cache if given vbuf is newer than what is in there.
  // If refreshing, make a new copy of the buffer.
  // Returns 0 if item was refreshed, non-zero if it was not.
  int refresh(Ptr<Valbuf> &vbuf);
};

extern GlobalCache GCache;

// work to do after transaction commits
// Right now, the work item is specialized to splitting a node, so
// it includes only the parameters needed for splitting.
struct WorkItem {
  WorkItem(){}
  WorkItem(const COid &c, void *sp){ coid=c; specificparm = sp; }
  COid coid; // where to split
  void *specificparm; // per-split parameter passed to splitter
  WorkItem *next, *prev;
};

struct KVTransaction {
  void AddWork(COid coid, int isleaf){
    if (!work) work = new LinkList<WorkItem>;
    WorkItem *wi = new WorkItem(coid, (void*) (long long) isleaf);
    work->pushTail(wi);
  }
  KVTransaction(){ work=0; readonly=1; }
  ~KVTransaction(){
    if (work){ 
      while (!work->empty()) delete work->popHead();
    }
    delete work;
  }

  int type;   // 0=in-memory, 1=remote
  union {
    LocalTransaction *lt;
    Transaction *t;
  } u;

  LinkList<WorkItem> *work;
  int readonly;
};

#ifdef __cplusplus
extern "C" {
#endif

int membeginTx(KVTransaction **tx);
int memcommitTx(KVTransaction *tx);
int memabortTx(KVTransaction *tx);
int memKVget(KVTransaction *tx, COid coid, char **buf, int *len);

// get variation that allocates pad extra space in buffer beyond received data.
// Padded space is NOT zeroed.
int memKVgetPad(KVTransaction *tx, COid coid, char **buf, int *len, int pad); 
int memKVput(KVTransaction *tx, COid &coid,  char *data, int len);
int memKVput2(KVTransaction *tx, COid &coid,  char *data1, int len1,
              char *data2, int len2);
int memKVput3(KVTransaction *tx, COid &coid,  char *data1, int len1,
              char *data2, int len2, char *data3, int len3);

int beginTx(KVTransaction **txp, bool remote=true, bool deferred=false);
int commitTx(KVTransaction *tx, Timestamp *retcommitts=0);
int abortTx(KVTransaction *tx);
int freeTx(KVTransaction *tx);
int beginSubTx(KVTransaction *tx, int level);
int abortSubTx(KVTransaction *tx, int level);
int releaseSubTx(KVTransaction *tx, int level);
  
int KVget(KVTransaction *tx, COid coid, Ptr<Valbuf> &buf);

// get variation that allocates pad extra space in buffer beyond received data.
// Padded space is NOT zeroed */
int KVput(KVTransaction *tx, COid coid,  char *data, int len);
int KVput2(KVTransaction *tx, COid coid,  char *data1, int len1,
           char *data2, int len2);
int KVput3(KVTransaction *tx, COid coid,  char *data1, int len1,
           char *data2, int len2, char *data3, int len3);

int KVreadSuperValue(KVTransaction *tx, COid coid, Ptr<Valbuf> &buf,
                     ListCell *cell, Ptr<RcKeyInfo> prki);
int KVwriteSuperValue(KVTransaction *tx, COid coid, SuperValue *sv);
#if DTREE_SPLIT_LOCATION != 1
  int KVlistadd(KVTransaction *tx, COid coid, ListCell *cell,
                Ptr<RcKeyInfo> prki, int flags);
#else
  // when split occurs at client, KVlistadd returns some extra information for
  // client to determine whether to split or not
  int KVlistadd(KVTransaction *tx, COid coid, ListCell *cell,
                Ptr<RcKeyInfo> prki, int flags, int *ncells, int *size);
#endif
int KVlistdelrange(KVTransaction *tx, COid coid, u8 intervalType,
                   ListCell *cell1, ListCell *cell2, Ptr<RcKeyInfo> prki);
int KVattrset(KVTransaction *tx, COid coid, u32  attrid, u64 attrval);

int KVtxreadonly(KVTransaction *tx); // return whether transaction is
                                     // read-only so far
  
#ifdef __cplusplus
}; // extern "C"

#endif
#endif

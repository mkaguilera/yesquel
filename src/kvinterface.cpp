//
// kvinterface.cpp
//
// Client interface to key-value storage system.
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
#include <stdarg.h>
#include <stdlib.h>
#include <time.h>
#include <malloc.h>

#include "options.h"
#include "tmalloc.h"
#include "os.h"
#include "util.h"

#include "dtreeaux.h"
#include "splitter-client.h"
#include "dtreesplit.h"

#ifdef NOGAIA
#define NOGAIA_BOOL 1
#else
#define NOGAIA_BOOL 0
#endif

#include "kvinterface.h"

#include "clientlib.h"
#include "clientlib-local.h"
#include "clientdir.h"
extern StorageConfig *SC;

GlobalCache GCache;

GlobalCache::GlobalCache() : 
  Cache(GLOBALCACHE_HASHTABLE_SIZE)
{
}

GlobalCache::~GlobalCache(){
  Cache.clear(0,0);
}

// auxiliary function to extract a vbuf from a GlobalCacheEntry,
// to be used in LookupExtract below
int GlobalCache::auxExtractVbuf(COid &coid, GlobalCacheEntry *gce, int status, SkipList<COid,GlobalCacheEntry> *b, u64 parm){
  Ptr<Valbuf> *vbuf = (Ptr<Valbuf> *) parm;
  if (status==0){ // found
    *vbuf = gce->vbuf;
    return 0;
  }
  else return -1; // not found
}

// looks up a coid. If there is an entry, sets vbuf to it.
// Does not copy buffer in vbuf, so caller should give an immutable buffer.
// Returns 0 if item was found, non-zero if not found.
int GlobalCache::lookup(COid &coid, Ptr<Valbuf> &vbuf){
  int res;
  res = Cache.lookupApply(coid, auxExtractVbuf, (u64) &vbuf);
  return res;
}

// Removes the cache entry for coid.
// Returns 0 if item was found and removed, non-zero if not found.
int GlobalCache::remove(COid &coid){
  return Cache.remove(coid,0);
}

// auxilliary function to be called by refresh.
// If gce is non-null, check if the passed vbuf (parm) has fresher timestamp; if so,
//    replace gce->vbuf with a copy of the passed vbuf
// If gce is null, add a new entry for coid to b, with a copy of the passed vbuf.
// Returns 0 if the gce entry as created or refreshed, -1 if it was not modified
int GlobalCache::auxReplaceVbuf(COid &coid, GlobalCacheEntry *gce, int status, SkipList<COid,GlobalCacheEntry> *b, u64 parm){
  int res;
  Ptr<Valbuf> *vbuf = (Ptr<Valbuf> *) parm;
  if (status){ // not there, insert
    GlobalCacheEntry newgce;
    //gce->coid = (*vbuf)->coid;
    newgce.vbuf = new Valbuf(**vbuf); // make a copy of data
    assert(COid::cmp(coid, (*vbuf)->coid)==0);
    b->insert(coid, newgce);
    res = 0; // item refreshed
  } else { // there already; check timestamp
    if (Timestamp::cmp(gce->vbuf->readTs, (*vbuf)->readTs) < 0){ // replace item
      res = 0; // item refreshed
      gce->vbuf = new Valbuf(**vbuf); // make a copy of data
    }
    else res = -1; // item not refreshed
  }
  return res;
}

// refresh cache if given vbuf is newer than what is in there.
// Returns 0 if item was refreshed, non-zero if it was not
int GlobalCache::refresh(Ptr<Valbuf> &vbuf){
  int res;
  res = Cache.lookupApply(vbuf->coid, auxReplaceVbuf, (u64) &vbuf);
  return res;
}

// Begins a transaction. If remote=0 then do an in-memory transaction.
// Otherwise, do it remotely. The function can offset the start timestamp
// by offsetms (positive means into the past, negative into the future).
int beginTx(KVTransaction **txp, bool remote, bool deferred){
  KVTransaction *tx;
  tx = *txp = new KVTransaction;
  KVLOG("Tx %p", tx);

  if (NOGAIA_BOOL) remote=0;
  tx->type = remote ? 1 : 0;

  if (!remote){ // local
    tx->u.lt = new LocalTransaction; // set any local values
    if (deferred){
      tx->u.lt->startDeferredTs();
    }
  }
  else {
    tx->u.t = new Transaction(SC);
    if (deferred){
      tx->u.t->startDeferredTs();
    }
  }
  return 0;
}

bool HasKVInterfaceInit = false;

#if (DTREE_SPLIT_LOCATION==1)
int moreSplitCallback(COid coid, int where, void *parm, int isleaf){
  KVTransaction *tx = (KVTransaction*) parm;
  if (where==0) tx->work->pushHead(new WorkItem(coid,(void*)(long long)isleaf));
  else tx->work->pushTail(new WorkItem(coid,(void*)(long long)isleaf));
  return 0;
}

void KVInterfaceInit(void){
  HasKVInterfaceInit = true;
}
#else

void KVInterfaceInit(void){
  HasKVInterfaceInit = true;
}
#endif

int commitTx(KVTransaction *tx, Timestamp *retcommitts){
  int res=-1;
  KVLOG("Tx %p", tx);

  if (tx->type==0) res = tx->u.lt->tryCommit(retcommitts);
  else res = tx->u.t->tryCommit(retcommitts);

  if (tx->work && !tx->work->empty()){
    assert(HasKVInterfaceInit);
    WorkItem *wi;
    while (!tx->work->empty()){
      wi = tx->work->popHead();
      if (res == 0){ // if committed...
#if (DTREE_SPLIT_LOCATION==1)
        int retries = 0;
        // do the split ourselves
        do { // repeat until split is successful
          ++retries;
          dputchar(1,'S');
          res = DtSplit(wi->coid, 0, true, moreSplitCallback, (void*) tx); 
          if (res){
            dputchar(1,'X');
            mssleep(10);
          } else dputchar(1,'O');
        } while (res && retries < DTREE_SPLIT_CLIENT_MAX_RETRIES);
        res = 0;
#endif
      }
      delete wi;
    }
  }
  return res;
}

int abortTx(KVTransaction *tx){
  int res=-1;
  tx->readonly=1;
  KVLOG("Tx %p", tx);
  if (tx->type==0) res = tx->u.lt->abort();
  else res = tx->u.t->abort();
  return res;
}

int freeTx(KVTransaction *tx){
  if (tx->type==0) delete tx->u.lt;
  else delete tx->u.t;
  delete tx;
  return 0;
}

int beginSubTx(KVTransaction *tx, int level){
  KVLOG("beginSubTx %p", tx);
  int res;
  if (tx->type==0) res = tx->u.lt->startSubtrans(level);
  else res = tx->u.t->startSubtrans(level);
  return res;
}

int abortSubTx(KVTransaction *tx, int level){
  KVLOG("abortSubTx %p", tx);
  int res;
  if (tx->type==0) res = tx->u.lt->abortSubtrans(level);
  else res = tx->u.t->abortSubtrans(level);
  return res;
}

int releaseSubTx(KVTransaction *tx, int level){
  KVLOG("releaseSubTx %p", tx);
  int res;
  if (tx->type==0) res = tx->u.lt->releaseSubtrans(level);
  else res = tx->u.t->releaseSubtrans(level);
  return res;
}



int KVget(KVTransaction *tx, COid coid, Ptr<Valbuf> &buf){
  int res=-1;

  //if (PERFECTREADCACHE_BOOL || tx->type==0)
  if (tx->type==0)
    res =  tx->u.lt->vget(coid, buf);
  else {
    assert(!(coid.cid >> 48 & EPHEMDB_CID_BIT)); // container should not be
                                                // ephemeral for remote txs
    res = tx->u.t->vget(coid, buf);
  }
  if (res) 
    buf=0;

  KVLOG("Tx %p cid %llx oid %llx bytes %d", tx, (long long)coid.cid,
        (long long)coid.oid, buf->len);
  return res;
}

int KVput(KVTransaction *tx, COid coid,  char *data, int len){
  int res=-1;
  tx->readonly = 0;
  KVLOG("Tx %p cid %llx oid %llx bytes %d", tx, (long long)coid.cid,
        (long long)coid.oid, len);

  if (tx->type==0)
    res = tx->u.lt->put(coid, data, len);
  else {
    assert(!(coid.cid >> 48 & EPHEMDB_CID_BIT)); // container should not
                                       // be ephemeral for remote txs
    // if (PERFECTREADCACHE_BOOL) res = tx->u.lt->put(coid, data, len);
    res = tx->u.t->put(coid, data, len);
  }
  return res;
}

int KVput2(KVTransaction *tx, COid coid,  char *data1, int len1, char *data2,
           int len2){
  int res=-1;
  tx->readonly = 0;
  KVLOG("Tx %p cid %llx oid %llx bytes %d", tx, (long long)coid.cid,
        (long long)coid.oid, len1+len2);

  if (tx->type==0)
    res = tx->u.lt->put2(coid,data1,len1,data2,len2);
  else {
    assert(!(coid.cid >> 48 & EPHEMDB_CID_BIT)); // container should not
                                                // be ephemeral for remote txs
    // if (PERFECTREADCACHE_BOOL)
    //   res = tx->u.lt->put2(coid,data1,len1,data2,len2);
    res = tx->u.t->put2(coid,data1,len1,data2,len2);
  }
  return res;
}

int KVput3(KVTransaction *tx, COid coid,  char *data1, int len1, char *data2,
           int len2, char *data3, int len3){
  int res=-1;
  tx->readonly = 0;
  KVLOG("Tx %p cid %llx oid %llx bytes %d", tx, (long long)coid.cid,
        (long long)coid.oid, len1+len2+len3);

  if (tx->type==0)
    res = tx->u.lt->put3(coid,data1,len1,data2,len2,data3,len3);
  else {
    assert(!(coid.cid >> 48 & EPHEMDB_CID_BIT)); // container should not
                                           // be ephemeral for remote txs
    // if (PERFECTREADCACHE_BOOL)
    //   res = tx->u.lt->put3(coid,data1,len1,data2,len2,data3,len3);
    res = tx->u.t->put3(coid,data1,len1,data2,len2,data3,len3);
  }
  return res;
}

//// returns:
////   0 if ok
////  -99 if value is not a supervalue
////  <0 for other errors
int KVreadSuperValue(KVTransaction *tx, COid coid, Ptr<Valbuf> &buf,
                     ListCell *cell, Ptr<RcKeyInfo> prki){
  int res=-1;
  if (tx->type==0)
    res =  tx->u.lt->vsuperget(coid, buf, cell, prki);
  else {
    assert(!(coid.cid >> 48 & EPHEMDB_CID_BIT)); // container should not
                                                 // be ephemeral for remote txs
    res = tx->u.t->vsuperget(coid, buf, cell, prki);
  }
  if (res) buf=0;

  KVLOG("Tx %p cid %llx oid %llx nattrs %d ncells %d", tx,
        (long long)coid.cid, (long long)coid.oid, buf->u.raw->Nattrs,
        buf->u.raw->Ncells);
  return res;
}

int KVwriteSuperValue(KVTransaction *tx, COid coid, SuperValue *sv){
  tx->readonly = 0;
  KVLOG("Tx %p cid %llx oid %llx nattrs %d ncells %d", tx,
        (long long)coid.cid, (long long)coid.oid, sv->Nattrs, sv->Ncells);
  assert(sv->CellType == 0 || sv->Ncells == 0 || sv->prki.isset());
    // to write non-int cells, must provide prki

  if (tx->type==0)
    return tx->u.lt->writeSuperValue(coid, sv);
  else {
    assert(!(coid.cid >> 48 & EPHEMDB_CID_BIT)); // container should not
                                                // be ephemeral for remote txs
    return tx->u.t->writeSuperValue(coid, sv);
  }
}

#if DTREE_SPLIT_LOCATION != 1
int KVlistadd(KVTransaction *tx, COid coid, ListCell *cell, Ptr<RcKeyInfo> prki,
              int flags){
#else
int KVlistadd(KVTransaction *tx, COid coid, ListCell *cell, Ptr<RcKeyInfo> prki,
              int flags, int *ncells, int *size){
#endif
  
  int res=-1;
  tx->readonly = 0;
  KVLOG("Tx %p cid %llx oid %llx flags %d", tx, (long long)coid.cid,
        (long long)coid.oid, flags);

  if (tx->type==0)
#if DTREE_SPLIT_LOCATION != 1
    res = tx->u.lt->listAdd(coid, cell, prki, flags);
#else
    res = tx->u.lt->listAdd(coid, cell, prki, flags, ncells, size);
#endif  
  else {
    assert(!(coid.cid >> 48 & EPHEMDB_CID_BIT)); // container should not
                                                // be ephemeral for remote txs
    // if (PERFECTREADCACHE_BOOL)
    //   res = tx->u.lt->listAdd(coid, cell, prki, flags);
#if DTREE_SPLIT_LOCATION != 1
    res = tx->u.t->listAdd(coid, cell, prki, flags);
#else
    res = tx->u.t->listAdd(coid, cell, prki, flags, ncells, size);
#endif
  }
  return res;
}

// deletes a range of cells
// intervalType indicates how the interval is to be treated. The possible
// values are
//     0=(cell1,cell2)   1=(cell1,cell2]   2=(cell1,inf)
//     3=[cell1,cell2)   4=[cell1,cell2]   5=[cell1,inf)
//     6=(-inf,cell2)    7=(-inf,cell2]    8=(-inf,inf)
// where inf is infinity
int KVlistdelrange(KVTransaction *tx, COid coid, u8 intervalType,
                   ListCell *cell1, ListCell *cell2, Ptr<RcKeyInfo> prki){
  int res=-1;
  tx->readonly = 0;
  KVLOG("Tx %p cid %llx oid %llx", tx, (long long)coid.cid,
        (long long)coid.oid);

  if (tx->type==0)
    res = tx->u.lt->listDelRange(coid, intervalType, cell1, cell2, prki);
  else {
    assert(!(coid.cid >> 48 & EPHEMDB_CID_BIT)); // container should not
                                                 // be ephemeral for remote txs
    // if (PERFECTREADCACHE_BOOL)
    //   res = tx->u.lt->listDelRange(coid, intervalType, cell1, cell2, prki);
    res = tx->u.t->listDelRange(coid, intervalType, cell1, cell2, prki);
  }
  return res;
}

int KVattrset(KVTransaction *tx, COid coid, u32 attrid, u64 attrval){
  int res=-1;
  tx->readonly = 0;
  KVLOG("Tx %p cid %llx oid %llx attrid %d attrval %llx", tx,
        (long long)coid.cid, (long long)coid.oid, attrid, (long long)attrval);

  if (tx->type==0)
    res = tx->u.lt->attrSet(coid, attrid, attrval);
  else {
    assert(!(coid.cid >> 48 & EPHEMDB_CID_BIT)); // container should not
                                                 // be ephemeral for remote txs
    // if (PERFECTREADCACHE_BOOL)
    //   res = tx->u.lt->attrSet(coid, attrid, attrval);
    res = tx->u.t->attrSet(coid, attrid, attrval);
  }
  return res;
}
 
// return whether transaction is read-only so far
int KVtxreadonly(KVTransaction *tx){
  return tx->readonly;
}

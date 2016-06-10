//
// logmem.h
//
// In-memory cache of log at storage server.
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
#include <unistd.h>
#include <stdarg.h>
#include <ctype.h>
#include <stddef.h>
#include <stdio.h>
#include <dirent.h>

#include <map>
#include <list>
#include <set>

#include "tmalloc.h"
#include "os.h"
#include "options.h"
#include "debug.h"
#include "logmem.h"
#include "storageserver.h"

using namespace std;

void LogOneObjectInMemory::print(COid &coid){
  SingleLogEntryInMemory *sleim;

  lock();
  printf(" LastRead %llx\n", (long long)LastRead.getd1());
  for (sleim = logentries.getFirst(); sleim != logentries.getLast();
       sleim = logentries.getNext(sleim)){
    printf(" cid %016llx oid %016llx ts %llx dirty %d\n  ", 
      (long long)coid.cid, (long long)coid.oid, (long long)sleim->ts.getd1(),
           sleim->flags & SLEIM_FLAG_DIRTY);
    sleim->tucoid->print();
  }
  printf("  ------ pending entries follow\n  ");
  for (sleim = pendingentries.getFirst(); sleim != pendingentries.getLast();
       sleim = pendingentries.getNext(sleim)){
    printf(" cid %016llx oid %016llx ts %llx dirty %d\n  ", 
      (long long)coid.cid, (long long)coid.oid, (long long)sleim->ts.getd1(),
           sleim->flags & SLEIM_FLAG_DIRTY);
    sleim->tucoid->print();
  }
  unlock();
}

void LogOneObjectInMemory::printdetail(COid &coid, bool locklooim){
  SingleLogEntryInMemory *sleim;

  if (locklooim) lock();
  printf(" LastRead %llx\n", (long long)LastRead.getd1());
  if (logentries.empty()){ printf(" logentries empty\n"); }
  else {
    printf(" logentries-------------\n");
    for (sleim = logentries.getFirst(); sleim != logentries.getLast();
         sleim = logentries.getNext(sleim)){
      printf("  ");
      sleim->printShort(coid);
      putchar('\n');
    }
  }
  if (pendingentries.empty()){ printf(" pendingentries empty\n"); }
  else {
    printf(" pendingingentries---------");

    for (sleim = pendingentries.getFirst(); sleim != pendingentries.getLast();
         sleim = pendingentries.getNext(sleim)){
      printf("  ");
      sleim->printShort(coid);
      putchar('\n');
    }
  }
  if (locklooim) unlock();
}

void LogInMemory::printAllLooim(){
  int nbuckets, i;
  SkipList<COid, LogOneObjectInMemory*> *bucket;
  SkipListNode<COid, LogOneObjectInMemory*> *ptr;
  Timestamp ts;
  Ptr<TxUpdateCoid> tucoid;
  int size;
  int nitems=0;

  ts.setNew();

  nbuckets = COidMap.GetNbuckets();
  for (i=0; i < nbuckets; ++i){
    bucket = COidMap.GetBucket(i);
    for (ptr = bucket->getFirst(); ptr != bucket->getLast();
         ptr = bucket->getNext(ptr)){
      ++nitems;
      // read entire oid
      size = readCOid(ptr->key, ts, tucoid, 0, 0);

      if (size >= 0){
        printf("COid %016llx:%016llx ", (long long)ptr->key.cid,
               (long long)ptr->key.oid);
        tucoid->printdetail(ptr->key);
        putchar('\n');
      } else if (size == GAIAERR_TOO_OLD_VERSION)
        printf("COid %016llx:%016llx *nodata*\n", (long long)ptr->key.cid,
               (long long)ptr->key.oid);
      else printf("COid %016llx:%016llx error %d\n", (long long)ptr->key.cid,
                  (long long)ptr->key.oid, size);
    }
  }
  printf("Total objects %d\n", nitems);
}

void LogInMemory::printAllLooimDetailed(){
  int nbuckets, i;
  SkipList<COid, LogOneObjectInMemory*> *bucket;
  SkipListNode<COid, LogOneObjectInMemory*> *ptr;
  Timestamp ts;
  Ptr<TxUpdateCoid> tucoid;
  int size;
  int nitems=0;

  ts.setNew();

  nbuckets = COidMap.GetNbuckets();
  for (i=0; i < nbuckets; ++i){
    bucket = COidMap.GetBucket(i);
    for (ptr = bucket->getFirst(); ptr != bucket->getLast();
         ptr = bucket->getNext(ptr)){
      ++nitems;
      printf("COid %016llx:%016llx-----------------------------------------------------\n",
             (long long)ptr->key.cid, (long long)ptr->key.oid);
      ptr->value->printdetail(ptr->key);
      printf(" Contents ");
      
      // read entire oid
      size = readCOid(ptr->key, ts, tucoid, 0, 0);
      if (size >= 0){
        tucoid->printdetail(ptr->key);
        putchar('\n');
      } else if (size == GAIAERR_TOO_OLD_VERSION)
        printf("COid %016llx:%016llx *nodata*\n", (long long)ptr->key.cid,
               (long long)ptr->key.oid);
      else printf("COid %016llx:%016llx error %d\n", (long long)ptr->key.cid,
                  (long long)ptr->key.oid, size);
    }
  }
  printf("Total objects %d\n", nitems);
}

#ifndef LOCALSTORAGE
LogInMemory::LogInMemory(DiskStorage *ds) :
  COidMap(COID_CACHE_HASHTABLE_SIZE)
#else
LogInMemory::LogInMemory(DiskStorage *ds) :
  COidMap(COID_CACHE_HASHTABLE_SIZE_LOCAL)   
#endif
{ DS = ds; SingleVersion = false; }

void LogInMemory::getAndLockaux(int res, LogOneObjectInMemory **looimptr){
  if (res) *looimptr = new LogOneObjectInMemory; // not found, so create object
}

// Return entry for an object and locks it for reading or writing.
// If entry does not exist, create it, reading object from disk
// to set the sole entry in the log.
//   coid is the coid of the object
//   writelock=true if locking for write, false if locking for read
// If createfirstlog is true, then create first log entry for object if object
// is not found
LogOneObjectInMemory *LogInMemory::getAndLock(COid& coid, bool writelock,
                                              bool createfirstlog){
  LogOneObjectInMemory *looim, **looimptr;
  int size, res;
  SingleLogEntryInMemory *sleim;

  res = COidMap.lookupInsert(coid, looimptr, getAndLockaux);
  looim = *looimptr;
  if (res==0){ // object found
    if (writelock) looim->lock();
    else looim->lockRead();
    return looim;
  }

  // object not found
  looim->lock();
  
  if (looim->logentries.getNitems() != 0){ // someone else created item
                                           // concurrently
    if (!writelock) { looim->unlock(); looim->lockRead(); } // change lock mode,
                                                            // as requested
    return looim;
  }
  
  // try to read object from disk
  size = DS->getCOidSize(coid);
  sleim = 0;

  if (size >= 0){ // oid on disk already, so read it
    sleim = new SingleLogEntryInMemory;
    sleim->ts.setIllegal(); // unknown yet, will be filled by DS->ReadOid below
    sleim->flags = 0; 

    // read it from disk
    Ptr<TxUpdateCoid> tucoid;
    res = DS->readCOid(coid, size, tucoid, sleim->ts); assert(res==0);
    // exactly one of twi or twsvi must be non-zero
    assert(tucoid->Writevalue && !tucoid->WriteSV || !tucoid->Writevalue
           && tucoid->WriteSV);
    assert(tucoid->Litems.getNitems() == 0);

    sleim->tucoid = tucoid;

    // insert one item into list
    looim->logentries.pushTail(sleim);
  } else {
    if (createfirstlog){  // create a first log entry
      sleim = new SingleLogEntryInMemory;
      sleim->ts.setLowest(); // empty entry has lowest timestamp
      sleim->flags = 0; 
      TxWriteItem *twi = new TxWriteItem(coid, 0);
      twi->len = 0;
      twi->buf = (char*) malloc(1);
      twi->rpcrequest = 0;
      twi->alloctype = 1;  // allocated via malloc
      sleim->tucoid = new TxUpdateCoid(twi);
      sleim->ts.setLowest(); 
      looim->logentries.pushTail(sleim);
    }
  }
#if (SYNC_TYPE != 3)
  // for SYNC_TYPE 3, lock() and lockRead() are identical
  if (!writelock) { looim->unlock(); looim->lockRead(); } // change lock mode,
                                                          // as requested
#endif
  return looim;
}

#ifndef NDEBUG
static int checkTucoid(Ptr<TxUpdateCoid> tucoid){
  int i;
  int someset=0;
  for (i=0; i < GAIA_MAX_ATTRS; ++i){
    assert((tucoid->SetAttrs[i] & 1) == tucoid->SetAttrs[i]); // only lowest bit
                                                              // should be set
    if (tucoid->SetAttrs[i]) someset=1;
  }
  if (!someset && tucoid->Litems.empty())
    assert(!tucoid->Writevalue && tucoid->WriteSV || tucoid->Writevalue &&
           !tucoid->WriteSV);
  //if (tucoid->SLAddItems.nitems > 0) assert(tucoid->SLpopulated);
  return 1;
}

static int checklog(LinkList<SingleLogEntryInMemory> &logentries){
  SingleLogEntryInMemory *sleim;
  Timestamp ts;

  if (logentries.getNitems() == 0) return 1;
  ts.setLowest();
  bool hasckp = false;
  for (sleim = logentries.getFirst(); sleim != logentries.getLast();
       sleim = logentries.getNext(sleim)){
    assert(Timestamp::cmp(ts, sleim->ts) <= 0);
    assert((sleim->flags & (SLEIM_FLAG_LAST-1)) == sleim->flags);
    assert(checkTucoid(sleim->tucoid));
    if (sleim->tucoid->Writevalue || sleim->tucoid->WriteSV) hasckp = true;
    ts = sleim->ts;
  }
  assert(hasckp);
  return 1;
}

static int checkpending(LinkList<SingleLogEntryInMemory> &pendingentries){
  SingleLogEntryInMemory *sleim;
  Timestamp ts;

  if (pendingentries.getNitems() == 0) return 1;
  ts.setLowest();
  for (sleim = pendingentries.getFirst(); sleim != pendingentries.getLast();
       sleim = pendingentries.getNext(sleim)){
    assert(Timestamp::cmp(ts, sleim->ts) <= 0);
    assert((sleim->flags & (SLEIM_FLAG_LAST-1)) == sleim->flags);
    assert(checkTucoid(sleim->tucoid));
    ts = sleim->ts;
  }
  return 1;
}

#endif

// Eliminates old entries from log. The eliminated entries are the ones that
// are subsumed by a newer entry and that are older than LOG_STALE_GC_MS
// relative to the given ts.
// Assumes looim->object_lock is held in write mode.
// Returns number of entries that were removed.

int LogInMemory::gClog(LogOneObjectInMemory *looim, Timestamp ts){
  SingleLogEntryInMemory *sleim, *sleimchkpoint=0;
  int ndeleted=0;

  ts.addMs(-LOG_STALE_GC_MS);

  // find the highest checkpoint with a timestamp <= ts
  for (sleim = looim->logentries.getFirst();
       sleim != looim->logentries.getLast();
       sleim = looim->logentries.getNext(sleim)){
    if (Timestamp::cmp(sleim->ts, ts) >= 0) break;
    if (sleim->tucoid->Writevalue || sleim->tucoid->WriteSV)
      sleimchkpoint = sleim;
  }
  if (!sleimchkpoint) return 0; // nothing to GC
  sleim = looim->logentries.getFirst();
  while (sleim != sleimchkpoint){
    ++ndeleted;
    looim->logentries.popHead();
    delete sleim;
    sleim = looim->logentries.getFirst();
  }
  return ndeleted;
}


// Applies the changes in a tucoid to a TxWriteSVItem twsvi. This is not
// intended to be used when tucoid includes an entire write to a value or
// supervalue, in which case the function returns an error.
//
// Requires twsvi != 0
// Returns number of listadd and listdelrange items applied, as well as a error
// result (res).
// If tucoid has a write to a value or supervalue, sets res != 0, otherwise sets
// res == 0
LogInMemory::NUpdates LogInMemory::applyTucoid(TxWriteSVItem *twsvi,
                                               Ptr<TxUpdateCoid> tucoid){
  NUpdates retval;
  assert(twsvi);
  memset((void*)&retval, 0, sizeof(NUpdates));
  
  if (tucoid->Writevalue || tucoid->WriteSV){
    retval.res = -1;
    return retval;
  }
    
  // apply any updates to attributes
  for (int i=0; i < GAIA_MAX_ATTRS; ++i){
    if (tucoid->SetAttrs[i]){ // setting attribute
      assert(twsvi->nattrs >= i);
      twsvi->attrs[i] = tucoid->Attrs[i]; // apply the change
    }
  }
  for (TxListItem *tli = tucoid->Litems.getFirst();
       tli != tucoid->Litems.getLast();
       tli = tucoid->Litems.getNext(tli)){
    if (tli->type==0){ // add item
      ++retval.nadd;
      TxListAddItem *tlai = dynamic_cast<TxListAddItem*>(tli);
      if (tlai->item.pprki.hasprki()){
        // provide prki to TxWriteSVItem if it doesn't have it already
        twsvi->setPrkiSticky(tlai->item.pprki.getprki());
      }
      // copy item since twsvi->cells will own it      
      // Use prki of enclosing TxWriteSVItem.
      twsvi->cells.insertOrReplace(new ListCellPlus(tlai->item, &twsvi->prki),0,
                                   ListCellPlus::del, 0);
    }
    else { // delrange item
      assert(tli->type==1);
      int type1, type2; // indicates type of left and right intervals
      ++retval.ndelrange;
      TxListDelRangeItem *tldri = dynamic_cast<TxListDelRangeItem*>(tli);
      TxWriteSVItem::convertOneIntervalTypeToTwoIntervalType(
                     tldri->intervalType, type1, type2);
      if (tldri->itemstart.pprki.hasprki()){
        // provide prki to TxWriteSVItem if it doesn't have it already        
        twsvi->setPrkiSticky(tldri->itemstart.pprki.getprki());
      }
      twsvi->cells.delRange(&tldri->itemstart, type1, &tldri->itemend, type2,
                            ListCellPlus::del, 0);
    }
  }
  
  return retval;
}

// read data of a given oid and version, starting from offset off
// with length len. If len==-1 then read until the end.
// Put the result in rettucoid. Returns
//          0 if ok
//         -1 if oid does not exist,
//         -2 if oid exists but version does not exist
//         -3 if oid exists, data to be read is pending, and read cannot be
//             deferred (deferredhandle==0)
//         -4 if oid is corrupted (e.g., attrset followed by a regular value)
//         -5 if oid exists, data to be read is pending, and read was deferred
// *!* TODO: check for -4 return value from caller
// Also returns the timestamp of the data that is actually read in *readts,
// if readts != 0.
// If function returns an error and *buf=0 when it is called, it is possible
// that *buf gets changed to an allocated buffer, so caller should free *buf
//
// int LogInMemory::readCOid(COid& coid, Timestamp ts, int len, char **destbuf,
// Timestamp *readts, int nolock){

int LogInMemory::readCOid(COid& coid, Timestamp ts,
                          Ptr<TxUpdateCoid> &rettucoid,
                          Timestamp *readts, void *deferredhandle){
  LogOneObjectInMemory *looim;
  SingleLogEntryInMemory *sleim;
  Ptr<TxUpdateCoid> tucoid;
  int retval = 0;
  int type = -1;
  int moveback=0, moveforward=0, moveforwardadd=0, moveforwarddel=0;
  SingleLogEntryInMemory *pendingsleim=0;  

  // try to find COid in memory
  looim = getAndLock(coid, true, true); assert(looim);

  //assert(checklog(looim->logentries));
  //assert(checkpending(looim->pendingentries));
  sleim = 0;
  if (ts.isIllegal()){ // read first available timestamp
    Timestamp pendingts;

    // get earliest pending entry
    sleim = looim->pendingentries.getFirst();
    if (sleim != looim->pendingentries.getLast()) pendingts = sleim->ts;
    else pendingts.setHighest(); // no pending entries

    // first latest logentry that is <= than earliest pending entry
    // (= is fine since tx will commit with a ts strictly bigger than pending
    // entries)
    sleim = looim->logentries.rGetFirst(); // start from end
    while (sleim != looim->logentries.rGetLast()){
      if (Timestamp::cmp(sleim->ts, pendingts) <= 0)
        break; // found entry <= pending entry
      sleim = looim->logentries.rGetNext(sleim);
    }
    if (sleim == looim->logentries.rGetLast()){
      retval = GAIAERR_TOO_OLD_VERSION; goto end;
    }

    ts = sleim->ts; // this is the timestamp we will read
    goto proceed_with_read;
  }

  // check to see if there are any pending reads with higher timestamp,
  // in which case we need to defer or fail the read
  pendingsleim = looim->pendingentries.getFirst();
  if (pendingsleim != looim->pendingentries.getLast()){
    // pendingentries not empty
    // enough to check first entry since pendingentries is sorted by timestamp
    if (Timestamp::cmp(pendingsleim->ts, ts) <= 0){
      //some pendingentry has smaller timestamp, defer or fail
      if (deferredhandle){
        // TODO: some way to garbage collect those pending sleims. Right now,
        // it will remain pending until the transaction commits or aborts. But
        // if the client died, it remains pending forever. Need a way to
        // cancel the pending data after a while. Problem is that the
        // transaction may have committed (and server doesn't yet know about
        // it), so we cannot just throw away the pending data. We need to
        // determine transaction status and if aborted, throw away, otherwise
        // mark it as non-pending.
        WaitingListItem *vli = new WaitingListItem(deferredhandle,
                                  pendingsleim->waitOnPending.next, ts);
        // add item to waitOnPending linklist (at beginning)        
        pendingsleim->waitOnPending.next = vli; 

        // remember highest waiting timestamp. This is to report to the client
        // doing the pending
        // transaction, when it commits
        if (Timestamp::cmp(ts, pendingsleim->waitingts) > 0)
          pendingsleim->waitingts = ts;
        retval = GAIAERR_DEFER_RPC;
      } else {
        retval = GAIAERR_PENDING_DATA; // trying to read pending data
      }
      goto end;
    }
  }

  // move backwards in log looking for correct timestamp to read from (first timestamp <= ts)
  for (sleim = looim->logentries.rGetFirst();
       sleim != looim->logentries.rGetLast();
       sleim = looim->logentries.rGetNext(sleim)){
    if (Timestamp::cmp(sleim->ts, ts) <= 0) break; // found it
    ++moveback;
  }
  if (sleim == looim->logentries.rGetLast()){
    // version is too old for what we have in log
    retval =  GAIAERR_TOO_OLD_VERSION;
    goto end;
  }

  proceed_with_read:

  if (readts) *readts = sleim->ts; // record the timestamp

  // now keep moving backwards in log until we find a checkpoint (a full write
  // for a value or supervalue)
  for (; sleim != looim->logentries.rGetLast();
       sleim = looim->logentries.rGetNext(sleim)){
    if (sleim->tucoid->Writevalue){ type=0; break; } // found checkpoint of
                                                     // regular value
    if (sleim->tucoid->WriteSV){ type=1; break; }  // found checkpoint of
                                                   // supervalue
    ++moveback;
  }

  if (sleim == looim->logentries.rGetLast()){
    assert(type==-1);
    retval = GAIAERR_TOO_OLD_VERSION; // missing checkpoint of object, so
                                      // cannot read it
    goto end;
  }

  tucoid = sleim->tucoid;
  assert(tucoid->Writevalue && !tucoid->WriteSV || !tucoid->Writevalue &&
         tucoid->WriteSV);
  assert(tucoid->Litems.getNitems()==0);
#ifdef DEBUG
  for (int i=0; i < GAIA_MAX_ATTRS; ++i) assert(!tucoid->SetAttrs[i]);
#endif

  if (type == 0){ // regular value
    sleim = looim->logentries.getNext(sleim); // look at next log entry
    if (sleim != looim->logentries.getLast() && Timestamp::cmp(sleim->ts, ts)
        <= 0){
      // corruption: write of value cannot be followed by further updates      
      retval = GAIAERR_CORRUPTED_LOG; 
      goto end;
    }
  } else { // super value
    TxWriteSVItem *twsvi = 0; // set if we need to create a new twsvi
    Timestamp lastts;
    assert(type==1);
    assert(tucoid->WriteSV);
    sleim = looim->logentries.getNext(sleim);
    //twsvi = new TxWriteSVItem(*twsvi); // make a copy of what is in
                                 // log since we will be applying updates to it

    // update deltas in current and next log entries (sleims)
    while (sleim != looim->logentries.getLast() &&
           Timestamp::cmp(sleim->ts, ts) <= 0){
      ++moveforward;
      lastts = sleim->ts;
      // assert(!(sleim->flags & SLEIM_FLAG_PENDING)); // we already checked
                                               // above that it is not pending
      // Here update twsvi with the updates in the sleim
      //tucoid = sleim->tucoid;

      if (!twsvi) twsvi = new TxWriteSVItem(*tucoid->WriteSV); // copy WriteSV
                                                               // in checkpoint
      
      NUpdates nupdates = applyTucoid(twsvi, sleim->tucoid);
      assert(nupdates.res==0);
      moveforwardadd += nupdates.nadd;
      moveforwarddel += nupdates.ndelrange;
      
      sleim = looim->logentries.getNext(sleim);
    }
    if (twsvi){
      // create tucoid with checkpoint and perhaps insert it to in-memory log
      tucoid = new TxUpdateCoid(twsvi);
      if (moveforwardadd >= LOG_CHECKPOINT_MIN_ADDITEMS ||
          moveforwarddel >= LOG_CHECKPOINT_MIN_DELRANGEITEMS ||
          moveforward > LOG_CHECKPOINT_MIN_ITEMS){ // only store checkpoint in
                                    // log if some of these conditions are met
        SingleLogEntryInMemory *toadd = new SingleLogEntryInMemory;
        toadd->ts = lastts;
        toadd->flags = SLEIM_FLAG_SNAPSHOT;
        //toadd->dirty = false; // no need to flush this to disk
        //toadd->pending = false;
        toadd->tucoid = tucoid;
        looim->logentries.addBefore(toadd, sleim);
        //assert(checklog(looim->logentries));
      }
    }
  }

  if (Timestamp::cmp(looim->LastRead, ts) < 0) looim->LastRead = ts;
  rettucoid = tucoid;
  gClog(looim, ts);
  dprintf(2, "readCOid: moveback %d moveforward %d", moveback, moveforward);
     
end:
  looim->unlock();
  return retval;
}

// after writing, buf will be owned by LogInMemory. Caller should have
// allocated it and should not free it.
int LogInMemory::writeCOid(COid& coid, Timestamp ts, Ptr<TxUpdateCoid> tucoid){
  LogOneObjectInMemory *looim;

  assert(tucoid->Writevalue&&!tucoid->WriteSV ||
         !tucoid->Writevalue&&tucoid->WriteSV);
  assert(tucoid->Litems.getNitems()==0);
#ifdef DEBUG
  for (int i=0; i < GAIA_MAX_ATTRS; ++i) assert(!tucoid->SetAttrs[i]);
#endif

//  printf("writeCOid %016llx %016llx ts %llx truncate %d off %d len %d buf %c%c%c pending %d\n",
//    (long long)coid.cid, (long long)coid.oid, (long long)ts.getd1(),
//    truncate, off, len, buf[0], buf[1], buf[2], pending);

  looim = getAndLock(coid, true, false);
  auxAddSleimToLogentries(looim, ts, true,tucoid);
  looim->unlock();

  return 0;
}

// Wakes up deferred RPCs in the waiting list of a sleim or move them to the
// sleim of another pending entry, based on the ts of the waiting list item.
// If that ts is < than all pending entries (though it suffices to check the
// first since the entries are ordered by their timestamp), then wake up,
// otherwise move.
// Motivation: if the ts is < than all pending entries, then deferred RPC will
// not block. Otherwise, there is no point in waking it up.
// The waiting list of the given sleim ends up empty.
void LogInMemory::wakeOrMoveWaitingList(LogOneObjectInMemory *looim,
                                        SingleLogEntryInMemory *sleim){
  LinkList<SingleLogEntryInMemory> *pendingentries = &looim->pendingentries;
  SingleLogEntryInMemory *firstsleim;
  if (pendingentries->empty()) firstsleim = 0; // nothing
  else firstsleim = pendingentries->getFirst();

  // For each deferred RPC indicated in the sleim, check if its timestamp <
  // smallest timestamp in pendingentries. If so, wake up RPC. Otherwise,
  // move RPC to entry with smallest timestamp in pendingentries
  WaitingListItem *next;
  for (WaitingListItem *vli = sleim->waitOnPending.next; vli; vli = next){
    next = vli->next;
    if (!firstsleim || Timestamp::cmp(vli->ts, firstsleim->ts) < 0){
      // RPC ready to be woken up
      serverAuxWakeDeferred(vli->ptr);
      delete vli;
    }
    else { // Other sleims are blocking the item. Move it to one such sleim
           // (eg, first one)
      // insert item into waitOnPending linklist of firstsleim
      vli->next = firstsleim->waitOnPending.next;
      firstsleim->waitOnPending.next = vli;
    }
  }
  sleim->waitOnPending.next = 0; // clear list since we either deleted or
                                 // moved each item

}

// Remove entry from pendingentries.
// If parameter move is true, add removed entry to logentries, setting its final
// timestamp to the given parameter.
// Wake up any deferred RPCs indicated in the sleim if no more pendingentries
// will block it. If some pendingentries may block it, move deferred RPCs to
// one of them (say, the earliest one).
// Then GC logentries.
// Each tx should have a single pending entry per object, so this
// function is effecting all the updates of a tx to a given object.
void LogInMemory::removeOrMovePendingToLogentries(COid& coid,
     SingleLogEntryInMemory *pendingentriesSleim, Timestamp ts, bool move){
  assert(pendingentriesSleim);
  LogOneObjectInMemory *looim=0;
  int res;

  res = COidMap.lookup(coid, looim);
  if (res) return; // not found

  looim->lock();
#ifdef DEBUG
  //assert(checklog(looim->logentries));
  //assert(checkpending(looim->pendingentries));
#endif

  LinkList<SingleLogEntryInMemory> *pendingentries = &looim->pendingentries;
  // remove from pendingentries
  pendingentries->remove(pendingentriesSleim);

  if (move){
    // Add a new sleim to logentries
    // We could have recycled the sleim, but we do not so that we can reuse
    // auxAddSleimToLogentries.
    auxAddSleimToLogentries(looim, ts, true, pendingentriesSleim->tucoid);
  }

  // wake up deferred RPCs or move them to another sleim
  wakeOrMoveWaitingList(looim, pendingentriesSleim);
  delete pendingentriesSleim;

  looim->unlock();
  return;
}

// flushes all entries in memory to disk.
// This function is not safe to be called when there is other activity in the
// storagenode.
void LogInMemory::flushToDisk(Timestamp &ts){
  int res;
  Ptr<TxUpdateCoid> tucoid;
  int nbuckets, i;
  SkipList<COid, LogOneObjectInMemory*> *bucket;
  SkipListNode<COid, LogOneObjectInMemory*> *ptr;

  // iterate over all oids in memory
  nbuckets = COidMap.GetNbuckets();
  for (i=0; i < nbuckets; ++i){
    bucket = COidMap.GetBucket(i);
    for (ptr = bucket->getFirst(); ptr != bucket->getLast();
         ptr = bucket->getNext(ptr)){
      // read entire oid
      res = readCOid(ptr->key, ts, tucoid, 0, 0);
      if (res >= 0){
        // write it to disk
       DS->writeCOid(ptr->key, tucoid, ts);
      }
    }
  }
}


// flushes all entries in memory to file.
// Returns 0 if ok, non-zero if error.
int LogInMemory::flushToFile(Timestamp &ts, char *flushfilename){
  int res, size;
  char *filename;
  FILE *f=0;
  int retval=0;
  Ptr<TxUpdateCoid> tucoid;
  int nbuckets, i;
  SkipList<COid, LogOneObjectInMemory*> *bucket;
  SkipListNode<COid, LogOneObjectInMemory*> *ptr;

  filename = flushfilename;
  f = fopen(filename, "wbS");
  if (!f){
    dprintf(1, "flushToFile: error opening file %s for writing", filename);
    goto error;
  }
  //delete [] filename;

  // iterate over all oids in memory
  nbuckets = COidMap.GetNbuckets();
  for (i=0; i < nbuckets; ++i){
    bucket = COidMap.GetBucket(i);
    for (ptr = bucket->getFirst(); ptr != bucket->getLast();
         ptr = bucket->getNext(ptr)){
      // read entire oid
      size = readCOid(ptr->key, ts, tucoid, 0, 0);
      if (size >= 0){
        // write coid
        res = (int) fwrite((void*)&ptr->key, 1, sizeof(COid), f);
        if (res != sizeof(COid)) goto error;
  
        res = DS->writeCOidToFile(f, tucoid);
        if (res) goto error;
      }
    }
  }

 end:
  if (f) fclose(f);
  return retval;

 error:
  retval = -1;
  goto end;
}


// load contents of disk into memory cache
void LogInMemory::loadFromDisk(void){
  char *dirname;
  DIR *dir=0;
  dirent *de=0, *result;
  int namemax, len;
  char *pathname=0;
  char *filename;
  int dirlen;
  struct stat statbuf;
  
  COid coid;
  Timestamp ts;
  int res;
  Ptr<TxUpdateCoid> tucoid;

  ts.setNew();

  dirname= DS->getDiskStoragePath();
  
  dir = opendir(dirname);
  if (!dir){
    printf("loadFromDisk: cannot open directory %s\n", dirname);
    goto end;
  }

  namemax = pathconf(dirname, _PC_NAME_MAX);
  if (namemax < 0) namemax = 1024;

  dirlen = strlen(dirname);
  pathname = (char*) malloc(dirlen + namemax + 1);
  pathname[dirlen+namemax] = 0;
  strcpy(pathname, dirname);
  
  // append / to directory if necessary
  if (pathname[dirlen-1] != '/'){
    strcat(pathname, "/");
    ++dirlen;
  }

  filename = pathname + dirlen;
  
  len = offsetof(struct dirent, d_name) + namemax + 1;
  de = (dirent*) malloc(len);

  while (1){
    res = readdir_r(dir, de, &result);
    if (res){
      printf("loadFromDisk: cannot read directory %s\n", dirname);
      goto end;
    }
    if (!result) break;
    strncpy(filename, result->d_name, namemax-2);
    
    printf("Readdir of %s\n", pathname);
    res = lstat(pathname, &statbuf);
    if (res) continue; // cannot stat
    if (!S_ISREG(statbuf.st_mode)) continue; // not a regular file
    
    coid = DiskStorage::FilenameToCOid(filename);
    // the call to readCOid will cause the object to be read from disk since
    // it is not in memory
    res = readCOid(coid, ts, tucoid, 0, 0); assert(res >= 0);
  }
  end:
  if (dir) closedir(dir);
  if (de) free(de);
  if (pathname) free(pathname);
}

// load contents of disk into memory cache
int LogInMemory::loadFromFile(char *flushfilename){
  int res;
  COid coid;
  char *filename;
  Timestamp ts;
  FILE *f=0;
  int retval = 0;
  Ptr<TxUpdateCoid> tucoid;

  filename = flushfilename;

  f = fopen(filename, "r");
  if (!f){
    dprintf(1, "loadFromFile: error opening file %s for reading", filename);
    goto error;
  }
  //delete [] filename;

  ts.setNew();

  while (!feof(f)){
    // read one object
    res = (int)fread((void*)&coid, 1, sizeof(COid), f); // coid
    if (res == 0){ 
      if (!feof(f)) goto error;
      continue;
    }
    if (res != sizeof(COid)) goto error;
    res = DS->readCOidFromFile(f, coid, tucoid); if (res) goto error;
    res = writeCOid(coid, ts, tucoid); if (res) goto error;
  }
 end:
  if (f) fclose(f);
  return retval;
 error:
  retval = -1;
  goto end;
}

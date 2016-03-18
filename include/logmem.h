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

#ifndef _LOGMEM_H
#define _LOGMEM_H

#include <list>
#include "options.h"
#include "gaiarpcaux.h"
#include "util.h"
#include "datastruct.h"
#include "pendingtx.h"
#include "diskstorage.h"

#define SLEIM_FLAG_DIRTY    0x01  // entry not yet written to disk
                                  // (not counting log)
//#define SLEIM_FLAG_PENDING  0x02  // entry is between prepare and commit
                                    // phase (reads fail if they get to such
                                    // entry)
#define SLEIM_FLAG_SNAPSHOT 0x04  // entry is a snapshot added for efficiency
      // while reading. The code assumes that these snapshot entries are only
      // added when reading in LogInMemory::readCOid, and not in other places.
      // In the future, should one desire to create snapshots in other places,
      // one must be sure to update the looim->LastRead timestamp to be bigger
      // than the timestamp of the snapshot entry, to prevent
      // subsequent transactions from committing with a smaller timestamp
      // (if such transaction would commit, it would make the snapshot
      // inconsistent)
#define SLEIM_FLAG_LAST     0x08  // not really a flag; just denotes one after
                                  // the last flag used, used in asserts

// linklist of pointers used below
struct WaitingListItem {
  void *ptr;     // deferred handles of RPCs waiting for the item to no
                 // longer be pending
  Timestamp ts;  // timestamp threshold for the RPC to be successful. The RPC
                 // will be successful only if there are no pending items with
                 // timestamp <= this value. Thus, there is no point in
                 // signaling the RPC if such timestamps still exist (doing
                 // so will only cause the RPC to get deferred again)
  WaitingListItem *next;
  WaitingListItem(void *p, WaitingListItem *n, Timestamp t) :
    ptr(p), ts(t), next(n) {}
  WaitingListItem(){ ptr = 0; next = 0; }
};

// A single log entry in memory
// First entry in log will also be treated as a log entry containing the entire
// object's data
class SingleLogEntryInMemory {
public:
  SingleLogEntryInMemory(){  waitingts.setIllegal(); }
  ~SingleLogEntryInMemory(){
    WaitingListItem *vli, *next;
    // free VoidListItem linklist
    for (vli = waitOnPending.next; vli; vli = next){
      next = vli->next;
      delete vli;
    }
  }

  Timestamp ts;
  int flags;
  WaitingListItem waitOnPending; // this list has the RPC deferred handles of
                                 // RPCs that are waiting for this sleim to no
                                 // longer be pending
  Timestamp waitingts;           // timestamp of highest waiting RPC. This is
                                 // returned to the client upon commit so that
                                 // client can adjust its clock if necessary
  Ptr<TxUpdateCoid> tucoid;

  SingleLogEntryInMemory *prev, *next;   // linklist stuff
  void printShort(COid tocheck){
    int len=0;
    WaitingListItem *wli;
    for (wli=waitOnPending.next; wli; wli = wli->next) ++len;
    printf("ts %llx flags %x waitOnPending.len %d data ",
           (long long)ts.getd1(), flags, len);
    tucoid->printdetail(tocheck);
  }
};

// entry for a given COid in LogInMemory
class LogOneObjectInMemory {
private:
  RWLock object_lock; // lock for object
public:
  LogOneObjectInMemory(){ LastRead.setLowest(); }
  LinkList<SingleLogEntryInMemory> logentries;
  LinkList<SingleLogEntryInMemory> pendingentries;

  Timestamp LastRead; // Largest timestamp of a read on object

  // convenience methods to lock/unlock looim
#ifndef SKIP_LOOIM_LOCKS
  void lock(){ object_lock.lock(); }
  void unlock(){ object_lock.unlock(); }
  void lockRead(){ object_lock.lockRead(); }
  void unlockRead(){ object_lock.unlockRead(); }
#else
  void lock(){ }
  void unlock(){ }
  void lockRead(){ }
  void unlockRead(){ }
#endif

  void print(COid &coid);
  void printdetail(COid &coid, bool locklooim=true);
};

class LogInMemory {
private:
  HashTableMT<COid,LogOneObjectInMemory *> COidMap;
  DiskStorage *DS;
  bool SingleVersion; // if true, keep at most one version per COid

  // auxilliary functions
  static void getAndLockaux(int res, LogOneObjectInMemory **looimptr);

public:
  LogInMemory(DiskStorage *ds);

  // Return entry for an object and locks it for reading or writing.
  // If entry does not exist, create it, reading object from disk
  // to set the sole entry in the log.
  //   coid is the coid of the object
  //   writelock=true if locking for write, false if locking for read
  // If createfirstlog is true, then create first log entry for object if
  // object is not found
  LogOneObjectInMemory *getAndLock(COid& coid, bool writelock,
                                   bool createfirstlog);

  void setSingleVersion(bool sv){ SingleVersion = sv; }

  // Eliminates old entries from log. The eliminated entries are the ones that
  // are subsumed by a newer entry and that are older than LOG_STALE_GC_MS
  // relative to the given ts.
  // Assumes looim->object_lock is held in write mode.
  // Returns number of entries that were removed.
  int gClog(LogOneObjectInMemory *looim, Timestamp ts);

  // Auxilliary function to add a sleim entry to the logentries of a looim.
  // Below we have a similar function to add to the pendingentries of a looim.
  // Assumes looim->object_lock is held in write mode.
  // Looks for the right place to add to ensure that sleim list remains ordered
  // by timestamp.
  // At the end, try to garbage collect the log
  inline void auxAddSleimToLogentries(LogOneObjectInMemory *looim, Timestamp ts,
                                      bool dirty, Ptr<TxUpdateCoid> tucoid){
    SingleLogEntryInMemory *sleim = new SingleLogEntryInMemory;
    SingleLogEntryInMemory *sleim2;
    LinkList<SingleLogEntryInMemory> *wheretoadd;
    //sleim->coid = coid;
    sleim->ts = ts;
    sleim->flags = 0;
    if (dirty) sleim->flags |= SLEIM_FLAG_DIRTY;
    //if (pending) sleim->flags |= SLEIM_FLAG_PENDING;
    wheretoadd = &looim->logentries;

    sleim->tucoid = tucoid;

    // find position where to add while maintaining sorted order
    for (sleim2 = wheretoadd->rGetFirst(); sleim2 != wheretoadd->rGetLast();
         sleim2 = wheretoadd->rGetNext(sleim2)){
      if (Timestamp::cmp(sleim2->ts, ts)<=0) break;
    }
    // check if this tucoid has a single Add element for something already in
    // log; if so, ignore it
    if (tucoid->Litems.getNitems() == 1 && tucoid->WriteSV == 0 &&
        tucoid->Writevalue == 0 && sleim2 != wheretoadd->rGetLast()){
      if (sleim2->flags == SLEIM_FLAG_SNAPSHOT){
        assert(sleim2->tucoid->WriteSV &&
               sleim2->tucoid->Litems.getNitems()==0);
        int i;
        for (i=0; i < GAIA_MAX_ATTRS; ++i) if (tucoid->SetAttrs[i]) break;
        if (i == GAIA_MAX_ATTRS){
          TxListItem *tli = tucoid->Litems.getFirst();
          if (tli->type == 0){
            TxListAddItem *tliadd = dynamic_cast<TxListAddItem*>(tli);
            if (sleim2->tucoid->WriteSV->cells.belongs(&tliadd->item)){
              //printf("auxAddSleimToLogentries: Item already there!\n");
              delete sleim;
              goto end; // item already there, nothing to do
            }
          }
        }
      }
    }
    if (sleim2 != wheretoadd->rGetLast()) wheretoadd->addAfter(sleim, sleim2);
    else wheretoadd->pushHead(sleim);

    if (SingleVersion){ // delete previous versions if any
      // search for a checkpoint
      for (sleim2 = wheretoadd->rGetFirst(); sleim2 != wheretoadd->rGetLast();
           sleim2 = wheretoadd->rGetNext(sleim2)){
        if (sleim2->tucoid->WriteSV || sleim2->tucoid->Writevalue) break;
      }
      if (sleim2 != wheretoadd->rGetLast()){ // entries before the found one?
        SingleLogEntryInMemory *sleim3, sleimnext;
        sleim3 = wheretoadd->getFirst();
        while (sleim3 != sleim2){ // delete entries up to sleim2 (not including)
          wheretoadd->popHead();
          delete sleim3;
          sleim3 = wheretoadd->getFirst();
        }
      }
    } else gClog(looim, ts);
   end:
    return;
  }

  // Auxilliary function to add a sleim entry to the pendingentries of looim.
  // (see also sister function auxAddSleimToLogentries above)
  // Assumes looim->object_lock is held in write mode.
  // Looks for the right place to add to ensure that sleim list remains
  // ordered by timestamp.
  // Returns sleim of entry added to the pendingentries
  inline SingleLogEntryInMemory *
  auxAddSleimToPendingentries(LogOneObjectInMemory *looim, Timestamp ts,
                              bool dirty, Ptr<TxUpdateCoid> tucoid){
    SingleLogEntryInMemory *sleim = new SingleLogEntryInMemory;
    SingleLogEntryInMemory *sleim2;
    LinkList<SingleLogEntryInMemory> *wheretoadd;
    //sleim->coid = coid;
    sleim->ts = ts;
    sleim->flags = 0;
    if (dirty) sleim->flags |= SLEIM_FLAG_DIRTY;
    //if (pending) sleim->flags |= SLEIM_FLAG_PENDING;
    wheretoadd = &looim->pendingentries;

    sleim->tucoid = tucoid;

    // find position where to add while maintaining sorted order
    for (sleim2 = wheretoadd->rGetFirst(); sleim2 != wheretoadd->rGetLast();
         sleim2 = wheretoadd->rGetNext(sleim2)){
      if (Timestamp::cmp(sleim2->ts, ts)<=0) break;
    }

    if (sleim2 != wheretoadd->rGetLast()) wheretoadd->addAfter(sleim, sleim2);
    else wheretoadd->pushHead(sleim);
    return sleim;
  }

  struct NUpdates {
    i16 nadd;
    i16 ndelrange;
    i16 res;
    i16 dummy;
  };
  // Applies the changes in a tucoid to a TxWriteSVItem twsvi. This is not
  // intended to be used when tucoid includes an entire write to a value or
  // supervalue, in which case the function returns an error.
  //
  // Requires twsvi != 0
  // Returns number of listadd and listdelrange items applied, as well as a
  // error result (res).
  // If tucoid has a write to a value or supervalue, sets res != 0, otherwise
  // sets res == 0
  static NUpdates applyTucoid(TxWriteSVItem *twsvi, Ptr<TxUpdateCoid> tucoid);
  
  // read data of a given oid and version, starting from offset off
  // with length len. If len==-1 then read until the end.
  // Put the result in retval, passed by reference. Returns
  //          0 if ok
  //         -1 if oid does not exist,
  //         -2 if oid exists but version does not exist
  //         -3 if oid exists but data to be read is pending
  //         -4 if oid is corrupted (e.g., attrset followed by a regular value)
  // Also returns the timestamp of the data that is actually read in *readts,
  // if readts != 0
  //int readCOid(COid& coid, Timestamp ts, int len, char **destbuf,
  //  Timestamp *readts, int nolock=0);
  int readCOid(COid& coid, Timestamp ts, Ptr<TxUpdateCoid> &rettucoid,
               Timestamp *readts, void *deferredhandle);

  // after writing, twi or twsvi will be owned by LogInMemory. Caller should
  // have allocated it and should not free it.
  int writeCOid(COid& coid, Timestamp ts, Ptr<TxUpdateCoid> tucoid);

  // Wakes up deferred RPCs in the waiting list of a sleim or move them to the
  // sleim of another pending entry, based on the given ts. If the given ts
  // is < than all pending entries (though it suffices to check the first
  // since the entries are ordered by their timestamp), then wake up, otherwise
  // move.
  // Motivation: if the ts is < than all pending entries, then deferred RPC will
  // not block. Otherwise, there is no point in waking it up.
  // The waiting list of the given sleim ends up empty.
  void wakeOrMoveWaitingList(LogOneObjectInMemory *looim,
                             SingleLogEntryInMemory *sleim);

  // Remove entry from pendingentries.
  // If parameter move is true, add removed entry to logentries, setting its
  // final timestamp to the given parameter.
  // Wake up any deferred RPCs indicated in the sleim if no more pendingentries
  // will block it. If some pendingentries may block it, move deferred RPCs to
  // one of them (say, the earliest one).
  // The GC logentries.
  // Each tx should have a single pending entry per object, so this
  // function is effecting all the updates of a tx to a given object.
  void removeOrMovePendingToLogentries(COid& coid,
       SingleLogEntryInMemory *pendingentriesSleim, Timestamp ts, bool move);

  // returns non-0 if coid is in log, 0 otherwise
  int coidInLog(COid &coid){
    LogOneObjectInMemory *looim;
    int res;
    res = COidMap.lookup(coid, looim);
    return res==0;
  }

  // flushes all entries in memory to disk or file
  void flushToDisk(Timestamp &ts);
  int flushToFile(Timestamp &ts, char *flushfilename=FLUSH_FILENAME);

  // load contents of disk or file into memory cache
  void loadFromDisk(void);
  int loadFromFile(char *flushfilename=FLUSH_FILENAME);

  // prints state of all objects
  void printAllLooim(); // print just latest content of each object
  void printAllLooimDetailed();  // print entire log of each object
};

#endif

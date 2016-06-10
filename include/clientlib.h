//
// clientlib.h
//
// Library for client to access storage servers
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

#ifndef _CLIENTLIB_H
#define _CLIENTLIB_H

#include <set>
#include <list>
#include <signal.h>
#include <sys/uio.h>

#include "debug.h"
#include "util.h"
#include "ipmisc.h"
#include "clientlib-common.h"
#include "clientdir.h"
#include "gaiarpcaux.h"
#include "datastruct.h"
#include "supervalue.h"

#define STARTTS_MAX_STALE 50  // maximum staleness for start timestamp in ms
//#define STARTTS_NO_READMYWRITES // comment in to start timestamp in the past
      // by STARTTS_MAX_STALE even if we committed, so a thread will not see
      // its own previous transactions. If commented out, the start timestamp
      // will be set to max(now-STARTTS_MAX_STALE,lastcommitts), so that
      // the thread will see its own committed transactions.

// largest time in the past for choosing a deferred start timestamp.
// With deferred timestamps, the start timestamp is chosen to be
// the timestamp of the first item read by the transaction. But if that
// timestamp is older than MAX_DEFERRED_START_TS, then the start timestamp
// will be now minus MAX_DEFERRED_START_TS
#define MAX_DEFERRED_START_TS 1000

#define TID_TO_RPCHASHID(tid) ((u32)tid.d1)  // rpc hashid to use for a given
     // tid. Currently, returns d1, which is the client's IP + PID. Thus, all
     // requests of the client are assigned the same rpc hashid, so they are
     // all handled by the same server thread.

// Initializes and uninitializes Gaia
StorageConfig *InitGaia(void);
void UninitGaia(StorageConfig *SC);

class Transaction
{
private:
  int State;          // 0=valid, -1=aborted, -2=aborted due to I/O error
  StorageConfig *Sc;
  Timestamp StartTs;
  Tid Id;
  Set<IPPortServerno> Servers;
  int readsTxCached;
  bool hasWrites;
  bool hasWritesCachable; // whether tx writes to cachable items
  int currlevel;          // current subtransaction level

  char *piggy_buf;   // data to be piggybacked
  IPPortServerno piggy_server; // server holding coid to be written
  COid piggy_coid;   // coid to be written
  int piggy_len;     // length of data. -1 means no data, -2 means data
                     // used to exist but does not exist any longer
                     // (in this case, we cannot add data again because
                     // the order does not match as piggy writes are
                     // always done first)
  int piggy_level;   // level of data
#ifdef GAIA_OCC
  Set<COid> ReadSet;
#endif

  TxCache txCache;
  
  void updateTxCache(COid &coid, Ptr<Valbuf> &buf); // clear pending ops and
                                                    // updates txcache
  
  // Try to read data locally using TxCache from transaction.
  // If there is data to be read, data is placed in *buf if *buf!=0;
  //   if *buf=0 then it gets allocated and should be freed later with
  //   readFreeBuf. Note that buffer is allocated only if there is
  //   data to be read.
  //
  // Returns: 0 = nothing read, 
  //          1 = all data read
  //          GAIAERR_TX_ENDED = cannot read because transaction is aborted
  //          GAIAERR_WRONG_TYPE = wrong type
  int tryLocalRead(COid &coid, Ptr<Valbuf> &buf, int typ);

  // ---------------------------- Prepare RPC ----------------------------------

  struct PrepareCallbackData {
    Semaphore sem; // to wait for response
    int serverno;
    PrepareRPCResp data;
    PrepareCallbackData *next, *prev;  // linklist stuff
  };

  // Prepare part of two-phase commit
  // remote2pc = true means that all remote replicas are contacted as
  //             part of two-phase commit. Otherwise, just the local
  //             servers are contacted
  // sets chosents to the timestamp chosen for transaction
  // Sets hascommitted to indicate if the transaction was also committed using
  // one-phase commit.
  // This is possible when the transaction spans only one server.
  int auxprepare(Timestamp &chosents, int &hascommitted);
  static void auxpreparecallback(char *data, int len, void *callbackdata);

  // ----------------------------- Commit RPC ----------------------------------

  struct CommitCallbackData {
    Semaphore sem; // to wait for response
    CommitRPCResp data;
    CommitCallbackData *prev, *next; // linklist stuff
  };

  static void auxcommitcallback(char *data, int len, void *callbackdata);

  // Commit part of two-phase commit
  // remote2pc = true means that all remote replicas are contacted as
  //             part of two-phase commit. Otherwise, just the local
  //             servers are contacted
  // Returns 0 if ok, -1 if cannot contact some server
  // If waittingts!=0, sets *waittingts to largest waitingts reported by servers
  //  or to smallest possible timestamp if no servers reported a legal timestamp
  int auxcommit(int outcome, Timestamp committs, Timestamp *waitingts);

  // ---------------------------- Subtrans RPC ---------------------------------

  struct SubtransCallbackData {
    Semaphore sem; // to wait for response
    SubtransRPCResp data;
    SubtransCallbackData *prev, *next; // linklist stuff
  };

  static void auxsubtranscallback(char *data, int len, void *callbackdata);
  int auxsubtrans(int level, int action);
  

public:
  Transaction(StorageConfig *sc);
  ~Transaction();

  // start a new transaction. There is no need to call this for a
  // newly created Transaction object. This is intended for recycling
  // the same object to execute a new transaction.
  int start();

  // start a transaction with a start timestamp that will be set when the
  // transaction first reads, to be the timestamp of the latest available
  // version to read.
  // Right now, these transactions must read something before committing. Later,
  // we should be able to extend this so that transactions can commit without
  // having read.
  int startDeferredTs(void);

  // write an object in the context of a transaction.
  // Returns status:
  //   0=no error
  //   -9=transaction is aborted
  //  -10=cannot contact server
  int write(COid coid, char *buf, int len);
  int writev(COid coid, int nbufs, iovec *bufs);

  // returns a status as in write() above
  int put(COid coid, char *buf, int len){ return write(coid, buf, len); }

  // put with 2 or 3 user buffers
  // returns a status as in write() above
  int put2(COid coid, char *data1, int len1, char *data2, int len2);
  int put3(COid coid, char *data1, int len1, char *data2, int len2, char *data3,
           int len3);

  // read a value into a Valbuf
  int vget(COid coid, Ptr<Valbuf> &buf);

  // read a supervalue into a Valbuf
  // cell and prki are passed just for the server to keep stats of
  // what cell triggered the read of the supervalue (to trigger
  // load splits). It is legal to pass cell=0 and prki=(nullptr) to indicate
  // no particular cell, in which case these parameters are ignored.
  int vsuperget(COid coid, Ptr<Valbuf> &buf, ListCell *cell,
                Ptr<RcKeyInfo> prki);

  static void readFreeBuf(char *buf); // frees a buffer returned by
                                      // readNewBuf() or get()
  static char *allocReadBuf(int len); // allocates a buffer that can be freed
       // by readFreeBuf. Normally, such a buffer comes from the RPC layer, but
       // when reading txcached data, we need to return a pointer to
       // such a type of buffer so caller can free it in the same way

  // add an object to a set in the context of a transaction
  // Returns 0 if ok, <0 if error (eg, -9 if transaction is aborted)
  int addset(COid coid, COid toadd);

  // remove an object from a set in the context of a transaction
  // Returns 0 if ok, <0 if error (eg, -9 if transaction is aborted)
  int remset(COid coid, COid toremove);

  // reads a super value. Returns the super value in *svret, as a newly
  // allocated SuperValue object.
  // Returns 0 if ok, non-0 if I/O error. In case of error,
  // *svret is not touched.
  //int readSuperValue(COid coid, SuperValue **svret);

  // writes a super value. Returns 0 if ok, non-0 if I/O error.
  // If super value has non-int cells, must provide svret->prki != (nullptr).
  int writeSuperValue(COid coid, SuperValue *svret);

  // Adds a cell to a supervalue.
  // If check&1: then the function checks whether cell can really be
  // added to coid. This amounts to checking that (1) cell is within the
  // interval covered by coid and (2) cell does not already exist. This is used
  // in the optimistic insert optimization in the dtree implementation, whereby
  // the client doesn't fetch the leaf node before trying to insert
  // but rather assumes that the leaf node is the correct one.
  // If check (1) fails, this function returns GAIAERR_CELL_OUTRANGE
  // and if check (2) fails, it returns GAIAERR_CELL_EXISTS
#if DTREE_SPLIT_LOCATION != 1
  int listAdd(COid coid, ListCell *cell, Ptr<RcKeyInfo> prki, int flags);
#else
  int listAdd(COid coid, ListCell *cell, Ptr<RcKeyInfo> prki, int flags,
              int *ncells=0, int *size=0);
#endif
  
  // deletes a range of cells
  // intervalType indicates how the interval is to be treated.
  // The possible values are
  //     0=(cell1,cell2)   1=(cell1,cell2]   2=(cell1,inf)
  //     3=[cell1,cell2)   4=[cell1,cell2]   5=[cell1,inf)
  //     6=(-inf,cell2)    7=(-inf,cell2]    8=(-inf,inf)
  // where inf is infinity
  int listDelRange(COid coid, u8 intervalType, ListCell *cell1,
                   ListCell *cell2, Ptr<RcKeyInfo> prki);
  
  // sets an attribute
  int attrSet(COid coid, u32 attrid, u64 attrvalue);

  // start a subtransaction with the given level, which
  // must be greater than currlevel. Return 0 if ok, non-0 if error.
  int startSubtrans(int level);

  // rollback the changes made by a subtransaction. Any updates
  // done with a higher level are discarded. Afterwards, currlevel
  // is set to level
  int abortSubtrans(int level);

  // release subtransactions, moving them to a lower level.
  // Any updates done with a higher level are changed to the given level.
  // Afterwards, currlevel is set to level
  int releaseSubtrans(int level);

  // try to commit
  // Return 0 if committed, non-0 if aborted
  int tryCommit(Timestamp *retcommitts=0);

  // try to abort
  int abort(void);
};

#endif

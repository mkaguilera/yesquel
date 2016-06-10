//
// clientlib-local.h
//
// Library for client to access a local emulation of a remote server.
// This is used to keep temporary tables created by the SQL processor.
// These tables are created and later destroyed. They are not shared and
// need not be persisted, so it does not make sense to keep them in
// the storage server.
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
#include "util.h"

#include "gaiarpcaux.h"
#include "newconfig.h"
#include "storageserver.h"
#include "storageserverstate.h"
#include "supervalue.h"
#include "clientlib-local.h"
#include "clientlib-common.h"
#include "record.h"
#include "clientlib.h"
#include "task.h"

#include "dtreeaux.h"

LocalTransaction::LocalTransaction()
{
  readsTxCached = 0;
  start();
}

LocalTransaction::~LocalTransaction(){
  txCache.clear();
}

// start a new transaction
int LocalTransaction::start(){
  // obtain a new timestamp from local clock (assumes synchronized clocks)
  StartTs.setNew();
  Id.setNew();
  txCache.clear();
  State = 0;  // valid
  hasWrites = false;
  currlevel = 0;
  return 0;
}

// start a new transaction
int LocalTransaction::startDeferredTs(){ return start(); }

static void iovecmemcpy(char *dest, iovec *bufs, int nbufs){
  for (int i=0; i < nbufs; ++i){
    memcpy((void*) dest, (void*) bufs[i].iov_base, bufs[i].iov_len);
    dest += bufs[i].iov_len;
  }
}

// write an object in the context of a transaction
int LocalTransaction::writev(COid coid, int nbufs, iovec *bufs){
  WriteRPCData *rpcdata;
  WriteRPCRespData *rpcresp;
  int respstatus;
  int totlen;
  char *buf;

  if (State != 0) return GAIAERR_TX_ENDED;

  hasWrites = true;

  rpcdata = new WriteRPCData;
  rpcdata->data = new WriteRPCParm;
  rpcdata->freedata = true;

  // fill out parameters
  rpcdata->data->tid = Id;
  rpcdata->data->cid = coid.cid;
  rpcdata->data->oid = coid.oid;
  rpcdata->data->level = currlevel;
  rpcdata->niovs = 0;
  rpcdata->iov = 0;
  totlen=0;
  for (int i=0; i < nbufs; ++i) totlen += bufs[i].iov_len;
  rpcdata->data->len = totlen;  // total length
  buf = LocalTransaction::allocReadBuf(totlen); // we use allocReadBuf because
    // we will place this buffer in the txcache below, inside a Valbuf (which is
    // destroyed by calling LocalTransaction::freeReadBuf())
  rpcdata->data->buf = buf;
  rpcdata->freedatabuf = 0; 
  iovecmemcpy(buf, bufs, nbufs);

  rpcresp = (WriteRPCRespData*) writeRpc(rpcdata);
  //delete rpcdata;

  if (!rpcresp){ State=-2; return -10; } // error contacting server

  // record written data
  // create a private copy of the data
  Valbuf *vb = new Valbuf;
  vb->type = 0; // regular value
  vb->coid = coid;
  vb->immutable = false;
  vb->commitTs.setIllegal();
  vb->readTs.setIllegal();
  vb->len = totlen;
  vb->u.buf = buf;
  //vb->u.buf = LocalTransaction::allocReadBuf(totlen);
  //memcpy(vb->u.buf, buf, totlen);
  //iovecmemcpy(vb->u.buf, bufs, nbufs); 
  delete rpcdata; // will not delete buf

  Ptr<Valbuf> vbuf = vb;
  txCache.setCache(coid, currlevel, vbuf);
  

  respstatus = rpcresp->data->status;

  if (respstatus) State=-2; // mark transaction as aborted due to I/O error
  delete rpcresp;

  return respstatus;
}

// write an object in the context of a transaction
int LocalTransaction::write(COid coid, char *buf, int len){
  iovec iov;
  iov.iov_len = len;
  iov.iov_base = buf;
  return writev(coid, 1, &iov);
}

int LocalTransaction::put2(COid coid, char *data1, int len1, char *data2,
                           int len2){
  iovec iovs[2];
  iovs[0].iov_len = len1;
  iovs[0].iov_base = data1;
  iovs[1].iov_len = len2;
  iovs[1].iov_base = data2;
  return writev(coid, 2, iovs);
}

int LocalTransaction::put3(COid coid, char *data1, int len1, char *data2,
                           int len2, char *data3, int len3){
  iovec iovs[3];
  iovs[0].iov_len = len1;
  iovs[0].iov_base = data1;
  iovs[1].iov_len = len2;
  iovs[1].iov_base = data2;
  iovs[2].iov_len = len3;
  iovs[2].iov_base = data3;
  return writev(coid, 3, iovs);
}

// Try to read data locally using TxCache from transaction.
// typ: 0 for value, 1 for supervalue
// Returns:       0 = nothing read, 
//                1 = all data read
//                GAIAERR_TX_ENDED = cannot read because transaction is aborted
//                GAIAERR_WRONG_TYPE = wrong type
int LocalTransaction::tryLocalRead(COid &coid, Ptr<Valbuf> &buf, int typ){
  TxCacheEntryList *tcel;
  int res;

  if (State) return GAIAERR_TX_ENDED;

  res = txCache.lookupCache(coid, tcel);
  if (!res){ // found
    if (typ != tcel->vbuf->type) return GAIAERR_WRONG_TYPE;
    buf = tcel->vbuf;
    return 1;
  }
  else return 0;
}

int LocalTransaction::vget(COid coid, Ptr<Valbuf> &buf){
  int reslocalread;
  ReadRPCData *rpcdata;
  ReadRPCRespData *rpcresp;
  int respstatus;
  bool defer;
  Valbuf *vbuf=0;
  EventSync readwait;

  if (State != 0){ buf = 0; return GAIAERR_TX_ENDED; }

  reslocalread = tryLocalRead(coid, buf, 0);
  if (reslocalread < 0) return reslocalread;
  if (reslocalread == 1){ // read completed already
    assert(buf->type == 0);
    return 0;
  }

  // add server index to set of servers participating in transaction
  // Not for read items
  // Servers.insert(server); 

  rpcdata = new ReadRPCData;
  rpcdata->data = new ReadRPCParm;
  rpcdata->freedata = true; 

  // fill out parameters
  rpcdata->data->tid = Id;
  rpcdata->data->ts = StartTs;
  rpcdata->data->cid = coid.cid;
  rpcdata->data->oid = coid.oid;
  rpcdata->data->len = -1;  // requested max bytes to read

  do {
    rpcresp = (ReadRPCRespData*) readRpc(rpcdata, (void*) &readwait, defer);
    if (defer){
      readwait.wait();
      readwait.reset();
    }
  } while (defer);
  delete rpcdata;

  if (!rpcresp){ // error contacting server
    State=-2; // mark transaction as aborted due to I/O error
    buf = 0;
    return GAIAERR_SERVER_TIMEOUT;
  }

  respstatus = rpcresp->data->status;
  if (respstatus){ buf = 0; goto end; }

  // fill out buf (returned value to user) with reply from RPC
  vbuf = new Valbuf;
  vbuf->type = 0;
  vbuf->coid = coid;
  vbuf->immutable = true;
  vbuf->commitTs = rpcresp->data->readts;
  vbuf->readTs = StartTs;
  vbuf->len = rpcresp->data->len;
  vbuf->u.buf = LocalTransaction::allocReadBuf(rpcresp->data->len);
  memcpy(vbuf->u.buf, rpcresp->data->buf, rpcresp->data->len);
  buf = vbuf;

  end:
  delete rpcresp;
  return respstatus;
}

int LocalTransaction::vsuperget(COid coid, Ptr<Valbuf> &buf, ListCell *cell,
                                Ptr<RcKeyInfo> prki){
  int reslocalread;
  FullReadRPCData *rpcdata;
  FullReadRPCRespData *rpcresp;
  int respstatus;
  bool defer;
  char *ptr;
  SuperValue *sv;
  FullReadRPCResp *r;
  Valbuf *vbuf;
  EventSync readwait;

  if (State != 0){ buf = 0; return GAIAERR_TX_ENDED; }

  reslocalread = tryLocalRead(coid, buf, 1);
  if (reslocalread < 0) return reslocalread;
  if (reslocalread == 1){ // read completed already
    assert(buf->type==1);
    return 0;
  }

  // add server index to set of servers participating in transaction
  // Not for read items
  // Servers.insert(server); 

  rpcdata = new FullReadRPCData;
  rpcdata->data = new FullReadRPCParm;
  rpcdata->freedata = true; 

  // fill out parameters
  rpcdata->data->tid = Id;
  rpcdata->data->ts = StartTs;
  rpcdata->data->cid = coid.cid;
  rpcdata->data->oid = coid.oid;
  rpcdata->data->prki = prki;
  if (cell) rpcdata->data->cell = *cell;
  else memset(&rpcdata->data->cell, 0, sizeof(ListCell));

  do {
    rpcresp = (FullReadRPCRespData *) fullreadRpc(rpcdata, (void*) &readwait,
                                                  defer);
    if (defer){
      readwait.wait();
      readwait.reset();
    }
  } while (defer);
  delete rpcdata;

  if (!rpcresp){ // error contacting server
    State=-2; // mark transaction as aborted due to I/O error
    buf = 0;
    return -10;
  }

  respstatus = rpcresp->data->status;
  if (respstatus){ buf = 0; goto end; }

  r = rpcresp->data; // for convenience

  vbuf = new Valbuf;
  vbuf->type = 1;
  vbuf->coid = coid;
  vbuf->immutable = true;
  vbuf->commitTs = r->readts;
  vbuf->readTs = StartTs;
  vbuf->len = 0; // not applicable for supervalue
  sv = new SuperValue;
  vbuf->u.raw = sv;

  sv->Nattrs = r->nattrs;
  sv->CellType = r->celltype;
  sv->Ncells = r->ncelloids;
  sv->CellsSize = r->lencelloids;
  sv->Attrs = new u64[sv->Nattrs]; assert(sv->Attrs);
  memcpy(sv->Attrs, r->attrs, sizeof(u64) * sv->Nattrs);
  sv->Cells = new ListCell[sv->Ncells];
  // fill out cells
  ptr = r->celloids;
  for (int i=0; i < sv->Ncells; ++i){
    // extract nkey
    u64 nkey;
    ptr += myGetVarint((unsigned char*) ptr, &nkey);
    sv->Cells[i].nKey = nkey;
    if (r->celltype == 0) sv->Cells[i].pKey = 0; // integer cell, set pKey=0
    else { // non-integer key, so extract pKey (nkey has its length)
      sv->Cells[i].pKey = new char[(int)nkey];
      memcpy(sv->Cells[i].pKey, ptr, (int)nkey);
      ptr += (int)nkey;
    }
    // extract childOid
    sv->Cells[i].value = *(Oid*)ptr;
    ptr += sizeof(u64); // space for 64-bit value in cell
  }
  sv->prki = r->prki;
  buf = vbuf;
  if (readsTxCached < MAX_READS_TO_TXCACHE){
    ++readsTxCached;
    txCache.setCache(coid, currlevel, buf);
  }
 end:
  delete rpcresp;
  return respstatus;
}

// free a buffer returned by Transaction::read
void LocalTransaction::readFreeBuf(char *buf){
  assert(buf);
  //delete [] buf;
  return Transaction::readFreeBuf(buf);
}

char *LocalTransaction::allocReadBuf(int len){
  //return new char[len];
  return Transaction::allocReadBuf(len);
}


// add an object to a set in the context of a transaction
int LocalTransaction::addset(COid coid, COid toadd){
  if (State != 0) return GAIAERR_TX_ENDED;
  return -1; // not implemented
}

// remove an object from a set in the context of a transaction
int LocalTransaction::remset(COid coid, COid toremove){
  if (State != 0) return GAIAERR_TX_ENDED;
  return -1; // not implemented
}


// ------------------------------ Prepare RPC -----------------------------------

// Prepare part of two-phase commit
// sets *chosents to the timestamp chosen for transaction
// Return 0 if all voted to commit, 1 if some voted to abort, 3 if error
// getting vote
int LocalTransaction::auxprepare(Timestamp &chosents){
  PrepareRPCData *rpcdata;
  PrepareRPCRespData *rpcresp;
  int decision;
  int vote;
  void *state=0;

  //committs.setNew();
  //*chosents = committs;

  rpcdata = new PrepareRPCData;
  rpcdata->data = new PrepareRPCParm;
  rpcdata->deletedata = true;

  // fill out parameters
  rpcdata->data->tid = Id;
  rpcdata->data->startts = StartTs;
  //rpcdata->data->committs = committs;
  rpcdata->data->onephasecommit = 0; // no need to use 1pc with local
                                     // transactions
  rpcdata->data->piggy_cid = 0;  // no piggyback write optimization
  rpcdata->data->piggy_oid = 0;  
  rpcdata->data->piggy_len = -1;  
  rpcdata->data->piggy_buf = 0;  

  rpcresp = (PrepareRPCRespData*) prepareRpc(rpcdata, state, 0);
  if (!rpcresp){ 
    // function wants us to call again with state after logging is finished, 
    // but there is no logging in the local version, so we call back immediately
    rpcresp = (PrepareRPCRespData*) prepareRpc(rpcdata, state, 0);
    assert(state==0);
  }
  delete rpcdata;
  if (rpcresp) vote = rpcresp->data->vote;
  else vote = -1; // I/O error

  // determine decision
  if (vote<0) decision = 3;
  else if (vote>0) decision = 1;
  else decision = 0;

  if (decision == 0){
    chosents = rpcresp->data->mincommitts;
    chosents.addEpsilon();
  }
  delete rpcresp;

  return decision;
}


// ------------------------------ Commit RPC -----------------------------------

// Commit part of two-phase commit
// outcome is 0 to commit, 1 to abort due to no vote, 3 to abort due to error
// preparing
int LocalTransaction::auxcommit(int outcome, Timestamp committs){
  CommitRPCData *rpcdata;
  CommitRPCRespData *rpcresp;
  int res;

  rpcdata = new CommitRPCData;
  rpcdata->data = new CommitRPCParm;
  rpcdata->freedata = true;

  // fill out parameters
  rpcdata->data->tid = Id;
  rpcdata->data->committs = committs;
  rpcdata->data->commit = outcome;

  rpcresp = (CommitRPCRespData *) commitRpc(rpcdata);
  delete rpcdata;
  res = 0;
  if (!rpcresp)
    res = GAIAERR_SERVER_TIMEOUT; // I/O error
  else {
    res = rpcresp->data->status;
    delete rpcresp;
  }
  return res;
}


// ----------------------------- Subtrans RPC ----------------------------------

int LocalTransaction::auxsubtrans(int level, int action){
  SubtransRPCData *rpcdata;
  SubtransRPCRespData *rpcresp;
  int res;

  rpcdata = new SubtransRPCData;
  rpcdata->data = new SubtransRPCParm;
  rpcdata->freedata = true;

  // fill out parameters
  rpcdata->data->tid = Id;
  rpcdata->data->level = level;
  rpcdata->data->action = action;

  rpcresp = (SubtransRPCRespData *) subtransRpc(rpcdata);
  delete rpcdata;
  res = 0;
  if (!rpcresp)
    res = GAIAERR_SERVER_TIMEOUT; // I/O error
  else {
    res = rpcresp->data->status;
    delete rpcresp;
  }
  return res;
}


//--------------------------- subtransactions ------------------------
int LocalTransaction::startSubtrans(int level){
  if (level <= currlevel) return -1;
  currlevel = level;
  return 0;
}

int LocalTransaction::abortSubtrans(int level){
  int res;
  assert(level <= currlevel);
  if (level < currlevel){
    res = auxsubtrans(level, 0); // tell servers to abort higher levels
    if (res) return res;
    txCache.abortLevel(level); // make changes to tx cache
    if (level < 0) level = 0;
    currlevel = level;
  }
  return 0;
}

int LocalTransaction::releaseSubtrans(int level){
  int res;
  assert(level <= currlevel);
  if (level < currlevel){
    res = auxsubtrans(level, 1); // tell servers to release higher levels
    if (res) return res;
    txCache.releaseLevel(level); // make changes to tx cache
    currlevel = level;
  }
  return 0;
}

//---------------------------- TryCommit -----------------------------

// try to commit
// Returns
//    0 if committed,
//    1 if aborted due to no vote, 
//    3 if aborted due to prepare failure,
//   <0 if commit error or transaction has ended

int LocalTransaction::tryCommit(Timestamp *retcommitts){
  int outcome;
  Timestamp committs;
  int res;

  if (State) return GAIAERR_TX_ENDED;

  if (!hasWrites) return 0; // nothing to commit

  // Prepare phase
  outcome = auxprepare(committs);

  // Commit phase
  res = auxcommit(outcome, committs);

  if (outcome == 0){
    if (retcommitts) *retcommitts = committs; // if requested, return commit
                                              // timestamp
  }

  State=-1;  // transaction now invalid

  // clear txCache
  txCache.clear();
  if (res < 0) return res;
  else return outcome;
}

// Abort transaction. Leaves it in valid state.
int LocalTransaction::abort(void){
  Timestamp dummyts;
  int res;

  if (State) return GAIAERR_TX_ENDED;
  if (!hasWrites) return 0; // nothing to commit

  // tell servers to throw away any outstanding writes they had
  dummyts.setIllegal();
  res = auxcommit(2, dummyts); // timestamp not really used for aborting txs

  // clear txCache
  txCache.clear();

  State=-1;  // transaction now invalid
  if (res) return res;
  return 0;
}

//// Read an object in the context of a transaction. Returns
//// a ptr that must be serialized by calling
//// rpcresp.demarshall(ptr).
//int LocalTransaction::readSuperValue(COid coid, SuperValue **svret){
//  FullReadRPCData *rpcdata;
//  FullReadRPCRespData *rpcresp;
//  int respstatus;
//  SuperValue *sv;
//
//  if (State != 0) return 0;
//
//  // here, try to locally read from write set
//  // **!**TO DO
//
//  rpcdata = new FullReadRPCData;
//  rpcdata->data = new FullReadRPCParm;
//  rpcdata->freedata = true; 
//
//  // fill out RPC parameters
//  rpcdata->data->tid = Id;
//  rpcdata->data->ts = StartTs;
//  rpcdata->data->cid = coid.cid;
//  rpcdata->data->oid = coid.oid;
//
//  // do the RPC
//  rpcresp = (FullReadRPCRespData *) fullreadRpc(rpcdata);
//  delete rpcdata;
//
//  if (!rpcresp){ State=-2; return -10; } // error contacting server
//
//  respstatus = rpcresp->data->status;
//  if (respstatus != 0){
//    delete rpcresp;
//    return respstatus;
//  }
//
//  // set local attributes
//  FullReadRPCResp *r = rpcresp->data; // for convenience
//  sv = new SuperValue;
//  sv->Nattrs = r->nattrs;
//  sv->Ncells = r->ncelloids;
//  sv->Attrs = new u64[r->nattrs]; assert(sv->Attrs);
//  memcpy(sv->Attrs, r->attrs, r->nattrs * sizeof(u64));
//  sv->Cells = new ListCell[sv->Ncells];
//  // fill out cells
//  char *ptr = r->celloids;
//  for (int i=0; i < sv->Ncells; ++i){
//    // extract nkey
//    int nkey = *(int*)ptr;
//    ptr += sizeof(int);
//    sv->Cells[i].nKey = nkey;
//    if (r->celltype == 0) sv->Cells[i].pKey = 0; // integer cell, set pKey=0
//    else { // non-integer key, so extract pKey (nkey has its length)
//      sv->Cells[i].pKey = new char[nkey];
//      memcpy(sv->Cells[i].pKey, ptr, nkey);
//      ptr += nkey;
//    }
//    // extract childOid
//    sv->Cells[i].value = *(Oid*)ptr;
//    ptr += sizeof(u64);
//  }
//  delete rpcresp;
//  *svret = sv;
//  return 0;
//}

int LocalTransaction::writeSuperValue(COid coid, SuperValue *sv){
  FullWriteRPCData *rpcdata;
  FullWriteRPCRespData *rpcresp;
  char *cells, *ptr;
  int respstatus, len, i;

  if (State != 0) return GAIAERR_TX_ENDED;

  hasWrites = true;

  // here, add write to the local write set
  Valbuf *vb = new Valbuf;
  vb->type = 1; // supervalue
  vb->coid = coid;
  vb->immutable = false;
  vb->commitTs.setIllegal();
  vb->readTs.setIllegal();
  vb->len = 0; // not applicable for supervalue
  vb->u.raw = new SuperValue(*sv);

  Ptr<Valbuf> buf = vb;
  txCache.setCache(coid, currlevel, buf);

  rpcdata = new FullWriteRPCData;
  rpcdata->data = new FullWriteRPCParm;
  rpcdata->freedata = true; 

  FullWriteRPCParm *fp = rpcdata->data; // for convenience

  // fill out RPC parameters
  fp->tid = Id;
  fp->cid = coid.cid;
  fp->oid = coid.oid;
  fp->level = currlevel;
  fp->nattrs = sv->Nattrs;
  fp->celltype = sv->CellType;
  fp->ncelloids = sv->Ncells;
  fp->prki = sv->prki;

  // calculate space needed for cells
  len = 0;
  for (i=0; i < sv->Ncells; ++i){
    if (sv->CellType == 0){ // int key
      len += myVarintLen(sv->Cells[i].nKey);
      assert(sv->Cells[i].pKey==0);
    }
    else len += myVarintLen(sv->Cells[i].nKey) +
           (int) sv->Cells[i].nKey; // non-int key
    len += sizeof(u64); // space for 64-bit value in cell
  }
  // fill celloids
  fp->lencelloids = len;
  fp->attrs = (u64*) sv->Attrs;
  cells = fp->celloids = new char[len];
  rpcdata->deletecelloids = cells; // free celloids when rpcdata is destroyed
  ptr = cells;
  for (i=0; i < sv->Ncells; ++i){
    ptr += myPutVarint((unsigned char*)ptr, sv->Cells[i].nKey);
    if (sv->CellType == 0) ; // int key, do nothing
    else { // non-int key, copy content in pKey
      memcpy(ptr, sv->Cells[i].pKey, (int) sv->Cells[i].nKey);
      ptr += sv->Cells[i].nKey;
    }
    // copy childOid
    memcpy(ptr, &sv->Cells[i].value, sizeof(u64));
    ptr += sizeof(u64);
  }

  // do the RPC
  rpcresp = (FullWriteRPCRespData *) fullwriteRpc(rpcdata);
  delete rpcdata;

  if (!rpcresp){ State=-2; return -10; } // error contacting server

  respstatus = rpcresp->data->status;
  if (respstatus) State = -2;
  delete rpcresp;
  return respstatus;
}

#if DTREE_SPLIT_LOCATION != 1
int LocalTransaction::listAdd(COid coid, ListCell *cell, Ptr<RcKeyInfo> prki,
                              int flags){
#else
int LocalTransaction::listAdd(COid coid, ListCell *cell, Ptr<RcKeyInfo> prki,
                              int flags, int *ncells, int *size){
#endif
  ListAddRPCData *rpcdata;
  ListAddRPCRespData *rpcresp;
  int respstatus;
  Ptr<Valbuf> vbuf;
  int vbufincache;
  TxCacheEntryList *tcel;
  int res;

  if (State) return GAIAERR_TX_ENDED;

  res = vsuperget(coid, vbuf, 0, 0); if (res) return res;
  assert(vbuf->type==1); // must be supervalue

  res = txCache.lookupCache(coid, tcel);
  vbufincache = res == 0;
  if (vbufincache){
    assert(tcel);
    vbuf = tcel->vbuf; // use vbuf in cache
  }

  if ((flags&1) && !(vbuf->u.raw->Attrs[DTREENODE_ATTRIB_FLAGS] &
                     DTREENODE_FLAG_LEAF))
    return GAIAERR_CELL_OUTRANGE;
  
  if (flags & 1){
    if (vbuf->u.raw->Ncells >= 1){
      int matches;
      myCellSearchNode(vbuf, cell->nKey, cell->pKey, 0, prki, &matches);
      if (matches) return 0; // found
    }
    flags &= ~1; // don't check again in listaddRpc, we already read the value
  }

  hasWrites = true;

  rpcdata = new ListAddRPCData;
  rpcdata->data = new ListAddRPCParm;
  rpcdata->freedata = true;

  // fill out parameters
  rpcdata->data->tid = Id;
  rpcdata->data->cid = coid.cid;
  rpcdata->data->oid = coid.oid;
  rpcdata->data->level = currlevel;
  rpcdata->data->flags = flags;
  rpcdata->data->ts = StartTs;
  rpcdata->data->prki = prki;
  rpcdata->data->cell = *cell;

  // this is the buf information really used by the marshaller

  void *state = 0;
  rpcresp = (ListAddRPCRespData *) listaddRpc(rpcdata, state);
  assert(state==0);
  delete rpcdata;

  if (!rpcresp){ // error contacting server
    State=-2; // mark transaction as aborted due to I/O error
    return -10;
  }

  respstatus = rpcresp->data->status;

  if (respstatus == GAIAERR_CELL_OUTRANGE) return respstatus;
  
  if (respstatus) State=-2; // mark transaction as aborted due to I/O error
  else {
    // insert into a cell and store it in txcache
    int index, matches=0;
    if (vbufincache && tcel->level != currlevel){
      vbuf = new Valbuf(*vbuf); // copy vbuf
      vbufincache = 0; // this new vbuf is not in cache, store it below
    }
    if (vbuf->u.raw->Ncells >= 1)
      index = myCellSearchNode(vbuf, cell->nKey, cell->pKey, 1, prki,
                               &matches);
    else index = 0; // no cells, so insert at position 0
    assert(0 <= index && index <= vbuf->u.raw->Ncells);
    if (!matches) vbuf->u.raw->InsertCell(index); // item not yet in list
    else {
      vbuf->u.raw->CellsSize -= vbuf->u.raw->Cells[index].size();
      vbuf->u.raw->Cells[index].Free(); // free old item to be replaced
    }
    new(&vbuf->u.raw->Cells[index]) ListCell(*cell);//placement constructor
    vbuf->u.raw->CellsSize += cell->size();
    if (!vbufincache)
      txCache.setCache(coid, currlevel, vbuf);

#if DTREE_SPLIT_LOCATION == 1
    if (ncells) *ncells = rpcresp->data->ncells;
    if (size) *size = rpcresp->data->size;
#endif
  }
  delete rpcresp;
  return respstatus;
}

// deletes a range of cells from a supervalue
int LocalTransaction::listDelRange(COid coid, u8 intervalType, ListCell *cell1,
                                   ListCell *cell2, Ptr<RcKeyInfo> prki){
  ListDelRangeRPCData *rpcdata;
  ListDelRangeRPCRespData *rpcresp;
  int respstatus;
  Ptr<Valbuf> vbuf;
  int vbufincache;
  TxCacheEntryList *tcel;
  int res;

  if (State != 0) return GAIAERR_TX_ENDED;

  res = vsuperget(coid, vbuf, 0, 0); if (res) return res;
  assert(vbuf->type==1); // must be supervalue

  res = txCache.lookupCache(coid, tcel);
  vbufincache = res == 0;
  if (vbufincache){
    assert(tcel);
    vbuf = tcel->vbuf; // use vbuf in cache
  }
  
  hasWrites = true;

  rpcdata = new ListDelRangeRPCData;
  rpcdata->data = new ListDelRangeRPCParm;
  rpcdata->freedata = true;

  // fill out parameters
  rpcdata->data->tid = Id;
  rpcdata->data->cid = coid.cid;
  rpcdata->data->oid = coid.oid;
  rpcdata->data->level = currlevel;
  rpcdata->data->prki = prki;
  rpcdata->data->intervalType = intervalType;
  rpcdata->data->cell1 = *cell1;
  rpcdata->data->cell2 = *cell2;

  // this is the buf information really used by the marshaller

  rpcresp = (ListDelRangeRPCRespData *) listdelrangeRpc(rpcdata);
  delete rpcdata;

  if (!rpcresp){ // error contacting server
    State=-2; // mark transaction as aborted due to I/O error
    return -10;
  }

  respstatus = rpcresp->data->status;

  if (respstatus) State=-2; // mark transaction as aborted due to I/O error
  else {
    // delete chosen cells and store node Valbuf in txcache
    int index1, index2;
    int matches1, matches2;

    if (vbufincache && tcel->level != currlevel){
      vbuf = new Valbuf(*vbuf); // copy vbuf
      vbufincache = 0; // this new vbuf is not in cache, store it below
    }

    if (intervalType < 6){
      index1 = myCellSearchNode(vbuf, cell1->nKey, cell1->pKey, 0, prki,
                                &matches1);
      if (index1 < 0){ respstatus = -1; goto end; }
      assert(0 <= index1 && index1 <= vbuf->u.raw->Ncells);
      if (matches1 && intervalType < 3) ++index1; // open interval,
                                                  // do not del cell1
    }
    else index1 = 0; // delete from -infinity

    if (index1 < vbuf->u.raw->Ncells){
      if (intervalType % 3 < 2){
        index2 = myCellSearchNode(vbuf, cell2->nKey, cell2->pKey, 0, prki,
                                  &matches2);
        if (index2 < 0){ respstatus = -1; goto end; }
        // must find value in cell
        assert(0 <= index2 && index2 <= vbuf->u.raw->Ncells);
        if (matches2 && intervalType % 3 == 0) --index2; // open interval,
                                                         // do not del cell2
        if (!matches2) --index2; // if does not match, back 1
      } else index2 = vbuf->u.raw->Ncells; // delete til +infinity

      if (index2 == vbuf->u.raw->Ncells) --index2;

      if (index1 <= index2){
        vbuf->u.raw->DeleteCellRange(index1, index2+1);
        if (!vbufincache)
          txCache.setCache(coid, currlevel, vbuf);
      }
    }
  }
  end:

  delete rpcresp;
  return respstatus;
}

// adds a cell to a supervalue
int LocalTransaction::attrSet(COid coid, u32 attrid, u64 attrvalue){
  AttrSetRPCData *rpcdata;
  AttrSetRPCRespData *rpcresp;
  int respstatus;
  Ptr<Valbuf> vbuf;
  int vbufincache;
  TxCacheEntryList *tcel;
  int res;

  if (State != 0) return GAIAERR_TX_ENDED;

  hasWrites = true;

  res = vsuperget(coid, vbuf, 0, 0); if (res) return res;
  assert(vbuf->type==1); // must be supervalue
  assert(vbuf->u.raw->Nattrs > (int)attrid);

  res = txCache.lookupCache(coid, tcel);
  vbufincache = res == 0;
  if (vbufincache){
    assert(tcel);
    vbuf = tcel->vbuf; // use vbuf in cache
  }

  rpcdata = new AttrSetRPCData;
  rpcdata->data = new AttrSetRPCParm;
  rpcdata->freedata = true;

  // fill out parameters
  rpcdata->data->tid = Id;
  rpcdata->data->cid = coid.cid;
  rpcdata->data->oid = coid.oid;
  rpcdata->data->level = currlevel;
  rpcdata->data->attrid = attrid;  
  rpcdata->data->attrvalue = attrvalue; 

  rpcresp = (AttrSetRPCRespData *) attrsetRpc(rpcdata);
  delete rpcdata;

  if (!rpcresp){ // error contacting server
    State=-2; // mark transaction as aborted due to I/O error
    return -10;
  }

  respstatus = rpcresp->data->status;

  if (respstatus) State=-2; // mark transaction as aborted due to I/O error
  else {
    // modify chosen attribute and store node Valbuf in txcache
    if (vbufincache && tcel->level != currlevel){
      vbuf = new Valbuf(*vbuf); // copy vbuf
      vbufincache = 0; // this new vbuf is not in cache, store it below
    }
    
    vbuf->u.raw->Attrs[attrid] = attrvalue;
    if (!vbufincache)
      txCache.setCache(coid, currlevel, vbuf);
  }

  delete rpcresp;
  return respstatus;
}


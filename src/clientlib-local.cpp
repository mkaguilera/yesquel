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
#include "record.h"
#include "clientlib.h"
#include "task.h"

#include "dtreeaux.h"

LocalTransaction::LocalTransaction() :
  TxCache()
{
  readsTxCached = 0;
  start();
}

LocalTransaction::~LocalTransaction(){
  clearTxCache();
}

void LocalTransaction::clearTxCache(void){
  TxCache.clear(0, TxCacheEntry::delEntry);
}

// start a new transaction
int LocalTransaction::start(){
  // obtain a new timestamp from local clock (assumes synchronized clocks)
  StartTs.setNew();
  Id.setNew();
  clearTxCache();
  State = 0;  // valid
  hasWrites = false;
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
  rpcdata->niovs = 0;
  rpcdata->iov = 0;
  totlen=0;
  for (int i=0; i < nbufs; ++i) totlen += bufs[i].iov_len;
  rpcdata->data->len = totlen;  // total length
  buf = LocalTransaction::allocReadBuf(totlen); // we use allocReadBuf because we will place
                       // this buffer in the txcache below, inside a Valbuf (which is
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
  updateTxCache(coid, vbuf);

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

int LocalTransaction::put2(COid coid, char *data1, int len1, char *data2, int len2){
  iovec iovs[2];
  iovs[0].iov_len = len1;
  iovs[0].iov_base = data1;
  iovs[1].iov_len = len2;
  iovs[1].iov_base = data2;
  return writev(coid, 2, iovs);
}

int LocalTransaction::put3(COid coid, char *data1, int len1, char *data2, int len2, char *data3, int len3){
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
  TxCacheEntry **cepp;
  int res;

  if (State) return GAIAERR_TX_ENDED;

  res = TxCache.lookup(coid, cepp);
  if (!res){ // found
    if (typ != (*cepp)->vbuf->type) return GAIAERR_WRONG_TYPE;
    buf = (*cepp)->vbuf;
    return 1;
  }
  else return 0;
}

void LocalTransaction::updateTxCache(COid &coid, Ptr<Valbuf> &buf){
  TxCacheEntry **cepp, *cep;
  int res;
  // remove any previous entry for coid
  res = TxCache.lookupInsert(coid, cepp);
  if (!res){ // element already exists; remove old entry
    assert(*cepp);
    delete *cepp;
  }
  cep = *cepp = new TxCacheEntry;
  cep->coid = coid;
  cep->vbuf = buf;
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

int LocalTransaction::vsuperget(COid coid, Ptr<Valbuf> &buf, ListCell *cell, GKeyInfo *ki){
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
  rpcdata->data->pKeyInfo = ki;
  if (cell) rpcdata->data->cell = *cell;
  else memset(&rpcdata->data->cell, 0, sizeof(ListCell));

  do {
    rpcresp = (FullReadRPCRespData *) fullreadRpc(rpcdata, (void*) &readwait, defer);
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
  sv->pki = CloneGKeyInfo(r->pki);
  buf = vbuf;
  if (readsTxCached < MAX_READS_TO_TXCACHE){
    ++readsTxCached;
    updateTxCache(coid, buf);
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
// Return 0 if all voted to commit, 1 if some voted to abort, 3 if error getting vote
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
  rpcdata->data->onephasecommit = 0; // no need to use 1pc with local transactions
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
// outcome is 0 to commit, 1 to abort due to no vote, 3 to abort due to error preparing
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
    if (retcommitts) *retcommitts = committs; // if requested, return commit timestamp
  }

  State=-1;  // transaction now invalid

  // clear TxCache
  clearTxCache();
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

  // clear TxCache
  clearTxCache();

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
  updateTxCache(coid, buf);

  rpcdata = new FullWriteRPCData;
  rpcdata->data = new FullWriteRPCParm;
  rpcdata->freedata = true; 

  FullWriteRPCParm *fp = rpcdata->data; // for convenience

  // fill out RPC parameters
  fp->tid = Id;
  fp->cid = coid.cid;
  fp->oid = coid.oid;
  fp->nattrs = sv->Nattrs;
  fp->celltype = sv->CellType;
  fp->ncelloids = sv->Ncells;
  fp->pKeyInfo = sv->pki;

  // calculate space needed for cells
  len = 0;
  for (i=0; i < sv->Ncells; ++i){
    if (sv->CellType == 0){ // int key
      len += myVarintLen(sv->Cells[i].nKey);
      assert(sv->Cells[i].pKey==0);
    }
    else len += myVarintLen(sv->Cells[i].nKey) + (int) sv->Cells[i].nKey; // non-int key
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

/* compares a cell key against intKey2/pIdxKey2. Use intKey2 if pIdxKey2==0 otherwise use pIdxKey2 */
/* inline */
static int compareNpKeyWithKey(i64 nKey1, char *pKey1, i64 nKey2, UnpackedRecord *pIdxKey2) {
  if (pIdxKey2) return myVdbeRecordCompare((int) nKey1, pKey1, pIdxKey2);
  else if (nKey1==nKey2) return 0;
  else return (nKey1 < nKey2) ? -1 : +1;
}

// Searches the cells of a node for a given key, using binary search.
// Returns the child pointer that needs to be followed for that key.
// If biasRight!=0 then optimize for the case the key is larger than any entries in node.
// Assumes that the path at the given level has some node (real or approx).
// Guaranteed to return an index between 0 and N where N is the number of cells
// in that node (N+1 is the number of pointers).
// Returns *matches!=0 if found key, *matches==0 if did not find key.
static int CellSearchUnpacked(Ptr<Valbuf> &vbuf, UnpackedRecord *pIdxKey,
                              i64 nkey, int biasRight, int *matches=0){
  int cmp;
  int bottom, top, mid;
  ListCell *cell;
  assert(vbuf->type==1); // must be supernode

  bottom=0;
  top=vbuf->u.raw->Ncells-1; /* number of keys on node minus 1 */
  if (top<0){ matches=0; return 0; } // there are no keys in node, so return index of only pointer there (index 0)
  do {
    if (biasRight){ mid = top; biasRight=0; } /* bias first search only */
    else mid=(bottom+top)/2;
    cell = &vbuf->u.raw->Cells[mid];
    cmp = compareNpKeyWithKey(cell->nKey, cell->pKey, nkey, pIdxKey);

    if (cmp==0) break; /* found key */
    if (cmp < 0) bottom=mid+1; /* mid < target */
    else top=mid-1; /* mid > target */
  } while (bottom <= top);
  // if key was found, then mid points to its index and cmp==0
  // if key was not found, then mid points to entry immediately before key (cmp<0) or after key (cmp>0)

  if (cmp<0) ++mid; // now mid points to entry immediately after key or to one past the last entry
                    // if key is greater than all entries
  // note: if key matches a cell (cmp==0), we follow the pointer to the left of the cell, which
  //       has the same index as the cell

  if (matches) *matches = cmp == 0 ? 1 : 0;
  assert(0 <= mid && mid <= vbuf->u.raw->Ncells);
  return mid;
}
static int CellSearchNode(Ptr<Valbuf> &vbuf, i64 nkey, void *pkey, int biasRight, 
                          GKeyInfo *ki, int *matches=0){
  UnpackedRecord *pIdxKey;   /* Unpacked index key */
  char aSpace[150];          /* Temp space for pIdxKey - to avoid a malloc */
  int res;

  if (pkey){
    pIdxKey = myVdbeRecordUnpack(ki, (int) nkey, pkey, aSpace, sizeof(aSpace));
    if (pIdxKey == 0) return SQLITE_NOMEM;
  } else pIdxKey = 0;
  res = CellSearchUnpacked(vbuf, pIdxKey, nkey, biasRight, matches);
  if (pkey) myVdbeDeleteUnpackedRecord(pIdxKey);
  return res;
}

#if DTREE_SPLIT_LOCATION != 1
int LocalTransaction::listAdd(COid coid, ListCell *cell, GKeyInfo *ki, int flags){
#else
int LocalTransaction::listAdd(COid coid, ListCell *cell, GKeyInfo *ki, int flags, int *ncells, int *size){
#endif
  ListAddRPCData *rpcdata;
  ListAddRPCRespData *rpcresp;
  int respstatus;
  Ptr<Valbuf> origvalue;
  int res;
  

  if (State) return GAIAERR_TX_ENDED;

  res = vsuperget(coid, origvalue, 0, 0); if (res) return res;
  assert(origvalue->type==1); // must be supervalue

  if ((flags&1) && !(origvalue->u.raw->Attrs[DTREENODE_ATTRIB_FLAGS] & DTREENODE_FLAG_LEAF)) return GAIAERR_CELL_OUTRANGE;
  
  if (flags & 1){
    if (origvalue->u.raw->Ncells >= 1){
      int matches;
      CellSearchNode(origvalue, cell->nKey, cell->pKey, 0, ki, &matches);
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
  rpcdata->data->flags = flags;
  rpcdata->data->ts = StartTs;
  rpcdata->data->pKeyInfo = ki;
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
    if (origvalue->u.raw->Ncells >= 1)
      index = CellSearchNode(origvalue, cell->nKey, cell->pKey, 1, ki, &matches);
    else index = 0; // no cells, so insert at position 0
    assert(0 <= index && index <= origvalue->u.raw->Ncells);
    if (!matches) origvalue->u.raw->InsertCell(index); // item not yet in list
    else {
      origvalue->u.raw->CellsSize -= origvalue->u.raw->Cells[index].size();
      origvalue->u.raw->Cells[index].Free(); // free old item to be replaced
    }
    new(&origvalue->u.raw->Cells[index]) ListCell(*cell); // placement constructor
    origvalue->u.raw->CellsSize += cell->size();
    updateTxCache(coid, origvalue);

#if DTREE_SPLIT_LOCATION == 1
    if (ncells) *ncells = rpcresp->data->ncells;
    if (size) *size = rpcresp->data->size;
#endif
  }
  delete rpcresp;
  return respstatus;
}

// deletes a range of cells from a supervalue
int LocalTransaction::listDelRange(COid coid, u8 intervalType, ListCell *cell1, ListCell *cell2, GKeyInfo *ki){
  ListDelRangeRPCData *rpcdata;
  ListDelRangeRPCRespData *rpcresp;
  int respstatus;
  Ptr<Valbuf> origvalue;
  int res;

  if (State != 0) return GAIAERR_TX_ENDED;

  res = vsuperget(coid, origvalue, 0, 0); if (res) return res;
  assert(origvalue->type==1); // must be supervalue

  hasWrites = true;

  rpcdata = new ListDelRangeRPCData;
  rpcdata->data = new ListDelRangeRPCParm;
  rpcdata->freedata = true;

  // fill out parameters
  rpcdata->data->tid = Id;
  rpcdata->data->cid = coid.cid;
  rpcdata->data->oid = coid.oid;
  rpcdata->data->pKeyInfo = ki;
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

    if (intervalType < 6){
      index1 = CellSearchNode(origvalue, cell1->nKey, cell1->pKey, 0, ki, &matches1);
      if (index1 < 0){ respstatus = -1; goto end; }
      assert(0 <= index1 && index1 <= origvalue->u.raw->Ncells);
      if (matches1 && intervalType < 3) ++index1; // open interval, do not del cell1
    }
    else index1 = 0; // delete from -infinity

    if (index1 < origvalue->u.raw->Ncells){
      if (intervalType % 3 < 2){
        index2 = CellSearchNode(origvalue, cell2->nKey, cell2->pKey, 0, ki, &matches2);
        if (index2 < 0){ respstatus = -1; goto end; }
        // must find value in cell
        assert(0 <= index2 && index2 <= origvalue->u.raw->Ncells);
        if (matches2 && intervalType % 3 == 0) --index2; // open interval, do not del cell2
      } else index2 = origvalue->u.raw->Ncells; // delete til +infinity

      if (index2 == origvalue->u.raw->Ncells) --index2;

      if (index1 <= index2){
        origvalue->u.raw->DeleteCellRange(index1, index2+1);
        updateTxCache(coid, origvalue);
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
  Ptr<Valbuf> origvalue;
  int res;

  if (State != 0) return GAIAERR_TX_ENDED;

  hasWrites = true;

  res = vsuperget(coid, origvalue, 0, 0); if (res) return res;
  assert(origvalue->type==1); // must be supervalue
  assert(origvalue->u.raw->Nattrs > (int)attrid);

  rpcdata = new AttrSetRPCData;
  rpcdata->data = new AttrSetRPCParm;
  rpcdata->freedata = true;

  // fill out parameters
  rpcdata->data->tid = Id;
  rpcdata->data->cid = coid.cid;
  rpcdata->data->oid = coid.oid;
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
    origvalue->u.raw->Attrs[attrid] = attrvalue;
    updateTxCache(coid, origvalue);
  }

  delete rpcresp;
  return respstatus;
}

#ifdef TESTMAIN

int main(int argc, char **argv)
{
  int res;
  COid coid;
  char *buf;
  int len;

  UniqueId::init();

  LocalTransaction t;

  coid.cid=0;
  coid.oid=0;

  t.put(coid, "hi", 3);
  t.tryCommit();

  t.start();
  t.put(coid, "me", 3);
  t.get(coid, &buf, &len);
  printf("Got len %d buf %s\n", len, buf);
  t.abort();
  t.tryCommit();

  t.start();
  t.get(coid, &buf, &len);
  printf("Got len %d buf %s\n", len, buf);
}
#endif



#ifdef TESTMAIN2

int main(int argc, char **argv)
{
  char buf[1024];
  int outcome;
  int res, c;
  u64 t1, t2, t3, t4;
  PreciseClock pc;
  bool truncate;
  char data[256];
  char *getbuf;
  int badargs;

  int operation, off, len;
  int readlen;
  COid coid;

  // remove path from argv[0]
  char *argv0 = strrchr(argv[0], '\\');
  if (argv0) ++argv0; else argv0 = argv[0];


  badargs=0;
  while ((c = getopt(argc,argv, "")) != -1){
    switch(c){
    default:
      ++badargs;
    }
  }
  if (badargs) exit(1); // bad arguments

  argc -= optind;

  if (argc != 3 && argc != 4 && argc != 5 && argc != 7){
    fprintf(stderr, "usage: %s r cid oid off len\n", argv0);
    fprintf(stderr, "usage: %s w cid oid off len truncate data\n", argv0);
    fprintf(stderr, "       %s g   cid oid\n", argv0);
    fprintf(stderr, "       %s p   cid oid data\n", argv0);
    exit(1);
  }

  // parse argument 1
  operation=tolower(*argv[optind]);
  if (!strchr("rwgp", operation)){
    fprintf(stderr, "valid operations are r,w,g,p\n", argv0);
    exit(1);
  }

  switch(operation){
  case 'r':
    if (argc != 5){ fprintf(stderr, "%s: r operation requires 4 arguments\n", argv0); exit(1); }
    coid.cid = (u64) atoi((char*) argv[optind+1]);
    coid.oid = (u64) atoi((char*) argv[optind+2]);
    off = atoi((char*) argv[optind+3]);
    len = atoi((char*) argv[optind+4]);
    *data = 0;
    break;
  case 'w':
    if (argc != 7){ fprintf(stderr, "%s: w operation requires 6 arguments\n", argv0); exit(1); }
    coid.cid = (u64) atoi((char*) argv[optind+1]);
    coid.oid = (u64) atoi((char*) argv[optind+2]);
    off = atoi((char*) argv[optind+3]);
    len = atoi((char*) argv[optind+4]);
    truncate = (bool) atoi((char*) argv[optind+5]);
    strncpy(data, argv[optind+6], sizeof(data));
    data[sizeof(data)-1]=0;
    break;
  case 'g':
    if (argc != 3){ fprintf(stderr, "%s: g operation requires 2 arguments\n", argv0); exit(1); }
    coid.cid = (u64) atoi((char*) argv[optind+1]);
    coid.oid = (u64) atoi((char*) argv[optind+2]);
    break;
  case 'p':
    if (argc != 4){ fprintf(stderr, "%s: p operation requires 3 arguments\n", argv0); exit(1); }
    coid.cid = (u64) atoi((char*) argv[optind+1]);
    coid.oid = (u64) atoi((char*) argv[optind+2]);
    strncpy(data, argv[optind+3], sizeof(data));
    data[sizeof(data)-1]=0;
    break;
  }

  UniqueId::init();

  LocalTransaction t;

  t.start();
  switch(operation){
  case 'r':
    t1 = pc.readus();
    res = t.read(coid, buf, off, len, &readlen);
    t2 = pc.readus();

    t3 = pc.readus();
    outcome = t.tryCommit();
    t4 = pc.readus();

    printf("read  res %d\n", res);
    printf("read data %s\n", buf);
    printf("read time %d\n", (int)(t2-t1));
    printf("comm time %d\n", (int)(t4-t3));
    printf("comm res  %d\n", outcome);
    break;
  case 'w':
    t1 = pc.readus();
    printf("Writing cid %llx oid %llx off %d len %d trunc %d data %s\n", (long long)coid.cid, (long long)coid.oid, off, len, truncate, data);
    res = t.write(coid, data, off, len, truncate);
    t2 = pc.readus();

    t3 = pc.readus();
    outcome = t.tryCommit();
    t4 = pc.readus();

    printf("write  res %d\n", res);
    printf("write time %d\n", (int)(t2-t1));
    printf("comm  time %d\n", (int)(t4-t3));
    printf("comm res  %d\n", outcome);
    break;
  case 'p':
    res = t.put(coid, data, strlen(data)+1);
    printf("put    res %d\n", res);
    outcome = t.tryCommit();
    break;
  case 'g':
    res = t.get(coid, &getbuf, &len);
    printf("get    res %d\n", res);
    printf("get    len %d\n", len);
    printf("get    buf %s\n", getbuf);
    t.readFreeBuf(buf);
    break;
  }
}
#endif


#ifdef TESTMAIN3

#include <stdlib.h>
#include <stdio.h>
#include <time.h>

int main(int argc, char **argv)
{
  int i, res, count, v1, v2, len1, len2;
  int cid;

  COid coid1, coid2;


  if (argc == 2) cid=atoi(argv[1]);
  else cid=0;

  srand((unsigned)time(0)); // seed PRNG

  UniqueId::init();

  LocalTransaction t;
  S = new StorageServerState(0);

  coid1.cid = cid;
  coid1.oid = 0;
  coid2.cid = cid;
  coid2.oid = 256;
  res = 0;
  count = 0;

  for (i=0; i<1000; ++i){
    if (rand()%2==0){
      t.read(coid1, (char*) &v1, 0, sizeof(int), &len1);
      if (len1==0) v1=0;
      ++v1;
      t.write(coid1, (char*) &v1, 0, sizeof(int), true);

      t.read(coid2, (char*) &v2, 0, sizeof(int), &len2);
      if (len2==0) v2=0;
      ++v2;
      t.write(coid2, (char*) &v2, 0, sizeof(int), true);
    } else {
      t.read(coid2, (char*) &v2, 0, sizeof(int), &len2);
      if (len2==0) v2=0;
      ++v2;
      t.write(coid2, (char*) &v2, 0, sizeof(int), true);

      t.read(coid1, (char*) &v1, 0, sizeof(int), &len1);
      if (len1==0) v1=0;
      ++v1;
      t.write(coid1, (char*) &v1, 0, sizeof(int), true);
    }
    res = t.tryCommit();
    count += res ? 1 : 0;
    printf("res %d v1 %d v2 %d\n", res, v1, v2);
    t.start();
  }
  printf("sum errors %d\n", count);
  delete S;
}
#endif

//
// clientlib.cpp
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

#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <malloc.h>
#include <signal.h>
#include <sys/uio.h>

#include <list>
#include <map>
#include <set>

#include "tmalloc.h"
#include "options.h"
#include "debug.h"
#include "util.h"
#include "clientlib.h"
#include "clientlib-common.h"
#include "clientdir.h"
#include "gaiarpcaux.h"
#include "newconfig.h"
#include "supervalue.h"
#include "record.h"
#include "dtreeaux.h"

StorageConfig *InitGaia(void){
  StorageConfig *SC=0;

  UniqueId::init();
  tinitScheduler(0);

  const char *configfile = getenv(GAIACONFIG_ENV);
  if (!configfile) configfile = GAIA_DEFAULT_CONFIG_FILENAME;
  SC = new StorageConfig(configfile);
  return SC;
}

void UninitGaia(StorageConfig *SC){
  if (SC) delete SC;
}

Transaction::Transaction(StorageConfig *sc)
{
  readsTxCached = 0;
  piggy_buf = 0;
  Sc = sc;
  start();
}

Transaction::~Transaction(){
  txCache.clear();
  if (piggy_buf) delete piggy_buf;
}

// start a new transaction
int Transaction::start(){
  // obtain a new timestamp from local clock (assumes synchronized clocks)
  StartTs.setNew();
  Id.setNew();
  txCache.clear();  
  State = 0;  // valid
  hasWrites = false;
  hasWritesCachable = false;
  currlevel = 0;
  if (piggy_buf) delete piggy_buf;
  piggy_len = -1;
  piggy_buf = 0;
  piggy_level = 0;
  return 0;
}

// start a transaction with a start timestamp that will be set when the
// transaction first read, to be the timestamp of the latest available version
// to read.
// Right now, these transactions must read something before committing. Later,
// we should be able to extend this so that transactions can commit without
// having read.
int Transaction::startDeferredTs(void){
  StartTs.setIllegal();
  Id.setNew();
  txCache.clear();
  State = 0;  // valid
  hasWrites = false;
  return 0;
}

static int ioveclen(iovec *bufs, int nbufs){
  int len = 0;
  for (int i=0; i < nbufs; ++i)len += bufs[i].iov_len;
  return len;
}

static void iovecmemcpy(char *dest, iovec *bufs, int nbufs){
  for (int i=0; i < nbufs; ++i){
    memcpy((void*) dest, (void*) bufs[i].iov_base, bufs[i].iov_len);
    dest += bufs[i].iov_len;
  }
}

// write an object in the context of a transaction
int Transaction::writev(COid coid, int nbufs, iovec *bufs){
  IPPortServerno server;
  WriteRPCData *rpcdata;
  WriteRPCRespData rpcresp;
  char *resp;
  int respstatus;
  int totlen;

  Sc->Od->GetServerId(coid, server);
  
  if (State) return GAIAERR_TX_ENDED;
  // add server index to set of servers participating in transaction
  hasWrites = true;

#ifdef GAIA_CLIENT_CONSISTENT_CACHE
  if (IsCoidCachable(coid))
    hasWritesCachable = true;
#endif

  totlen = ioveclen(bufs, nbufs);
  
#ifdef GAIA_WRITE_ON_PREPARE
  if (piggy_len == -1){ // note that we should not piggy if piggy_len==-2
    // there's room for write piggyback
    assert(piggy_buf == 0);
    if (totlen <= GAIA_WRITE_ON_PREPARE_MAX_BYTES){ // not too many bytes
      // record information about write
      piggy_buf = new char[totlen];
      piggy_server = server;
      piggy_coid = coid;
      piggy_len = totlen;
      piggy_level = currlevel;
      iovecmemcpy(piggy_buf, bufs, nbufs);
      respstatus = 0; // return ok
      goto skiprpc;
    } else piggy_len = -2; // do not piggy later otherwise we might
                           // reorder the writes. This is conservative
                           // and could be optimized (it will
                           // avoid using piggy if the first write
                           // is too large)
  }
#endif

  Servers.insert(server);

  rpcdata = new WriteRPCData;
  rpcdata->data = new WriteRPCParm;
  rpcdata->freedata = true;
  rpcdata->freedatabuf = 0; // buffer comes from caller, so do not free it

  // fill out parameters
  rpcdata->data->tid = Id;
  rpcdata->data->cid = coid.cid;
  rpcdata->data->oid = coid.oid;
  rpcdata->data->level = currlevel;
  rpcdata->data->buf = 0;  // data->buf is not used by client

  // this is the buf information really used by the marshaller
  rpcdata->niovs=nbufs;
  rpcdata->iov = new iovec[nbufs];
  for (int i=0; i < nbufs; ++i){
    rpcdata->iov[i] = bufs[i]; 
  }
  rpcdata->data->len = totlen;  // total length

  resp = Sc->Rpcc->syncRPC(server.ipport, WRITE_RPCNO,
                           FLAG_HID(TID_TO_RPCHASHID(Id)), rpcdata);

  if (!resp){ // error contacting server
    //State=-2; // mark transaction as aborted due to I/O error
    return GAIAERR_SERVER_TIMEOUT;
  }

  rpcresp.demarshall(resp);

#ifdef GAIA_CLIENT_CONSISTENT_CACHE
  // refresh client cache metadata
  Sc->CCache->report(server.serverno, rpcresp.data->versionNoForCache,
                     rpcresp.data->tsForCache, rpcresp.data->reserveTsForCache);
#endif
  
  respstatus = rpcresp.data->status;
  free(resp);
  
 skiprpc:
  // record written data
  // create a private copy of the data
  Valbuf *vb = new Valbuf;
  vb->type = 0; // regular value
  vb->coid = coid;
  vb->immutable = false;
  vb->commitTs.setIllegal();
  vb->readTs.setIllegal();
  vb->len = totlen;
  vb->u.buf = Transaction::allocReadBuf(totlen);
  iovecmemcpy(vb->u.buf, bufs, nbufs); 

  Ptr<Valbuf> buf = vb;
  txCache.setCache(coid, currlevel, buf);

  //if (respstatus) State=-2; // mark transaction as aborted due to I/O error
  return respstatus;
}

// write an object in the context of a transaction
int Transaction::write(COid coid, char *buf, int len){
  iovec iovecbuf;
  iovecbuf.iov_base = buf;
  iovecbuf.iov_len = len;
  return writev(coid, 1, &iovecbuf);
}

int Transaction::put2(COid coid, char *data1, int len1, char *data2, int len2){
  iovec buf[2];
  buf[0].iov_base = data1;
  buf[0].iov_len = len1;
  buf[1].iov_base = data2;
  buf[1].iov_len = len2;
  return writev(coid, 2, buf);
}

int Transaction::put3(COid coid, char *data1, int len1, char *data2, int len2,
                      char *data3, int len3){
  iovec buf[3];
  buf[0].iov_base = data1;
  buf[0].iov_len = len1;
  buf[1].iov_base = data2;
  buf[1].iov_len = len2;
  buf[2].iov_base = data3;
  buf[2].iov_len = len3;
  return writev(coid, 3, buf);
}

// Try to read data locally using txCache from transaction.
// Returns a buffer from the tx cache, which caller should not change.
// typ: 0 for value, 1 for supervalue
// Returns:       0 = nothing read, 
//                1 = all data read
//                GAIAERR_TX_ENDED = cannot read because transaction is aborted
//                GAIAERR_WRONG_TYPE = wrong type
int Transaction::tryLocalRead(COid &coid, Ptr<Valbuf> &buf, int typ){
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

int Transaction::vget(COid coid, Ptr<Valbuf> &buf){
  IPPortServerno server;
  int reslocalread;

  ReadRPCData *rpcdata;
  ReadRPCRespData rpcresp;
  char *resp;
  int respstatus=0;

  Valbuf *vbuf;  
  int res;

  Sc->Od->GetServerId(coid, server);

  if (State){ buf = 0; return GAIAERR_TX_ENDED; }

  reslocalread = tryLocalRead(coid, buf, 0);
  if (reslocalread < 0) return reslocalread;
  if (reslocalread == 1){ // read completed already
    assert(buf->type == 0);
    return 0;
  }

#ifdef GAIA_OCC
  // add server index to set of servers participating in transaction
  Servers.insert(server); 
  ReadSet.insert(coid);
#endif

#ifdef GAIA_CLIENT_CONSISTENT_CACHE
  if (IsCoidCachable(coid)){
    int rescache;
    rescache = Sc->CCache->lookup(server.serverno, coid, buf, StartTs);
    if (!rescache){ // found it
      assert(buf->type == 0);
      buf = new Valbuf(*buf);  // make a copy
      goto skiprpc;
    }
  }
#endif

  rpcdata = new ReadRPCData;
  rpcdata->data = new ReadRPCParm;
  rpcdata->freedata = true; 

  // fill out parameters
  rpcdata->data->tid = Id;
  rpcdata->data->ts = StartTs;
  rpcdata->data->cid = coid.cid;
  rpcdata->data->oid = coid.oid;
  rpcdata->data->len = -1;  // requested max bytes to read

  resp = Sc->Rpcc->syncRPC(server.ipport, READ_RPCNO,
                           FLAG_HID(TID_TO_RPCHASHID(Id)), rpcdata);

  if (!resp){ // error contacting server
    //State=-2; // mark transaction as aborted due to I/O error
    buf = 0;
    return GAIAERR_SERVER_TIMEOUT;
  }

  rpcresp.demarshall(resp);

#ifdef GAIA_CLIENT_CONSISTENT_CACHE
  // refresh client cache metadata
  Sc->CCache->report(server.serverno, rpcresp.data->versionNoForCache,
                     rpcresp.data->tsForCache, rpcresp.data->reserveTsForCache);
#endif
  
  respstatus = rpcresp.data->status;
  if (respstatus){ free(resp); buf = 0; return respstatus; }

  if (StartTs.isIllegal()){ // if tx had no start timestamp, set it
    u64 readtsage = rpcresp.data->readts.age();
    if (readtsage > MAX_DEFERRED_START_TS){
      //dprintf(1,"Deferred: beyond max deferred age by %lld ms\n",
      //        (long long)readtsage);
      StartTs.setOld(MAX_DEFERRED_START_TS);
    }
    else StartTs = rpcresp.data->readts;
  }

  // fill out buf (returned value to user) with reply from RPC
  vbuf = new Valbuf;
  vbuf->type = 0;
  vbuf->coid = coid;
  vbuf->immutable = true;
  vbuf->commitTs = rpcresp.data->readts;
  vbuf->readTs = StartTs;
  vbuf->len = rpcresp.data->len;
  vbuf->u.buf = rpcresp.data->buf;
  buf = vbuf;

#ifdef GAIA_CLIENT_CONSISTENT_CACHE
  if (IsCoidCachable(coid)){
    Sc->CCache->set(server.serverno, coid, new Valbuf(*vbuf)); // copy valbuf
                                                        // for consistent cache
  }
#endif

 skiprpc:
  res = txCache.applyPendingOps(coid, buf, readsTxCached<MAX_READS_TO_TXCACHE);
  if (res<0) return res;
  if (readsTxCached < MAX_READS_TO_TXCACHE || res > 0) ++readsTxCached;

  return respstatus;
}

int Transaction::vsuperget(COid coid, Ptr<Valbuf> &buf, ListCell *cell,
                           Ptr<RcKeyInfo> prki){
  IPPortServerno server;
  int reslocalread;
  FullReadRPCData *rpcdata;
  FullReadRPCRespData rpcresp;
  char *resp;
  int respstatus;
  int res;

  Sc->Od->GetServerId(coid, server);  
  if (State){ buf = 0; return GAIAERR_TX_ENDED; }
  
  reslocalread = tryLocalRead(coid, buf, 1);
  if (reslocalread < 0) return reslocalread;
  if (reslocalread == 1){
    assert(buf->type==1);
    return 0; // read completed already
  }

#ifdef GAIA_OCC
  // add server index to set of servers participating in transaction
  Servers.insert(server); 
  ReadSet.insert(coid);
#endif

  rpcdata = new FullReadRPCData;
  rpcdata->data = new FullReadRPCParm;
  rpcdata->freedata = true; 

  // fill out parameters
  rpcdata->data->tid = Id;
  rpcdata->data->ts = StartTs;
  rpcdata->data->cid = coid.cid;
  rpcdata->data->oid = coid.oid;
  rpcdata->data->prki = prki;
  if (cell){
    rpcdata->data->cellPresent = 1;
    rpcdata->data->cell = *cell;
  }
  else {
    rpcdata->data->cellPresent = 0;
    memset(&rpcdata->data->cell, 0, sizeof(ListCell));
  }

  resp = Sc->Rpcc->syncRPC(server.ipport, FULLREAD_RPCNO,
                           FLAG_HID(TID_TO_RPCHASHID(Id)), rpcdata);

  if (!resp){ // error contacting server
    //State=-2; // mark transaction as aborted due to I/O error
    buf = 0;
    return GAIAERR_SERVER_TIMEOUT;
  }

  rpcresp.demarshall(resp);

#ifdef GAIA_CLIENT_CONSISTENT_CACHE
  // refresh client cache metadata
  Sc->CCache->report(server.serverno, rpcresp.data->versionNoForCache,
                     rpcresp.data->tsForCache, rpcresp.data->reserveTsForCache);
#endif
  
  respstatus = rpcresp.data->status;
  if (respstatus){ free(resp); buf = 0; return respstatus; }

  FullReadRPCResp *r = rpcresp.data; // for convenience

  if (StartTs.isIllegal()){ // if tx had no start timestamp, set it
    i64 readtsage = rpcresp.data->readts.age();
    if (readtsage > MAX_DEFERRED_START_TS){
      //printf("\nDeferred: beyond max deferred age by %lld ms ",
      //        (long long)readtsage);
      StartTs.setOld(MAX_DEFERRED_START_TS);
    }
    else StartTs = rpcresp.data->readts;
  }

  Valbuf *vbuf = new Valbuf;
  vbuf->type = 1;
  vbuf->coid = coid;
  vbuf->immutable = true;
  vbuf->commitTs = r->readts;
  vbuf->readTs = StartTs;
  vbuf->len = 0; // not applicable for supervalue
  SuperValue *sv = new SuperValue;
  vbuf->u.raw = sv;

  sv->Nattrs = r->nattrs;
  sv->CellType = r->celltype;
  sv->Ncells = r->ncelloids;
  sv->CellsSize = r->lencelloids;
  sv->Attrs = new u64[sv->Nattrs]; assert(sv->Attrs);
  memcpy(sv->Attrs, r->attrs, sizeof(u64) * sv->Nattrs);
  sv->Cells = new ListCell[sv->Ncells];
  // fill out cells
  char *ptr = r->celloids;
  for (int i=0; i < sv->Ncells; ++i){
    // extract nkey
    u64 nkey;
    ptr += myGetVarint((unsigned char*) ptr, &nkey);
    sv->Cells[i].nKey = nkey;
    if (r->celltype == 0) sv->Cells[i].pKey = 0; // integer cell, set pKey=0
    else { // non-integer key, so extract pKey (nkey has its length)
      sv->Cells[i].pKey = new char[(unsigned)nkey];
      memcpy(sv->Cells[i].pKey, ptr, (unsigned)nkey);
      ptr += nkey;
    }
    // extract childOid
    sv->Cells[i].value = *(Oid*)ptr;
    ptr += sizeof(u64); // space for 64-bit value in cell
  }
  sv->prki = r->prki;
  buf = vbuf;

  res = txCache.applyPendingOps(coid, buf, readsTxCached<MAX_READS_TO_TXCACHE);
  if (res<0) return res;
  if (readsTxCached < MAX_READS_TO_TXCACHE || res > 0) ++readsTxCached;
  free(resp); // free response buffer
  return respstatus;
}

// free a buffer returned by Transaction::read
void Transaction::readFreeBuf(char *buf){
  assert(buf);
  ReadRPCRespData::clientFreeReceiveBuffer(buf);
}

char *Transaction::allocReadBuf(int len){
  return ReadRPCRespData::clientAllocReceiveBuffer(len);
}

// add an object to a set in the context of a transaction
int Transaction::addset(COid coid, COid toadd){
  if (State) return GAIAERR_TX_ENDED;
  return GAIAERR_NOT_IMPL; // not implemented
}

// remove an object from a set in the context of a transaction
int Transaction::remset(COid coid, COid toremove){
  if (State) return GAIAERR_TX_ENDED;
  return GAIAERR_NOT_IMPL; // not implemented
}


// ------------------------------ Prepare RPC ----------------------------------

// static method
void Transaction::auxpreparecallback(char *data, int len, void *callbackdata){
  PrepareCallbackData *pcd = (PrepareCallbackData*) callbackdata;
  PrepareRPCRespData rpcresp;
  if (data){
    rpcresp.demarshall(data);
    pcd->data = *rpcresp.data;
  } else {
    pcd->data.vote = -1;   // indicates an error (no vote)
  }
  pcd->sem.signal();
  return; // free buffer
}

// Prepare part of two-phase commit
// sets chosents to the timestamp chosen for transaction
// Return 0 if all voted to commit, 1 if some voted to abort, 3 if error in
// getting some vote.
// Sets hascommitted to indicate if the transaction was also committed using
// one-phase commit.
// This is possible when the transaction spans only one server.
int Transaction::auxprepare(Timestamp &chosents, int &hascommitted){
  IPPortServerno server;
  SetNode<IPPortServerno> *it;
  PrepareRPCData *rpcdata;
  PrepareCallbackData *pcd;
  LinkList<PrepareCallbackData> pcdlist(true);
  int decision;
  Set<IPPortServerno> *serverset;
  Timestamp committs;

  serverset = &Servers;

#ifdef GAIA_WRITE_ON_PREPARE
  if (piggy_buf) serverset->insert(piggy_server);
#endif  

  //committs.setNew();
  //if (Timestamp::cmp(committs, StartTs) < 0)
  // committs = StartTs; // this could happen if (a) clock is not monotonic
  //                     // or (b) startts is deferred
  //chosents = committs;
  committs = StartTs;

#ifndef DISABLE_ONE_PHASE_COMMIT
  hascommitted = (serverset->getNitems()==1 ? 1 : 0) // tx writes to 1 item
                        && !hasWritesCachable;   // and it is not cachable item
#else
  hascommitted = 0;
#endif

  for (it = serverset->getFirst(); it != serverset->getLast();
       it = serverset->getNext(it)){
    server = it->key;

    rpcdata = new PrepareRPCData;
    rpcdata->data = new PrepareRPCParm;
    rpcdata->deletedata = true;

    // fill out parameters
    rpcdata->data->tid = Id;
    rpcdata->data->startts = StartTs;
    //rpcdata->data->committs = committs;
    rpcdata->data->onephasecommit = hascommitted;

#ifdef GAIA_WRITE_ON_PREPARE
    if (piggy_buf && IPPortServerno::cmp(server, piggy_server)==0) {
      assert(piggy_len >= 0);
      rpcdata->data->piggy_cid = piggy_coid.cid;
      rpcdata->data->piggy_oid = piggy_coid.oid;
      rpcdata->data->piggy_len = piggy_len;
      rpcdata->data->piggy_buf = piggy_buf;
      // do not set rpcdata->freedatabuf to later free piggy_buf
      // since piggy_buf belongs to the transaction (transaction will free
      // it)
    }
    else {
      //printf("Sending prepare without piggybuf\n");
      rpcdata->data->piggy_cid = 0;
      rpcdata->data->piggy_oid = 0;
      rpcdata->data->piggy_len = -1;
      rpcdata->data->piggy_buf = 0;
    }
#else
    rpcdata->data->piggy_cid = 0;
    rpcdata->data->piggy_oid = 0;
    rpcdata->data->piggy_len = -1;
    rpcdata->data->piggy_buf = 0;
#endif    

#ifdef GAIA_OCC
    // fill up the readset parameter
    rpcdata->deletereadset = true;
    int sizereadset = ReadSet.getNitems();
    rpcdata->data->readset_len = sizereadset;
    rpcdata->data->readset = new COid[sizereadset];
    SetNode<COid> *itreadset;
    int pos;
    // first, count the number of entries
    for (pos = 0, itreadset = ReadSet.getFirst();
         itreadset != ReadSet.getLast();
         ++pos, itreadset = ReadSet.getNext(itreadset)){
      rpcdata->data->readset[pos] = itreadset->key;
    }
#else
    rpcdata->deletereadset = false; // nothing to delete
    rpcdata->data->readset_len = 0;
    rpcdata->data->readset = 0;
#endif

    pcd = new PrepareCallbackData;
    pcd->serverno = server.serverno;
    pcdlist.pushTail(pcd);

    Sc->Rpcc->asyncRPC(server.ipport, PREPARE_RPCNO,
                       FLAG_HID(TID_TO_RPCHASHID(Id)), rpcdata,
                       auxpreparecallback, pcd); 
  }

  decision = 0; // commit decision
  for (pcd = pcdlist.getFirst(); pcd != pcdlist.getLast();
       pcd = pcdlist.getNext(pcd)){
    pcd->sem.wait(INFINITE);
#ifdef GAIA_CLIENT_CONSISTENT_CACHE
    if (pcd->data.vote != -1){ // got response from server
      // refresh client cache metadata
      Sc->CCache->report(pcd->serverno, pcd->data.versionNoForCache,
                         pcd->data.tsForCache, pcd->data.reserveTsForCache);
    }
#endif
    
    if (pcd->data.vote){   // did not get response or got abort vote
      if (decision < 3){
        if (pcd->data.vote < 0) decision = 3; // error getting some vote
        else decision = 1; // someone voted to abort
      }
    }
    if (decision==0){ // still want to commit
      if (Timestamp::cmp(pcd->data.mincommitts, committs) > 0)
        committs = pcd->data.mincommitts;  // keep track of largest seen
                                           // timestamp in committs
    }
  }
  if (decision==0){ // commit decision
    committs.addEpsilon(); // next higher timestamp
    chosents = committs;
  } else chosents.setIllegal();

  return decision;
}


// ------------------------------ Commit RPC -----------------------------------

// static method
void Transaction::auxcommitcallback(char *data, int len, void *callbackdata){
  CommitCallbackData *ccd = (CommitCallbackData*) callbackdata;
  CommitRPCRespData rpcresp;
  if (data){
    rpcresp.demarshall(data);
    // record information from response
    ccd->data = *rpcresp.data;
  } else {
    ccd->data.status = -1;  // mark error
    ccd->data.waitingts.setIllegal();
  }
  ccd->sem.signal();
  return; // free buffer
}

// Commit part of two-phase commit
// outcome is 0 to commit, 1 to abort due to prepare no vote, 2 to abort (user-initiated),
// 3 to abort due to prepare failure
int Transaction::auxcommit(int outcome, Timestamp committs,
                           Timestamp *waitingts){
  IPPortServerno server;
  CommitRPCData *rpcdata;
  CommitCallbackData *ccd;
  LinkList<CommitCallbackData> ccdlist(true);
  Set<IPPortServerno> *serverset;
  SetNode<IPPortServerno> *it;
  int res;

  serverset = &Servers;

  res = 0;
  for (it = serverset->getFirst(); it != serverset->getLast();
       it = serverset->getNext(it)){
    server = it->key;

    rpcdata = new CommitRPCData;
    rpcdata->data = new CommitRPCParm;
    rpcdata->freedata = true;

    // fill out parameters
    rpcdata->data->tid = Id;
    rpcdata->data->committs = committs;
    rpcdata->data->commit = outcome;

    ccd = new CommitCallbackData;
    ccdlist.pushTail(ccd);

    Sc->Rpcc->asyncRPC(server.ipport, COMMIT_RPCNO,
                       FLAG_HID(TID_TO_RPCHASHID(Id)), rpcdata,
                       auxcommitcallback, ccd);
  }

  if (waitingts) waitingts->setLowest();

  for (ccd = ccdlist.getFirst(); ccd != ccdlist.getLast();
       ccd = ccdlist.getNext(ccd)){
    ccd->sem.wait(INFINITE);
    if (ccd->data.status < 0) res = GAIAERR_SERVER_TIMEOUT; // error contacting
                                                            // server
    else {
      if (waitingts && !ccd->data.waitingts.isIllegal() &&
          Timestamp::cmp(ccd->data.waitingts, *waitingts) > 0){
        *waitingts = ccd->data.waitingts; // update largest waitingts found
      }
    }
  }

  return res;
}

//--------------------------- subtransactions ------------------------
// start a subtransaction with the given level, which
// must be greater than currlevel
int Transaction::startSubtrans(int level){
  if (level <= currlevel) return -1;
  currlevel = level;
  return 0;
}

// rollback the changes made by a subtransaction. Any updates
// done with a higher level are discarded. Afterwards, currlevel
// is set to level
int Transaction::abortSubtrans(int level){
  int res;
  assert(level <= currlevel);
  if (level < currlevel){
    res = auxsubtrans(level, 0); // tell servers to abort higher levels
    if (res) return res;
    if (piggy_level > level){
      if (piggy_buf) delete piggy_buf;
      piggy_buf = 0;
      piggy_len = -2;
      piggy_level = 0;
    }
      
    txCache.abortLevel(level); // make changes to tx cache
    if (level < 0) level = 0;
    currlevel = level;
  }
  return 0;
}

// release subtransactions, moving them to a lower level.
// Any updates done with a higher level are changed to the given level.
// Afterwards, currlevel is set to level
int Transaction::releaseSubtrans(int level){
  int res;
  assert(level <= currlevel);
  if (level < currlevel){
    res = auxsubtrans(level, 1); // tell servers to release higher levels
    if (res) return res;
    if (piggy_level > level) piggy_level = level;
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

int Transaction::tryCommit(Timestamp *retcommitts){
  int outcome;
  Timestamp committs, waitingts;
  int hascommitted;
  int res;

  if (State) return GAIAERR_TX_ENDED;

#ifdef GAIA_OCC
  if (!hasWrites && ReadSet.getNitems() <= 1) return 0; // nothing to commit
#else
  if (!hasWrites) return 0; // nothing to commit
#endif

  // Prepare phase
  outcome = auxprepare(committs, hascommitted);

  if (outcome == 0 && !hascommitted){
#ifdef GAIA_CLIENT_CONSISTENT_CACHE
    if (hasWritesCachable){
      int tosleep = -committs.ageus();
      if (tosleep > 0){
        ++tosleep;
        usleep(tosleep);
      }
    }
    else Timestamp::catchup(committs);
#else
    Timestamp::catchup(committs);    
#endif
  }

  if (!hascommitted){
    // Commit phase
    res = auxcommit(outcome, committs, &waitingts);
    Timestamp::catchup(waitingts);
  } else res = 0;

  if (outcome==0){
    if (retcommitts) *retcommitts = committs; // if requested, return commit
                                              // timestamp
    // update lastcommitts
    //if (lastcommitts < committs.getd1()) lastcommitts = committs.getd1();
  }

  State=-1;  // transaction now invalid

  // clear txCache
  txCache.clear();
  if (res < 0) return res;
  else return outcome;
}

// Abort transaction. Leaves it in valid state.
int Transaction::abort(void){
  Timestamp dummyts;
  int res;

  if (State) return GAIAERR_TX_ENDED;
  if (!hasWrites) return 0; // nothing to commit

  // tell servers to throw away any outstanding writes they had
  dummyts.setIllegal();
  res = auxcommit(2, dummyts, 0); // timestamp not really used for aborting txs

  // clear txCache
  txCache.clear();

  State=-1;  // transaction now invalid
  if (res) return res;
  return 0;
}

// ----------------------------- Subtrans RPC ----------------------------------

// static method
void Transaction::auxsubtranscallback(char *data, int len, void *callbackdata){
  SubtransCallbackData *ccd = (SubtransCallbackData*) callbackdata;
  SubtransRPCRespData rpcresp;
  if (data){
    rpcresp.demarshall(data);
    // record information from response
    ccd->data = *rpcresp.data;
  } else {
    ccd->data.status = -1;  // mark error
  }
  ccd->sem.signal();
  return; // free buffer
}

// Auxilliary function to invoke subtrans RPC to all servers
// in the serverset of transaction
// action 0 means abort, 1 means release
int Transaction::auxsubtrans(int level, int action){
  IPPortServerno server;
  SubtransRPCData *rpcdata;
  SubtransCallbackData *ccd;
  LinkList<SubtransCallbackData> ccdlist(true);
  Set<IPPortServerno> *serverset;
  SetNode<IPPortServerno> *it;
  int res;

  serverset = &Servers;

  res = 0;
  for (it = serverset->getFirst(); it != serverset->getLast();
       it = serverset->getNext(it)){
    server = it->key;

    rpcdata = new SubtransRPCData;
    rpcdata->data = new SubtransRPCParm;
    rpcdata->freedata = true;

    // fill out parameters
    rpcdata->data->tid = Id;
    rpcdata->data->level = level;
    rpcdata->data->action = action;

    ccd = new SubtransCallbackData;
    ccdlist.pushTail(ccd);

    Sc->Rpcc->asyncRPC(server.ipport, SUBTRANS_RPCNO,
                       FLAG_HID(TID_TO_RPCHASHID(Id)), rpcdata,
                       auxsubtranscallback, ccd);
  }

  for (ccd = ccdlist.getFirst(); ccd != ccdlist.getLast();
       ccd = ccdlist.getNext(ccd)){
    ccd->sem.wait(INFINITE);
    if (ccd->data.status < 0) res = GAIAERR_SERVER_TIMEOUT; // error contacting server
  }

  return res;
}

//// Read an object in the context of a transaction. Returns
//// a ptr that must be serialized by calling
//// rpcresp.demarshall(ptr).
//int Transaction::readSuperValue(COid coid, SuperValue **svret){
//  IPPortServerno server;
//  FullReadRPCData *rpcdata;
//  FullReadRPCRespData rpcresp;
//  char *resp;
//  int respstatus;
//  SuperValue *sv;
//
//  Sc->Od->GetServerId(coid, server);
//
//  if (State) return GAIAERR_TX_ENDED;
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
//  resp = Sc->Rpcc->syncRPC(server.ipport, FULLREAD_RPCNO,
//            FLAG_HID(TID_TO_RPCHASHID(Id)), rpcdata);
//
//  if (!resp){ /*State=-2;*/ return GAIAERR_SERVER_TIMEOUT; } // error
//                                                          // contacting server
//  rpcresp.demarshall(resp);
//  respstatus = rpcresp.data->status;
//  if (respstatus != 0){
//    free(resp);
//    return respstatus;
//  }
//
//  // set local attributes
//  FullReadRPCResp *r = rpcresp.data; // for convenience
//  sv = new SuperValue;
//  sv->Nattrs = r->nattrs;
//  sv->CellType = r->celltype;
//  sv->Ncells = r->ncelloids;
//  sv->CellsSize = r->lencelloids;
//  sv->Attrs = new u64[r->nattrs]; assert(sv->Attrs);
//  memcpy(sv->Attrs, r->attrs, r->nattrs * sizeof(u64));
//  sv->Cells = new ListCell[sv->Ncells];
//  // fill out cells
//  char *ptr = r->celloids;
//  for (int i=0; i < sv->Ncells; ++i){
//    // extract nkey
//    u64 nkey;
//    ptr += myGetVarint((unsigned char*) ptr, &nkey);
//    sv->Cells[i].nKey = nkey;
//    if (r->celltype == 0) sv->Cells[i].pKey = 0; // integer cell, set pKey=0
//    else { // non-integer key, so extract pKey (nkey has its length)
//      sv->Cells[i].pKey = new char[nkey];
//      memcpy(sv->Cells[i].pKey, ptr, nkey);
//      ptr += nkey;
//    }
//    // extract childOid
//    sv->Cells[i].value = *(Oid*)ptr;
//    ptr += sizeof(u64); // space for 64-bit value in cell
//  }
//  free(resp);
//  *svret = sv;
//  return 0;
//}

int Transaction::writeSuperValue(COid coid, SuperValue *sv){
  IPPortServerno server;
  FullWriteRPCData *rpcdata;
  FullWriteRPCRespData rpcresp;
  char *cells, *ptr;
  char *resp;
  int respstatus, len, i;

  Sc->Od->GetServerId(coid, server);
  if (State) return GAIAERR_TX_ENDED;

  // add server index to set of servers participating in transaction
  hasWrites = true;
  Servers.insert(server);

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
      ptr += (int) sv->Cells[i].nKey;
    }
    // copy childOid
    memcpy(ptr, &sv->Cells[i].value, sizeof(u64));
    ptr += sizeof(u64);
  }

  // do the RPC
  resp = Sc->Rpcc->syncRPC(server.ipport, FULLWRITE_RPCNO, FLAG_HID(TID_TO_RPCHASHID(Id)), rpcdata);

  if (!resp){ /* State=-2; */ return GAIAERR_SERVER_TIMEOUT; } // error
                                                          // contacting server

  rpcresp.demarshall(resp);

#ifdef GAIA_CLIENT_CONSISTENT_CACHE
  // refresh client cache metadata
  Sc->CCache->report(server.serverno, rpcresp.data->versionNoForCache,
                     rpcresp.data->tsForCache, rpcresp.data->reserveTsForCache);
#endif
  
  respstatus = rpcresp.data->status;
  free(resp);
  // if (respstatus) State = -2;
  return respstatus;
}

#if DTREE_SPLIT_LOCATION != 1
int Transaction::listAdd(COid coid, ListCell *cell, Ptr<RcKeyInfo> prki,
                         int flags){
#else
int Transaction::listAdd(COid coid, ListCell *cell, Ptr<RcKeyInfo> prki,
                         int flags, int *ncells, int *size){
#endif
  IPPortServerno server;
  ListAddRPCData *rpcdata;
  ListAddRPCRespData rpcresp;
  char *resp;
  int respstatus;
  Ptr<Valbuf> vbuf;
  int localread;

  Sc->Od->GetServerId(coid, server);
  if (State) return GAIAERR_TX_ENDED;

  localread = tryLocalRead(coid, vbuf, 1);
  if (localread < 0) return localread;

  if (flags & 1){
    if (localread == 1){ // data present in txcache, check it
      if (vbuf->u.raw->Attrs[DTREENODE_ATTRIB_FLAGS] & DTREENODE_FLAG_LEAF)
        return GAIAERR_CELL_OUTRANGE; // must be leaf
      assert(!txCache.hasPendingOps(coid));
      //applyPendingOps(coid, vbuf);
      if (vbuf->u.raw->Ncells >= 1){
	int matches;
	myCellSearchNode(vbuf, cell->nKey, cell->pKey, 0, prki, &matches);
	if (matches) return 0; // found, nothing to do
      }
      // data in txcache, but key not present, so continue to ask server to
      // listadd it
    } else { // data not in txcache, but we may have pending ops
      // optimization below isn't correct since we must check whether node is
      // leaf and key is in range in any case, optimization below helps only if
      // key was previously added in the same transaction
      //res = checkPendingOps(coid, cell->nKey, cell->pKey, prki);
      //if (res == 1) return 0;
    }
  }
  
  // add server index to set of servers participating in transaction
  hasWrites = true;
  Servers.insert(server);

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
  resp = Sc->Rpcc->syncRPC(server.ipport, LISTADD_RPCNO,
                           FLAG_HID(TID_TO_RPCHASHID(Id)), rpcdata);

  if (!resp){ // error contacting server
    // State=-2; // mark transaction as aborted due to I/O error
    return GAIAERR_SERVER_TIMEOUT;
  }

  rpcresp.demarshall(resp);

#ifdef GAIA_CLIENT_CONSISTENT_CACHE
  // refresh client cache metadata
  Sc->CCache->report(server.serverno, rpcresp.data->versionNoForCache,
                     rpcresp.data->tsForCache, rpcresp.data->reserveTsForCache);
#endif

  respstatus = rpcresp.data->status;
  if (respstatus == GAIAERR_CELL_OUTRANGE){ free(resp); return respstatus; }
  
  if (respstatus) ; // State=-2; // mark transaction as aborted due to I/O error
  
  PendingOpsEntry *poe;
  poe = new PendingOpsEntry;
  poe->type = 0; // add
  poe->level = currlevel;
  poe->u.add.cell.copy(*cell);
  poe->prki = prki;

  if (localread == 0) txCache.addPendingOps(coid, poe); // no data in cache
  else {
    assert(vbuf->type==1);
    txCache.applyOneOp(coid, poe);
    delete poe;
  }

#if DTREE_SPLIT_LOCATION == 1
  if (ncells) *ncells = rpcresp.data->ncells;
  if (size) *size = rpcresp.data->size;
#endif
  free(resp);
  return respstatus;
}

// Deletes a range of cells from a supervalue
// intervalType indicates how the interval is to be treated.
// The possible values are
//     0=(cell1,cell2)   1=(cell1,cell2]   2=(cell1,inf)
//     3=[cell1,cell2)   4=[cell1,cell2]   5=[cell1,inf)
//     6=(-inf,cell2)    7=(-inf,cell2]    8=(-inf,inf)
// where inf is infinity
int Transaction::listDelRange(COid coid, u8 intervalType, ListCell *cell1,
                              ListCell *cell2, Ptr<RcKeyInfo> prki){
  IPPortServerno server;
  ListDelRangeRPCData *rpcdata;
  ListDelRangeRPCRespData rpcresp;
  char *resp;
  int respstatus;
  Ptr<Valbuf> vbuf;
  int localread;

  Sc->Od->GetServerId(coid, server);
 if (State) return GAIAERR_TX_ENDED;

  localread = tryLocalRead(coid, vbuf, 1);
  if (localread < 0) return localread;

  // add server index to set of servers participating in transaction
  hasWrites = true;
  Servers.insert(server);

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

  resp = Sc->Rpcc->syncRPC(server.ipport, LISTDELRANGE_RPCNO,
                           FLAG_HID(TID_TO_RPCHASHID(Id)), rpcdata);

  if (!resp){ // error contacting server
    // State=-2; // mark transaction as aborted due to I/O error
    return GAIAERR_SERVER_TIMEOUT;
  }

  rpcresp.demarshall(resp);
  
#ifdef GAIA_CLIENT_CONSISTENT_CACHE
  // refresh client cache metadata
  Sc->CCache->report(server.serverno, rpcresp.data->versionNoForCache,
                     rpcresp.data->tsForCache, rpcresp.data->reserveTsForCache);
#endif
  
  respstatus = rpcresp.data->status;
  free(resp);

  if (respstatus) ; // State=-2; // mark transaction as aborted due to I/O error
  else {

    PendingOpsEntry *poe;
    poe = new PendingOpsEntry;
    poe->type = 1; // delrange
    poe->level = currlevel;
    poe->u.delrange.cell1.copy(*cell1);
    poe->u.delrange.cell2.copy(*cell2);
    poe->u.delrange.intervtype = intervalType;
    poe->prki = prki;

    if (localread == 0) txCache.addPendingOps(coid, poe); // no data in cache
    else {
      assert(vbuf->type==1);
      txCache.applyOneOp(coid, poe);
      delete poe;
    }
  }

  return respstatus;
}
  
// adds a cell to a supervalue
int Transaction::attrSet(COid coid, u32 attrid, u64 attrvalue){
  IPPortServerno server;
  AttrSetRPCData *rpcdata;
  AttrSetRPCRespData rpcresp;
  char *resp;
  int respstatus;
  int localread;
  
  Ptr<Valbuf> vbuf;

  Sc->Od->GetServerId(coid, server);  
  if (State) return GAIAERR_TX_ENDED;

  localread = tryLocalRead(coid, vbuf, 1);
  if (localread < 0) return localread;
    
  // add server index to set of servers participating in transaction
  hasWrites = true;
  Servers.insert(server);

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

  resp = Sc->Rpcc->syncRPC(server.ipport, ATTRSET_RPCNO,
                           FLAG_HID(TID_TO_RPCHASHID(Id)), rpcdata);

  if (!resp){ // error contacting server
    // State=-2; // mark transaction as aborted due to I/O error
    return GAIAERR_SERVER_TIMEOUT;
  }

  rpcresp.demarshall(resp);
  respstatus = rpcresp.data->status;
  free(resp);

  if (respstatus) ; // State=-2; // mark transaction as aborted due to I/O error
  else {
    PendingOpsEntry *poe;
    poe = new PendingOpsEntry;
    poe->type = 2; // attrset
    poe->level = currlevel;
    poe->u.attrset.attrid = attrid;
    poe->u.attrset.attrvalue = attrvalue;

    if (localread == 0) txCache.addPendingOps(coid, poe); // no data in cache
    else {
      assert(vbuf->type==1);
      txCache.applyOneOp(coid, poe);
      delete poe;
    }
  }

  return respstatus;
}


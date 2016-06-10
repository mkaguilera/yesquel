//
// storageserver.cpp
//
// Implements the RPCs at the storage server.
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
#include <stdarg.h>
#include <ctype.h>
#include <stddef.h>

#include <map>
#include <list>
#include <set>

#include "tmalloc.h"
#include "os.h"
#include "options.h"
#include "debug.h"
#include "gaiarpcaux.h"
#include "gaiarpcauxfunc.h"

#include "dtreeaux.h"

#include "storageserver.h"
#include "storageserverstate.h"

#include "ccache.h"
#include "ccache-server.h"

#define SHORT_OP_LOG // if defined, output short messages for the operation log

#if defined(STORAGESERVER_SPLITTER) && !defined(LOCALSTORAGE)
#include "storageserver-splitter.h"
#include "splitter-client.h"
#include "clientdir.h"
#include "kvinterface.h"
extern StorageConfig *SC;
#endif

StorageServerState *S=0;

// if hc==0 then this is for the local storage server
void initStorageServer(HostConfig *hc){
  S = new StorageServerState(hc);
#if defined(STORAGESERVER_SPLITTER) && !defined(LOCALSTORAGE)
  if (hc) initServerSplitter(); // no server splitter for local storage server
#endif
}

Marshallable *nullRpc(NullRPCData *d){
  NullRPCRespData *resp;
  assert(S); // if this assert fails, forgot to call initStorageServer()

  dprintf(2, "Got ping");
  dshowchar('_');

  resp = new NullRPCRespData;
  resp->data = new NullRPCResp;
  resp->freedata = true;
  resp->data->reserved = 0;
  return resp;
}

Marshallable *getstatusRpc(GetStatusRPCData *d){
  GetStatusRPCRespData *resp;
  assert(S); // if this assert fails, forgot to call initStorageServer()

  dprintf(2, "Got getstatus");
  dshowchar('G');

  resp = new GetStatusRPCRespData;
  resp->data = new GetStatusRPCResp;
  resp->freedata = true;
  resp->data->reserved = 0;
  return resp;
}

Marshallable *writeRpc(WriteRPCData *d){
  Ptr<PendingTxInfo> pti;
  WriteRPCRespData *resp;
  Ptr<TxRawCoid> *trcoidptr;
  Ptr<TxRawCoid> trcoid;
  TxWriteItem *twi;
  COid coid;
  int res;
  char *buf=0;
  int status=0;

  assert(S); // if this assert fails, forgot to call initStorageServer()
  dshowchar('w');

  coid.cid = d->data->cid;
  coid.oid = d->data->oid;

#ifndef SHORT_OP_LOG
  dprintf(1,"WRITE    tid %016llx:%016llx coid %016llx:%016llx len %d lev %d",
    (long long)d->data->tid.d1, (long long)d->data->tid.d2,
    (long long)coid.cid, (long long)coid.oid, d->data->len,
    d->data->level);
#else
  dshortprintf(1, "WRITE    %016llx:%016llx len %d lev %d",
               (long long)coid.cid, (long long)coid.oid, d->data->len,
               d->data->level);
#endif

  S->cPendingTx.getInfo(d->data->tid, pti);

  twi = new TxWriteItem(coid, d->data->level);
  buf = (char*) malloc(d->data->len);
  //buf = new char[d->data->len]; // make private copy of data
  memcpy(buf, d->data->buf, d->data->len);
  twi->alloctype=1; // if set, will free twi->buf latter but not twi->rpcrequest

  res = pti->coidinfo.lookupInsert(coid, trcoidptr);
  if (res){ 
    *trcoidptr = new TxRawCoid;
    trcoid = *trcoidptr;
  } else trcoid = *trcoidptr;

  // create and add new item to trcoid
  twi->len = d->data->len;
  twi->buf = buf;
  twi->rpcrequest = (char*) d;
  trcoid->add(twi);

  if (IsCoidCachable(coid))
    pti->updatesCachable = true; // marks tx as updating cachable data

  //pti->unlock();
  resp = new WriteRPCRespData;
  resp->data = new WriteRPCResp;
  resp->data->status = status;
  updateRPCResp(resp->data); // updated piggybacked fields for client caching
  resp->freedata = true;

  return resp;
}

Marshallable *readRpc(ReadRPCData *d, void *handle, bool &defer){
  ReadRPCRespData *resp;
  int res;
  COid coid;
  Timestamp readts;
  Ptr<TxUpdateCoid> tucoid;

  assert(S); // if this assert fails, forgot to call initStorageServer()
  dshowchar('r');
#ifndef SHORT_OP_LOG
  dprintf(1, "READ     tid %016llx:%016llx coid %016llx:%016llx "
          "ts %016llx:%016llx len %d",
          (long long)d->data->tid.d1, (long long)d->data->tid.d2,
          (long long)d->data->cid, (long long)d->data->oid,
          (long long)d->data->ts.getd1(), (long long)d->data->ts.getd2(),
          d->data->len);
#else
  dshortprintf(1, "READ     %016llx:%016llx",
               (long long)d->data->cid, (long long)d->data->oid);
#endif
  
  readts.setIllegal();
  coid.cid=d->data->cid;
  coid.oid=d->data->oid;

  res = S->cLogInMemory.readCOid(coid, d->data->ts, tucoid, &readts, handle);

  if (res == GAIAERR_DEFER_RPC){ // defer the RPC
    defer = true; // defer RPC (go to sleep instead of finishing task)
    return 0;
  }
  if (!res && tucoid->WriteSV) res = GAIAERR_WRONG_TYPE; // wrong type
  resp = new ReadRPCRespData;
  resp->data = new ReadRPCResp;
  if (res<0){
    resp->data->status = res;
    resp->data->readts.setIllegal();
    resp->data->buf = 0;
    resp->data->len = 0;
    resp->freedata = false;
  }
  else { // no error
    // create and add new item to read set
    //Ptr<PendingTxInfo> pti;
    //S->cPendingTx.getInfo(d->data->tid, pti);
    //tri = new TxReadItem;
    //tri->coid.cid = d->data->cid;
    //tri->coid.oid = d->data->oid;

    //pti->unlock();
    TxWriteItem *twi=tucoid->Writevalue;
    assert(twi);
    char *buf;
    int len = twi->len;

    buf = twi->buf;
    resp->freedatabuf = 0; // nothing to delete later

    resp->data->status = 0;
    resp->data->readts = readts;
    resp->data->len = len;
    resp->data->buf = buf;
    resp->freedata = true;
    resp->tucoid = tucoid;
  }

  updateRPCResp(resp->data); // updated piggybacked fields for client caching
  
#ifndef SHORT_OP_LOG
  dprintf(1, "READR    tid %016llx:%016llx coid %016llx:%016llx "
          "ts %016llx:%016llx len %d [len %d status %d readts %016llx:%016llx]",
          (long long)d->data->tid.d1, (long long)d->data->tid.d2,
          (long long)d->data->cid, (long long)d->data->oid,
          (long long)d->data->ts.getd1(), (long long)d->data->ts.getd2(),
          d->data->len, resp->data->len, resp->data->status,
          (long long)resp->data->readts.getd1(),
          (long long)resp->data->readts.getd2());
#else
  dshortprintf(1, "READR    %016llx:%016llx [len %d res %d]",
               (long long)d->data->cid, (long long)d->data->oid,
               resp->data->len, resp->data->status);
#endif

  defer = false;
  return resp;
}

Marshallable *fullwriteRpc(FullWriteRPCData *d){
  Ptr<PendingTxInfo> pti;
  FullWriteRPCRespData *resp;
  Ptr<TxRawCoid> *trcoidptr;
  Ptr<TxRawCoid> trcoid;
  TxWriteSVItem *twsvi;
  COid coid;
  int res;
  int status=0;

  assert(S); // if this assert fails, forgot to call initStorageServer()
  dshowchar('W');
#ifndef SHORT_OP_LOG
  dprintf(1, "WRITESV  tid %016llx:%016llx coid %016llx:%016llx nattrs %d "
          "ncells %d lencells %d lev %d",
          (long long)d->data->tid.d1, (long long)d->data->tid.d2,
          (long long)d->data->cid, (long long)d->data->oid,
          d->data->nattrs, d->data->ncelloids, d->data->lencelloids,
          d->data->level);
#else
  dshortprintf(1, "WRITESV  %016llx:%016llx ncells %d lev %d",
       (long long)d->data->cid, (long long)d->data->oid, d->data->ncelloids,
       d->data->level);
#endif

  S->cPendingTx.getInfo(d->data->tid, pti);

  twsvi = fullWriteRPCParmToTxWriteSVItem(d->data);

  coid.cid = d->data->cid;
  coid.oid = d->data->oid;

  res = pti->coidinfo.lookupInsert(coid, trcoidptr);
  if (res){
    *trcoidptr = new TxRawCoid;
    trcoid = *trcoidptr;
  } else trcoid = *trcoidptr;
  trcoid->add(twsvi);

  //pti->unlock();
  resp = new FullWriteRPCRespData;
  resp->data = new FullWriteRPCResp;
  resp->data->status = status;
  updateRPCResp(resp->data); // updated piggybacked fields for client caching
  
  resp->freedata = true;

  return resp;
}

Marshallable *fullreadRpc(FullReadRPCData *d, void *handle, bool &defer){
  FullReadRPCRespData *resp;
  int res;
  COid coid;
  Timestamp readts;
  Ptr<TxUpdateCoid> tucoid;

  assert(S); // if this assert fails, forgot to call initStorageServer()
  dshowchar('R');

#ifndef SHORT_OP_LOG
  dprintf(1, "READSV   tid %016llx:%016llx coid %016llx:%016llx "
          "ts %016llx:%016llx",
    (long long)d->data->tid.d1, (long long)d->data->tid.d2,
    (long long)d->data->cid, (long long)d->data->oid,
    (long long)d->data->ts.getd1(), (long long)d->data->ts.getd2())
#else
  dshortprintf(1, "READSV   %016llx:%016llx",
               (long long)d->data->cid, (long long)d->data->oid);
#endif
  
  readts.setIllegal();
  coid.cid=d->data->cid;
  coid.oid=d->data->oid;

#if defined(STORAGESERVER_SPLITTER) && !defined(LOCALSTORAGE) && (DTREE_SPLIT_LOCATION != 1) && defined(DTREE_LOADSPLITS)
  if (d->data->cellPresent){
    ListCellPlus *cell;
    //printf("FULLREADRPC got cell nkey %lld (%llx) pkey %p\n",
    //   (long long) d->data->cell.nKey, (long long) d->data->cell.nKey,
    //   d->data->cell.pKey);
    cell = new ListCellPlus(d->data->cell, d->data->prki);
    ReportAccess(coid, cell);
  }
#endif  
  res = S->cLogInMemory.readCOid(coid, d->data->ts, tucoid, &readts, handle);

  if (res == GAIAERR_DEFER_RPC){ // defer the RPC
    defer = true; // special value to mark RPC as deferred
    return 0;
  }
  if (!res && tucoid->Writevalue) res = GAIAERR_WRONG_TYPE; // wrong type
  resp = new FullReadRPCRespData;
  resp->data = new FullReadRPCResp;
  if (res<0){
    resp->data->status = res;
    resp->data->readts.setIllegal();
    resp->data->nattrs = 0;
    resp->data->celltype = 0;
    resp->data->ncelloids = 0;
    resp->data->lencelloids = 0;
    resp->data->attrs = 0;
    resp->data->celloids = 0;
    resp->freedata = 1;
    resp->deletecelloids = 0;
    resp->twsvi = 0;
  }
  else { // no error
    // create and add new item to read set
    //Ptr<PendingTxInfo> pti;
    //S->cPendingTx.getInfo(d->data->tid, pti);
    //tri = new TxReadItem;
    //tri->coid.cid = d->data->cid;
    //tri->coid.oid = d->data->oid;

    //pti->unlock();
    TxWriteSVItem *twsvi = tucoid->WriteSV;
    assert(twsvi);

    int ncelloids, lencelloids;
    char *buf = twsvi->getCelloids(ncelloids, lencelloids);

    resp->data->status = 0;
    resp->data->readts = readts;
    resp->data->nattrs = twsvi->nattrs;
    resp->data->celltype = twsvi->celltype;
    resp->data->ncelloids = twsvi->cells.getNitems();
    resp->data->lencelloids = lencelloids;
    resp->data->attrs = twsvi->attrs;
    resp->data->celloids = buf;
    resp->data->prki = twsvi->prki;

    resp->freedata = 1;
    resp->deletecelloids = 0; // do not free celloids since it belongs to twsvi
    resp->twsvi = 0;
    resp->tucoid = tucoid;
  }

  updateRPCResp(resp->data); // updated piggybacked fields for client caching

#ifndef SHORT_OP_LOG
  dprintf(1, "READSVR  tid %016llx:%016llx coid %016llx:%016llx "
          "ts %016llx:%016llx [celltype %d ncells %d status %d "
          "readts %016llx:%016llx]",
          (long long)d->data->tid.d1, (long long)d->data->tid.d2,
          (long long)d->data->cid, (long long)d->data->oid,
          (long long)d->data->ts.getd1(), (long long)d->data->ts.getd2(),
          resp->data->celltype, resp->data->ncelloids, resp->data->status,
          (long long)resp->data->readts.getd1(),
          (long long)resp->data->readts.getd2());
#else
  dshortprintf(1, "READSVR  %016llx:%016llx [ncells %d res %d]",
    (long long)d->data->cid, (long long)d->data->oid, resp->data->ncelloids,
               resp->data->status);
#endif
  defer = false;
  return resp;
}

// Sets ret_tucoid with the result of applying all outstanding operations
// on tx_trcoid to the committed version of the coid as of the given readTs.
// The returned tucoid will have WriteSV set with the latest supervalue.
// If the function is passed a non-set tx_trcoid, then it just returns
// the tucoid of the committed version as of the given readTs.
//
// Input: the coid concerned, the tucoid with outstanding ops (or 0), and the
//        start timestamp of the transaction
// Output: ret_tucoid has WriteSV set.
//         returns 0 if no error, <0 if error
static int getCurrTucoid(COid &coid, Ptr<TxRawCoid> tx_trcoid,
                        const Timestamp &readTs, Ptr<TxUpdateCoid> &ret_tucoid){
  TxListItem *lastwrite = 0;
  TxWriteSVItem *twsvi = 0;
  TxListItem *curr, *next;
  Ptr<TxUpdateCoid> log_tucoid;
  int res;

  if (tx_trcoid.isset()){
    // scan first to find last write or writeSV, if any
    for (curr = tx_trcoid->getFirst(); curr != tx_trcoid->getLast();
         curr = tx_trcoid->getNext(curr)){
      if (curr->type == 2 || curr->type==3) lastwrite=curr;
    }
    // check last write or writeSV
    if (lastwrite){
      if (lastwrite->type == 2){ // write
        // make sure there is nothing else
        assert(tx_trcoid->getNext(lastwrite)==tx_trcoid->getLast());
        // produce a tucoid with write and return it
        TxWriteItem *twi = dynamic_cast<TxWriteItem*>(lastwrite); assert(twi);
        ret_tucoid = new TxUpdateCoid(new TxWriteItem(*twi));
        return 0;
      }
      // writeSV
      assert(lastwrite->type == 3);
      curr = tx_trcoid->getNext(lastwrite); // start after write
      twsvi = dynamic_cast<TxWriteSVItem*>(lastwrite);
      
    } else {
      twsvi = 0; // get it from log
      curr = tx_trcoid->getFirst(); // start at first item
    }
  } else {
    twsvi = 0; // get it from log
    curr = 0; // nothing in tx to apply
  }
  // at this point:
  //  (1) twsvi==0 means that we should get it from log, and
  //      twsvi!=0 if already got it from transaction's write,
  //  (2) curr==0 if we do not need to apply any updates from tx
  //      curr!=0 if we need to apply updates starting at curr
  if (!twsvi){ // get it from log
    // read log to retrieve last known value for coid
    res = S->cLogInMemory.readCOid(coid, readTs, log_tucoid, 0, 0);
    if (res) return res;
    
    if (log_tucoid->Writevalue){ // currently, we only support supervalues here
      return GAIAERR_WRONG_TYPE;
    }
    assert(log_tucoid->WriteSV);
    twsvi = log_tucoid->WriteSV;
    if (!tx_trcoid.isset() || curr == tx_trcoid->getLast()){ // no further items
      ret_tucoid = log_tucoid;
      return 0;
    }
  }
  assert(twsvi);
  if (!curr || curr == tx_trcoid->getLast()){ // no items in tx to apply
    ret_tucoid = new TxUpdateCoid(new TxWriteSVItem(*twsvi));
    return 0;
  }
  // clone tucoid so we can apply updates of transaction
  twsvi = new TxWriteSVItem(*twsvi);
  Ptr<TxUpdateCoid> curr_tucoid = new TxUpdateCoid(twsvi);
  
  // now, we have a twsvi to start with, and the rest of items to
  // apply to it
  while (curr != tx_trcoid->getLast()){
    int captured;
    next = tx_trcoid->getNext(curr);
    captured = curr->applyItemToTucoid(curr_tucoid, false);
    assert(!captured);
    curr = next;
  }
  ret_tucoid = curr_tucoid;
  return 0;
}

Marshallable *listaddRpc(ListAddRPCData *d, void *&state){
  Ptr<PendingTxInfo> pti;
  ListAddRPCRespData *resp;
  Ptr<TxRawCoid> *trcoidptr;
  Ptr<TxRawCoid> trcoid = 0;
  Ptr<TxUpdateCoid> tucoidcurr = 0;
  TxWriteSVItem *twsvitmp;
  TxListAddItem *tlai = 0;  
  COid coid;
  int res;
  int status=0;
  int flags;
#if DTREE_SPLIT_LOCATION == 1  // provide info about node size in response
  int ncells = -1;
  int size = -1;
#endif
  
  assert(S); // if this assert fails, forgot to call initStorageServer()
  dshowchar('+');
#ifndef SHORT_OP_LOG
  dprintf(1, "LISTADD  tid %016llx:%016llx coid %016llx:%016llx nkey %lld "
          "flags %d lev %d",
	  (long long)d->data->tid.d1, (long long)d->data->tid.d2,
          (long long)d->data->cid, (long long)d->data->oid,
          (long long)d->data->cell.nKey, d->data->flags,
          d->data->level);
#else
  dshortprintf(1, "LISTADD  %016llx:%016llx nkey %lld flags %d lev %d",
               (long long)d->data->cid, (long long)d->data->oid,
               (long long)d->data->cell.nKey, d->data->flags,
               d->data->level);
#endif

  coid.cid = d->data->cid;
  coid.oid = d->data->oid;
  flags = d->data->flags;

#if (DTREE_SPLIT_LOCATION != 1) && !defined(LOCALSTORAGE)
  if (!state && !(flags&2)){
    // being called the first time and do not bypass throttling
    void *serversplitterstate = tgetSharedSpace(THREADCONTEXT_SPACE_SPLITTER);
    int delay = ExtractThrottleFromServerSplitterState(serversplitterstate)->
                getCurrentDelay();
    if (delay){
      //printf("Throttling for delay %d\n", delay);
      state = (void*) (long long) delay;
      return 0;
    }
  }
#endif  

  S->cPendingTx.getInfo(d->data->tid, pti);

  // when flags&1: check that we are at leaf node and that cell is in scope for
  //     this node. In that case, add cell to node if it isn't there already
  // Returns:
  //   0 if cell was added or it existed already
  //   GAIAERR_CELL_OUTRANGE if this is the wrong place to add the cell
  //                         (not leaf or outside scope)
  //   another value: if there was an error
  
  if (flags&1){ // check boundaries and existence of item
    trcoidptr = 0;
    pti->coidinfo.lookup(coid, trcoidptr); // check if tx wrote coid
    res = getCurrTucoid(coid, trcoidptr ? *trcoidptr : 0, d->data->ts,
                        tucoidcurr);
    if (res){ status = res; goto end; }
    if (tucoidcurr->Writevalue) res = GAIAERR_WRONG_TYPE; // wrong type
    if (res){ status = res; goto end; }
    twsvitmp = tucoidcurr->WriteSV; assert(twsvitmp);
    if (!(twsvitmp->attrs[DTREENODE_ATTRIB_FLAGS] & DTREENODE_FLAG_LEAF)){
      // not leaf
      status = GAIAERR_CELL_OUTRANGE;
      goto end;
    }
      
    if (d->data->prki.isset()){
      // provide prki to TxWriteSVItem if it doesn't have it already      
      twsvitmp->setPrkiSticky(d->data->prki);
    }
    ListCellPlus *c1, *c2;
    ListCellPlus c(d->data->cell, &twsvitmp->prki);
    c1 = twsvitmp->cells.keyInInterval(&c, &c, 7); // search (-inf,cell]
    if (!c1 && twsvitmp->attrs[DTREENODE_ATTRIB_LEFTPTR]){
      // nothing to the left and not leftmost node
      status = GAIAERR_CELL_OUTRANGE; goto end;
    }
    
    c2 = twsvitmp->cells.keyInInterval(&c, &c, 5); // search [cell, inf)
    if (!c2 && twsvitmp->attrs[DTREENODE_ATTRIB_RIGHTPTR]){
      // nothing to the right and not rightmost node
      status = GAIAERR_CELL_OUTRANGE;
      goto end;
    }

    if (c2 && ListCellPlus::cmp(*c2,c) == 0){ // found item
      status = 0;
      goto end;
    }
  }
  
  res = pti->coidinfo.lookupInsert(coid, trcoidptr);
  if (res){ *trcoidptr = new TxRawCoid; }
  trcoid = *trcoidptr;

  tlai = new TxListAddItem(coid, d->data->prki,
                           d->data->cell, d->data->level);
  trcoid->add(tlai);
  
#if DTREE_SPLIT_LOCATION == 1 // provide info about size of node in response
  if (!tucoidcurr.isset()){
    res = getCurrTucoid(coid, trcoid, d->data->ts, tucoidcurr);
    if (res == GAIAERR_PENDING_DATA) goto end; // return no error but no info
    if (res){ status = res; goto end; }
    assert(tucoidcurr.isset());
    ncells = 0; // the add we just did is included in tucoidcurr
    size = 0;
  } else {
    ncells = 1; // the add we just did is not included in tucoidcurr
    size = CellSize(&tlai->item);
  }
    
  ncells += tucoidcurr->WriteSV->cells.getNitems();
  size += ListCellsSize(tucoidcurr->WriteSV->cells);
#endif

 end:
  //pti->unlock();
  resp = new ListAddRPCRespData;
  resp->data = new ListAddRPCResp;
  resp->data->status = status;
#if DTREE_SPLIT_LOCATION == 1   // provide info about size of node in response
  resp->data->ncells = ncells;
  resp->data->size = size;
#endif
  
  updateRPCResp(resp->data); // updated piggybacked fields for client caching
  
  resp->freedata = true;

  return resp;
}

Marshallable *listdelrangeRpc(ListDelRangeRPCData *d){
  Ptr<PendingTxInfo> pti;
  ListDelRangeRPCRespData *resp;
  Ptr<TxRawCoid> *trcoidptr;
  Ptr<TxRawCoid> trcoid;
  COid coid;
  int res;
  int status=0;

  assert(S); // if this assert fails, forgot to call initStorageServer()
  dshowchar('-');
#ifndef SHORT_OP_LOG
  dprintf(1, "LISTDELR tid %016llx:%016llx coid %016llx:%016llx "
          "nkey1 %lld nkey2 %lld type %d lev %d",
          (long long)d->data->tid.d1, (long long)d->data->tid.d2,
          (long long)d->data->cid, (long long)d->data->oid,
          (long long)d->data->cell1.nKey, (long long)d->data->cell2.nKey,
          (int)d->data->intervalType, d->data->level);
#else
  dshortprintf(1, "LISTDELR %016llx:%016llx nkey1 %lld nkey2 %lld type %d "
        "lev %d",
        (long long)d->data->cid, (long long)d->data->oid,
        (long long)d->data->cell1.nKey, (long long)d->data->cell2.nKey,
        (int)d->data->intervalType, d->data->level);
#endif

  coid.cid = d->data->cid;
  coid.oid = d->data->oid;

  S->cPendingTx.getInfo(d->data->tid, pti);

  res = pti->coidinfo.lookupInsert(coid, trcoidptr);
  if (res){ *trcoidptr = new TxRawCoid; }
  trcoid = *trcoidptr;

  TxListDelRangeItem *tldri = new TxListDelRangeItem(coid, d->data->prki,
       d->data->intervalType, d->data->cell1, d->data->cell2, d->data->level);
  trcoid->add(tldri);

  //pti->unlock();
  resp = new ListDelRangeRPCRespData;
  resp->data = new ListDelRangeRPCResp;
  resp->data->status = status;
  updateRPCResp(resp->data); // updated piggybacked fields for client caching
  
  resp->freedata = true;

  return resp;
}

Marshallable *attrsetRpc(AttrSetRPCData *d){
  Ptr<PendingTxInfo> pti;
  AttrSetRPCRespData *resp;
  Ptr<TxRawCoid> *trcoidptr;
  Ptr<TxRawCoid> trcoid;
  COid coid;
  int res;
  int status=0;

  assert(S); // if this assert fails, forgot to call initStorageServer()
  dshowchar('a');
#ifndef SHORT_OP_LOG
  dprintf(1, "ATTRSET  tid %016llx:%016llx coid %016llx:%016llx attrid %x "
          "attrvalue %llx lev %d",
          (long long)d->data->tid.d1, (long long)d->data->tid.d2,
          (long long)d->data->cid, (long long)d->data->oid, d->data->attrid,
          (long long)d->data->attrvalue, d->data->level);
#else
  dshortprintf(1, "ATTRSET  %016llx:%016llx attrid %x attrvalue %llx lev %d",
              (long long)d->data->cid, (long long)d->data->oid, d->data->attrid,
              (long long)d->data->attrvalue, d->data->level);
#endif

  coid.cid = d->data->cid;
  coid.oid = d->data->oid;

  S->cPendingTx.getInfo(d->data->tid, pti);

  res = pti->coidinfo.lookupInsert(coid, trcoidptr);
  if (res){ *trcoidptr = new TxRawCoid; }
  trcoid = *trcoidptr;
  assert(d->data->attrid < GAIA_MAX_ATTRS);
  TxSetAttrItem *tsai = new TxSetAttrItem(coid, d->data->attrid,
                                          d->data->attrvalue, d->data->level);
  trcoid->add(tsai);
  //pti->unlock();
  resp = new AttrSetRPCRespData;
  resp->data = new AttrSetRPCResp;
  resp->data->status = status;
  resp->freedata = true;

  return resp;
}

int doCommitWork(CommitRPCParm *parm, Ptr<PendingTxInfo> pti,
                 Timestamp &waitingts); // forward definition

struct PREPARERPCState {
  Timestamp proposecommitts;
  int status;
  int vote;
  Ptr<PendingTxInfo> pti;
  PREPARERPCState(Timestamp &pts, int s, int v, Ptr<PendingTxInfo> pt) :
    proposecommitts(pts), status(s), vote(v) { pti = pt; }
};

// PREPARERPC is called with state=0 for the first time.
// If it returns 0, it has issued a request to log to disk, and it wants to be
//   called again with state set to the value it had when it returned.
// If it returns non-null, it is done.
// rpctasknotify is a parameter that is passed on to the disk logging function
//  so that it knows what to notify when the logging is finished. In the local
//  version this parameter is 0.

Marshallable *prepareRpc(PrepareRPCData *d, void *&state, void *rpctasknotify){
  PrepareRPCRespData *resp;
  Ptr<PendingTxInfo> pti;
  Ptr<TxUpdateCoid> tucoid;
  LogOneObjectInMemory *looim;
  int vote;
  Timestamp startts, proposecommitts;
  Timestamp dummywaitingts;
  SkipListNode<COid,Ptr<TxRawCoid> > *ptr;
  SimpleLinkList<LogOneObjectInMemory*> looim_list;
  SimpleLinkListItem<LogOneObjectInMemory*> *looim_list_it;
  int status=0;
  int res;
  int waitforlog;
  PREPARERPCState *pstate = (PREPARERPCState*) state;
  int immediatetransition=0;

  assert(S); // if this assert fails, forgot to call initStorageServer()
  dshowchar('p');
  //committs = d->data->committs;
  proposecommitts = d->data->startts; // begin with the startts,
                                      // the minimum possible proposed commit ts
  startts = d->data->startts;

  if (pstate == 0){
#if defined(GAIA_OCC) || defined(GAIA_WRITE_ON_PREPARE)
    // GAIA_WRITE_ON_PREPARE optimization might send a commit without any prior
    // update to the server (due to the optimization), in which case
    // tid won't be known to server, so getInfoNoCreate below would fail.
    // That is why we call getInfo in this case
    S->cPendingTx.getInfo(d->data->tid, pti);
    res = 0;
#else
    res = S->cPendingTx.getInfoNoCreate(d->data->tid, pti);
#endif
    if (res){ // tid not found
      printf("Yesquel critical error: server got unknown tid on prepare\n");
      fflush(stdout);
      abort();
      vote=1; goto end;
    } 
    if (pti->status == PTISTATUS_VOTEDYES){ vote=0; goto end; }
    if (pti->status == PTISTATUS_VOTEDNO){ vote=1; goto end; }
    if (pti->status == PTISTATUS_CLEAREDABORT){
      printf("Yesquel critical error: tx status os cleared abort on prepare\n");
      fflush(stdout);
      abort();
      vote=1;
      goto end;
    }

#ifdef GAIA_WRITE_ON_PREPARE
    if (d->data->piggy_len >= 0){
      Ptr<TxRawCoid> *trcoidptr;
      COid coid;

      coid.cid = d->data->piggy_cid;
      coid.oid = d->data->piggy_oid;

      if (IsCoidCachable(coid))
        pti->updatesCachable = true; // mark tx as updating cachable data

      res = pti->coidinfo.lookupInsert(coid, trcoidptr);
      if (res){ 
        *trcoidptr = new TxRawCoid;
        Ptr<TxRawCoid> trcoid = *trcoidptr;
        
        TxWriteItem *twi = new TxWriteItem(coid, 0);

        // create and add new item to write set
        twi->len = d->data->piggy_len;
        // make private copy of data        
        char *buf = (char*) malloc(d->data->piggy_len);
        memcpy(buf, d->data->piggy_buf, d->data->piggy_len);
        twi->buf = buf;
        twi->alloctype=1; // if set, later free twi->buf but not twi->rpcrequest
        twi->rpcrequest = 0;
        trcoid->add(twi);
      } else {
        // do nothing; ignore piggyback value since value was overwritten
      }
    }
#endif

    if (pti->updatesCachable){
      // if tx updates some cachable data then ensure proposecommitts is after
      // advanceTs
      Timestamp advts = S->cCCacheServerState.getAdvanceTs();
      advts.addEpsilon();
      if (Timestamp::cmp(proposecommitts, advts) < 0) proposecommitts = advts;
    }

    vote = 0;  // vote yes, unless we decide otherwise below

#ifdef GAIA_OCC
    // check readset to see if it has changed
    // This OCC implementation is simplistic, because it does not do two things
    // that should be done:
    //   1. One needs to lock the entire readset and writeset before checking,
    //      rather than locking, checking, and unlocking one at a time. And to
    //      avoid deadlocks, one needs to lock in coid order.
    //   2. One needs to keep the readset in the pendingupdates until commit
    //     time
    // Without these two things, the implementation will be faster but
    // incorrect. This is ok for our purposes since we want to get an upper
    // bound on performance.
    // Another thing which can be improved: the client currently sends the
    // entire readset to all participants, but it suffices to send only those
    // objects that each participant is responsible for. In the loop below, we
    // check all other objects (those that the participant is not responsible
    // for will not be found), which could be optimized if the client had not
    // sent everything.
    int pos;
    COid coid;
    //myprintf("Checking readset of len %d\n", d->data->readset_len);

    for (pos = 0; pos < d->data->readset_len; ++pos){
      coid = d->data->readset[pos];
      res = S->cLogInMemory.coidInLog(coid);
      if (res){ // found coid, this is one that we are responsible for
        looim = S->cLogInMemory.getAndLock(coid, true, false); assert(looim);
        // check for pending updates
        if (!looim->pendingentries.empty()){ 
          vote = 1; 
        }
        else {
          // check for updates in logentries
          // it suffices to look at last entry in log
          SingleLogEntryInMemory *sleim = looim->logentries.rGetFirst();
          if (sleim != looim->logentries.rGetLast()){
            if (Timestamp::cmp(sleim->ts, startts) >= 0){
              vote = 1; // something changed
            }
          }
        }
        looim->unlock();
      }
      if (vote){
       // already found conflict, no need to keep checking for more
        goto done_checking_votes;
      }
    }
#endif

    // for each oid in transaction's write set, in order
    for (ptr = pti->coidinfo.getFirst(); ptr != pti->coidinfo.getLast();
         ptr = pti->coidinfo.getNext(ptr)){
      // get the looims for a given coid and acquire write lock on that object
      // (note we are getting locks in oid order in this loop, which avoids
      //  deadlocks)
      looim = S->cLogInMemory.getAndLock(ptr->key, true, false);
      //looim->printdetail(ptr->key, false);
      looim_list.pushTail(looim); // looims that we locked

      // check last-read timestamp
      if (Timestamp::cmp(proposecommitts, looim->LastRead) < 0) 
        proposecommitts = looim->LastRead; // track largest read timestamp seen

      // check for conflicts with other transactions in log
      LinkList<SingleLogEntryInMemory> *entries = &looim->logentries; 
      if (!entries->empty()){
        SingleLogEntryInMemory *sleim;
        // for each update in the looim's log
        for (sleim = entries->rGetFirst(); sleim != entries->rGetLast();
             sleim = entries->rGetNext(sleim)){
          if (sleim->flags & SLEIM_FLAG_SNAPSHOT){
            // ignore this entry as it was artificially inserted for efficiency
            continue;
          }
          if (Timestamp::cmp(sleim->ts, startts) <= 0) break; // all done
          // startts < sleim->ts so we must check for conflicts with this entry
          tucoid = ptr->value->getTucoid(ptr->key);
          if (sleim->tucoid->hasConflicts(tucoid, sleim)){
            // conflict, must abort
            vote = 1;
            break;
          }
        }
      }

      // now check for conflicts with pending transactions
      entries = &looim->pendingentries; 
      if (!entries->empty()){
        SingleLogEntryInMemory *sleim;
        // for each update in the looim's log
        for (sleim = entries->getFirst(); sleim != entries->getLast();
             sleim = entries->getNext(sleim)){
          tucoid = ptr->value->getTucoid(ptr->key);
          if (sleim->tucoid->hasConflicts(tucoid, sleim)){
            // conflict, must abort
            vote = 1;
            break;
          }
        }
      }
    }

#ifdef GAIA_OCC
    done_checking_votes:
#endif

    if (vote){ // if aborting, then release locks immediately
      pti->status = PTISTATUS_VOTEDNO;
      for (looim_list_it = looim_list.getFirst();
           looim_list_it != looim_list.getLast();
           looim_list_it = looim_list.getNext(looim_list_it)){
        looim = looim_list_it->item;
        looim->unlock();
      }
      waitforlog = 0;
    }
    else { // vote is to commit
      pti->status = PTISTATUS_VOTEDYES;
      // (4) add entry to in-memory pendingentries
      ptr = pti->coidinfo.getFirst();
      looim_list_it = looim_list.getFirst();
      looim = looim_list_it->item;
      // iterate over coidinfo and looim_list in sync.
      // Note that items were added to looim_list in COid order
      while (ptr != pti->coidinfo.getLast()){
        tucoid = ptr->value->getTucoid(ptr->key);
        SingleLogEntryInMemory *sleim =
          S->cLogInMemory.auxAddSleimToPendingentries(looim, proposecommitts,
                                              true, tucoid);
        // if ptr->value->getTucoid()->pendingentriesSleim is set, then a tx is
        // adding multiple sleims for one object. This should not be the case
        // since all updates to an object are accumulated in one tucoid, which
        // is then added as a single entry. If the tx adds many sleims for an
        // object, the logic in LogInMemory::removeOrMovePendingToLogentries
        // must be revised to move all of those entries (currently, it only
        // moves one)
        assert(!tucoid->pendingentriesSleim);
        
        // remember sleim so that we can quickly find it at commit time
        tucoid->pendingentriesSleim = sleim;
        looim->unlock(); // ok to release lock even before we log, since
                         // the transaction is still marked as pending and
                         // we have not yet returned the vote (and the
                         // transaction will not commit before we return the
                         // vote, meaning that others will not be able to read
                         // it before we return the vote)
        ptr = pti->coidinfo.getNext(ptr);
        looim_list_it = looim_list.getNext(looim_list_it);
        looim = looim_list_it->item;
      }
      assert(looim_list_it == looim_list.getLast());

      // log the writes and vote
      // Note that we are writing the proposecommitts not the real committs,
      // which is determined only later (as the max of all the proposecommitts)
      waitforlog = S->cDiskLog.logUpdatesAndYesVote(d->data->tid,
                   proposecommitts, pti, rpctasknotify);
      if (!rpctasknotify) assert(waitforlog == 0);
    }

    if (waitforlog){
      assert(rpctasknotify);
      // save variables and restore them later below after we return with
      // state != 0
      assert(pstate == 0);
      pstate = new PREPARERPCState(proposecommitts, status, vote, pti);
      state = (void*) pstate;
      return 0;
    }
    else {
      immediatetransition = 1;
    }
  }

  if (immediatetransition || pstate){ // we transitioned to the new state
    if (pstate){ // restore local variables
      proposecommitts = pstate->proposecommitts;
      status = pstate->status;
      vote = pstate->vote;
      pti = pstate->pti;
      delete pstate;
      state = 0;
    }

    // note: variables used below should be restored from pstate above
    if (d->data->onephasecommit){
      CommitRPCParm parm;
      parm.commit = vote;
      parm.committs = proposecommitts;
      parm.committs.addEpsilon();
      parm.tid = d->data->tid;
      // we use dummywaitingts (and discard this value) since waitingts is
      // not useful in 1PC where there is no delay between prepare and commit
      status = doCommitWork(&parm, pti, dummywaitingts); 
    }
  }

  end:
  if (!vote && pti->updatesCachable){
    // indicate one more tx that (a) updates cachable data and
    // (b) has successfully prepared (vote yes) but not committed yet
    S->cCCacheServerState.incPreparing();
  }

  // note: variables used below should be restored from pstate above
  resp = new PrepareRPCRespData;
  resp->data = new PrepareRPCResp;
  resp->data->vote = vote;
  resp->data->status = status;
  resp->data->mincommitts = proposecommitts;
  resp->freedata = true;
  updateRPCResp(resp->data); // updated piggybacked fields for client caching

  //pti->unlock();
  

#ifndef SHORT_OP_LOG
  dprintf(1, "PREPARE  tid %016llx:%016llx startts %016llx:%016llx "
          "mincommitts %016llx:%016llx piggylen %d vote %s onephase %d",
          (long long)d->data->tid.d1,
          (long long)d->data->tid.d2, (long long)d->data->startts.getd1(),
          (long long)d->data->startts.getd2(),
          (long long)proposecommitts.getd1(),
          (long long)proposecommitts.getd2(), d->data->piggy_len,
          vote ? "no" : "yes", d->data->onephasecommit);
#else
  dshortprintf(1, "PREPARE  %016llx:%016llx piggylen %d vote %d onephase %d",
               (long long)d->data->tid.d1, (long long)d->data->tid.d2,
               d->data->piggy_len, vote, d->data->onephasecommit);
#endif

  return resp;
}

// checks whether a tucoid might cause a node to grow
// Assumes tucoid comes from a pti whose lock is held (as is the case in
// doCommitWork)
int checkTucoidForGrowth(Ptr<TxUpdateCoid> tucoid){
  if (tucoid->WriteSV){ return 1; } // if tx is writing value, then yes
  // now check each update
  
  for (TxListItem *tli = tucoid->Litems.getFirst();
       tli != tucoid->Litems.getLast();
       tli = tucoid->Litems.getNext(tli)){
    if (tli->type == 0){ return 1;} // listadd item
    if (tli->type == 1){ return 1;} // listdelrange item
  }
  return 0;
}

// does the actual work in COMMITRPC
// Assumes lock is held in pti.
int doCommitWork(CommitRPCParm *parm, Ptr<PendingTxInfo> pti,
                 Timestamp &waitingts){
  SkipListNode<COid,Ptr<TxRawCoid> > *ptr;
  Ptr<TxUpdateCoid> tucoid;
  SingleLogEntryInMemory *pendingsleim;
#if (DTREE_SPLIT_LOCATION != 1) && !defined(LOCALSTORAGE)
  Set<COid> toSplit;
#endif
  int status=0;
  waitingts.setIllegal();

  if (pti->updatesCachable){
    // decrement # of txs that change cachable data and are preparing but not
    // committed, and bump version number if committing
    S->cCCacheServerState.donePreparing(parm->commit == 0, parm->committs);
    // it is ok to do this here, before applying the changes to the cachable
    // object(s), since those changes are still pending and therefore reads are
    // not allowed on those object(s) (such reads will be postponed). This makes
    // it impossible for clients to read the old versions of those objects with
    // the new cache version number just bumped.
  }
  
  if (parm->commit == 0){ // commit
    for (ptr = pti->coidinfo.getFirst(); ptr != pti->coidinfo.getLast();
         ptr = pti->coidinfo.getNext(ptr)){
      tucoid = ptr->value->getTucoid(ptr->key);
      pendingsleim = tucoid->pendingentriesSleim;
      if (pendingsleim){
        if (Timestamp::cmp(waitingts, pendingsleim->waitingts) < 0)
          waitingts = pendingsleim->waitingts;
        S->cLogInMemory.removeOrMovePendingToLogentries(ptr->key, pendingsleim,
                                                        parm->committs, true);
        tucoid->pendingentriesSleim = 0;
#if (DTREE_SPLIT_LOCATION != 1) && !defined(LOCALSTORAGE)
        // check if coid has listadd, listdelrange, or fullwrite operations
        if (checkTucoidForGrowth(tucoid)){
          int res;
          Ptr<TxUpdateCoid> tucoid;
          // check if the coid has become too large
          res = S->cLogInMemory.readCOid(ptr->key, parm->committs, tucoid,0,0);
          if (res!=0){
            printf("Warning: readCOid returned %d when checking for need "
                   "to split\n", res);
            // TODO: handle the case when read is pending. Right now, it just
            //       returns res == -3 (because deferredhandle==0 in the call to
            //       readCOid), but the right thing to do is to pass a
            //       deferredhandle that later checks if the coid is too large
            //       and, if so, splits.
          } else {
            TxWriteSVItem *twsvi = tucoid->WriteSV;          
            if (twsvi){
              int ncells = twsvi->cells.getNitems();
              // split if too many cells
              if (ncells > DTREE_SPLIT_SIZE) toSplit.insert(ptr->key);
              else {
                int sizecells = ListCellsSize(twsvi->cells);
                // split if cell size is too large, but not if too few cells
                if (sizecells > DTREE_SPLIT_SIZE_BYTES && ncells >= 2)
                  toSplit.insert(ptr->key);
              } // else
            } // if
          } // else
        } // if checkTucoidForGrowth
#endif
      } // if pendingsleim
    } // for
  
    S->cDiskLog.logCommitAsync(parm->tid, parm->committs);

#if (DTREE_SPLIT_LOCATION != 1) && !defined(LOCALSTORAGE)
    // now issue splits
    SetNode<COid> *coidnode;
    for (coidnode = toSplit.getFirst(); coidnode != toSplit.getLast();
         coidnode = toSplit.getNext(coidnode)){
      SplitNode(coidnode->key, 0);
    }
#endif    
  } else {
    // note: abort due to application (parm->commit == 2) does not
    // require removing writes from cLogInMemory or logging an abort,
    // since the prepare phase was never done
    if (parm->commit == 1 || parm->commit == 3){
      for (ptr = pti->coidinfo.getFirst(); ptr != pti->coidinfo.getLast();
           ptr = pti->coidinfo.getNext(ptr)){
        tucoid = ptr->value->getTucoid(ptr->key);
        pendingsleim = tucoid->pendingentriesSleim;
        if (pendingsleim){
          if (Timestamp::cmp(waitingts, pendingsleim->waitingts) < 0)
            waitingts = pendingsleim->waitingts;
          S->cLogInMemory.removeOrMovePendingToLogentries(ptr->key,
                               pendingsleim, parm->committs, false);
        tucoid->pendingentriesSleim = 0;
        }
      }
      S->cDiskLog.logAbortAsync(parm->tid, parm->committs);
    }
    pti->clear();   // delete update items in transaction
    // question: will this free the tucoids within the pti? I believe so,
    // because upon deleting the skiplist nodes, the nodes' destructor will
    // call the destructor of Ptr<TxUpdateCoid>. This needs to be checked,
    // otherwise a memory leak will occur.
    // Also check that removing the pti later (when tx commits) will not
    // deallocate the tucoid since there is a sleim that smart-points to the
    // tucoid in the object's logentries.
  }

  // remove information about tid, freeing up memory
  S->cPendingTx.removeInfo(parm->tid);
  return status;
}

Marshallable *commitRpc(CommitRPCData *d){
  CommitRPCRespData *resp;
  Ptr<PendingTxInfo> pti;

  int res=0;

  assert(S); // if this assert fails, forgot to call initStorageServer()
  dshowchar('c');
#ifndef SHORT_OP_LOG
  dprintf(1, "COMMIT   tid %016llx:%016llx ts %016llx:%016llx commit %d",
          (long long)d->data->tid.d1, (long long)d->data->tid.d2,
          (long long)d->data->committs.getd1(),
          (long long)d->data->committs.getd2(),
          d->data->commit);
#else
  dshortprintf(1, "COMMIT   %016llx:%016llx commit %d",
               (long long)d->data->tid.d1, (long long)d->data->tid.d2,
    d->data->commit);
#endif

  resp = new CommitRPCRespData;
  resp->data = new CommitRPCResp;

  res = S->cPendingTx.getInfoNoCreate(d->data->tid, pti);
  if (res) 
    ; // did not find tid. Nothing to do. This is likely because of
      // the GAIA_WRITE_ON_PREPARE optimization
  else {
    if (pti->status == PTISTATUS_CLEAREDABORT){
      printf("Yesquel critical error: tx status is cleared abort on commit\n");
      fflush(stdout);
      abort();
    }
    else res = doCommitWork(d->data, pti, resp->data->waitingts);
    //pti->unlock();
  }

  resp->data->status = res;
  resp->freedata = true;

  return resp;
}

Marshallable *subtransRpc(SubtransRPCData *d){
  SubtransRPCRespData *resp;
  Ptr<PendingTxInfo> pti;

  int res=0;

  assert(S); // if this assert fails, forgot to call initStorageServer()
  dshowchar('s');
#ifndef SHORT_OP_LOG
  dprintf(1, "SUBTRA   tid %016llx:%016llx lev %d action %d",
          (long long)d->data->tid.d1, (long long)d->data->tid.d2,
          d->data->level, d->data->action);
#else
  dshortprintf(1, "SUBTRA   %016llx:%016llx lev %d action %d",
               (long long)d->data->tid.d1, (long long)d->data->tid.d2,
               d->data->level, d->data->action);
#endif

  resp = new SubtransRPCRespData;
  resp->data = new SubtransRPCResp;

  res = S->cPendingTx.getInfoNoCreate(d->data->tid, pti);
  if (res) 
    ; // did not find tid. Nothing to do. This is likely because of
      // the GAIA_WRITE_ON_PREPARE optimization
  else {
    if (pti->status == PTISTATUS_CLEAREDABORT){
      printf("Yesquel critical error: tx status is cleared abort "
             "on subtrans\n");
      fflush(stdout);
      abort();
    }
    else {
      SkipListNode<COid,Ptr<TxRawCoid> > *ptr, *next;
      Ptr<TxRawCoid> trcoid;
      int empty;
      // iterate over all objects updated by transaction
      for (ptr = pti->coidinfo.getFirst(); ptr != pti->coidinfo.getLast();
           ptr = next){
        next = pti->coidinfo.getNext(ptr);
        trcoid = ptr->value;
        switch(d->data->action){
        case 0: // abort
          empty = trcoid->abortLevel(d->data->level);
          if (empty){
            Ptr<TxRawCoid> tmptrcoid;
            pti->coidinfo.lookupRemove(ptr->key, 0, tmptrcoid);
          }
          break;
        case 1: // release
          trcoid->releaseLevel(d->data->level);
          break;
        default: // error
          assert(0);
          break;
        }
      }
    }
    //pti->unlock();
  }

  resp->data->status = 0;
  resp->freedata = true;

  return resp;
}

Marshallable *shutdownRpc(ShutdownRPCData *d){
  int status = 0;
  ShutdownRPCRespData *resp;

  // schedule exit to occur 2 seconds from now, to give time to answer request
  assert(S); // if this assert fails, forgot to call initStorageServer()
  dshowchar('s');

  if (d->data->level == 0){
#if defined(STORAGESERVER_SPLITTER) && !defined(LOCALSTORAGE)
    int ExtractQueueFromServerSplitterState(void *sss);
    void *sss = tgetSharedSpace(THREADCONTEXT_SPACE_SPLITTER);
    int missing;
    if ((missing = ExtractQueueFromServerSplitterState(sss)) != 0){
      printf("Cannot stop splitter due to split queue of size %d\n", missing);
      status = 1;
    }
    else if (SC){
      //SC->disconnectClients();
      delete SC;
      SC = 0;
    }
#endif
  } else if (d->data->level == 1){
#if defined(STORAGESERVER_SPLITTER) && !defined(LOCALSTORAGE)
    printf("Shutting down server in 2 seconds...\n");
    scheduleExit(); // schedule calling exit after 2 seconds
#endif
  }
  resp = new ShutdownRPCRespData;
  resp->data = new ShutdownRPCResp;
  resp->freedata = true;
  resp->data->status = status;
  resp->data->reserved = 0;
  return resp;
}

Marshallable *startsplitterRpc(StartSplitterRPCData *d){
  StartSplitterRPCRespData *resp;

  assert(S); // if this assert fails, forgot to call initStorageServer()
  dshowchar('S');

#if defined(STORAGESERVER_SPLITTER) && !defined(LOCALSTORAGE)
  int res;
  extern int startSplitter(void);
  extern char *Configfile;
  dprintf(1, "Starting splitter based on config %s", Configfile);
  res = startSplitter();
  if (res){ dprintf(1, "Splitter already running"); }
  else { dprintf(1, "Splitter started"); }

#endif

  resp = new StartSplitterRPCRespData;
  resp->data = new StartSplitterRPCResp;
  resp->freedata = true;
  resp->data->reserved = 0;
  return resp;
}

Marshallable *flushfileRpc(FlushFileRPCData *d){
  FlushFileRPCRespData *resp;
  char *filename;
  Timestamp ts;
  int res;
  assert(S); // if this assert fails, forgot to call initStorageServer()
  dshowchar('f');

  ts.setNew();
  mssleep(1000); // wait for a second

  if (!d->data->filename || strlen(d->data->filename)==0)
    filename = (char*)FLUSH_FILENAME; // no filename provided
  else filename = d->data->filename;
  dprintf(1, "Flushing to file %s", filename);
  res = S->cLogInMemory.flushToFile(ts, filename);
  dprintf(1, "Flushing done, result %d", res);

  resp = new FlushFileRPCRespData;
  resp->data = new FlushFileRPCResp;
  resp->freedata = true;
  resp->data->status = res;
  resp->data->reserved = 0;
  return resp;
}

Marshallable *loadfileRpc(LoadFileRPCData *d){
  LoadFileRPCRespData *resp;
  char *filename;
  int res;
  assert(S); // if this assert fails, forgot to call initStorageServer()
  dshowchar('l');

  if (!d->data->filename || strlen(d->data->filename)==0)
    filename = (char*)FLUSH_FILENAME; // no filename provided
  else filename = d->data->filename;
  dprintf(1, "Loading from file %s", filename);
  res = S->cLogInMemory.loadFromFile(filename);
  dprintf(1, "Loading done, result %d", res);

  resp = new LoadFileRPCRespData;
  resp->data = new LoadFileRPCResp;
  resp->freedata = true;
  resp->data->status = res;
  resp->data->reserved = 0;
  return resp;
}

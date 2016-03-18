//
// splitter-client.cpp
//
// Functions to be used by client-based splitter. Also includes
// functions for clients to obtain new rowid's.
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
#include <float.h>

#include <map>
#include <list>

#include "tmalloc.h"
#include "options.h"
#include "debug.h"
#include "splitter-client.h"
#include "gaiarpcaux.h"
#include "clientdir.h"
#include "splitter-server.h"
#include "splitterrpcaux.h"
#include "dtreeaux.h"

extern StorageConfig *SC;

i64 GetRowidLocal(Cid cid, i64 hint){
  static SkipList<U64,i64> KnownRowids;
  i64 *value, retval;
  U64 cidu64(cid);
  int res;

  if (hint){
    res = KnownRowids.lookupInsert(cidu64, value);
    if (!res) retval = ++(*value); // found
    else retval = *value = hint;   // not found, use hint and remember it
  } else {
    res = KnownRowids.lookup(cidu64, value);
    if (!res) retval = ++(*value); // found
    else retval = 0;
  }
  return retval;
}

i64 GetRowidFromServer(Cid cid, i64 hint){
#ifdef NOGAIA
  return GetRowidLocal(cid, hint);
#endif
  IPPortServerno dest;
  GetRowidRPCData *parm;
  GetRowidRPCRespData rpcresp;
  char *resp;
  i64 rowid;
  COid fakecoid;

  assert(SC);

  if (cid >> 48 & EPHEMDB_CID_BIT)
    return GetRowidLocal(cid, hint);  // local container

  fakecoid.cid = cid;
  fakecoid.oid = 0;    // use oid 0 to determine what server will
                       // handle getrowid requests for a given cid

  SC->Od->GetServerId(fakecoid, dest);
  
  parm = new GetRowidRPCData;
  parm->data = new GetRowidRPCParm;
  parm->data->cid = cid;
  parm->data->hint = hint;
  parm->freedata = true;
  resp = SC->Rpcc->syncRPC(dest.ipport, SS_GETROWID_RPCNO,
                           FLAG_HID((u32)cid), parm);

  if (!resp) return 0;
  rpcresp.demarshall(resp);
  rowid = rpcresp.data->rowid;

  free(resp);
  return rowid;
}

// computes delay and expiration due to a node that is too large
int SplitterThrottle::computeDelayFromQueue(SplitterStats &load,
                                            u64& expiration){
  int duration=0;
  int delay=0;
  int m;
  //m = load.splitQueueSize/10;
  //if (m >= 1){ // kick in when queue size is 10 or more
  //  // at size 10 add 1ms, at size 100 add 1s, increasing exponentially
  //  // between these
  //  if (m < 10) delay = 1<<m;
  //  else delay = 1024; // capped at 1024ms
  //  duration = (int) ceil(load.splitTimeAvg * load.splitQueueSize/2);
  //}
  //expiration = Time::now() + duration;
  //return delay;
  m = load.splitQueueSize/10;
  if (m >= 5){ // kick in when queue size is 50 or more
    m -= 4;
    // at size 50 add 1ms, at size 500 add 1s, increasing exponentially
    // between these
    if (m < 10) delay = 1<<m;
    else delay = 1024; // capped at 1024ms
    duration = (int) ceil(load.splitTimeAvg * load.splitQueueSize/2);
  }
  expiration = Time::now() + duration;
  return delay;
}

int SplitterThrottle::computeDelayFromTimeRetrying(SplitterStats &load,
                                                   u64& expiration){
  int m;
  int delay=0;
  int duration=0;
  //m = (int)(load.splitTimeRetryingMs/100);
  //if (m >= 1){ // kick in at 100ms
  //  m = m/100;  // at 100ms add 1ms, at 10000ms add 1s
  //  if (m < 10) delay = 1<<m;
  //  else delay = 1024;  // capped at 1024ms
  //  duration = delay*2;
  //  //if (duration < 1000) duration=1000;
  //}
  //expiration = Time::now() + duration;
  //return delay;
  m = (int)(load.splitTimeRetryingMs/100);
  if (m >= 5){ // kick in at 500ms
    m -= 4;
    m = m/100;  // at 500ms add 1ms, at 50000ms add 1s
    if (m < 10) delay = 1<<m;
    else delay = 1024;  // capped at 1024ms
    duration = delay*2;
    //if (duration < 1000) duration=1000;
  }
  expiration = Time::now() + duration;
  return delay;
}

// Currently, this is not enabled since no place in the code reports
// the node size.
// To report the node size, add this statement:
//     SD->GetThrottle(coid)->ReportNodeSize(node.Ncells(), node.CellsSize());
// The problem is that we should report a real node size at the storage server,
// not the node size within a transaction, as the latter can grow very
// quickly beyond the limit
int SplitterThrottle::computeDelayFromNodesize(unsigned nelements,
                                              unsigned nbytes, u64& expiration){
  int m;
  int delayelements=0;
  int delaybytes=0;
  int delay;
  int duration;
  m = nelements/DTREE_SPLIT_SIZE;
  if (m >= 2){ // kick in at twice the expected number of elements
    if (m < 12) delayelements = 1<<(m-2); // at 2x add 1ms, at 12x add 1s,
                                       // increasing exponentially between these
    else delayelements = 1024; // capped at 1024ms
  }
  m = nbytes/DTREE_SPLIT_SIZE_BYTES;
  if (m >= 2){ // kick in at twice the expected size
    if (m < 12) delaybytes = 1<<(m-2); // at 2x add 1ms, at 12x add 1s,
                                      // increasing exponentially between these
    else delaybytes = 1024; // capped at 1024ms
  }
  // pick max delay due to node size or node bytes
  if (delayelements > delaybytes) delay=delayelements;
  else delay = delaybytes;

  duration = delay*2; 
  //if (duration < 1000 && delay > 0) duration=1000;//...but at least one second
  expiration = Time::now() + duration;
  return delay;
}

SplitterThrottle::SplitterThrottle(){
  int i;
  for (i=0; i < SPLITTER_THROTTLE_NMETRICS; ++i){
    Delays[i] = 0;
    Expirations[i] = 0;
  }
}

void SplitterThrottle::ReportLoad(SplitterStats &newload){
  int delay0, delay1;
  u64 expiration0, expiration1;
  Load = newload;
  delay0 = computeDelayFromQueue(newload, expiration0);
  delay1 = computeDelayFromTimeRetrying(newload, expiration1);
  //lock.lock();
  Delays[0] = delay0;
  Delays[1] = delay1;
  Expirations[0] = expiration0;
  Expirations[1] = expiration1;
  //lock.unlock();
}

void SplitterThrottle::ReportNodeSize(unsigned nelements, unsigned nbytes){
  int delay;
  u64 expiration;
  Nelements = nelements;
  Nbytes = nbytes;
  delay = computeDelayFromNodesize(nelements, nbytes, expiration);
  //lock.lock();
  if (Expirations[2] < Time::now() || Delays[2] < delay){
    Expirations[2] = expiration;
    Delays[2] = delay;
  }
  //lock.unlock();
}

int SplitterThrottle::getCurrentDelay(void){
  int delay=0;
  u64 now;
  int i, iwin;
  now = Time::now();
  //lock.lockRead();
  iwin=0;
  for (i=0; i < SPLITTER_THROTTLE_NMETRICS; ++i){
    if (Expirations[i] > now){
      if (delay < Delays[i]){ delay = Delays[i]; iwin=i; }
    }
  }
  //lock.unlockRead();
  
  if (delay){ dprintf(1, "D%d,%d ", delay,iwin); }
  return delay;
}

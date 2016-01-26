//
// ccache-server.cpp
//
// Consistent caching, server side. This file keeps the state at the
// server required for consistent caching at the client
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

#include "inttypes.h"
#include "util.h"
#include "ccache.h"
#include "ccache-server.h"

CCacheServerState::CCacheServerState(){
  versionNo = Time::now();  // TODO: technically, this should be a monotonic
                            // counter: when we restart, we want
                            // a version number bigger than even before,
                            // otherwise the cache will not work as expected.
                            // We are approximating that using the system
                            // clock, which is ok but failure-prone
  advanceTs.setOld(-CACHE_RESERVE_TIME); // set timestamp in the future
  preparing = false;
}

u64 CCacheServerState::incVersionNo(const Timestamp &newts){
  ts = newts;
  return AtomicInc64(&versionNo);
}

// CAS preparing from 0 to 1. Returns 0 if successful,
// non-0 otherwise
void CCacheServerState::incPreparing(){ 
  AtomicInc32(&preparing);
}

// clears preparing status
void CCacheServerState::donePreparing(bool committed, const Timestamp &newts){
  AtomicDec32(&preparing);
  if (committed) incVersionNo(newts);
}


Tlocal u64 ccacheLastUpdate = 0;

Timestamp CCacheServerState::updateAdvanceTs(){
  u64 t1;
  t1 = Time::now();
  if (t1 - ccacheLastUpdate < CACHE_RESERVE_TIME/10){
    // avoid updating the global variable too often. Hopefully, this
    // will reduce processor cache traffic
    return advanceTs;
  }
  
  if (preparing) return advanceTs; // do not advance if preparing a tx
                                   // that modifies cachable data
  ccacheLastUpdate = t1;

  
  Timestamp retval;
  retval.setOld(-CACHE_RESERVE_TIME); // set timestamp in the future
  advanceTs = retval;
  MemBarrier(); // ensure advanceTs is visible to other threads
  return retval;
}

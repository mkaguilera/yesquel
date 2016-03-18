//
// ccache.cpp
//
// Consistent cache of key-value pairs for client library.
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
#include "ccache.h"

// Processes the vno and adv timestamp of a given server.
// If cached version is older, clear the cache and remember new version
//   and timestamp.
// If it is the same, update adv timestamp.
// Otherwise do nothing.
int ClientCache::report(int serverno, u64 vno, Timestamp &ts, Timestamp &advts){
  assert(0 <= serverno && serverno < Nservers);
  ClientCachePerServer *ccps = Caches + serverno;

  if (ccps->versionNo == vno){
    // it should be safe to update advanceTs without grabbing the lock
    // in the x86 architecture because reordering of those updates
    // will not affect correctness (it may set the timestamp to
    // lower values than it should be, but that is ok).
    ccps->advanceTs = advts; // update advanceTs
    return 0;
  }
  if (ccps->versionNo > vno) return -1;
  ccps->lock.lock();
  ccps->cachemap.clear(0,0);
  ccps->versionNo = vno;
  ccps->ts = ts;
  ccps->advanceTs = advts;
  ccps->lock.unlock();
  return 1;
}

// lookup coid in the cache for given server.
// If found, sets buf to cached value and returns 0.
// Otherwise, leaves buf untouched and returns non-0.
int ClientCache::lookup(int serverno, COid &coid, Ptr<Valbuf> &buf,
                        Timestamp &readTs){
  assert(0 <= serverno && serverno < Nservers);
  int res;
  Ptr<Valbuf> *pbuf;
  ClientCachePerServer *ccps = Caches + serverno;
  ccps->lock.lockRead();
  if (Timestamp::cmp(ccps->ts, readTs) < 0 &&
      Timestamp::cmp(readTs, ccps->advanceTs) <= 0){
    res = ccps->cachemap.lookup(coid, pbuf);
    if (!res) buf = *pbuf;
  } else res = -1;
  ccps->lock.unlockRead();
  return res;
}

// sets the cache of a given server if it is not set already.
// This function makes a copy of the buffer, so caller
// continues to own the passed buf.
// Returns 0 if the cache was set, non-zero if the cache already had the item
int ClientCache::set(int serverno, COid &coid, Ptr<Valbuf> buf){
  assert(0 <= serverno && serverno < Nservers);
  int res, retval;
  ClientCachePerServer *ccps = Caches + serverno;
  Ptr<Valbuf> *pbuf;

  assert(buf->immutable);
  ccps->lock.lock();
  res = ccps->cachemap.lookupInsert(coid, pbuf);
  if (res){
    *pbuf = buf;
    retval = 0;
  } else { // cache already has the item
    // coid is already in cache; must have same value
    Ptr<Valbuf> oldbuf = *pbuf;
    assert(oldbuf->type == buf->type);
    if (buf->type == 0){
      assert(oldbuf->len == buf->len);
      assert(memcmp(oldbuf->u.buf, buf->u.buf, oldbuf->len) == 0);
    }
    retval = -1;
  }
  ccps->lock.unlock();
  return retval;
}

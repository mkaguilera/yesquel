//
// ccache-server.h
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

#ifndef _CCACHE_SERVER_H
#define _CCACHE_SERVER_H


#include "inttypes.h"
#include "os.h"

class CCacheServerState {
private:
  Align4 u64 versionNo; // current version number of data at server
  Timestamp ts;         // current timestamp of data at server
  Timestamp advanceTs;  // advance timestamp
  Align4 u32 preparing; // number of transactions that (a) modify cachable
                        // and (b) have prepared but not committed
public:
  CCacheServerState();
  u64 getVersionNo(){ return versionNo; } // return version number
  const Timestamp &getTs(){ return ts; } // return timestamp
  const Timestamp &getAdvanceTs(){ return advanceTs; } // return adv timestamp
  u32 getPreparing(){ return preparing; }
  
  u64 incVersionNo(const Timestamp &newts); // increase version number by one
  void incPreparing(); // increments preparing. This is called in
                       // successful prepare phase (yes vote) of a transaction
                       // that changes cachable state.
  void donePreparing(bool committed, const Timestamp &newts); // decrements
        // preparing and, if committed is true, bump version. This is called
        // after the commit of a transaction that changes cachable state.
  Timestamp updateAdvanceTs(); // update advance timestamp
};

// this macro sets the piggybacked fields of an RPC response. It is
// intended to be used in the server RPC implementations. The piggybacked
// fields are the version number and advance timestamp. This macro
// also advances the advance timestamp
#if defined(LOCALSTORAGE) || !defined(GAIA_CLIENT_CONSISTENT_CACHE)
#define updateRPCResp(varp) \
   varp->versionNoForCache = 0;  \
   varp->reserveTsForCache.setIllegal()
#else
#define updateRPCResp(varp) \
   varp->versionNoForCache = S->cCCacheServerState.getVersionNo();  \
   varp->tsForCache = S->cCCacheServerState.getTs(); \
   varp->reserveTsForCache = S->cCCacheServerState.updateAdvanceTs()
#endif

#endif

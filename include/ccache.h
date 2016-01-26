//
// ccache.h
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

#ifndef _CCACHE_H
#define _CCACHE_H

//
// This cache is different from the GlobalCache in kvinterface.h because
// that cache can return stale results, while this cache does not.
//
// Each server keeps a single version number for all its cached data.
// Clients use this version number to determine whether to invalidate
// all the data for a given server or not. The current version number
// at the server is piggybacked on many RPC replies to the client.
//
// In addition, the server keeps a reserve timestamp for its cached
// data.  This is a promise that no updates will occur with a
// timestamp <= the reserve timestamp. The server continually advances
// the reserve timestamp to a fixed time CACHE_RESERVE_TIME into the
// future, unless there is an outstanding transaction that (a) updates
// one of the cached items, and (b) is between its prepare and commit
// phases. In this case, the server keeps the reserve timestamp as it
// is, since some client is trying to change the cached data. On the
// prepare phase of a transaction that changes a cached item, the
// server votes with a timestamp at least as great as the reserve
// timestamp.  This ensures that the client coordinator does not pick
// a commit timestamp smaller than the reserve timestamp, thereby
// ensuring that the cached item does not receive a smaller timestamp.
//
// The server piggybacks the reserve timestamp on many RPC replies to the
// clients (together with the version number of the cached data).
//
// Clients store cached data for each server, together with the
// version number and the reserve timestamp. If the client wishes to
// read cached data with a timestamp smaller than the reserve
// timestamp, the client can consistently read from its cache if the
// data is there. This is because the server guarantees that the data
// does not change with a smaller timestamp.
//
// When the client receives the piggybacked reserve timestamp and
// version number, it does one of two things. If the version number
// matches the number it has for that server, that means that the
// server's data has not changed, and so the client updates the
// reserve timestamp to the piggybacked value, if it is larger.  If,
// on the other hand, the version number does not match, then the
// client clears all cached items for that server and sets the version
// number and reserve timestamp to the received value.
//
// Note that when a server returns a new version number (one that the
// client has not seen yet), this is effectively a cache invalidation
// message.
//
// If the client wishes to read data with a timestamp higher than the
// reserve timestamp of the target server, or if it wishes to read
// data that is not currently cached, then the client must contact the
// server directly. Upon doing so, the client obtains the data
// together with a version number and reserve timestamp. If the
// version number is older than what the client has for the server, it
// ignores the message as stale. If the version number is bigger, the
// client clears the cached items, updates its version number and
// reserve timestamp, and stores the data in the cache. If the version
// is the same, the client stores the data in the cache.
//
// When the client wishes to update a cached item, it will run the
// commit protocol as usual. But the prepare phase will return an
// unusually high timestamp, because the reserve timestamp is in the
// future.  If the client gets a timestamp in the future according to
// its clock, it waits until its clock reaches the timestamp before
// proceeding to the commit phase, so that the commit does not happen
// with a timestamp in the future. This is not strictly necessary, but
// ensures that a read in a subsequent transaction sees the updated
// data, since the subsequent transaction will have a start timestamp
// higher than the commit timestamp of the transaction that modified
// the data.
//
// This scheme trades off write performance for read performance.
// Writes require additional time since the client will wait for an
// amount approximately equal to the CACHE_RESERVE_TIME.  This wait
// allows other clients to read the data without having to consult the
// server, provided that they communicated with the server recently
// (their reserve timestamp is fresh) and their cache is up-to-date
// (matches the version number they saw in the last communication with
// the server).
//
// Note that the timestamps refer to the logical timestamps
// used in transactions. This scheme remains correct even if
// the clocks of clients and servers are not synchronized,
// though in this case there might be liveness/progress problems.

#include "datastruct.h"
#include "gaiatypes.h"
#include "os.h"
#include "valbuf.h"


#define CACHE_RESERVE_TIME 1543 // how much time in ms to reserve before updates

// This macro returns a boolean of whether a coid is cachable or not.
// Currently, only the database metadata is cached, which consists of
// dbid!=0, tableid=0, oid=0
#if defined(LOCALSTORAGE) || !defined(GAIA_CLIENT_CONSISTENT_CACHE)
#define IsCoidCachable(coid) false // nothing is cachable with local storage
#else
#define IsCoidCachable(coid) (getDbid(coid.cid)!=0 && getiTable(coid.cid)==0 && coid.oid==0) // only table metadata is cachable
#endif

// information kept for each server
struct ClientCachePerServer {
  ClientCachePerServer(){
    versionNo = 0; // invalid version number
    advanceTs.setLowest();
  }

  ~ClientCachePerServer(){
    cachemap.clear(0,0);
  }

  RWLock lock;        // to protect fields below.
  u64 versionNo;      // version number for cached data
  Timestamp ts;       // timestamp of data in cache
  Timestamp advanceTs;  // advance timestamp. Data in cache is consistent with
                        // all timestamps in [ts,advanceTs]
  SkipList<COid, Ptr<Valbuf> > cachemap; // actual contents of cache
};

class ClientCache {
private:
  int Nservers; // number of servers
  ClientCachePerServer *Caches;
  
public:
  ClientCache(int nservers){
    Nservers = nservers;
    Caches = new ClientCachePerServer[nservers];
  }

  ~ClientCache(){
    delete [] Caches;
  }

  // reports the vno and adv timestamp of a given server.
  // If cached version is older, clear the cache and remember new version
  //   and timestamp.
  // If it is the same, update adv timestamp.
  // Otherwise do nothing.
  int report(int serverno, u64 vno, Timestamp &ts, Timestamp &advts);

  // lookup coid in the cache for given server.
  // If found, sets buf to cached value and returns 0.
  // Otherwise, leaves buf untouched and returns non-0.
  int lookup(int serverno, COid &coid, Ptr<Valbuf> &buf, Timestamp &readTs);

  // sets the cache of a given server if it is not set already.
  // This function makes a copy of the buffer, so caller
  // continues to own the passed buf.
  // Returns 0 if the cache was set, non-zero if the cache already had the item
  int set(int serverno, COid &coid, Ptr<Valbuf> buf);
};

#endif

//
// coid.cpp
//
// Things related to coids and their bit allocations
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

#include <stdlib.h>

#include "options.h"
#include "tmalloc.h"
#include "os.h"
#include "util.h"
#include "clientlib.h"
#include "clientdir.h"

#include "coid.h"
#include "kvinterface.h"

Tlocal SimplePrng *RndServerPrng=0;
Tlocal u64 MyOidIssuerId = 0;
Tlocal u32 MyOidCounter = 0; // next available counter

// Check to see if table with given cid had been created before; if
// not then write a marker to indicate that it exists.
// Returns true if it exists, false otherwise
bool checkTableExists(u64 cid){
  COid coid0, coid1;
  int res;
  KVTransaction *tx;
  Ptr<Valbuf> buf;
  
  bool remote = (getDbid(cid) & EPHEMDB_CID_BIT) ? false : true;
  SuperValue sv;
  bool exists = false;

  coid0.cid = cid;
  setOid(&coid0.oid, 0, 0, 0); // pick issuerid 0 and counter 0 (root)
  
  coid1.cid = cid;
  setOid(&coid1.oid, 0, 1, 0); // pick issuerid 0 and counter 1 (marker)

  // table does not exist if root is empty and marker is empty.
  // In this case, set the marker to non-empty
  while (1){
    beginTx(&tx, remote);
    res = KVreadSuperValue(tx, coid0, buf, 0, 0);
    if (!res){
      exists = true;
      goto end; // successful read root, table exists
    }
    res = KVreadSuperValue(tx, coid1, buf, 0, 0);
    if (!res){
      exists = true;
      goto end; // successful read marker, table exists
    }
    
    if (res == GAIAERR_WRONG_TYPE){
      // now write marker
      res = KVwriteSuperValue(tx, coid1, &sv);
      if (!res){  // no problem writing, let's try to commit it
        res = commitTx(tx);
        if (!res){ // no problem committing, table does not exist and has been marked to exist now
          exists = false;
          goto end;
        }
      } 
    }
    // retry transaction
    freeTx(tx);
    tx = 0;
    mssleep(300); // wait a little
  }
 end:
  if (tx) freeTx(tx);
  return exists;
}

// Find a free table id
// Return 0 if no more table ids are available
u64 findFreeiTable(u64 dbid, bool transient){
  u64 iTable;
  u64 cid;
  for (iTable = (transient ? 3 : 2); (iTable & ~0x7fffffffLL) == 0;
       iTable += 2){
    cid = getCidTable(dbid, iTable);
    if (!checkTableExists(cid)) break;
  }
  if (iTable & ~0x7fffffffLL) return 0; // no more table ids available
  return iTable;
}

// change oid to have a random serverid
void setRandomServerid(u64 *oid){
  u32 serverid;
  assert(oid);
  if (!RndServerPrng) RndServerPrng = new SimplePrng();
  serverid = (u32) RndServerPrng->next() & 0xffff;
  *oid &= ~0xffffLL; // clear lower 16 bits
  *oid |= serverid; // set lower 32 bits to random serverid
}

// constructs oid from its components
void setOid(u64 *oid, u64 issuerid, u64 counter, u64 serverid){
  assert((issuerid & ~0xffffffffLL)==0); // only low 32 bits should be set
  assert((counter & ~0xffffLL)==0); // only low 16 bits should be set
  assert((serverid & ~0xffffLL)==0); // only low 16 bits should be set
  assert(oid);
  *oid = (issuerid << 32) | (counter << 16) | serverid;
}

// The following functions are responsible for allocating issuerids and oids

// sets MyIssuerId to a new issuerid
void NewIssuerId(bool remote){
  KVTransaction *tx;
  COid coid;
  Ptr<Valbuf> buf;
  int res;
  u64 issuerid;

  coid.cid = 0; // bookkeeping cid
  coid.oid = 0; // for last used issuerid
  while (1){
    beginTx(&tx, remote);

    res = KVget(tx, coid, buf);
    if (res) goto retry; // error, retry
    if (buf->len == 0) issuerid = 0; // object does not exist,
                                     // so last issuerid is 0
    else {
      // extract previous issuerid from buf
      assert(buf->len == sizeof(u64));
      issuerid = *(u64*)buf->u.buf;
      assert((issuerid & ~0xffffffffLL) == 0); // should have 32 bits
    }

    ++issuerid;
    assert((issuerid & ~0xffffffffLL) == 0);
    
    res = KVput(tx, coid, (char*) &issuerid, sizeof(u64));
    if (res) goto retry; // error, retry

    res = commitTx(tx);
    if (res) goto retry; // error, retry
    freeTx(tx);
    break;

   retry:
    freeTx(tx);
    mssleep(100);
  }
  MyOidIssuerId = issuerid;
  MyOidCounter = 0;
}

// gets a new oid with serverid set to 0
Oid NewOid(bool remote){
  Oid retval;
  if (MyOidIssuerId == 0 || MyOidCounter == 0xffff)
    NewIssuerId(remote);
  assert(MyOidCounter <= 0xffff);
  
  retval = (MyOidIssuerId << 32) | (MyOidCounter << 16);
  ++MyOidCounter;
  return retval;
}

/* Database identifiers in use */
RWLock UsedDBIds_l;
Set<U64> UsedDBIds;
Align4 u64 LastUsedDBId = 0;

static u64 strHash(const char *ptr, int size){
  u64 h=0;
  assert(size >= 0);
  while (size > 0){
    h = (h<<3) ^ h ^ tolower(*ptr);
    ++ptr;
    --size;
  }
  return h;
}

// returns dbid associated with a name
u64 nameToDbid(const char *dbname, bool ephemeral){
  u64 newdbid;
  newdbid = (u32) strHash(dbname, strlen(dbname));
  if (ephemeral) newdbid |= EPHEMDB_CID_BIT;
  else newdbid &= ~EPHEMDB_CID_BIT;
  return newdbid;
}

// marks dbid as used
void markusedDBId(u64 dbid){
  UsedDBIds_l.lock();
  U64 tmp(dbid);
  UsedDBIds.insert(tmp);
  UsedDBIds_l.unlock();
}

// returns a new memory dbid
u64 newMemDBId(bool ephemeral){
  int looped=0;
  u64 newid;
  int res;
  
  UsedDBIds_l.lock();
  do {
    newid = AtomicInc64(&LastUsedDBId); // candidate
    if ((newid & ~0x7fffffff) != 0){
      if (looped){
        printf("Out of ids, bailing out\n");
        assert(1); // no more ids available
        exit(1);
      }
      FetchAndAdd64(&LastUsedDBId, 2-newid);
      looped=1;
      newid = 1;
    }
    if (ephemeral) newid |= EPHEMDB_CID_BIT;
    U64 tmp(newid);
    res = UsedDBIds.insert(tmp);
  } while (res);  // ensure candidate is unused
  UsedDBIds_l.unlock();
  assert((newid & ~0xffffffff) == 0);
  return newid;
}

// frees a memory dbid
void freeMemDBId(u64 dbid){
  int res;
  UsedDBIds_l.lock();
  U64 tmp(dbid);
  res = UsedDBIds.remove(tmp); assert(res==0);
  UsedDBIds_l.unlock();
}

// check if dbid is ephemeral
bool isDBIdEphemeral(u64 dbid){
  return dbid & EPHEMDB_CID_BIT;
}

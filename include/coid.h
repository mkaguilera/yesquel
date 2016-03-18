//
// coid.h
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

#ifndef _COID_H
#define _COID_H

#include "gaiatypes.h"

// Allocation of bits in coid
//
//   Item   Length Description
//  ---------------------------
//   dbid    16    Database id, obtained from the database name
//      1     1    Constant. Indicates this is a tree node instead of a value
// tableid   15    Id of table within database. Monotonically increasing
//                 The last bit is 1 if the tableid is transient, 0 otherwise
// serverid  32    Id of server storing node
//
// issuerid  48    Id of client allocating node
// counter   16    Monotonic counter used by issuerid to obtain unique coids
//
// New allocation of bits on coid
//
//   Item   Length Description
//  ---------------------------
//   dbid    32    Database id, obtained from the database name
//      1     1    Constant. Indicates this is a tree node instead of a value
// tableid   31    Id of table within database. Monotonically increasing
//                 The last bit is 1 if the tableid is transient, 0 otherwise
// issuerid  32    Id of client allocating node
// counter   16    Monotonic counter used by issuerid to obtain unique coids
// serverid  16    Id of server storing node
//
// The first 64 bits is the cid, the last 64 bits is the oid.
//
// There are some special coids which do not exactly follow the
// pattern above:
//
// 1. dbid 0 is reserved for bookkeeping:
//    dbid=0 tableid=0 oid=0 value with first unused issuerid
//
// 2. dbid d tableid 0 oid 0 stores metadata for database d (d != 0)
//
// 3. dbid d tableid t oid 0 stores the root node of the tree t in database d.
//    This also indicates whether tableid t exists already or not and this
//    information is used to find a free tableid
// 4. dbid d, tableid t, issuerid 0 is reserved for server to
//    allocate its own nodes. This is used for server to allocate
//    the second node of the tree if DTREE_NOFIRSTNODE is not defined

// Note: If changing the format, also change IsCacheable() macro in ccache.h

#define EPHEMDB_CID_BIT    0x80000000  // bit in dbid indicating ephemeral db
#define DTREE_CID_BIT  0x0000000080000000LL // bit in cid indicating tree node
#define DATA_CID(cid) (cid & ~DTREE_CID_BIT) // data cid associated with
                                              //tree cid


// Check to see if table with given cid had been created before.
// Returns true if it exists, false otherwise
bool checkTableExists(u64 cid);

// Find a free table id
// Return 0 if no more table ids are available
u64 findFreeiTable(u64 dbid, bool transient);

// returns the cid of the tree for table iTable within database dbid
inline u64 getCidTable(u64 dbid, u64 iTable){
  assert((iTable & ~0x7fffffffLL) == 0); // high bit should be clear
  return ((u64)dbid<<32) | DTREE_CID_BIT | (u64)iTable;
}

void setRandomServerid(u64 *oid); // change oid to have a random serverid
void setOid(u64 *oid, u64 issuerid, u64 counter, u64 serverid); // constructs
  // oid from its components
inline u64 getDbid(u64 cid){ return cid >> 32; } // return dbid of given cid
inline u64 getiTable(u64 cid){ return cid & 0x7fffffffULL;} // return table id
                                                          // of given cid
void NewIssuerId(bool remote); // sets MyIssuerId to a new issuerid
Oid NewOid(bool remote); // gets a new oid with serverid set to 0
u64 nameToDbid(const char *dbname, bool ephemeral); // returns dbid associated
                                                    // with a name
void markusedDBId(u64 dbid); // marks dbid as used
u64 newMemDBId(bool ephemeral); // returns a new memory dbid
void freeMemDBId(u64 dbid); // frees a memory dbid
bool isDBIdEphemeral(u64 dbid); // check if dbid is ephemeral

#endif

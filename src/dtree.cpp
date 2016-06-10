//
// dtree.cpp
//
// Distributed B-tree. This is the main implementation of the client-side
// of the distributed B-tree that interfaces with SQlite. This file has
// several functions called by SQLite (functions sqlite3*), but implements
// them in a different way than SQLite, by storing data in storage servers
// shared by many SQLite query processors.
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


#define _DTREE_C

#include "os.h"
#include "gaiarpcaux.h"
#include "dtreeaux.h"
#include "supervalue.h"
#include "util.h"

#include "coid.h"
#include "dtreesplit.cpp"
#include "splitter-client.h"

// Header stored before the data in a data KV pair.
// This header is not used for Dtree nodes.
struct DataHeader {
  int dummy;
};

/* forward definitions */

static int DtCreateTable(KVTransaction *tx, u64 dbid, bool allocateiTable,
                         Pgno *piTable, int createTabFlags);
int DtReadData(BtCursor *pCur);
int DtWriteData(BtCursor *pCur, u64 nkey, char *pdata, int ndata);
static int saveAllCursors(BtShared *pBt, u64 cidTable, BtCursor *pExcept);
static Pgno btreePagecount(BtShared *pBt);
#ifdef SQLITE_DEBUG
static int cursorHoldsMutex(BtCursor *p);
#endif


// -------------------------------------------------------------------------

// replace node in level with real node given its oid, and sets its index to 0
static int ReadReal(BtCursor *pCur, int level, COid newcoid, ListCell *cell,
                    Ptr<RcKeyInfo> prki){
  int res;
  res = auxReadReal(pCur->pBtree->tx, newcoid, pCur->node[level], cell, prki);
  if (res==0){
    pCur->nodetype[level] = 1; // real node
    pCur->nodeIndex[level]=0; // we start with leftmost pointer
  }
  return res;
}

/* read the root node of a database */
int ReadDbMetadata(KVTransaction *tx, u64 dbid, int *len, char **buf){
  COid coid;
  int res;
  Ptr<Valbuf> vbuf;
  coid.cid = getCidTable(dbid, 0);
  coid.oid = 0;
  res=KVget(tx, coid, vbuf); if (res){ *len = 0; return res; }
  assert(vbuf->type==0);
  *buf = (char *) malloc(vbuf->len);
  memcpy(*buf, vbuf->u.buf, vbuf->len);
  *len = vbuf->len;
  return res;
}

/* write the root node of a database */
int WriteDbMetadata(KVTransaction *tx, u64 dbid, DbMetadataInfo *dmi){
  COid coid;
  int res;
  coid.cid = getCidTable(dbid, 0);
  coid.oid = 0;
  res=KVput(tx, coid, (char*) dmi, sizeof(DbMetadataInfo));
  return res;
}

/* create the database */
int dtreeCreateDatabase(u64 dbid){
  KVTransaction *tx;
  DbMetadataInfo dmi;
  Pgno table1;
  int rc, res;

  DTREELOG("database %x", dbid);
  tx=0;
  rc = 0;
  bool remote = !isDBIdEphemeral(dbid);
  beginTx(&tx, remote);

  /* write the database metadata information */
  memset((void*) &dmi, 0, sizeof(DbMetadataInfo));
  dmi.readVersion = 1;
  dmi.writeVersion = 1;
  res = WriteDbMetadata(tx, dbid, &dmi);
  if (res){ 
    rc = res; goto end; 
  }

  /* create table 1 */
  table1 = 1;
  res = DtCreateTable(tx, dbid, false, &table1, BTREE_INTKEY);
  if (res){ 
    rc = res; goto end; 
  }
  res = commitTx(tx);
  if (res){ 
    rc = res; goto end; 
  }
 end:
  freeTx(tx);
  return rc;
}


void YesqlInitGlobals(){
 
}

/*
** Open a database file.
** 
** zFilename is the name of the database file.  If zFilename is NULL
** then an ephemeral database is created.  The ephemeral database might
** be exclusively in memory, or it might use a disk-based memory cache.
** Either way, the ephemeral database will be automatically deleted 
** when sqlite3BtreeClose() is called.
**
** If zFilename is ":memory:" then an in-memory database is created
** that is automatically destroyed when it is closed.
**
** The "flags" parameter is a bitmask that might contain bits
** BTREE_OMIT_JOURNAL and/or BTREE_NO_READLOCK.  The BTREE_NO_READLOCK
** bit is also set if the SQLITE_NoReadlock flags is set in db->flags.
** These flags are passed through into sqlite3PagerOpen() and must
** be the same values as PAGER_OMIT_JOURNAL and PAGER_NO_READLOCK.
**
** If the database is already opened in the same database connection
** and we are in shared cache mode, then the open will fail with an
** SQLITE_CONSTRAINT error.  We cannot allow two or more BtShared
** objects in the same database connection since doing so will lead
** to problems with locking.
*/
int sqlite3BtreeOpen(
  const char *zFilename,  /* Name of the file containing the BTree database */
  sqlite3 *db,            /* Associated database handle */
  Btree **ppBtree,        /* Pointer to new Btree object written here */
  int flags,              /* Options */
  int vfsFlags            /* Flags passed through to sqlite3_vfs.xOpen() */
){
  sqlite3_vfs *pVfs;             /* The VFS to use for this btree */
  BtShared *pBt = 0;             /* Shared part of btree structure */
  Btree *p;                      /* Handle to return */
  sqlite3_mutex *mutexOpen = 0;  /* Prevents a race condition. Ticket #3537 */
  int rc = SQLITE_OK;            /* Result code from this function */
  u8 nReserve;                   /* Byte of unused space on each page */

  u32 newdbid;   /* identity of database */
  int createdb;  /* whether to create database or not */
  int transientdb; // whether database is transient or not
  int res;

  /* True if opening an ephemeral, temporary database */
  const int isTempDb = zFilename==0 || zFilename[0]==0;

  /* Set the variable isMemdb to true for an in-memory database, or 
  ** false for a file-based database.
  */
#ifdef SQLITE_OMIT_MEMORYDB
  int isMemdb = 0;
#else
  int isMemdb = (zFilename && strcmp(zFilename, ":memory:")==0)
                       || (isTempDb && sqlite3TempInMemory(db));
#endif

  DTREELOG("zFilename %s flags %x vfsFlags %x", zFilename, flags, vfsFlags);
  assert(db!=0);
  assert(sqlite3_mutex_held(db->mutex));
  assert((flags&0xff)==flags);   /* flags fit in 8 bits */
  assert((flags & BTREE_UNORDERED)==0 || (flags & BTREE_SINGLE)!=0); // Only a
                            // BTREE_SINGLE database can be BTREE_UNORDERED
  assert((flags & BTREE_SINGLE)==0 || isTempDb); // A BTREE_SINGLE database is
                                     // always a temporary and/or ephemeral

  transientdb = (vfsFlags & SQLITE_OPEN_TRANSIENT_DB) ? 1 : 0;

  /* set flags */
  if (db->flags & SQLITE_NoReadlock) flags |= BTREE_NO_READLOCK;
  if (isTempDb) isMemdb=1;  // always stores temporary dbs in memory
  if (isMemdb) flags |= BTREE_MEMORY;
  if ((vfsFlags & SQLITE_OPEN_MAIN_DB)!=0 && (isMemdb || isTempDb)){
    vfsFlags = (vfsFlags & ~SQLITE_OPEN_MAIN_DB) | SQLITE_OPEN_TEMP_DB;
  }

  //pVfs = db->pVfs;
  p = (Btree*) sqlite3MallocZero(sizeof(Btree));
  if (!p){ 
    DTREELOG("  return %d", SQLITE_NOMEM); return SQLITE_NOMEM; 
  }
  p->inTrans = TRANS_NONE;
  p->db = db;
#ifndef SQLITE_OMIT_SHARED_CACHE
  p->lock.pBtree = p;
  p->lock.iTable = 1;
#endif


  /*
  ** The following asserts make sure that structures used by the btree are
  ** the right size.  This is to guard against size changes that result
  ** when compiling on a different architecture.
  */
  assert(sizeof(i64)==8 || sizeof(i64)==4);
  assert(sizeof(u64)==8 || sizeof(u64)==4);
  assert(sizeof(u32)==4);
  assert(sizeof(u16)==2);
  assert(sizeof(Pgno)==8);
  
  pBt = (BtShared*) sqlite3MallocZero(sizeof(*pBt));
  if (pBt==0){
    rc = SQLITE_NOMEM;
    goto btree_open_out;
  }

  pBt->openFlags = (u8)flags;
  pBt->db = db;
  p->pBt = pBt;
  
  pBt->pCursor = 0;
  pBt->pPage1 = 0;
#ifdef SQLITE_SECURE_DELETE
  pBt->secureDelete = 1;
#endif
  pBt->pageSize = 512;
  nReserve = 0;
  pBt->pageSizeFixed = 1;
  pBt->usableSize = pBt->pageSize - nReserve;
  assert((pBt->pageSize & 7)==0);  /* 8-byte alignment of pageSize */
  *ppBtree = p;

  if (flags & BTREE_MEMORY){
    newdbid = newMemDBId(transientdb || isMemdb); // get a new database id
    createdb = 1; /* create the database */
  } else {
    KVTransaction *t;
    char *tmpbuf;
    int len;

    assert(zFilename && *zFilename);
    newdbid = nameToDbid(zFilename, transientdb);
    markusedDBId(newdbid);    

    /* check to see if it exists */
    bool remote = !isDBIdEphemeral(newdbid);
    beginTx(&t, remote);
    tmpbuf = 0;
    res = ReadDbMetadata(t, newdbid, &len, &tmpbuf);
    if (tmpbuf) free(tmpbuf);
    //abortTx(t);
    freeTx(t);
    if (res){ 
      rc = SQLITE_IOERR; goto btree_open_out; 
    }
    createdb = (len == 0); /* if len==0 then  create db */
  }

  if (createdb){
    res = dtreeCreateDatabase(newdbid); /* create the database */
    if (res){ 
      rc = SQLITE_IOERR; goto btree_open_out; 
    }
  }
  if (pBt->nPage==0) pBt->nPage++;
  pBt->KVdbid = newdbid;

  /* here, create the cache structures in the future */

btree_open_out:
  if (rc!=SQLITE_OK){
    if (pBt && pBt->pPager){
      sqlite3PagerClose(pBt->pPager);
    }
    if (pBt->pPage1){ free(pBt->pPage1); pBt->pPage1=0; }
    sqlite3_free(pBt);
    sqlite3_free(p);
    *ppBtree = 0;
  }else{
    /* If the B-Tree was successfully opened, set the pager-cache size to the
    ** default value. Except, when opening on an existing shared pager-cache,
    ** do not change the pager-cache size.
    */
    if (sqlite3BtreeSchema(p, 0, 0)==0){
      // There is no pager or pager cache in Yesquel
      // sqlite3PagerSetCachesize(p->pBt->pPager, SQLITE_DEFAULT_CACHE_SIZE);
    }
  }
  if (mutexOpen){
    assert(sqlite3_mutex_held(mutexOpen));
    sqlite3_mutex_leave(mutexOpen);
  }
  DTREELOG("  return %d", rc);
  return rc;
}

/*
 This function does the actual work of creating a table
  (sqlite3BtreeCreateTable calls it).
 Its interface is similar to the of sqlite3BtreeCreateTable, except that
 allocateiTable indicates if piTable should be set to a newly allocated
 iTable. It also takes a KVTransaction as a
 parameter instead of a Btree
*/

static int DtCreateTable(KVTransaction *tx, u64 dbid, bool allocateiTable,
                         Pgno *piTable, int createTabFlags){
  COid coid, coidfirst;
  int res;
  SuperValue rootNode, firstNode;

  if (allocateiTable){
    *piTable = findFreeiTable(dbid, createTabFlags == BTREE_TRANSIENT);
    if (*piTable == 0) return SQLITE_FULL; // no available ids to create table
  }
  
  coid.cid = getCidTable(dbid, *piTable);
  coid.oid = 0;

#ifndef DTREE_NOFIRSTNODE
  coidfirst.cid = coid.cid;
  setOid(&coidfirst.oid, 0, 2, 0);   // pick issuerid 0 and counter 2
  setRandomServerid(&coidfirst.oid); // pick random serverid for first node
  DTreeNode::InitSuperValue(&firstNode,
                            (createTabFlags == BTREE_INTKEY) ? 0 : 1);
  firstNode.Attrs[DTREENODE_ATTRIB_FLAGS] = DTREENODE_FLAG_LEAF
    | ((createTabFlags == BTREE_INTKEY) ? DTREENODE_FLAG_INTKEY : 0);
  firstNode.Attrs[DTREENODE_ATTRIB_HEIGHT] = 0;
  firstNode.Attrs[DTREENODE_ATTRIB_LASTPTR] = 0;
  firstNode.Attrs[DTREENODE_ATTRIB_LEFTPTR] = 0;
  firstNode.Attrs[DTREENODE_ATTRIB_RIGHTPTR] = 0;
  res = KVwriteSuperValue(tx, coidfirst, &firstNode);  
  if (res) return SQLITE_IOERR;
#endif

  DTreeNode::InitSuperValue(&rootNode,
                            (createTabFlags == BTREE_INTKEY) ? 0 : 1);
#ifndef DTREE_NOFIRSTNODE
  rootNode.Attrs[DTREENODE_ATTRIB_FLAGS] =
    ((createTabFlags == BTREE_INTKEY) ? DTREENODE_FLAG_INTKEY : 0);
  rootNode.Attrs[DTREENODE_ATTRIB_HEIGHT] = 1;
  rootNode.Attrs[DTREENODE_ATTRIB_LASTPTR] = coidfirst.oid;
#else
  rootNode.Attrs[DTREENODE_ATTRIB_FLAGS] = DTREENODE_FLAG_LEAF |
    ((createTabFlags == BTREE_INTKEY) ? DTREENODE_FLAG_INTKEY : 0);
  rootNode.Attrs[DTREENODE_ATTRIB_HEIGHT] = 0;
  rootNode.Attrs[DTREENODE_ATTRIB_LASTPTR] = 0;
#endif
  rootNode.Attrs[DTREENODE_ATTRIB_LEFTPTR] = 0;
  rootNode.Attrs[DTREENODE_ATTRIB_RIGHTPTR] = 0;
  LOG("putting first table at container %llx", (long long) coid.cid);

  res = KVwriteSuperValue(tx, coid, &rootNode);
  if (res) return SQLITE_IOERR;

  return SQLITE_OK;
}

/*
** Create a new BTree table.  Write into *piTable the page
** number for the root page of the new table.
**
** The type of type is determined by the flags parameter.  Only the
** following values of flags are currently in use.  Other values for
** flags might not work:
**
**     BTREE_INTKEY|BTREE_LEAFDATA     Used for SQL tables with rowid keys
**     BTREE_ZERODATA                  Used for SQL indices
*/
// This function is like sqlite3BtreeCreateTable, except that it lets the
// caller choose the table number (page) by setting *piTable to a number other
// than zero.
int sqlite3BtreeCreateTableChooseTable(Btree *p, Pgno *piTable, int flags){
  int res;
  BtShared *pBt = p->pBt;
  KVTransaction *tx;
  bool createtx;
  int nretries = 0;

  DTREELOG("btree %p flags %d", p, flags);
  assert(!pBt->readOnly);
  assert(pBt->inTransaction==TRANS_WRITE);

  createtx = p->tx == 0; // create a transaction just for this, if p->tx is
                         // invalid

 retry:
  if (createtx){
    bool remote = !isDBIdEphemeral(pBt->KVdbid);
    beginTx(&tx, remote);
  }
  else tx = p->tx; // use transaction if it already exists

  sqlite3BtreeEnter(p);
  res = DtCreateTable(tx, pBt->KVdbid, *piTable==0, piTable, flags);
  sqlite3BtreeLeave(p);
  if (createtx){
    if (!res){
      res = commitTx(tx);
      if (res){
        if (++nretries < 30){
          DTREELOG(" error committing tx, retrying");
          freeTx(tx);
          goto retry;
        } else DTREELOG(" error committing tx, past max retries");
      }
    }
    freeTx(tx);
  }
  DTREELOG("  return %d", res);
  return res;
}

int sqlite3BtreeCreateTable(Btree *p, Pgno *piTable, int flags){
  *piTable = 0;
  return sqlite3BtreeCreateTableChooseTable(p, piTable, flags);
}

static int dtreeRestoreCursorPosition(BtCursor *pCur);
// Restore cursor position. If cursor's eState is CURSOR_VALID or
// CURSOR_INVALID, then do nothing and return SQLITE_OK.
// Otherwise, call btreeRestoreCursorPosition
#define restoreCursorPosition(p) \
  (p->eState>=CURSOR_REQUIRESEEK ? \
         dtreeRestoreCursorPosition(p) : \
         SQLITE_OK)

/*
** Create a new cursor for the BTree whose root is on the page
** iTable. If a read-only cursor is requested, it is assumed that
** the caller already has at least a read-only transaction open
** on the database already. If a write-cursor is requested, then
** the caller is assumed to have an open write transaction.
**
** If wrFlag==0, then the cursor can only be used for reading.
** If wrFlag==1, then the cursor can be used for reading or for
** writing if other conditions for writing are also met.  These
** are the conditions that must be met in order for writing to
** be allowed:
**
** 1:  The cursor must have been opened with wrFlag==1
**
** 2:  Other database connections that share the same pager cache
**     but which are not in the READ_UNCOMMITTED state may not have
**     cursors open with wrFlag==0 on the same table.  Otherwise
**     the changes made by this write cursor would be visible to
**     the read cursors in the other database connection.
**
** 3:  The database must be writable (not on read-only media)
**
** 4:  There must be an active transaction.
**
** No checking is done to make sure that page iTable really is the
** root page of a b-tree.  If it is not, then the cursor acquired
** will not work correctly.
**
** It is assumed that the sqlite3BtreeCursorZero() has been called
** on pCur to initialize the memory space prior to invoking this routine.
*/
static int btreeCursor(
  Btree *p,                              /* The btree */
  Pgno iTable,                           /* Root page of table to open */
  int wrFlag,                            /* 1 to write. 0 read-only */
  struct KeyInfo *pKeyInfo,              /* First arg to comparison function */
  BtCursor *pCur                         /* Space for new cursor */
){
  BtShared *pBt = p->pBt;                /* Shared b-tree handle */

  assert(sqlite3BtreeHoldsMutex(p));
  assert(wrFlag==0 || wrFlag==1);
  assert((iTable & ~0x7fffLL) == 0); /* top bits must be clear */

  /* The following assert statements verify that if this is a sharable 
  ** b-tree database, the connection is holding the required table locks, 
  ** and that no other connection has any open cursor that conflicts with 
  ** this lock.  */
  /*assert(hasSharedCacheTableLock(p, iTable, pKeyInfo!=0, wrFlag+1));
    assert(wrFlag==0 || !hasReadConflicts(p, iTable)); */

  /* Assert that the caller has opened the required transaction. */
  assert(p->inTrans>TRANS_NONE);
  assert(wrFlag==0 || p->inTrans==TRANS_WRITE);
  /*assert(pBt->pPage1 && pBt->pPage1->aData);*/

  if (NEVER(wrFlag && pBt->readOnly)){
    return SQLITE_READONLY;
  }
  if (iTable==1 && btreePagecount(pBt)==0){
    return SQLITE_EMPTY;
  }

  /* Now that no other errors can occur, finish filling in the BtCursor
  ** variables and link the cursor into the BtShared list.  */
  pCur->rootCid = getCidTable(pBt->KVdbid, iTable);
  pCur->pKeyInfo = pKeyInfo;
  pCur->pBtree = p;
  pCur->pBt = pBt;
  pCur->wrFlag = (u8)wrFlag;
  pCur->pNext = pBt->pCursor;
  new(&pCur->data) Ptr<Valbuf>; // if this doesn't compile,
                           // try memset(&pCur->data, 0, sizeof(Ptr<Valbuf>));
  pCur->intKey = pKeyInfo ? 0 : 1;
  memset(pCur->node, 0, sizeof(Ptr<DTreeNode>) * DTREE_MAX_LEVELS);
//#ifndef NDEBUG
  memset(pCur->nodetype, 0xff, DTREE_MAX_LEVELS);
  memset(pCur->nodeIndex, 0xff, sizeof(u32) * DTREE_MAX_LEVELS);
//#endif

  /* TO FILL: anything extra to initialize? */
  if (pCur->pNext){
    pCur->pNext->pPrev = pCur;
  }
  pBt->pCursor = pCur;
  pCur->eState = CURSOR_INVALID;
  pCur->cachedRowid = 0;
  return SQLITE_OK;
}
int sqlite3BtreeCursor(
  Btree *p,                                   /* The btree */
  Pgno iTable,                                 /* Root page of table to open */
  int wrFlag,                                 /* 1 to write. 0 read-only */
  struct KeyInfo *pKeyInfo,                   /* First arg to xCompare() */
  BtCursor *pCur                              /* Write new cursor here */
){
  int rc;
  DTREELOG("btree %p iTable %llx wrFlag %d", p, (long long)iTable, wrFlag);
  sqlite3BtreeEnter(p);
  rc = btreeCursor(p, iTable, wrFlag, pKeyInfo, pCur);
  sqlite3BtreeLeave(p);
  DTREELOG("  return %d", rc);
  return rc;
}

/* compares a cell key against intKey2/pIdxKey2. Use intKey2
   if pIdxKey2==0 otherwise use pIdxKey2 */
/* inline */
int compareCellWithKey(u8 *cellData1, i64 intKey2, UnpackedRecord *pIdxKey2) {
  i64 nKey1;
  cellData1 += getVarint(cellData1, (u64*) &nKey1);
  assert(nKey1 == (int)nKey1);
  if (pIdxKey2) return sqlite3VdbeRecordCompare((int) nKey1, cellData1,
                                                pIdxKey2);
  else if (nKey1==intKey2) return 0;
  else return (nKey1 < intKey2) ? -1 : +1;
}

/* compares a cell key against intKey2/pIdxKey2. Use intKey2 if pIdxKey2==0
   otherwise use pIdxKey2 */
/* inline */
int compareNpKeyWithKey(i64 nKey1, char *pKey1, i64 nKey2,
                        UnpackedRecord *pIdxKey2) {
  if (pIdxKey2) return sqlite3VdbeRecordCompare((int)nKey1, pKey1, pIdxKey2);
  else if (nKey1==nKey2) return 0;
  else return (nKey1 < nKey2) ? -1 : +1;
}

// Searches the cells of a node for a given key, using binary search.
// Returns the child pointer that needs to be followed for that key.
// If biasRight!=0 then optimize for the case the key is larger than any
// entries in node.
// Assumes that the path at the given level has some node (real or approx).
// Guaranteed to return an index between 0 and N where N is the number of cells
// in that node (N+1 is the number of pointers).
// Returns *matches!=0 if found key, *matches==0 if did not find key.
static int CellSearchNodeUnpacked(DTreeNode &node, UnpackedRecord *pIdxKey,
                                  i64 nkey, int biasRight, int *matches=0){
  int cmp;
  int bottom, top, mid;
  ListCell *cell;

  bottom=0;
  top=node.Ncells()-1; /* number of keys on node minus 1 */
  if (top<0){ if (matches) *matches=0; return 0; } // there are no keys in
                    // node, so return index of only pointer there (index 0)
  do {
    if (biasRight){ mid = top; biasRight=0; } /* bias first search only */
    else mid=(bottom+top)/2;
    cell = &node.Cells()[mid];
    cmp = compareNpKeyWithKey(cell->nKey, cell->pKey, nkey, pIdxKey);

    if (cmp==0) break; /* found key */
    if (cmp < 0) bottom=mid+1; /* mid < target */
    else top=mid-1; /* mid > target */
  } while (bottom <= top);
  // if key was found, then mid points to its index and cmp==0
  // if key was not found, then mid points to entry immediately before key
  //     (cmp<0) or after key (cmp>0)

  if (cmp<0) ++mid; // now mid points to entry immediately after key or to one
                    // past the last entry if key is greater than all entries
  // note: if key matches a cell (cmp==0), we follow the pointer to the left of
  // the cell, which has the same index as the cell

  if (matches) *matches = cmp == 0 ? 1 : 0;
  assert(0 <= mid && mid <= node.Ncells());
  return mid;
}
static int CellSearchNode(DTreeNode &node, i64 nkey, void *pkey,
                          KeyInfo *pKeyInfo, int biasRight){
  UnpackedRecord *pIdxKey;   /* Unpacked index key */
  char aSpace[150];          /* Temp space for pIdxKey - to avoid a malloc */
  int res;

  if (pkey){
    pIdxKey = sqlite3VdbeRecordUnpack(pKeyInfo, (int)nkey, pkey, aSpace,
                                      sizeof(aSpace));
    if (pIdxKey == 0) return SQLITE_NOMEM;
  } else pIdxKey = 0;
  res = CellSearchNodeUnpacked(node, pIdxKey, nkey, biasRight);
  if (pkey) sqlite3VdbeDeleteUnpackedRecord(pIdxKey);
  return res;
}

// Adjusts the index at given level so that it points to the targetoid.
// Returns 0 if index was adjusted, -1 if there were no pointers at given
// level to targetoid
static int AdjustIndex(BtCursor *pCur, int level, Oid targetoid){
  int i, n;
  DTreeNode *dtn = &pCur->node[level]; // for convenience
  n = dtn->Ncells()+1; // number of pointers
  for (i=0; i < n; ++i){
    if (dtn->GetPtr(i) == targetoid){ // found it
      pCur->nodeIndex[level] = i; // adjust index
      break;
    }
  }
  if (i==n) return -1; // did not find it
  else return 0;
}

// Find the real node that should be in the path at given level, pointing to the
// next level. We are given a cell guide, such that a search from the root
// for the cell will arrive at the next level. Sets pCur->node[level] to the
// appropriate real node.
//
// Returns 0 if successful, < 0 if error, >0 if confused.
// The function returns a confused result due to cached nodes in the cursor.
// Specifically, the function will first try reading the real value of the
// cached node at the given level. If that real node does not point to the
// sought target at level+1, then the cached information is bogus and the
// function returns a confused status.

// In this case, this
// function will perform a real search from the root, update the
// cursor at all levels until the leaf, and set the new pCur->leafLevel.
//
// Assumes that
// (1) given level is higher than leaf,
// (2) pCur->node[level+1] is a real node
// (3) pCur->node[lvel+1] appears in the path from root to the leaf containing
//     cellguide
//
// Returns the
int DtFindRealLevelPath(BtCursor *pCur, int level, ListCell *cellguide){
  Oid child;
  int res;
  Oid targetoid;
  COid coidtmp;

  assert(level < pCur->levelLeaf);

  coidtmp.cid = pCur->rootCid;
  targetoid = pCur->node[level+1].NodeOid(); // this is the oid we seek

  // if node at level is not real, then read it
  if (pCur->nodetype[level] == 0){
    coidtmp.oid = pCur->node[level].NodeOid();
    res = ReadReal(pCur, level, coidtmp, 0, 0);
    if (res == GAIAERR_WRONG_TYPE) return 1; // confused
    if (res) return res;
  }
  assert(pCur->nodetype[level] == 1);
  assert(pCur->nodeIndex[level] <= pCur->node[level].Ncells());
  child = pCur->node[level].GetPtr(pCur->nodeIndex[level]);
  if (child == targetoid) return 0; // found
  // index[level] is not correct then adjust it by looking for another index
  // that points to target node
  res = AdjustIndex(pCur, level, targetoid);
  if (!res) return 0; // successfully found pointer at "level" and adjusted
                      // index, so we are done
  return 1; // confused
}

// This function can be called when pCur is known to have bogus information.
// It will perform a search of real nodes from the root, looking for cellguide,
// until it arrives at a node pointing to targetoid or to a leaf node. It will
// set the search nodes in pCur accordingly. If no level points to targetoid,
// it sets pCur->levelLeaf and returns -1.
// Otherwise, it returns the level pointing to targetoid.
// The function may also return a gaia error node (<0) if it got one while
// reading nodes.
int DtRefreshCursor(BtCursor *pCur, ListCell *cellguide, Oid targetoid){
  int res;
  int i, index;
  Oid oid;
  COid coidtmp;
  int levelsought;

  coidtmp.cid = pCur->rootCid;
  oid = DTREE_ROOT_OID;
  i = 0;
  levelsought = -1;
  while (1){
    coidtmp.oid = oid;
    res = ReadReal(pCur, i, coidtmp, 0, 0); if (res) return res;

    // find index for key by doing a binary search
    index = CellSearchNode(pCur->node[i], cellguide->nKey, cellguide->pKey,
                           pCur->pKeyInfo, false);
    assert(0 <= index && index <= pCur->node[i].Ncells());
    pCur->nodeIndex[i] = index;

    if (pCur->node[i].isLeaf()) break;

    // follow child pointer to get oid of next level
    oid = pCur->node[i].GetPtr(pCur->nodeIndex[i]);
    if (oid == targetoid){
      levelsought = i;
      break;
    }
    ++i;
  }

  return levelsought;
}

/*
** Reads the data of a tree node at the cursor.
** Requires the cursor to be valid and of type intKey
*/
int DtReadData(BtCursor *pCur){
  COid coid;
  int res;
  assert(pCur->eState == CURSOR_VALID || pCur->eState == CURSOR_DIRECT);
  assert(pCur->intKey);

  coid.cid = DATA_CID(pCur->rootCid);
  if (pCur->eState == CURSOR_DIRECT) coid.oid = pCur->directIntKey;
  else { // pCur->eState == CURSOR_VALID
    int levelleaf = pCur->levelLeaf;
    int index = pCur->nodeIndex[levelleaf];
    coid.oid = pCur->node[levelleaf].Cells()[index].nKey;
  }

  pCur->data=0;
  res = KVget(pCur->pBtree->tx, coid, pCur->data);
  return res;
}

/*
** Writes the data of a b-skiplist node at the cursor.
** Requires the cursor to be valid, of type intKey, and pCur->data != NULL.
*/
int DtWriteData(BtCursor *pCur, u64 nkey, char *pdata, int ndata){
  COid coid;
  int res;
  DataHeader dh;
  strcpy((char*) &dh.dummy, "DAT");

  coid.cid = DATA_CID(pCur->rootCid);
  coid.oid = nkey;

  res = KVput2(pCur->pBtree->tx, coid, (char*) &dh, sizeof(DataHeader), pdata,
               ndata);
  return res;
}

// bypass Dtree and try to retrieve data directly from KV store
int DtMovetoDirect(BtCursor *pCur, UnpackedRecord *pIdxKey, i64 intKey,
                   int *pRes){
  int res;
  assert(pIdxKey==0);

  // create a cell for key being sought
  pCur->directIntKey = intKey;
  pCur->eState = CURSOR_DIRECT;

  res = DtReadData(pCur);
  if (res || !pCur->data.isset() || pCur->data->len < (int)sizeof(DataHeader)){
    // did not find it
    pCur->data = 0;
    pCur->eState = CURSOR_INVALID;
    *pRes=-1;
  }
  else *pRes=0; // found it

  return res;
}
// Traverse the Dtree to find a given key.
// First, traverse the local cache and see if it leads to the key.
// Otherwise, start fetching nodes from TKVS as necessary to find the key.
// Returns 0 if ok, non-0 if error (with the sqlite status codes).
// Sets *pRes as follows:
//     *pRes<0      No entry matches key, and the cursor is left at entry
//                  immediately before the key OR the table is empty
//                  and the cursor is left pointing to nothing.
//     *pRes==0     An entry matches key, and the cursor is left pointing to it.
//     *pRes>0      No entry matches key, and the cursor is left at entry
//                  immediately after the key.
// Sets pCur->levelLeaf
int DtMovetoUnpackedaux(BtCursor *pCur, UnpackedRecord *pIdxKey, i64 nKey,
                        char *pKey, int biasRight, int *pRes, bool tryDirect){
  // highest level at which key belongs inside the node. This is the negation
  // of the situation where the key is smaller than the smallest key in node,
  // or larger than the largest key.
  // This is relevant for the following reason:
  //   If level is at the leaf (so the node is real not approximate) and key
  //     belongs in the node, then the leaf node is authoritative for telling
  //     if key exists or not.
  //  On the other hand if key does not belong to the node, the key may be
  // somewhere else, since we traversed the tree using approximate pointers
  int highestNonExtremeLevel = -1;
  int level, real, res, index, matches;
  COid coid, coid2, prevcoid;
  ListCell cell;
  Ptr<RcKeyInfo> prki;

  cell.nKey = nKey;
  cell.pKey = pKey;
  cell.value = 0xabcdabcdabcdabcd; // not used
  if (pIdxKey) prki = (RcKeyInfo*) pIdxKey->pKeyInfo;

  DTREELOG("BtCursor %p pIdxKey %p nKey %lld biasRight %d direct %d",
           pCur, pIdxKey, (long long)nKey, biasRight, tryDirect);

  assert(cursorHoldsMutex(pCur));
  assert(sqlite3_mutex_held(pCur->pBtree->db->mutex));
  assert(pRes);
  assert((pIdxKey==0)==(pCur->pKeyInfo==0));
  
  pCur->data=0;
  prevcoid.cid = coid2.cid = coid.cid = pCur->rootCid;

  if (tryDirect && pCur->eState==CURSOR_DIRECT && pCur->intKey){
    if (pIdxKey==0 && pCur->directIntKey == nKey){
      // cursor key matches direct key
      *pRes=0;
      DTREELOG("  return %d", 0);
      return 0;
    }
  }
  if (pCur->eState==CURSOR_VALID && pCur->intKey){
    int levelleaf = pCur->levelLeaf;
    assert(pCur->node[levelleaf].isLeaf());
    if (pCur->node[levelleaf].Cells()[pCur->nodeIndex[levelleaf]].nKey == nKey){
      /* cursor key already matches */
      *pRes = 0;
      DTREELOG("  return %d", 0);
      return 0;
    }
    if (pCur->node[levelleaf].RightPtr() == 0    // last node in tree
    && pCur->nodeIndex[levelleaf] == pCur->node[levelleaf].Ncells()-1 // at last
                                                                      // cell
    && pCur->node[levelleaf].Cells()[pCur->nodeIndex[levelleaf]].nKey < nKey)
    {
      // cursor key is last and smaller than requested key */
      *pRes = -1; // we are before the key
      DTREELOG("  return %d", 0);
      return 0;
    }
  }
  if (tryDirect && pCur->intKey){
    res = DtMovetoDirect(pCur, pIdxKey, nKey, pRes);
    if (res){ DTREELOG("  return %d", SQLITE_IOERR); return SQLITE_IOERR; }
    if (*pRes==0){ DTREELOG("  return %d", 0); return 0; }
  }

  // using cached data, traverse from root to leaf, updating current path
  // during traversal, if we did not follow the extreme pointers at a level,
  // set highestNonExtremeTraversal to that level. In the end, the variable
  // ends up with the highest such level
  level = 0;
  coid.oid = DTREE_ROOT_OID; // start with root
  do {
    res = auxReadCacheOrReal(pCur->pBtree->tx, coid, pCur->node[level], real,
                             &cell, prki);
    if (res == GAIAERR_WRONG_TYPE){  // not a supervalue
      //printf("Found unexpected non-supervalue\n");
      if (level == 0){
       // root not supervalue, so tree is deleted        
        pCur->eState = CURSOR_INVALID;
        DTREELOG("  return %d", SQLITE_EMPTY);
        return SQLITE_EMPTY;
      } 
      auxRemoveCache(prevcoid); // the node pointing to this non-supervalue
                               // node is bogus, remove it from cache
      --level; // move up one level
      goto skip_cache_traversal; // start upward real traversal
    }
    if (res){
      pCur->eState = CURSOR_INVALID;
      DTREELOG("  return %d", SQLITE_IOERR);
      return SQLITE_IOERR;
    }
    pCur->nodetype[level] = real ? 1 : 0;
    index = CellSearchNodeUnpacked(pCur->node[level], pIdxKey, nKey,
                                   biasRight, &matches);
    pCur->nodeIndex[level] = index;
    if (matches || 0 < index && index < pCur->node[level].Ncells())
      highestNonExtremeLevel = level; // key belongs inside the node

    if (pCur->node[level].isLeaf()) break; // got to leaf

    prevcoid.oid = coid.oid;
    coid.oid = pCur->node[level].GetPtr(index);
    ++level;
  } while (level < DTREE_MAX_LEVELS);
  // here, level == leaf node level
  if (level == DTREE_MAX_LEVELS){ // too much garbage in cache
    level = 0;
    goto skip_cache_traversal;
  }

  if (pCur->nodetype[level] == 1){ // if node is real
    if (highestNonExtremeLevel == level){ // key belongs inside a real leaf
                                         // node, so traversal was fruitful
      if (matches) *pRes=0;
      else *pRes=1; // because CellSearchNodeUnpacked returns an index right
                    // after the found key, not before
      pCur->eState = CURSOR_VALID;
      pCur->levelLeaf = level;
      DTREELOG("  return %d", 0);
      return 0;
    }
    // here index == pCur->nodeIndex[level]

    // if leaf is rightmost leaf and key > largest key in node, then set pRes
    // and return
    if (pCur->node[level].RightPtr() == 0 &&
        index == pCur->node[level].Ncells()){
      if (index == 0){ // no cells in the leaf node
        //assert(level==0);  // must be the root node; table is empty
        *pRes=-1;
        pCur->eState = CURSOR_INVALID;
        pCur->levelLeaf = level;
        DTREELOG("  return %d", 0);
        return 0; 
      }
      --pCur->nodeIndex[level];
      --index;  // updating index to keep it identical to nodeIndex[level]
      assert(index >= 0);
      *pRes=-1;  // cursor before key now
      pCur->eState = CURSOR_VALID;
      pCur->levelLeaf = level;
      DTREELOG("  return %d", 0);
      return 0;
    }

    // if leaf is leftmost leaf and key < smallest key in node, then set
    // pRes and return
    if (pCur->node[level].LeftPtr() == 0 && index == 0){
      *pRes=1; // cursor after key
      pCur->eState = CURSOR_VALID;
      pCur->levelLeaf = level;
      DTREELOG("  return %d", 0);
      return 0;
    }
  }

  // do the upward traversal looking for a place where key belongs inside
  // a real node
  // level = highestNonExtremeLevel; // this is just an optimization
 skip_cache_traversal:
  assert(level >= 0);
  while (1){
    if (pCur->nodetype[level] == 0){ // approx node
      // fetch real node at level from TKVS
      coid2.oid = pCur->node[level].NodeOid();
      res = auxReadReal(pCur->pBtree->tx, coid2, pCur->node[level],
                        &cell, prki); 
      if (res==GAIAERR_WRONG_TYPE){
        // not supervalue
        if (level == 0){
          pCur->eState = CURSOR_INVALID;
          DTREELOG("  return %d", SQLITE_EMPTY);
          return SQLITE_EMPTY;
        } // root not supervalue, so tree is deleted
        --level; // move up
        continue;
      }
      if (res){
        pCur->eState = CURSOR_INVALID;
        DTREELOG("  return %d", SQLITE_IOERR);
        return SQLITE_IOERR;
      }
      pCur->nodetype[level] = 1; // now it is a real node
    } // else node is already real
    index = CellSearchNodeUnpacked(pCur->node[level], pIdxKey, nKey, biasRight,
                                   &matches);

    // if key belongs inside the node
    if (matches || 0 < index && index < pCur->node[level].Ncells())
      break; // found a good level
    if (level == 0)
      break; // did not find a good level, but no more levels to search
    --level;
  }

  // now level == 0 or we found node where key belongs inside. Moreover, index
  // is set to be the pointer to follow
  pCur->nodeIndex[level] = index;
  // while (node at level is not leaf)
  while (pCur->node[level].isInner()){
    // follow pointer to child
    coid.oid = pCur->node[level].GetPtr(index);
    ++level;
    assert(level < DTREE_MAX_LEVELS);
    // read child
    res = auxReadReal(pCur->pBtree->tx, coid, pCur->node[level], &cell, prki);
    if (res == GAIAERR_WRONG_TYPE)
      res = SQLITE_CORRUPT; // not a supervalue, so tree is corrupted
    if (res){
      pCur->eState = CURSOR_INVALID;
      DTREELOG("  return %d", SQLITE_IOERR);
      return SQLITE_IOERR;
    }
    pCur->nodetype[level] = 1; // mark it as real node
    // search for key
    index = CellSearchNodeUnpacked(pCur->node[level], pIdxKey, nKey,
                                   biasRight, &matches);
    pCur->nodeIndex[level] = index;
  }
  pCur->levelLeaf = level;

  // if table is empty...
  if (pCur->node[level].Ncells() == 0){
    // this should occur only at the root
    assert(level==0 || level==1);
    *pRes=-1; 
    pCur->eState = CURSOR_INVALID;
    DTREELOG("  return %d", 0);
    return 0; 
  }

  if (matches) *pRes = 0;
  else {
    if (index < pCur->node[level].Ncells()) *pRes = 1;
    else { // index pointing after last cell in leaf node
      --pCur->nodeIndex[level];
      *pRes = -1;
    }
  }
  pCur->eState = CURSOR_VALID;
  DTREELOG("  return %d", 0);
  return 0;
}
/*
** In this version of BtreeMoveto, pKey is a packed index record
** such as is generated by the OP_MakeRecord opcode.  Unpack the
** record and then call BtreeMovetoUnpacked() to do the work.
*/
int DtMovetoaux(
  BtCursor *pCur,     /* Cursor open on the btree to be searched */
  const void *pKey,   /* Packed key if the btree is an index */
  i64 nKey,           /* Integer key for tables.  Size of pKey for indices */
  int bias,           /* Bias search to the high end */
  int *pRes,          /* Write search results here */
  bool tryDirect      // whether to try a direct seek or not
){
  int rc;                    /* Status code */
  UnpackedRecord *pIdxKey;   /* Unpacked index key */
  char aSpace[150];          /* Temp space for pIdxKey - to avoid a malloc */

  if (pKey){
    assert(nKey==(i64)(int)nKey);
    pIdxKey = sqlite3VdbeRecordUnpack(pCur->pKeyInfo, (int)nKey, pKey, aSpace,
                                      sizeof(aSpace));
    if (pIdxKey == 0) return SQLITE_NOMEM;
  } else pIdxKey = 0;
  rc = DtMovetoUnpackedaux(pCur, pIdxKey, nKey, (char*) pKey, bias, pRes,
                           tryDirect);
  if (pKey) sqlite3VdbeDeleteUnpackedRecord(pIdxKey);
  return rc;
}
int sqlite3BtreeMovetoUnpacked(BtCursor *pCur, UnpackedRecord *pIdxKey,
                                   i64 intKey, int biasRight, int *pRes){
  char *pKey;
  int res, nKey;
  assert(testRecordPack(pIdxKey, BTREE_FILE_FORMAT));
  pKey = myVdbeRecordPack(pIdxKey, BTREE_FILE_FORMAT, nKey);

#ifndef NODIRECTSEEK
  res = DtMovetoUnpackedaux(pCur, pIdxKey, intKey, pKey, biasRight, pRes, true);
#else
  res = DtMovetoUnpackedaux(pCur, pIdxKey, intKey, pKey, biasRight, pRes,false);
#endif
  free(pKey);
  return res;
}

int DtMovetoUnpackedNoDirect(BtCursor *pCur, UnpackedRecord *pIdxKey,
                             i64 intKey, int biasRight, int *pRes){
  char *pKey;
  int res, nKey;
  assert(testRecordPack(pIdxKey, BTREE_FILE_FORMAT));
  pKey = myVdbeRecordPack(pIdxKey, BTREE_FILE_FORMAT, nKey);
  
  res = DtMovetoUnpackedaux(pCur, pIdxKey, intKey, pKey, biasRight, pRes,false);
  free(pKey);
  return res;
}

int btreeInsertOptimistic(BtCursor *pCur, const void *pKey, i64 nKey,
                     const void *pData, int nData, int nZero, int appendBias);

//int btreeCheckOptimistic(BtCursor *pCur, const void *pKey, i64 nKey,
// int *pres);


// // bypass Dtree and try to retrieve data directly from KV store
// int DtExistsFast(BtCursor *pCur, UnpackedRecord *pIdxKey, i64 intKey,
//  int *pRes){
//   int res;
//   assert(pIdxKey==0); // currently only implemented for integer keys

//   res = btreeCheckOptimistic(pCur, 0, intKey, pRes);
//   if (res){ // optimistic check failed
//     res = sqlite3BtreeMovetoUnpacked(pCur, 0, intKey, 0, pRes);
//   }
//   //else { // sanity check
//   //  int pres2;
//   //  res = sqlite3BtreeMovetoUnpacked(pCur, 0, intKey, 0, &pres2);
//   //  assert(res==0);
//   //  assert(*pRes && pres2 || !*pRes && !pres2);
//   // }
//   return res;
// }

// Try to find a key using cached data. This will necessarily
// stop before getting to a leaf node, since leafs are never cached.
// Returns non-zero if there was a problem traversing in the cache
// (e.g., a loop).
// Stores in dest the place where we arrived.
int DtCacheMovetoUnpackedaux(BtCursor *pCur, UnpackedRecord *pIdxKey, i64 nKey,
                             int biasRight, Oid *dest){
  int level, res, index, matches;
  COid coid;

  // algo is to just do a downward traversal using cached data, until cache
  // read fails
  DTREELOG("BtCursor %p pIdxKey %p nKey %lld biasRight %d", pCur,
           pIdxKey, (long long)nKey, biasRight);
  

  assert(cursorHoldsMutex(pCur));
  assert(sqlite3_mutex_held(pCur->pBtree->db->mutex));
  assert((pIdxKey==0)==(pCur->pKeyInfo==0));
  
  pCur->data=0;
  coid.cid = pCur->rootCid;

  // using cached data, traverse from root to leaf, updating current path
  // during traversal, if we did not follow the extreme pointers at a level,
  // set highestNonExtremeTraversal to that level. In the end, the variable
  // ends up with the highest such level
  level = 0;
  coid.oid = DTREE_ROOT_OID; // start with root
  do {
    res = auxReadCache(coid, pCur->node[level]);
    if (res) break;
    
    pCur->nodetype[level] = 0;
    index = CellSearchNodeUnpacked(pCur->node[level], pIdxKey, nKey, biasRight,
                                   &matches);
    pCur->nodeIndex[level] = index;
    if (pCur->node[level].isLeaf()){ // got to leaf
      assert(0); // shouldn't happen reading data from cache
      return -1;
    }

    coid.oid = pCur->node[level].GetPtr(index);
    ++level;
  } while (level < DTREE_MAX_LEVELS);
  // here, level == leaf node level
  if (level == DTREE_MAX_LEVELS) return -1; // too much garbage in cache

  pCur->nodetype[level] = 0;
  pCur->nodeIndex[level] = 0; // we haven't actually read this level, so fake
                             // index at 0
  
  if (dest) *dest = coid.oid;
  pCur->levelLeaf = level;
  return 0;
}

int DtCacheMovetoaux(BtCursor *pCur, const void *pKey, i64 nKey, int bias,
                     Oid *dest){
  int rc;                    // Status code
  UnpackedRecord *pIdxKey;   // Unpacked index key
  char aSpace[150];          // Temp space for pIdxKey - to avoid a malloc

  if (pKey){
    assert(nKey==(i64)(int)nKey);
    pIdxKey = sqlite3VdbeRecordUnpack(pCur->pKeyInfo, (int)nKey, pKey, aSpace,
                                      sizeof(aSpace));
    if (pIdxKey == 0) return SQLITE_NOMEM;
  } else pIdxKey = 0;
  rc = DtCacheMovetoUnpackedaux(pCur, pIdxKey, nKey, bias, dest);
  if (pKey) sqlite3VdbeDeleteUnpackedRecord(pIdxKey);
  return rc;
}


// seek the cursor from its direct position
int DtMovefromDirect(BtCursor *pCur){
  int pr, res;
  assert(pCur->eState == CURSOR_DIRECT && pCur->intKey);
  // seek
  res = DtMovetoUnpackedaux(pCur, 0, (i64)pCur->directIntKey, 0, 0, &pr,false);
  if (res) return res;
  assert(pr==0 && pCur->eState == CURSOR_VALID); // must find it
  return 0;
}

 
/*
** Insert a new record into the BTree.  The key is given by (pKey,nKey)
** and the data is given by (pData,nData).  The cursor is used only to
** define what table the record should be inserted into.  The cursor
** is left pointing at a random location.
**
** For an INTKEY table, only the nKey value of the key is used.  pKey is
** ignored.  For a ZERODATA table, the pData and nData are both ignored.
**
** If the seekResult parameter is non-zero, then a successful call to
** MovetoUnpacked() to seek cursor pCur to (pKey, nKey) has already
** been performed. seekResult is the search result returned (a negative
** number if pCur points at an entry that is smaller than (pKey, nKey), or
** a positive value if pCur points at an entry that is larger than 
** (pKey, nKey)). 
**
** If the seekResult parameter is non-zero, then the caller guarantees that
** cursor pCur is pointing at the existing copy of a row that is to be
** overwritten.  
**     [MKA: this appears to be wrong and contradicts the paragraph above.
**           if seekResult is non-zero then pCur may be pointing before
**           or after the key. Also see comment on seekResult comment below]
** If the seekResult parameter is 0, then cursor pCur may
** point to any entry or to no entry at all and so this function has to seek
** the cursor before the new key can be inserted.
*/
int sqlite3BtreeInsert(
  BtCursor *pCur,                /* Insert data into the table of this cursor */
  const void *pKey, i64 nKey,    /* The key of the new record */
  const void *pData, int nData,  /* The data of the new record */
  int nZero,                     /* Number of extra 0 bytes to append to data */
  int appendBias,                /* True if this is likely an append */
  int seekResult                 /* Result of prior MovetoUnpacked() call */
){
  i64 intKey;
  int levelleaf=0, res;
  COid coid;
  KVTransaction *tx = pCur->pBtree->tx;
  ListCell cell;
#if DTREE_SPLIT_LOCATION == 1
  int ncells=0, size=0;
#endif

  DTREELOG("BtCursor %p pKey %p nKey %lld pData %p nData %d nZero %d "
           "appendBias %d seekResult %d", pCur, pKey,
           (long long)nKey, pData, nData, nZero, appendBias, seekResult);

  pCur->data=0;

  if (seekResult && pCur->eState == CURSOR_VALID){
    // skip seeking, since we seeked before
    //  printf("insert: No seek so no optimistic insert\n");
  } 
  else if (pCur->eState == CURSOR_DIRECT && pCur->intKey && pKey==0 &&
           pCur->directIntKey == nKey){
    // skip seeking since we did a directseek and found it
    //printf("insert: direct seek so no optimistic insert\n");
    seekResult=0;
  }
  else {
#ifdef DTREE_OPTIMISTIC_INSERT
    res = btreeInsertOptimistic(pCur, pKey, nKey, pData, nData, nZero,
                                appendBias);
    if (res==0){ // successful optimistic insert
      DTREELOG("  return %d", 0);
      return 0;
    }
#endif
  //if (!seekResult || pCur->eState != CURSOR_VALID){ // traverse only if
  //                                                  // needed
    res = DtMovetoaux(pCur, pKey, nKey, appendBias, &seekResult, false);
    if (res){ DTREELOG("  return %d", res); return res; }
  }

  // here, if seekResult==0, then item is already on the tree, so no operation
  // is needed
  if (seekResult){
    intKey = pCur->intKey;
    assert(intKey && !pKey || !intKey && pKey);

    /* write to KV store */
    levelleaf = pCur->levelLeaf;
    coid.cid = pCur->rootCid;
    coid.oid = pCur->node[levelleaf].NodeOid();
    // create a cell with key
    cell.nKey = nKey;
    cell.pKey = (char*) pKey;
    cell.value = 0xabcdabcdabcdabcd; // not used

#if DTREE_SPLIT_LOCATION != 1    
    res = KVlistadd(tx, coid, &cell, (RcKeyInfo*) pCur->pKeyInfo, 0);
#else
    res = KVlistadd(tx, coid, &cell, (RcKeyInfo*) pCur->pKeyInfo,
                    0, &ncells, &size);
#endif
    if (res){ DTREELOG("  return %d", SQLITE_IOERR); return SQLITE_IOERR; }
  }

  if (nData) {
    res = DtWriteData(pCur, nKey, (char*) pData, nData);
    if (res){ DTREELOG("  return %d", SQLITE_IOERR); return SQLITE_IOERR; }
  }

#if DTREE_SPLIT_LOCATION == 1  
  if (seekResult){
    if (tx->type==1){
      if (ncells > DTREE_SPLIT_SIZE || size > DTREE_SPLIT_SIZE_BYTES){
        coid.cid = pCur->rootCid;
        coid.oid = pCur->node[levelleaf].NodeOid();
        assert(!isDBIdEphemeral(getDbid(coid.cid)));  // container should not
                                               // be ephemeral for remote txs
        tx->AddWork(coid, 1);
      }
    }
  }
#endif
  
  DTREELOG("  return %d", 0);
  return 0;
}

// returns 0 if successful, non-0 if optimistic insert failed
int btreeInsertOptimistic(BtCursor *pCur, const void *pKey, i64 nKey,
                   const void *pData, int nData, int nZero, int appendBias){
  i64 intKey;
  int res;
  COid coid;
  Oid dest;
  KVTransaction *tx = pCur->pBtree->tx;
  ListCell cell;

  //if (tx->type == 0) return -1; // don't do this for in-memory txs

  DTREELOG("BtCursor %p pKey %p nKey %lld pData %p nData %d nZero %d "
           "appendBias %d", pCur, pKey, (long long)nKey, pData, nData,
           nZero, appendBias);

  pCur->data=0;

  res = DtCacheMovetoaux(pCur, pKey, nKey, appendBias, &dest);
  if (res){ DTREELOG("  return %d", res); return res; }

  intKey = pCur->intKey;
  assert(intKey && !pKey || !intKey && pKey);
  coid.cid = pCur->rootCid;
  coid.oid = dest;
  cell.nKey = nKey;
  cell.pKey = (char*) pKey;
  cell.value = 0xabcdabcdabcdabcd; // not used
  
#if DTREE_SPLIT_LOCATION != 1
  // KVlistadd checks that coid is a leaf node and the key is within range.
  // If so, the key is inserted if it is not inserted already
  res = KVlistadd(tx, coid, &cell, (RcKeyInfo*) pCur->pKeyInfo, 1);
#else
  int ncells=0, size=0;
  res = KVlistadd(tx, coid, &cell, (RcKeyInfo*) pCur->pKeyInfo, 1,
                  &ncells, &size);
#endif

  if (res){
    pCur->eState = CURSOR_INVALID; // we arrived at wrong coid
    // **!** check that callers won't be bothered by CURSOR_INVALID
    return -1; // optimistic insert failed
  }
  pCur->eState = CURSOR_DIRECT;

  if (nData) {
    res = DtWriteData(pCur, nKey, (char*) pData, nData);
    if (res){ DTREELOG("  return %d", SQLITE_IOERR); return SQLITE_IOERR; }
  }

#if DTREE_SPLIT_LOCATION == 1
  if (tx->type==1){
    if (ncells > DTREE_SPLIT_SIZE || size > DTREE_SPLIT_SIZE_BYTES){
      assert(!isDBIdEphemeral(getDbid(coid.cid))); // container should
                                             // not be ephemeral for remote txs
      tx->AddWork(coid, 1);
    }
  }
#endif  
  
  DTREELOG("  return %d", 0);
  return 0;
}

// Given a path with a real node at given level and a cell that can be used to
// find that node, delete entry pointed to by index.
// Assumes that node in path at level is real.
int DtDelete(BtCursor *pCur, int level, ListCell *lc){
  // ensure cursor is valid
  // ndeletable = Ncells if leaf-node or Ncells+1 if non-leaf nodes
  // ensure index is between 0 and ndeletable-1
  // if ndeletable > 1  // node will not be destroyed by delete
  //    if within cell range then
  //      call range delete on cell
  //      rewrite cell after (or lastpointer) to create WW conflict with
  //          another delete
  //    else // deleting lastpointer
  //      remember pointer of last real cell
  //      call range delete on last real cell
  //      set last pointer to remembered pointer
  //   return ok
  // // entire node is destroyed by delete
  // if leftpointer != 0 then set its rightpointer to be our rightpointer
  // if rightpointer != 0 then set its leftpointer to be our leftpointer
  // if (level >= 1) // non-root
  //   find real parent and index that points to us (write this info on
  //            current path)
  //   invoke DtDelete on level-1
  // else // root special handling
  //   // we are removing the last cell in root
  //   if non-leaf node
  //      clear last pointer
  //      mark as leaf node
  //   else
  //      call delete on only cell
  //      write dummy last pointer to create WW conflict
  // return ok
  assert(pCur->eState == CURSOR_VALID);

  DTreeNode *dtn = &pCur->node[level]; // for convenience and speed
  SuperValue *raw = dtn->raw->u.raw;
  int index = pCur->nodeIndex[level];
  COid coid;
  int res;
  ListCell cell;

  if (pCur->intKey && dtn->isLeaf()){ // if intkey table and leaf node,
                                      // remove KV object with data
    coid.cid = DATA_CID(pCur->rootCid);
    coid.oid = dtn->Cells()[index].nKey;
    res = KVput(pCur->pBtree->tx, coid, 0, 0); // delete object
    if (res) return SQLITE_IOERR;
  }

  coid.cid = pCur->rootCid;
  coid.oid = dtn->NodeOid();

  // ndeletable = Ncells if leaf-node or Ncells+1 if non-leaf nodes
  int ndeletable = (dtn->isLeaf() ? raw->Ncells : raw->Ncells+1);
  // ensure index is between 0 and ndeletable-1
  assert(0 <= index && index < ndeletable);

  // if ndeletable > 1  // node will not be destroyed by delete
  if (ndeletable > 1){
    // if within cell range then
    if (index < raw->Ncells){
      // call range delete on cell
      cell = dtn->Cells()[index];
      res = KVlistdelrange(pCur->pBtree->tx, coid, 4, &cell, &cell,
                           (RcKeyInfo*) pCur->pKeyInfo);
      if (res) return SQLITE_IOERR;

      // The stuff below is no longer needed since we are defining a delrange
      // to conflict with a delrange in the same cell
      //// rewrite cell after (or lastpointer) to create WW conflict with
      //// another delete
      //if (index+1 < raw->Ncells){
      //  // rewrite cell after
      //  ListCell cell2;
      //  cell2 = dtn->Cells()[index+1];
      //  res = KVlistadd(pCur->pBtree->tx, coid, &cell2,
      //                  (RcKeyInfo*) pCur->pKeyInfo);
      //  if (res) return res;
      //} else { // rewrite lastpointer
      //  res = KVattrset(pCur->pBtree->tx, coid, DTREENODE_ATTRIB_LASTPTR,
      //                  dtn->LastPtr());
      //  if (res) return res;
      //}
    }
    // else we are deleting lastpointer
    else {
      // remember pointer of last real cell
      Oid child = raw->Cells[raw->Ncells-1].value;
      // call range delete on last real cell
      res = KVlistdelrange(pCur->pBtree->tx, coid, 4,
                 &raw->Cells[raw->Ncells-1], &raw->Cells[raw->Ncells-1],
                           (RcKeyInfo*) pCur->pKeyInfo);
      if (res) return SQLITE_IOERR;
      // set last pointer to remembered pointer
      res = KVattrset(pCur->pBtree->tx, coid, DTREENODE_ATTRIB_LASTPTR, child);
      if (res) return SQLITE_IOERR;
    }
    // return ok
    return 0;
  }

  COid coidneighbor;
  coidneighbor.cid = pCur->rootCid;
  coidneighbor.oid = 0;

  /* entire node is destroyed by delete */
  // if leftpointer != 0 then set its rightpointer to be our rightpointer
  if (dtn->LeftPtr()){
    coidneighbor.oid = dtn->LeftPtr();
    res=KVattrset(pCur->pBtree->tx, coidneighbor, DTREENODE_ATTRIB_RIGHTPTR,
                  dtn->RightPtr());
    if (res) return SQLITE_IOERR;
  }
  // if rightpointer != 0 then set its leftpointer to be our leftpointer
  if (dtn->RightPtr()){
    coidneighbor.oid = dtn->RightPtr();
    res=KVattrset(pCur->pBtree->tx, coidneighbor, DTREENODE_ATTRIB_LEFTPTR,
                  dtn->LeftPtr());
    if (res) return SQLITE_IOERR;
  }

  // if (level >= 1) // non-root
  if (level >= 1){
    res = KVput(pCur->pBtree->tx, coid, 0, 0); // delete object
    
    // find real parent and index that points to us (write this info on current path)
    res = DtFindRealLevelPath(pCur, level-1, lc);
    if (res<0) return SQLITE_IOERR;
    if (res>0){ // confused by cache
      level = DtRefreshCursor(pCur, lc, pCur->node[level].NodeOid());
      if (level < 0){
        printf("level %d\n", level);
        fflush(stdout);
      }
      assert(level >= 0);
      ++level; // DtRefreshCursor returns parent pointing to given nodeoid
    }
    // invoke DtDelete on level-1
    res = DtDelete(pCur, level-1, lc); if (res) return SQLITE_IOERR;
  }
  else { // root special handling
    // we are removing the last cell in root
    // if non-leaf node
    if (dtn->isInner()){
      // clear last pointer
      res=KVattrset(pCur->pBtree->tx, coid, DTREENODE_ATTRIB_LASTPTR, 0);
      if (res) return SQLITE_IOERR;
      // set node level to 0
      res=KVattrset(pCur->pBtree->tx, coid, DTREENODE_ATTRIB_HEIGHT, 0);
      //  mark as leaf node     
      res=KVattrset(pCur->pBtree->tx, coid, DTREENODE_ATTRIB_FLAGS,
                    dtn->Flags() | DTREENODE_FLAG_LEAF);
      if (res) return SQLITE_IOERR;
    } else {
      // call delete on only cell
      cell = dtn->Cells()[index];
      res = KVlistdelrange(pCur->pBtree->tx, coid, 4, &cell, &cell,
                           (RcKeyInfo*) pCur->pKeyInfo);
      if (res) return SQLITE_IOERR;
      // the stuff below is no longer needed since a delrange conflicts with
      // another delrange in the same cell
      //// write dummy last pointer to create WW conflict
      //res = KVattrset(pCur->pBtree->tx, coid, DTREENODE_ATTRIB_LASTPTR, 0);
      //if (res) return SQLITE_IOERR;
    }
  }
  // return ok
  return 0;
}

/*
** Delete the entry that the cursor is pointing to.  The cursor
** is left pointing at an arbitrary location.
*/
int sqlite3BtreeDelete(BtCursor *pCur){
  DTREELOG("BtCursor %p", pCur);
  // ensure cursor is valid
  int res;

  // if cursor is direct, seek it to appropriate location
  if (pCur->eState == CURSOR_DIRECT){
    res = DtMovefromDirect(pCur);
    if (res){ DTREELOG("  return %d", SQLITE_IOERR); return SQLITE_IOERR; }
  }
  if (pCur->eState != CURSOR_VALID){
    DTREELOG("  return %d", SQLITE_ERROR);
    return SQLITE_ERROR;
  }
  
  assert(pCur->node[pCur->levelLeaf].isLeaf());
  assert(pCur->nodetype[pCur->levelLeaf] == 1); // leaf must be real node
  pCur->data=0;
  res = saveAllCursors(pCur->pBt, pCur->rootCid, pCur);
  if (res){ DTREELOG("  return %d", SQLITE_IOERR); return SQLITE_IOERR; }

  // obtain a real cell at the leaf and its keyinfo. This is used to
  // locate the ascentors of the leaf, in case we need to delete the cell and
  // hence update the parents.
  assert(pCur->node[pCur->levelLeaf].Ncells());
  ListCell lc(pCur->node[pCur->levelLeaf].Cells()[0]);
  //Ptr<RcKeyInfo> prki = pCur->node[pCur->levelLeaf].Prki();
  
  res = DtDelete(pCur, pCur->levelLeaf, &lc);
  
  lc.Free();
  //free(prki);
              
  pCur->eState = CURSOR_INVALID;
  if (res){ DTREELOG("  return %d", SQLITE_IOERR); return SQLITE_IOERR; }
  DTREELOG("  return %d", 0);
  return 0;
}

/* Move the cursor to the first entry in the table.  Return SQLITE_OK
** on success.  Set *pRes to 0 if the cursor actually points to something
** or set *pRes to 1 if the table is empty.
** Sets pCur->levelLeaf
*/
int DtFirst(BtCursor *pCur, int *pRes){
  int level, real, res;
  COid coid;
  COid coid2;

  pCur->data=0;
  coid2.cid = coid.cid = pCur->rootCid;

  // using cached data, traverse from root to leaf, updating current path
  level = 0;
  coid.oid = DTREE_ROOT_OID; // start with root
  do {
    res = auxReadCacheOrReal(pCur->pBtree->tx, coid, pCur->node[level],
                             real, 0, 0);
    if (res==GAIAERR_WRONG_TYPE){ // not supervalue
      if (level == 0){
        // root not supervalue, so tree is deleted        
        pCur->eState = CURSOR_INVALID;
        return SQLITE_EMPTY;
      } 
      --level; // move up one level
      break; // skip rest of cache traversal
    }
    if (res){ pCur->eState = CURSOR_INVALID; return SQLITE_IOERR; }
    pCur->nodetype[level] = real ? 1 : 0;
    pCur->nodeIndex[level] = 0;
    if (pCur->node[level].isLeaf()) break; // got to leaf

    coid.oid = pCur->node[level].GetPtr(0); // follow leftmost pointer
    ++level;
    assert(level < DTREE_MAX_LEVELS);
  } while (1);

  assert(pCur->node[0].NodeOid() == DTREE_ROOT_OID); // sanity checking

  // search real nodes upwards until we find a good one (leftmost one)
  do {
    if (pCur->nodetype[level] == 0){ // not a real node
      coid2.oid = pCur->node[level].NodeOid();
      res = auxReadReal(pCur->pBtree->tx, coid2, pCur->node[level], 0, 0); 
      if (res){
        if (res == GAIAERR_WRONG_TYPE){
          // wrong node type, keep searching upwards
          --level;
          continue;
        } 
        pCur->eState = CURSOR_INVALID; return SQLITE_IOERR;
      }
      pCur->nodetype[level] = 1; // mark it as real node
    }
    if (pCur->node[level].LeftPtr() == 0) break; // found a good node
    --level;
  } while (level >= 0);
  assert(level >= 0); // at least root should be good

  // now move down following leftmost pointers until we get to the leaf
  while (pCur->node[level].isInner()){
    pCur->nodeIndex[level] = 0;
    coid.oid = pCur->node[level].GetPtr(0);
    ++level;
    res = auxReadReal(pCur->pBtree->tx, coid, pCur->node[level], 0, 0);
    if (res==GAIAERR_WRONG_TYPE) res = SQLITE_CORRUPT; // not a supervalue,
                                                        // so tree is corrupted
    if (res){ pCur->eState = CURSOR_INVALID; return SQLITE_IOERR; }
    pCur->nodetype[level] = 1;
    assert(pCur->node[level].LeftPtr() == 0); // must be leftmost node in level
  }
  pCur->levelLeaf = level;
  if (pCur->node[level].Ncells() == 0){ // leaf node is empty
    *pRes = 1;
    pCur->eState = CURSOR_INVALID;
    return 0;
  }
  pCur->nodeIndex[level] = 0; // point to first entry
  *pRes = 0;
  pCur->eState = CURSOR_VALID;
  return 0;
}
int sqlite3BtreeFirst(BtCursor *pCur, int *pRes){
  int res;
  DTREELOG("BtCursor %p", pCur);
  res = DtFirst(pCur, pRes);
  DTREELOG("  return %d", res);
  return res;
}

/* Move the cursor to the last entry in the table.  Return SQLITE_OK
** on success.  Set *pRes to 0 if the cursor actually points to something
** or set *pRes to 1 if the table is empty.
** Sets pCur->levelLeaf
*/
int DtLast(BtCursor *pCur, int *pRes){
  int level, real, res;
  COid coid;
  COid coid2;

  pCur->data=0;

  coid2.cid = coid.cid = pCur->rootCid;


  // using cached data, traverse from root to leaf, updating current path
  level = 0;
  coid.oid = DTREE_ROOT_OID; // start with root
  do {
    res = auxReadCacheOrReal(pCur->pBtree->tx, coid, pCur->node[level], real,
                             0, 0);
    if (res==GAIAERR_WRONG_TYPE){ // not supervalue
      if (level == 0){
        // root not supervalue, so tree is deleted
        pCur->eState = CURSOR_INVALID;
        return SQLITE_EMPTY;
      }
      --level; // move up one level
      break; // skip rest of cache traversal
    }
    if (res){ pCur->eState = CURSOR_INVALID; return SQLITE_IOERR; }
    pCur->nodetype[level] = real ? 1 : 0;
    pCur->nodeIndex[level] = pCur->node[level].Ncells();
    if (pCur->node[level].isLeaf()) break; // got to leaf

    coid.oid = pCur->node[level].LastPtr(); // follow rightmost pointer
    ++level;
    assert(level < DTREE_MAX_LEVELS);
  } while (1);

  assert(pCur->node[0].NodeOid() == DTREE_ROOT_OID); // sanity checking

  // search real nodes upwards until we find a good one (rightmost one)
  do {
    if (pCur->nodetype[level] == 0){ // not a real node
      coid2.oid = pCur->node[level].NodeOid();
      res = auxReadReal(pCur->pBtree->tx, coid2, pCur->node[level], 0, 0); 
      if (res){
        if (res == GAIAERR_WRONG_TYPE){
          // wrong node type, keep searching upwards
          --level;
          continue;
        }
        pCur->eState = CURSOR_INVALID; return SQLITE_IOERR;
      }
      pCur->nodetype[level] = 1; // mark it as real node
    }
    if (pCur->node[level].RightPtr() == 0) break; // found a good node
    --level;
  } while (level >= 0);
  assert(level >= 0); // at least root should be good

  // now move down following rightmost pointers until we get to the leaf
  while (pCur->node[level].isInner()){
    pCur->nodeIndex[level] = pCur->node[level].Ncells();
    coid.oid = pCur->node[level].LastPtr();
    ++level;
    res = auxReadReal(pCur->pBtree->tx, coid, pCur->node[level], 0, 0);
    if (res==GAIAERR_WRONG_TYPE)  // not a supervalue, so tree is corrupted
      res = SQLITE_CORRUPT;
    if (res){ pCur->eState = CURSOR_INVALID; return SQLITE_IOERR; }
    pCur->nodetype[level] = 1;
    assert(pCur->node[level].RightPtr() == 0); // most be leftmost node in level
  }
  pCur->levelLeaf = level;

  if (pCur->node[level].Ncells() == 0){ // leaf node is empty
    *pRes = 1;
    pCur->eState = CURSOR_INVALID;
    return 0;
  }
  pCur->nodeIndex[level] = pCur->node[level].Ncells()-1; // point to last entry
  *pRes = 0;
  pCur->eState = CURSOR_VALID;
  return 0;
}
int sqlite3BtreeLast(BtCursor *pCur, int *pRes){ 
  DTREELOG("BtCursor %p", pCur);
  return DtLast(pCur, pRes); 
}

/*
** Advance the cursor to the next entry in the database.  If
** successful then set *pRes=0.  If the cursor
** was already pointing to the last entry in the database before
** this routine was called, then set *pRes=1.
*/
int sqlite3BtreeNext(BtCursor *pCur, int *pRes){
  COid coid;
  int res;

  DTREELOG("BtCursor %p", pCur);
  assert(cursorHoldsMutex(pCur));
  assert(pRes!=0);

  pCur->data=0;

  if (pCur->eState == CURSOR_DIRECT){
    res = DtMovefromDirect(pCur);
    if (res){ DTREELOG("  return %d", SQLITE_IOERR); return SQLITE_IOERR; }
  }
  
  res = restoreCursorPosition(pCur);
  if (res != SQLITE_OK){ DTREELOG("  return %d", res); return res; }
  if (pCur->eState==CURSOR_INVALID){
    *pRes = 1;
    DTREELOG("  return %d", 0);
    return 0;
  }
  if (pCur->skipNext>0){
    pCur->skipNext = 0;
    *pRes = 0;
    DTREELOG("  return %d", 0);
    return 0;
  }
  pCur->skipNext = 0;

  assert(pCur->eState == CURSOR_VALID);
  int levelleaf = pCur->levelLeaf;
  ++pCur->nodeIndex[levelleaf];
  if (pCur->nodeIndex[levelleaf] < pCur->node[levelleaf].Ncells()){
    // still cells in this node
    *pRes=0;
    DTREELOG("  return %d", 0);
    return 0;
  }

  if (pCur->node[levelleaf].RightPtr()){ // there is a next node
    /* move to next node */
    coid.cid = pCur->rootCid;
    coid.oid = pCur->node[levelleaf].RightPtr();
    res = auxReadReal(pCur->pBtree->tx, coid, pCur->node[levelleaf], 0, 0);
    if (res){ DTREELOG("  return %d", SQLITE_IOERR); return SQLITE_IOERR; }
    pCur->nodetype[levelleaf] = 1; // mark as real node
    pCur->nodeIndex[levelleaf] = 0; // start at first cell
    assert(pCur->node[levelleaf].Ncells() > 0); // cannot be empty
    *pRes=0;
    pCur->eState = CURSOR_VALID;
  }
  else *pRes=1; /* this is the last cell in last node  */
  DTREELOG("  return %d", 0);
  return 0;
} 

/*
** Step the cursor to the back to the previous entry in the database.  If
** successful then set *pRes=0.  If the cursor
** was already pointing to the first entry in the database before
** this routine was called, then set *pRes=1.
*/
int sqlite3BtreePrevious(BtCursor *pCur, int *pRes){
  COid coid;
  int res;

  DTREELOG("BtCursor %p", pCur);
  assert(cursorHoldsMutex(pCur));
  assert(pRes!=0);

  pCur->data=0;

  if (pCur->eState == CURSOR_DIRECT){
    res = DtMovefromDirect(pCur);
    if (res){ DTREELOG("  return %d", SQLITE_IOERR); return SQLITE_IOERR; }
  }
  
  res = restoreCursorPosition(pCur);
  if (res){ DTREELOG("  return %d", res); return res; }
  if (pCur->eState==CURSOR_INVALID){
    *pRes = 1;
    DTREELOG("  return %d", 0);
    return 0;
  }
  if (pCur->skipNext<0){
    pCur->skipNext = 0;
    *pRes = 0;
    DTREELOG("  return %d", 0);
    return 0;
  }
  pCur->skipNext = 0;

  assert(pCur->eState == CURSOR_VALID);
  int levelleaf = pCur->levelLeaf;
  if (pCur->nodeIndex[levelleaf] > 0){ /* still cells in this node */
    --pCur->nodeIndex[levelleaf];
    *pRes=0;
    DTREELOG("  return %d", 0);
    return 0;
  }

  if (pCur->node[levelleaf].LeftPtr()){ // there is a previous node
    /* move to next node */
    coid.cid = pCur->rootCid;
    coid.oid = pCur->node[levelleaf].LeftPtr();
    res = auxReadReal(pCur->pBtree->tx, coid, pCur->node[levelleaf], 0, 0);
    if (res){ DTREELOG("  return %d", SQLITE_IOERR); return SQLITE_IOERR; }
    pCur->nodetype[levelleaf] = 1; // mark as real node
    assert(pCur->node[levelleaf].Ncells() > 0); // cannot be empty
    pCur->nodeIndex[levelleaf] = pCur->node[levelleaf].Ncells()-1; // start at
                                                                   // last cell
    *pRes=0;
    pCur->eState = CURSOR_VALID;
  }
  else *pRes=1; /* this is the last cell in last node  */
  DTREELOG("  return %d", 0);
  return 0;
}

/*
** The first argument, pCur, is a cursor opened on some b-tree. Count the
** number of entries in the b-tree and write the result to *pnEntry.
**
** SQLITE_OK is returned if the operation is successfully executed. 
** Otherwise, if an error is encountered (i.e. an IO error or database
** corruption) an SQLite error code is returned.
*/
int sqlite3BtreeCount(BtCursor *pCur, i64 *pnEntry){
  int res;
  int pres;
  int nentries=0;
  int levelleaf;
  COid coid;

  DTREELOG("BtCursor %p", pCur);

  pCur->data=0;

  // move cursor to first entry
  res = DtFirst(pCur, &pres);
  if (res){ DTREELOG("  return %d", SQLITE_IOERR); return SQLITE_IOERR; }
  if (pres == 1){
    // empty table
    *pnEntry=0;
    DTREELOG("  return %d", 0);
    return 0;
  }

  coid.cid = pCur->rootCid;
  levelleaf = pCur->levelLeaf;

  do {
    nentries += pCur->node[levelleaf].Ncells();
    coid.oid = pCur->node[levelleaf].RightPtr();
    if (coid.oid == 0) break; // no more right neighbors
    res = auxReadReal(pCur->pBtree->tx, coid, pCur->node[levelleaf], 0, 0);
    if (res){ DTREELOG("  return %d", SQLITE_IOERR); return SQLITE_IOERR; }
    pCur->nodetype[levelleaf] = 1; // real type
  } while (1);
  *pnEntry = nentries;
  DTREELOG("  return %d", 0);
  return 0;
}

// Delete all child nodes of given oid and below, recursively, including any
// data nodes of leaf nodes.
// Also current oid if it is not the root, or it eraseRoot=true.
static int auxClearTable(Btree *pBtree, int level, COid coid, int *pnChange,
                         bool eraseRoot){
  int res;
  SuperValue *raw;
  COid childcoid;

  int i;
  DTreeNode ptrNode;

  // read real oid at level
  res = auxReadReal(pBtree->tx, coid, ptrNode, 0, 0);
  raw = ptrNode.raw->u.raw;

  // if node is leaf
  if (ptrNode.isLeaf()){
    if (pnChange) pnChange += raw->Ncells;
    if (ptrNode.isIntKey()){
      COid coiddata;

      // for each cell in node
      for (i=0; i < raw->Ncells; ++i){
        // write 0 to data node
        coiddata.cid = DATA_CID(coid.cid);
        coiddata.oid = raw->Cells[i].nKey;
        res = KVput(pBtree->tx, coiddata, 0, 0);
        if (res) return SQLITE_IOERR;
      }
    }
  }
  else { // not a leaf node
    // for each pointer in node
    childcoid.cid = coid.cid;
    for (i=0; i <= raw->Ncells; ++i){
      // childoid = node pointed to
      childcoid.oid = ptrNode.GetPtr(i);
      res = auxClearTable(pBtree, level+1, childcoid, pnChange, eraseRoot);
      if (res) return SQLITE_IOERR;
    }
  }
  // if eraseRoot or current node not root
  if (eraseRoot || !ptrNode.isRoot()){
    // erase oid by writing 0 to it
    res = KVput(pBtree->tx, coid, 0, 0);
    if (res) return SQLITE_IOERR;
  }
  else { // root node, but we should not erase it, just empty it
    SuperValue sv;
    DTreeNode::InitSuperValue(&sv, ptrNode.isIntKey() ? 0 : 1);
    sv.Attrs[DTREENODE_ATTRIB_FLAGS] = DTREENODE_FLAG_LEAF |
      (ptrNode.isIntKey() ? DTREENODE_FLAG_INTKEY : 0);
    sv.Attrs[DTREENODE_ATTRIB_HEIGHT] = 0;
    sv.Attrs[DTREENODE_ATTRIB_LASTPTR] = 0;
    sv.Attrs[DTREENODE_ATTRIB_LEFTPTR] = 0;
    sv.Attrs[DTREENODE_ATTRIB_RIGHTPTR] = 0;
    res = KVwriteSuperValue(pBtree->tx, coid, &sv);
    if (res) return SQLITE_IOERR;
  }
  return 0;
}

// Auxilliary function used by sqlite3BtreeDropTable and sqlite3BtreeClearTable.
// Parameters p, iTable, and pnChange are as in sqlite3BtreeClearTable.
// Parameter eraseRoot determines whether the root node is erased (needed
// for sqlite3BtreeDropTable) or not (needed for sqlite3BtreeClearTable).
static int DtClearTable(Btree *p, u64 iTable, int *pnChange, bool eraseRoot){
  BtShared *pBt = p->pBt;
  //KVTransaction *tx = p->tx;
  u64 cidTable;
  COid coid;
  int res;

  assert(p->inTrans==TRANS_WRITE);
  cidTable = getCidTable(pBt->KVdbid, iTable);

  res = saveAllCursors(pBt, cidTable, 0); if (res != SQLITE_OK) return res;

  coid.cid = cidTable;
  coid.oid = DTREE_ROOT_OID;
  return auxClearTable(p, 0, coid, pnChange, eraseRoot);
}

/*
** Erase all information in a table and add the root of the table to
** the freelist.  Except, the root of the principle table (the one on
** page 1) is never added to the freelist.
**
** Parameter p indicates the database, iTable indicates the root
** of the table to be dropped, and piMoved is a parameter needed only
** for AUTOVACUUM (which we do not have in Yesql).
*/
int sqlite3BtreeDropTable(Btree *p, u64 iTable, int *piMoved){
  int rc;
  DTREELOG("btree %p iTable %llx", p, (long long)iTable);
  sqlite3BtreeEnter(p);
  assert (p->inTrans==TRANS_WRITE);
  rc = DtClearTable(p, iTable, 0, true);
  sqlite3BtreeLeave(p);
  DTREELOG("  return %d", rc);
  return rc;
}

/*
** Delete all information from a single table in the database.  iTable is
** the page number of the root of the table.  After this routine returns,
** the root page is empty, but still exists.
**
** This routine will fail with SQLITE_LOCKED if there are any open
** read cursors on the table.  Open write cursors are moved to the
** root of the table.
**
** If pnChange is not NULL, then table iTable must be an intkey table. The
** integer value pointed to by pnChange is incremented by the number of
** entries in the table.
*/
int sqlite3BtreeClearTable(Btree *p, u64 iTable, int *pnChange){
  int rc;
  DTREELOG("btree %p iTable %llx", p, (long long)iTable);
  sqlite3BtreeEnter(p);
  assert(p->inTrans==TRANS_WRITE);
  rc = DtClearTable(p, iTable, pnChange, false);
  sqlite3BtreeLeave(p);
  DTREELOG("  return %d", rc);
  return rc;
}

/*
** For the entry that cursor pCur is point to, return as
** many bytes of the key or data as are available on the local
** b-tree page.  Write the number of available bytes into *pAmt.
**
** The pointer returned is ephemeral.  The key/data may move
** or be destroyed on the next call to any Btree routine,
** including calls from other threads against the same cache.
** Hence, a mutex on the BtShared should be held prior to calling
** this routine.
**
** These routines is used to get quick access to key and data
** in the common case where no overflow pages are used.
*/
const void *sqlite3BtreeKeyFetch(BtCursor *pCur, int *pAmt){
  DTREELOG("BtCursor %p", pCur);
  assert(sqlite3_mutex_held(pCur->pBtree->db->mutex));
  assert(cursorHoldsMutex(pCur));
  assert(pCur->eState==CURSOR_VALID);

  int levelleaf = pCur->levelLeaf;
  int index = pCur->nodeIndex[levelleaf];

  if (pCur->intKey) *pAmt = 0;
  else *pAmt = (int) pCur->node[levelleaf].Cells()[index].nKey;
  return pCur->node[levelleaf].Cells()[index].pKey;
}
const void *sqlite3BtreeDataFetch(BtCursor *pCur, int *pAmt){
  int res;
  DTREELOG("BtCursor %p", pCur);
  assert(sqlite3_mutex_held(pCur->pBtree->db->mutex));
  assert(cursorHoldsMutex(pCur));
  assert(pCur->eState == CURSOR_VALID || pCur->eState == CURSOR_DIRECT);

  if (!pCur->data.isset()){ 
    res = DtReadData(pCur); 
    assert(pCur->data->type==0);
    if (res || pCur->data->len < (int)sizeof(DataHeader)){
      *pAmt = 0;
      return 0;
    }
  }
  assert(pCur->data->type==0);
  *pAmt = pCur->data->len-sizeof(DataHeader);
  return pCur->data->u.buf+sizeof(DataHeader);
}

 
/*
** Set *pSize to the size of the buffer needed to hold the value of
** the key for the current entry.  If the cursor is not pointing
** to a valid entry, *pSize is set to 0. 
**
** For a table with the INTKEY flag set, this routine returns the key
** itself, not the number of bytes in the key.
**
** The caller must position the cursor prior to invoking this routine.
** 
** This routine cannot fail.  It always returns SQLITE_OK.  
*/
int sqlite3BtreeKeySize(BtCursor *pCur, i64 *pSize){
  DTREELOG("BtCursor %p", pCur);
  assert(cursorHoldsMutex(pCur));
  assert(pCur->eState==CURSOR_INVALID || pCur->eState==CURSOR_VALID ||
         pCur->eState==CURSOR_DIRECT);
  if (pCur->eState != CURSOR_VALID && pCur->eState != CURSOR_DIRECT){
    *pSize = 0;
  } else {
    int levelleaf = pCur->levelLeaf;
    int index = pCur->nodeIndex[levelleaf];

    *pSize = pCur->node[levelleaf].Cells()[index].nKey;
  }
  DTREELOG("  return %d", 0);
  return 0;
}

/*
** Set *pSize to the number of bytes of data in the entry the
** cursor currently points to.
**
** The caller must guarantee that the cursor is pointing to a non-NULL
** valid entry.  In other words, the calling procedure must guarantee
** that the cursor has Cursor.eState==CURSOR_VALID.
**
*/
int sqlite3BtreeDataSize(BtCursor *pCur, u32 *pSize){
  int res;
  DTREELOG("BtCursor %p", pCur);
  assert(cursorHoldsMutex(pCur));
  assert(pCur->eState == CURSOR_VALID || pCur->eState == CURSOR_DIRECT);
  if (!pCur->data.isset()){
    res = DtReadData(pCur);
    if (res){ DTREELOG("  return %d", SQLITE_IOERR); return SQLITE_IOERR; }
  }
  if (pCur->data->len >= (int)sizeof(DataHeader)) 
    *pSize = pCur->data->len - (int)sizeof(DataHeader);
  else *pSize = 0;
  DTREELOG("  return %d", 0);
  return 0;
}

/*
** Read part of the key associated with cursor pCur.  Exactly
** "amt" bytes will be transfered into pBuf[].  The transfer
** begins at "offset".
**
** The caller must ensure that pCur is pointing to a valid row
** in the table.
**
** Return SQLITE_OK on success or an error code if anything goes
** wrong.  An error is returned if "offset+amt" is larger than
** the available payload.
*/
int sqlite3BtreeKey(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
  char *aPayload;

  DTREELOG("BtCursor %p offset %u amt %u", pCur, offset, amt);
  assert(cursorHoldsMutex(pCur));
  assert(pCur->eState==CURSOR_VALID);

  int levelleaf = pCur->levelLeaf;
  int index = pCur->nodeIndex[levelleaf];

  aPayload = pCur->node[levelleaf].Cells()[index].pKey;
  memcpy(pBuf, aPayload+offset, amt);

  DTREELOG("  return %d", 0);
  return 0;
}

/*
** Read part of the data associated with cursor pCur.  Exactly
** "amt" bytes will be transfered into pBuf[].  The transfer
** begins at "offset".
**
** Return SQLITE_OK on success or an error code if anything goes
** wrong.  An error is returned if "offset+amt" is larger than
** the available payload.
*/
int sqlite3BtreeData(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
  int res;
  DTREELOG("BtCursor %p offset %u amt %u", pCur, offset, amt);
  assert(pCur->eState == CURSOR_VALID || pCur->eState == CURSOR_DIRECT);
  assert(cursorHoldsMutex(pCur));
  if (!pCur->data.isset()){
    res = DtReadData(pCur);
    if (res){ DTREELOG("  return %d", SQLITE_IOERR); return SQLITE_IOERR; }
  }
  assert(offset+amt+sizeof(DataHeader) <= (u32) pCur->data->len);
  assert(pCur->data->type==0);
  memcpy(pBuf, pCur->data->u.buf+offset+sizeof(DataHeader), amt);
  DTREELOG("  return %d", 0);
  return 0;
}

/*
** Argument pCsr must be a cursor opened for writing on an 
** INTKEY table currently pointing at a valid table entry. 
** This function modifies the data stored as part of that entry.
**
** Only the data content may only be modified, it is not possible to 
** change the length of the data stored. If this function is called with
** parameters that attempt to write past the end of the existing data,
** no modifications are made and SQLITE_CORRUPT is returned.
*/
int sqlite3BtreePutData(BtCursor *pCur, u32 offset, u32 amt, void *z){
  int rc, res;
  DTREELOG("BtCursor %p offset %u amt %u", pCur, offset, amt);
  assert(cursorHoldsMutex(pCur));
  assert(sqlite3_mutex_held(pCur->pBtree->db->mutex));

  rc = restoreCursorPosition(pCur);
  if (rc){ DTREELOG("  return %d", rc); return rc; }
  assert(pCur->eState!=CURSOR_REQUIRESEEK);
  if (pCur->eState!=CURSOR_VALID && pCur->eState!=CURSOR_DIRECT){
    DTREELOG("  return %d", SQLITE_ABORT);
    return SQLITE_ABORT;
  }

  /* Check some assumptions: 
  **   (a) the cursor is open for writing,
  **   (b) there is a read/write transaction open,
  **   (c) the connection holds a write-lock on the table (if required),
  **   (d) there are no conflicting read-locks, and
  **   (e) the cursor points at a valid row of an intKey table.
  */
  if (!pCur->wrFlag){
    DTREELOG("  return %d", SQLITE_READONLY);
    return SQLITE_READONLY;
  }
  assert(!pCur->pBt->readOnly && pCur->pBt->inTransaction==TRANS_WRITE);

  if (!pCur->data.isset()){
    res = DtReadData(pCur);
    if (res){ DTREELOG("  return %d", SQLITE_IOERR); return SQLITE_IOERR; }
  }
  
  Valbuf *vb = new Valbuf(*pCur->data);
  Ptr<Valbuf> vbuf = vb;
  assert(offset+amt+sizeof(DataHeader) <= (u32) pCur->data->len);
  memcpy(vbuf->u.buf+offset+sizeof(DataHeader), (char*) z, amt);

  int levelleaf = pCur->levelLeaf;
  int index = pCur->nodeIndex[levelleaf];
  i64 nkey = pCur->node[levelleaf].Cells()[index].nKey;

  assert(pCur->data->type==0);
  res = DtWriteData(pCur, nkey, (char*) pCur->data->u.buf+sizeof(DataHeader),
                    pCur->data->len-sizeof(DataHeader)); // write to KV store
  if (res){ DTREELOG("  return %d", SQLITE_IOERR); return SQLITE_IOERR; }
  pCur->data = vbuf;

  DTREELOG("  return %d", 0);
  return 0;
}

// --------------------------------------------------------------------------

/*
** Change the way data is synced to disk in order to increase or decrease
** how well the database resists damage due to OS crashes and power
** failures.  Level 1 is the same as asynchronous (no syncs() occur and
** there is a high probability of damage)  Level 2 is the default.  There
** is a very low but non-zero probability of damage.  Level 3 reduces the
** probability of damage to near zero but with a write performance reduction.
*/
int sqlite3BtreeSetSafetyLevel(
  Btree *p,              /* The btree to set the safety level on */
  int level,             /* PRAGMA synchronous.  1=OFF, 2=NORMAL, 3=FULL */
  int fullSync,          /* PRAGMA fullfsync. */
  int ckptFullSync       /* PRAGMA checkpoint_fullfync */
){
  // do nothing, this function passes through to the pager,
  // but there is no pager
  DTREELOG("btree %p level %d fullsync %d ckptfullsync %d", p, level,
           fullSync, ckptFullSync);
  return 0;
}

/*
** Return TRUE if the given btree is set to safety level 1.  In other
** words, return TRUE if no sync() occurs on the disk files.
*/
int sqlite3BtreeSyncDisabled(Btree *p){
  DTREELOG("btree %p", p);
  return 0;   /* sync is never disabled */
}

/*
** Change the default pages size and the number of reserved bytes per page.
** Or, if the page size has already been fixed, return SQLITE_READONLY 
** without changing anything.
**
** The page size must be a power of 2 between 512 and 65536.  If the page
** size supplied does not meet this constraint then the page size is not
** changed.
**
** Page sizes are constrained to be a power of two so that the region
** of the database file used for locking (beginning at PENDING_BYTE,
** the first byte past the 1GB boundary, 0x40000000) needs to occur
** at the beginning of a page.
**
** If parameter nReserve is less than zero, then the number of reserved
** bytes per page is left unchanged.
**
** If the iFix!=0 then the pageSizeFixed flag is set so that the page size
** and autovacuum mode can no longer be changed.
*/
int sqlite3BtreeSetPageSize(Btree *p, int pageSize, int nReserve, int iFix){
  int rc = 0;
  BtShared *pBt = p->pBt;

  DTREELOG("btree %p pagesize %d nreserve %d ifix %d", p, pageSize, nReserve,
           iFix);
  assert(nReserve>=-1 && nReserve<=255);
  sqlite3BtreeEnter(p);
  if (pBt->pageSizeFixed){
    sqlite3BtreeLeave(p);
    DTREELOG("  return %d", SQLITE_READONLY);
    return SQLITE_READONLY;
  }
  if (nReserve<0){
    nReserve = pBt->pageSize - pBt->usableSize;
  }
  assert(nReserve>=0 && nReserve<=255);
  if (pageSize>=512 && pageSize<=SQLITE_MAX_PAGE_SIZE &&
        ((pageSize-1)&pageSize)==0){
    assert((pageSize & 7)==0);
    assert(!pBt->pCursor); /* OLD assert(!pBt->pPage1 && !pBt->pCursor); */
    pBt->pageSize = (u32)pageSize;
    /* OLD freeTempSpace(pBt); */
  }
  /* rc = sqlite3PagerSetPagesize(pBt->pPager, &pBt->pageSize, nReserve); */
  rc = 0;
  pBt->usableSize = pBt->pageSize - (u16)nReserve;
  if (iFix) pBt->pageSizeFixed = 1;
  sqlite3BtreeLeave(p);
  DTREELOG("  return %d", rc);
  return rc;
}

/*
** Return the currently defined page size
*/
int sqlite3BtreeGetPageSize(Btree *p){
  DTREELOG("btree %p", p);
  return p->pBt->pageSize;
}


/*
** Set the maximum page count for a database if mxPage is positive.
** No changes are made if mxPage is 0 or negative.
** Regardless of the value of mxPage, return the maximum page count.
*/
int sqlite3BtreeMaxPageCount(Btree *p, int mxPage){
  DTREELOG("btree %p mxPage %d", p, mxPage);
  return mxPage; /* this is a pager passthrough function, do nothing */
}

/*
** Return the size of the database file in pages. If there is any kind of
** error, return ((unsigned int)-1).
*/
static Pgno btreePagecount(BtShared *pBt){
  return pBt->nPage;
}
u32 sqlite3BtreeLastPage(Btree *p){
  assert(sqlite3BtreeHoldsMutex(p));
  assert(((p->pBt->nPage)&0x8000000)==0);
  DTREELOG("btree %p", p);
  return (int)btreePagecount(p->pBt);
}

/*
** Set the secureDelete flag if newFlag is 0 or 1.  If newFlag is -1,
** then make no changes.  Always return the value of the secureDelete
** setting after the change.
*/
int sqlite3BtreeSecureDelete(Btree *p, int newFlag){
  int b;
  DTREELOG("btree %p newFlag %d", p, newFlag);
  if (p==0) return 0;
  sqlite3BtreeEnter(p);
  if (newFlag>=0){
    p->pBt->secureDelete = (newFlag!=0) ? 1 : 0;
  } 
  b = p->pBt->secureDelete;
  sqlite3BtreeLeave(p);
  return b;
}

/*
** Return the number of bytes of space at the end of every page that
** are intentionally left unused.  This is the "reserved" space that is
** sometimes used by extensions.
*/
int sqlite3BtreeGetReserve(Btree *p){
  int n;
  DTREELOG("btree %p", p);
  sqlite3BtreeEnter(p);
  n = p->pBt->pageSize - p->pBt->usableSize;
  sqlite3BtreeLeave(p);
  return n;
}


/*
** Change the 'auto-vacuum' property of the database. If the 'autoVacuum'
** parameter is non-zero, then auto-vacuum mode is enabled. If zero, it
** is disabled. The default value for the auto-vacuum property is 
** determined by the SQLITE_DEFAULT_AUTOVACUUM macro.
*/
int sqlite3BtreeSetAutoVacuum(Btree *p, int autoVacuum){
#ifdef SQLITE_OMIT_AUTOVACUUM
  return SQLITE_READONLY;
#else
  BtShared *pBt = p->pBt;
  int rc = SQLITE_OK;
  u8 av = (u8)autoVacuum;

  DTREELOG("btree %p autoVacuum %d", p, autoVacuum);
  
  sqlite3BtreeEnter(p);
  if (pBt->pageSizeFixed && (av ?1:0)!=pBt->autoVacuum){
    rc = SQLITE_READONLY;
  }else{
    pBt->autoVacuum = av ?1:0;
    pBt->incrVacuum = av==2 ?1:0;
  }
  sqlite3BtreeLeave(p);
  return rc;
#endif
}

/*
** Return the value of the 'auto-vacuum' property. If auto-vacuum is 
** enabled 1 is returned. Otherwise 0.
*/
int sqlite3BtreeGetAutoVacuum(Btree *p){
#ifdef SQLITE_OMIT_AUTOVACUUM
  return BTREE_AUTOVACUUM_NONE;
#else
  int rc;
  DTREELOG("btree %p", p);
  
  sqlite3BtreeEnter(p);
  rc = (
    (!p->pBt->autoVacuum)?BTREE_AUTOVACUUM_NONE:
    (!p->pBt->incrVacuum)?BTREE_AUTOVACUUM_FULL:
    BTREE_AUTOVACUUM_INCR
 );
  sqlite3BtreeLeave(p);
  return rc;
#endif
}

/*
** Attempt to start a new transaction. A write-transaction
** is started if the second argument is nonzero, otherwise a read-
** transaction.  If the second argument is 2 or more and exclusive
** transaction is started, meaning that no other process is allowed
** to access the database.  A preexisting transaction may not be
** upgraded to exclusive by calling this routine a second time - the
** exclusivity flag only works for a new transaction.
**
** A write-transaction must be started before attempting any 
** changes to the database.  None of the following routines 
** will work unless a transaction is started first:
**
**      sqlite3BtreeCreateTable()
**      sqlite3BtreeCreateIndex()
**      sqlite3BtreeClearTable()
**      sqlite3BtreeDropTable()
**      sqlite3BtreeInsert()
**      sqlite3BtreeDelete()
**      sqlite3BtreeUpdateMeta()
**
** If an initial attempt to acquire the lock fails because of lock contention
** and the database was previously unlocked, then invoke the busy handler
** if there is one.  But if there was previously a read-lock, do not
** invoke the busy handler - just return SQLITE_BUSY.  SQLITE_BUSY is 
** returned when there is already a read-lock in order to avoid a deadlock.
**
** Suppose there are two processes A and B.  A has a read lock and B has
** a reserved lock.  B tries to promote to exclusive but is blocked because
** of A's read lock.  A tries to promote to reserved but is blocked by B.
** One or the other of the two processes must give way or there can be
** no progress.  By returning SQLITE_BUSY and not invoking the busy callback
** when A already has a read lock, we encourage A to give up and let B
** proceed.
*/
int sqlite3BtreeBeginTrans(Btree *p, int wrflag){
  //sqlite3 *pBlock = 0;
  BtShared *pBt = p->pBt;
  int rc = SQLITE_OK;
  bool remote;

  DTREELOG("btree %p wrflag %d", p, wrflag);
  
  sqlite3BtreeEnter(p);
  btreeIntegrity(p);

  /* If the btree is already in a write-transaction, or it
  ** is already in a read-transaction and a read-transaction
  ** is requested, this is a no-op.
  */
  if (p->inTrans==TRANS_WRITE || (p->inTrans==TRANS_READ && !wrflag)){
    goto trans_begun;
  }

  /* Write transactions are not possible on a read-only database */
  if (pBt->readOnly && wrflag){
    rc = SQLITE_READONLY;
    goto trans_begun;
  }

  remote = !isDBIdEphemeral(pBt->KVdbid);
  if (p->tx){
    freeTx(p->tx);
    p->tx = 0;
  }
  rc = beginTx(&p->tx, remote);

  if (rc==SQLITE_OK){
    if (p->inTrans==TRANS_NONE){
      pBt->nTransaction++;
    }
    p->inTrans = (wrflag?TRANS_WRITE:TRANS_READ);
    if (p->inTrans>pBt->inTransaction){
      pBt->inTransaction = p->inTrans;
    }
  }

trans_begun:
  btreeIntegrity(p);
  sqlite3BtreeLeave(p);
  DTREELOG("  return %d", rc);
  return rc;
}

static void btreeEndTransaction(Btree *p){
  BtShared *pBt = p->pBt;
  assert(sqlite3BtreeHoldsMutex(p));

  if (p->inTrans>TRANS_NONE && p->db->activeVdbeCnt>1){
    /* If there are other active statements that belong to this database
    ** handle, downgrade to a read-only transaction. The other statements
    ** may still be reading from the database.  */
    p->inTrans = TRANS_READ;
  }else{
    /* If the handle had any kind of transaction open, decrement the 
    ** transaction count of the shared btree. If the transaction count 
    ** reaches 0, set the shared state to TRANS_NONE. The unlockBtreeIfUnused()
    ** call below will unlock the pager.  */
    if (p->inTrans!=TRANS_NONE){
      pBt->nTransaction--;
      if (0==pBt->nTransaction){
        pBt->inTransaction = TRANS_NONE;
      }
    }

    /* Set the current transaction state to TRANS_NONE and unlock the 
    ** pager if this call closed the only read or write transaction.  */
    p->inTrans = TRANS_NONE;
  }

  btreeIntegrity(p);
}

/*
** Do both phases of a commit.
*/
int sqlite3BtreeCommit(Btree *p){
  int res, rc=0;
  DTREELOG("btree %p", p);

  sqlite3BtreeEnter(p);
  if (p->inTrans == TRANS_WRITE){
    res = commitTx(p->tx);
    if (res){
      if (res < 0) rc = SQLITE_CORRUPT;
      else if (res == 1) rc = SQLITE_BUSY;
      else if (res == 3) rc = SQLITE_PROTOCOL;
      else rc = SQLITE_INTERNAL;
    }
  }
  if (p->inTrans>TRANS_NONE && p->db->activeVdbeCnt>1){
    /* If there are other active statements that belong to this database
    ** handle, downgrade to a read-only transaction. The other statements
    ** may still be reading from the database.  */
    //downgradeAllSharedCacheTableLocks(p);
    p->inTrans = TRANS_READ;
    if (p->tx){
      freeTx(p->tx);
      p->tx = 0;
    }
    //assert(0); // this won't work
  }else{
    /* If the handle had any kind of transaction open, decrement the 
    ** transaction count of the shared btree. If the transaction count 
    ** reaches 0, set the shared state to TRANS_NONE. The unlockBtreeIfUnused()
    ** call below will unlock the pager.  */
    if (p->inTrans!=TRANS_NONE){
      //clearAllSharedCacheTableLocks(p);
      p->pBt->nTransaction--;
      if (0==p->pBt->nTransaction){
        p->pBt->inTransaction = TRANS_NONE;
      }
    }
    if (p->tx){
      freeTx(p->tx);
      p->tx = 0;
    }

    /* Set the current transaction state to TRANS_NONE and unlock the 
    ** pager if this call closed the only read or write transaction.  */
    p->inTrans = TRANS_NONE;
  }

  btreeEndTransaction(p);
  sqlite3BtreeLeave(p);
  DTREELOG("  return %d", rc);
  return rc;
}

/*
** Rollback the transaction in progress.  All cursors will be
** invalided by this operation.  Any attempt to use a cursor
** that was open at the beginning of this operation will result
** in an error.
**
** This will release the write lock on the database file.  If there
** are no active cursors, it also releases the read lock.
*/
int sqlite3BtreeRollback(Btree *p){
  int rc;
  BtShared *pBt = p->pBt;

  DTREELOG("btree %p", p);
  sqlite3BtreeEnter(p);
  rc = saveAllCursors(pBt, 0, 0);
#ifndef SQLITE_OMIT_SHARED_CACHE
  if (rc!=SQLITE_OK){
    /* This is a horrible situation. An IO or malloc() error occurred whilst
    ** trying to save cursor positions. If this is an automatic rollback (as
    ** the result of a constraint, malloc() failure or IO error) then 
    ** the cache may be internally inconsistent (not contain valid trees) so
    ** we cannot simply return the error to the caller. Instead, abort 
    ** all queries that may be using any of the cursors that failed to save.
    */
    sqlite3BtreeTripAllCursors(p, rc);
  }
#endif

  if (p->inTrans==TRANS_WRITE){
    assert(TRANS_WRITE==pBt->inTransaction);

    /* abort KV transaction */
    rc = abortTx(p->tx);

    /* The rollback may have destroyed the pPage1->aData value.  So
    ** call btreeGetPage() on page 1 again to make
    ** sure pPage1->aData is set correctly. */
    //if (btreeGetPage(pBt, 1, &pPage1, 0)==SQLITE_OK){
    //  int nPage = get4byte(28+(u8*)pPage1->aData);
    //  testcase(nPage==0);
    //  if (nPage==0) sqlite3PagerPagecount(pBt->pPager, &nPage);
    //  testcase(pBt->nPage!=nPage);
    //  pBt->nPage = nPage;
    //  releasePage(pPage1);
    //}
    pBt->inTransaction = TRANS_READ;
  }
  if (p->tx){
    freeTx(p->tx);
    p->tx = 0;
  }

  btreeEndTransaction(p);
  sqlite3BtreeLeave(p);
  DTREELOG("  return %d", rc);
  return rc;
}

/*
** Start a statement subtransaction. The subtransaction can can be rolled
** back independently of the main transaction. You must start a transaction 
** before starting a subtransaction. The subtransaction is ended automatically 
** if the main transaction commits or rolls back.
**
** Statement subtransactions are used around individual SQL statements
** that are contained within a BEGIN...COMMIT block.  If a constraint
** error occurs within the statement, the effect of that one statement
** can be rolled back without having to rollback the entire transaction.
**
** A statement sub-transaction is implemented as an anonymous savepoint. The
** value passed as the second parameter is the total number of savepoints,
** including the new anonymous savepoint, open on the B-Tree. i.e. if there
** are no active savepoints and no other statement-transactions open,
** iStatement is 1. This anonymous savepoint can be released or rolled back
** using the sqlite3BtreeSavepoint() function.
*/
int sqlite3BtreeBeginStmt(Btree *p, int iStatement){
  DTREELOG("btree %p iStatement %d", p, iStatement);
  int res;
  assert(p->tx);
  res = beginSubTx(p->tx, iStatement);
  if (res) return SQLITE_INTERNAL;
  return SQLITE_OK;
}

// begin hack for deciding the number of cells when creating a table
// When parsing the CREATE TABLE statement, we call BSkipHackSaveTableName
// to save the name of the table (the name is split into name1 and name2 if
// it has a period, otherwise name2=0). Later, when we create the root
// node of the table, we can consult the saved table name to decide
// how many cells to use.
//    A better solution would be to decide these parameters at a higher
// level (eg, soon after the CREATE TABLE statement is parsed), and then
// pass down this information until we get it. For now, we just implement
// the hack.
Tlocal static char *SaveName = 0;

void BSkipHackSaveTableName(const char *name){
  if (SaveName) delete [] SaveName;

  if (name){
    SaveName = new char[strlen(name)+1];
    strcpy(SaveName, name);
  } else SaveName=0;
}

/*
** Return non-zero if a transaction is active.
*/
int sqlite3BtreeIsInTrans(Btree *p){
  assert(p==0 || sqlite3_mutex_held(p->db->mutex));
  DTREELOG("btree %p", p);
  return (p && (p->inTrans==TRANS_WRITE));
}

/*
** Return non-zero if a read (or write) transaction is active.
*/
int sqlite3BtreeIsInReadTrans(Btree *p){
  DTREELOG("btree %p", p);
  assert(p);
  assert(sqlite3_mutex_held(p->db->mutex));
  return p->inTrans!=TRANS_NONE;
}

int sqlite3BtreeIsInBackup(Btree *p){
  DTREELOG("btree %p", p);
  assert(p);
  assert(sqlite3_mutex_held(p->db->mutex));
  return p->nBackup!=0;
}

/*
** This function returns a pointer to a blob of memory associated with
** a single shared-btree. The memory is used by client code for its own
** purposes (for example, to store a high-level schema associated with 
** the shared-btree). The btree layer manages reference counting issues.
**
** The first time this is called on a shared-btree, nBytes bytes of memory
** are allocated, zeroed, and returned to the caller. For each subsequent 
** call the nBytes parameter is ignored and a pointer to the same blob
** of memory returned. 
**
** If the nBytes parameter is 0 and the blob of memory has not yet been
** allocated, a null pointer is returned. If the blob has already been
** allocated, it is returned as normal.
**
** Just before the shared-btree is closed, the function passed as the 
** xFree argument when the memory allocation was made is invoked on the 
** blob of allocated memory. The xFree function should not call sqlite3_free()
** on the memory, the btree layer does that.
*/
void *sqlite3BtreeSchema(Btree *p, int nBytes, void(*xFree)(void *)){
  BtShared *pBt = p->pBt;
  DTREELOG("btree %p nbytes %d", p, nBytes);
  
  sqlite3BtreeEnter(p);
  if (!pBt->pSchema && nBytes){
    pBt->pSchema = sqlite3DbMallocZero(0, nBytes);
    pBt->xFreeSchema = xFree;
  }
  sqlite3BtreeLeave(p);
  return pBt->pSchema;
}

/*
** Return SQLITE_LOCKED_SHAREDCACHE if another user of the same shared 
** btree as the argument handle holds an exclusive lock on the 
** sqlite_master table. Otherwise SQLITE_OK.
*/
int sqlite3BtreeSchemaLocked(Btree *p){
  DTREELOG("btree %p", p);
  return 0; /* no locks used here, so schema is never locked */
}


int sqlite3BtreeLockTable(Btree *p, u64 iTable, u8 isWriteLock){
  DTREELOG("btree %p itable %llx iswritelock %d", p, (long long)iTable,
           (int) isWriteLock);
  return 0; /* nothing to do, there are no locks for tables */
}


/*
** The second argument to this function, op, is always SAVEPOINT_ROLLBACK
** or SAVEPOINT_RELEASE. This function either releases or rolls back the
** savepoint identified by parameter iSavepoint, depending on the value 
** of op.
**
** Normally, iSavepoint is greater than or equal to zero. However, if op is
** SAVEPOINT_ROLLBACK, then iSavepoint may also be -1. In this case the 
** contents of the entire transaction are rolled back. This is different
** from a normal transaction rollback, as no locks are released and the
** transaction remains open.
*/
int sqlite3BtreeSavepoint(Btree *p, int op, int iSavepoint){
  DTREELOG("btree %p op %d iSavepoint %d", p, op, iSavepoint);
  int res;
  assert(p->tx);
  
  switch(op){
  case SAVEPOINT_RELEASE:
    res = releaseSubTx(p->tx, iSavepoint);
    break;
  case SAVEPOINT_ROLLBACK:
    res = abortSubTx(p->tx, iSavepoint);
    break;
  default:
    assert(0);
    res = -1;
    break;
  }
  if (res) return SQLITE_INTERNAL;
  else return SQLITE_OK;
}

/*
** Return the full pathname of the underlying database file.
**
** The pager filename is invariant as long as the pager is
** open so it is safe to access without the BtShared mutex.
*/
const char *sqlite3BtreeGetFilename(Btree *p){
  DTREELOG("btree %p", p);
  return "KVSTORE_DATABASE_FILENAME";
}

/*
** Return the pathname of the journal file for this database. The return
** value of this routine is the same regardless of whether the journal file
** has been created or not.
**
** The pager journal filename is invariant as long as the pager is
** open so it is safe to access without the BtShared mutex.
*/
const char *sqlite3BtreeGetJournalname(Btree *p){
  DTREELOG("btree %p", p);
  return "KVSTORE_JOURNAL_FILENAME";
}

/*
** A write-transaction must be opened before calling this function.
** It performs a single unit of work towards an incremental vacuum.
**
** If the incremental vacuum is finished after this function has run,
** SQLITE_DONE is returned. If it is not finished, but no error occurred,
** SQLITE_OK is returned. Otherwise an SQLite error code. 
*/
int sqlite3BtreeIncrVacuum(Btree *p){
  DTREELOG("btree %p", p);
  return 0;  /* incremental vacuum not implemented */
}

/*
** This routine sets the state to CURSOR_FAULT and the error
** code to errCode for every cursor on BtShared that pBtree
** references.
**
** Every cursor is tripped, including cursors that belong
** to other database connections that happen to be sharing
** the cache with pBtree.
**
** This routine gets called when a rollback occurs.
** All cursors using the same cache must be tripped
** to prevent them from trying to use the btree after
** the rollback.  The rollback may have deleted tables
** or moved root pages, so it is not sufficient to
** save the state of the cursor.  The cursor must be
** invalidated.
*/
void sqlite3BtreeTripAllCursors(Btree *pBtree, int errCode){
  BtCursor *p;
  DTREELOG("btree %p errCode %d", pBtree, errCode);
  sqlite3BtreeEnter(pBtree);
  for(p=pBtree->pBt->pCursor; p; p=p->pNext){
    sqlite3BtreeClearCursor(p);
    p->eState = CURSOR_FAULT;
    p->cursorFaultError = errCode;
  }
  sqlite3BtreeLeave(pBtree);
}

/*
** This function may only be called if the b-tree connection already
** has a read or write transaction open on the database.
**
** Read the meta-information out of a database file.  Meta[0]
** is the number of free pages currently in the database.  Meta[1]
** through meta[15] are available for use by higher layers.  Meta[0]
** is read-only, the others are read/write.
** 
** The schema layer numbers meta values differently.  At the schema
** layer (and the SetCookie and ReadCookie opcodes) the number of
** free pages is not visible.  So Cookie[0] is the same as Meta[1].
*/
int sqlite3BtreeGetMeta(Btree *p, int idx, u32 *pMeta){
  BtShared *pBt = p->pBt;
  int len, res, rc;

  DTREELOG("btree %p idx %d", p, idx);
  rc = SQLITE_OK;
  sqlite3BtreeEnter(p);
  assert(p->inTrans>TRANS_NONE);
  assert(idx>=0 && idx<BTREE_LAST_METADATA);

  res = 0;
#if YS_SCHEMA_CACHE == 1
  // this option reads the metadata only once, and therefore does not
  // support changes in the metadata (= changes in table schemas)
  if (!pBt->pPage1) res = ReadDbMetadata(p->tx, pBt->KVdbid, &len,
                                         (char**) &pBt->pPage1);
  if (res) rc = SQLITE_IOERR;
  else *pMeta = pBt->pPage1->metadata[idx];
#else
  // If YS_SCHEMA_CACHE==0, we always read the metadata.
  // If YS_SCHEMA_CACHE==2, we already read the metadata, but Gaia
  //   serves those reads from a consistent cache
  if (pBt->pPage1){ free(pBt->pPage1); pBt->pPage1=0; }
  res = ReadDbMetadata(p->tx, pBt->KVdbid, &len, (char**) &pBt->pPage1);
  if (res) rc = SQLITE_IOERR;
  else *pMeta = pBt->pPage1->metadata[idx];
#endif  

  sqlite3BtreeLeave(p);
  DTREELOG("  return %d", rc);
  
  return rc;
}

/*
** Write meta-information back into the database.  Meta[0] is
** read-only and may not be written.
*/
int sqlite3BtreeUpdateMeta(Btree *p, int idx, u32 iMeta){
  BtShared *pBt = p->pBt;
  int len, res, rc;
  
  DTREELOG("btree %p idx %d iMeta %u", p, idx, iMeta);
  assert(idx>=0 && idx<BTREE_LAST_METADATA);
  assert(p->inTrans==TRANS_WRITE);

  rc = SQLITE_OK;
  sqlite3BtreeEnter(p);
  res = 0;
  if (pBt->pPage1){ free(pBt->pPage1); pBt->pPage1=0; }
  res = ReadDbMetadata(p->tx, pBt->KVdbid, &len, (char**) &pBt->pPage1);
  if (res){ rc = SQLITE_IOERR; goto end; }
  pBt->pPage1->metadata[idx] = iMeta;
  res = WriteDbMetadata(p->tx, pBt->KVdbid, pBt->pPage1);
  if (res){ rc = SQLITE_IOERR; goto end; }
 end:
  sqlite3BtreeLeave(p);
  DTREELOG("  return %d", rc);
  return rc;
}

/*
** Return the size of a BtCursor object in bytes.
**
** This interfaces is needed so that users of cursors can preallocate
** sufficient storage to hold a cursor.  The BtCursor object is opaque
** to users so they cannot do the sizeof() themselves - they must call
** this routine.
*/
int sqlite3BtreeCursorSize(void){
  DTREELOG("");
  return ROUND8(sizeof(BtCursor));
}

/*
** Return TRUE if the cursor is not pointing at an entry of the table.
**
** TRUE will be returned after a call to sqlite3BtreeNext() moves
** past the last entry in the table or sqlite3BtreePrev() moves past
** the first entry.  TRUE is also returned if the table is empty.
*/
int sqlite3BtreeEof(BtCursor *pCur){
  /* TODO: What if the cursor is in CURSOR_REQUIRESEEK but all table entries
  ** have been deleted? This API will need to change to return an error code
  ** as well as the boolean result value.
  */
  DTREELOG("BtCursor %p", pCur);
  return (pCur->eState != CURSOR_VALID && pCur->eState != CURSOR_DIRECT);
}

/*
** Initialize memory that will be converted into a BtCursor object.
**
** The simple approach here would be to memset() the entire object
** to zero.  But it turns out that the stuff at the end of the array
** need not be zeroed and they are large, so we can save a lot
** of run-time by skipping the initialization of those elements.
*/
void sqlite3BtreeCursorZero(BtCursor *p){
  DTREELOG("BtCursor %p", p);
  memset(p, 0, OFFSETOF(BtCursor, levelLeaf));
  //p->Node.Init();
}

void DtFreeCursorFields(BtCursor *pCur); /* forward definition */

/*
** Close a cursor.  The read lock on the database file is released
** when the last cursor is closed.
*/
int sqlite3BtreeCloseCursor(BtCursor *pCur){
  Btree *pBtree = pCur->pBtree;
  
  DTREELOG("BtCursor %p", pCur);
  if (pBtree){
    BtShared *pBt = pCur->pBt;
    sqlite3BtreeEnter(pBtree);
    sqlite3BtreeClearCursor(pCur);
    if (pCur->pPrev){
      pCur->pPrev->pNext = pCur->pNext;
    }else{
      pBt->pCursor = pCur->pNext;
    }
    if (pCur->pNext){
      pCur->pNext->pPrev = pCur->pPrev;
    }

    DtFreeCursorFields(pCur);

    /* sqlite3_free(pCur); */
    sqlite3BtreeLeave(pBtree);
  }
  return 0;
}

#ifdef SQLITE_DEBUG
static int cursorHoldsMutex(BtCursor *p){
  return sqlite3_mutex_held(p->pBt->mutex);
}
#endif

/*
** Save the current cursor position in the variables BtCursor.nKey 
** and BtCursor.pKey. The cursor's state is set to CURSOR_REQUIRESEEK, and
** the buffer is released. This function is normally called when the caller
** knows that a subsequent operation will possibly mess up with cursor.
**
** The caller must ensure that the cursor is valid (has eState==CURSOR_VALID)
** prior to calling this routine.  
*/
static int saveCursorPosition(BtCursor *pCur){
  int rc;

  assert(CURSOR_VALID==pCur->eState);
  assert(0==pCur->savepKey);
  assert(cursorHoldsMutex(pCur));

  rc = sqlite3BtreeKeySize(pCur, &pCur->savenKey);
  assert(rc==0);  /* KeySize() cannot fail */

  /* If this is an intKey table, then the above call to BtreeKeySize()
  ** stores the integer key in pCur->nKey. In this case this value is
  ** all that is required. Otherwise, if pCur is not open on an intKey
  ** table, then malloc space for and store the pCur->nKey bytes of key 
  ** data.
  */
  if (!pCur->intKey){
    void *pKey = sqlite3Malloc((int)pCur->savenKey);
    if (pKey){
      rc = sqlite3BtreeKey(pCur, 0, (int)pCur->savenKey, pKey);
      if (rc==0) pCur->savepKey = pKey;
      else sqlite3_free(pKey);
    }else rc = SQLITE_NOMEM;
  }
  assert(!pCur->intKey || !pCur->savepKey);

  if (rc==0){
    pCur->data=0;
    pCur->eState = CURSOR_REQUIRESEEK;
  }

  return rc;
}


/*
** Save the positions of all cursors (except pExcept) that are open on
** the table iDbidTable. Usually, this is called just before cursor
** pExcept is used to modify the table (BtreeDelete() or BtreeInsert()).
*/
static int saveAllCursors(BtShared *pBt, u64 cidTable, BtCursor *pExcept){
  BtCursor *p;
  assert(sqlite3_mutex_held(pBt->mutex));
  assert(pExcept==0 || pExcept->pBt==pBt);
  for(p=pBt->pCursor; p; p=p->pNext){
    if (p!=pExcept && (0==cidTable || p->rootCid==cidTable) && 
        p->eState==CURSOR_VALID){
      int rc = saveCursorPosition(p);
      if (rc) return rc;
    }
  }
  return 0;
}


/*
** Restore the cursor to the position it was in (or as close to as possible)
** when saveCursorPosition() was called. Note that this call deletes the 
** saved position info stored by saveCursorPosition(), so there can be
** at most one effective restoreCursorPosition() call after each 
** saveCursorPosition().
** This function should be called only if cursor eState is CURSOR_REQUIRESEEK
** or CURSOR_FAULT. Macro below deals with the case when eState is CURSOR_VALID
** or CURSOR_INVALID.
*/
static int dtreeRestoreCursorPosition(BtCursor *pCur){
  int rc;
  assert(cursorHoldsMutex(pCur));
  assert(pCur->eState>=CURSOR_REQUIRESEEK);
  if (pCur->eState==CURSOR_FAULT){
    return pCur->cursorFaultError;
  }
  pCur->eState = CURSOR_INVALID;
  
  rc = DtMovetoaux(pCur, pCur->savepKey, pCur->savenKey, 0, &pCur->skipNext,
                   true);
  if (rc==SQLITE_OK){
    sqlite3_free(pCur->savepKey);
    pCur->savepKey = 0;
    assert(pCur->eState==CURSOR_VALID || pCur->eState==CURSOR_INVALID);
  }
  return rc;
}

/*
** Determine whether or not a cursor has moved from the position it
** was last placed at.  Cursors can move when the row they are pointing
** at is deleted out from under them.
**
** Basically, if the cursor's eState is CURSOR_VALID or CURSOR_INVALID,
** then cursor has not moved. If its eState is CURSOR_REQUIRESEEK,
** then try to restore cursor's position. If successful, then cursor has
** not moved, otherwise it has.
**
** This routine returns an error code if something goes wrong.  The
** integer *pHasMoved is set to one if the cursor has moved and 0 if not.
*/
int sqlite3BtreeCursorHasMoved(BtCursor *pCur, int *pHasMoved){
  int rc;
  DTREELOG("BtCursor %p", pCur);
  rc = restoreCursorPosition(pCur);
  if (rc){ *pHasMoved = 1; return rc; }
  if ((pCur->eState != CURSOR_VALID && pCur->eState != CURSOR_DIRECT) ||
      pCur->skipNext!=0) *pHasMoved = 1;
  else *pHasMoved = 0;
  return 0;
}

/*
** Set the cached rowid value of every cursor in the same database file
** as pCur and having the same root page number as pCur.  The value is
** set to iRowid.
**
** Only positive rowid values are considered valid for this cache.
** The cache is initialized to zero, indicating an invalid cache.
** A btree will work fine with zero or negative rowids.  We just cannot
** cache zero or negative rowids, which means tables that use zero or
** negative rowids might run a little slower.  But in practice, zero
** or negative rowids are very uncommon so this should not be a problem.
*/
void sqlite3BtreeSetCachedRowid(BtCursor *pCur, sqlite3_int64 iRowid){
  BtCursor *p;
  DTREELOG("BtCursor %p iRowid %lld", pCur, (long long)iRowid);
  for(p=pCur->pBt->pCursor; p; p=p->pNext){
    if (p->rootCid==pCur->rootCid) p->cachedRowid = iRowid;
  }
  assert(pCur->cachedRowid==iRowid);
}

/*
** Return the cached rowid for the given cursor.  A negative or zero
** return value indicates that the rowid cache is invalid and should be
** ignored.  If the rowid cache has never before been set, then a
** zero is returned.
*/
sqlite3_int64 sqlite3BtreeGetCachedRowid(BtCursor *pCur){
  DTREELOG("BtCursor %p", pCur);
  return pCur->cachedRowid;
}

/*
** This routine does a complete check of the given BTree file.  aRoot[] is
** an array of pages numbers were each page number is the root page of
** a table.  nRoot is the number of entries in aRoot.
**
** A read-only or read-write transaction must be opened before calling
** this function.
**
** Write the number of error seen in *pnErr.  Except for some memory
** allocation errors,  an error message held in memory obtained from
** malloc is returned if *pnErr is non-zero.  If *pnErr==0 then NULL is
** returned.  If a memory allocation error occurs, NULL is returned.
*/
char *sqlite3BtreeIntegrityCheck(
  Btree *p,     /* The btree to be checked */
  int *aRoot,   /* An array of root pages numbers for individual trees */
  int nRoot,    /* Number of entries in aRoot[] */
  int mxErr,    /* Stop reporting errors after this many */
  int *pnErr    /* Write number of errors seen to this variable */
){
  DTREELOG("Btree %p", p);
  *pnErr=0;
  return 0;
}

/*
** Return the pager associated with a BTree.  This routine is used for
** testing and debugging only.
*/
Pager *sqlite3BtreePager(Btree *p){
  DTREELOG("Btree %p", p);
  return 0;
}

/* 
** Set a flag on this cursor to cache the locations of pages from the 
** overflow list for the current row. This is used by cursors opened
** for incremental blob IO only.
**
** This function sets a flag only. The actual page location cache
** (stored in BtCursor.aOverflow[]) is allocated and used by function
** accessPayload() (the worker function for sqlite3BtreeData() and
** sqlite3BtreePutData()).
*/
void sqlite3BtreeCacheOverflow(BtCursor *pCur){
  DTREELOG("BtCursor %p", pCur);
}
 
/* free up allocated fields in cursor  */
/* inline */
void DtFreeCursorFields(BtCursor *pCur){
  int i;
  if (pCur->savepKey){ sqlite3_free(pCur->savepKey); pCur->savepKey=0; }
  pCur->data = 0;
  for (i=0; i < DTREE_MAX_LEVELS; ++i)
    pCur->node[i].raw = 0; // zero out smart pointers
}

/*
** Clear the current cursor position.
*/
void sqlite3BtreeClearCursor(BtCursor *pCur){
  DTREELOG("BtCursor %p", pCur);
  assert(cursorHoldsMutex(pCur));
  DtFreeCursorFields(pCur);
  pCur->savepKey = 0;
  pCur->eState = CURSOR_INVALID;
}
 
/*
** Set both the "read version" (single byte at byte offset 18) and 
** "write version" (single byte at byte offset 19) fields in the database
** header to iVersion.
*/
int sqlite3BtreeSetVersion(Btree *pBtree, int iVersion){
  BtShared *pBt = pBtree->pBt;
  int rc;
 
  DTREELOG("Btree %p iVersion %d", pBtree, iVersion);
  assert(pBtree->inTrans==TRANS_NONE);
  assert(iVersion==1 || iVersion==2);

  pBt->doNotUseWAL = (u8)(iVersion==1);

  rc = sqlite3BtreeBeginTrans(pBtree, 0);
  if (rc==SQLITE_OK){
    if (pBt->pPage1->readVersion!=(u8)iVersion ||
        pBt->pPage1->writeVersion !=(u8)iVersion){
      rc = sqlite3BtreeBeginTrans(pBtree, 2);
      if (rc==SQLITE_OK){
        /* rc = sqlite3PagerWrite(pBt->pPage1->pDbPage); */
        if (rc==SQLITE_OK){
          pBt->pPage1->readVersion = (u8)iVersion;
          pBt->pPage1->writeVersion = (u8)iVersion;
        }
        /* TODO: schedule write to page1. But this is not used */
      }
    }
  }

  pBt->doNotUseWAL = 0;
  return rc;
}
 
/*
** Return true if the given BtCursor is valid.  A valid cursor is one
** that is currently pointing to a row in a (non-empty) table.
** This is a verification routine is used only within assert() statements.
*/
int sqlite3BtreeCursorIsValid(BtCursor *pCur){
  DTREELOG("BtCursor %p", pCur);
  return pCur && (pCur->eState==CURSOR_VALID || pCur->eState==CURSOR_DIRECT);
}

/*
** Run a checkpoint on the Btree passed as the first argument.
**
** Return SQLITE_LOCKED if this or any other connection has an open 
** transaction on the shared-cache the argument Btree is connected to.
**
** Parameter eMode is one of SQLITE_CHECKPOINT_PASSIVE, FULL or RESTART.
*/
int sqlite3BtreeCheckpoint(Btree *p, int eMode, int *pnLog, int *pnCkpt){
  DTREELOG("Btree %p eMode %d", p, eMode);
  return SQLITE_IOERR;
}

/*
** Enable or disable the shared pager and schema features.
**
** This routine has no effect on existing database connections.
** The shared cache setting effects only future calls to
** sqlite3_open(), sqlite3_open16(), or sqlite3_open_v2().
*/
SQLITE_API int sqlite3_enable_shared_cache(int enable){
  DTREELOG("enable %d", enable);
  sqlite3GlobalConfig.sharedCacheEnabled = enable;
  return SQLITE_OK;
}

/*
** Close an open database and invalidate all cursors.
*/
int sqlite3BtreeClose(Btree *p){
  BtShared *pBt = p->pBt;
  BtCursor *pCur;

  DTREELOG("btree %p", p);

  /* Close all cursors opened via this handle.  */
  assert(sqlite3_mutex_held(p->db->mutex));
  sqlite3BtreeEnter(p);
  pCur = pBt->pCursor;
  while (pCur){
    BtCursor *pTmp = pCur;
    pCur = pCur->pNext;
    if (pTmp->pBtree==p){
      sqlite3BtreeCloseCursor(pTmp);
    }
  }

  if (pBt->pPage1){ free(pBt->pPage1); pBt->pPage1=0; }

  if (pBt->openFlags & BTREE_MEMORY){
    /* remove dbid from list of used ids */
    freeMemDBId(pBt->KVdbid);
  }

  /* Rollback any active transaction and free the handle structure.
  ** The call to sqlite3BtreeRollback() drops any table-locks held by
  ** this handle.
  */
  sqlite3BtreeRollback(p);
  sqlite3BtreeLeave(p);

  /* If there are still other outstanding references to the shared-btree
  ** structure, return now. The remainder of this procedure cleans 
  ** up the shared-btree.
  */
  assert(p->wantToLock==0 && p->locked==0);
  if(1){    /* !p->sharable || removeFromSharingList(pBt)){ */
    /* The pBt is no longer on the sharing list, so we can access
    ** it without having to hold the mutex.
    **
    ** Clean out and delete the BtShared object.
    */
    assert(!pBt->pCursor);
    /*sqlite3PagerClose(pBt->pPager); */
    if (pBt->xFreeSchema && pBt->pSchema){
      pBt->xFreeSchema(pBt->pSchema);
    }
    sqlite3DbFree(0, pBt->pSchema);
    sqlite3_free(pBt);
  }

#ifndef SQLITE_OMIT_SHARED_CACHE
  assert(p->wantToLock==0);
  assert(p->locked==0);
  if (p->pPrev) p->pPrev->pNext = p->pNext;
  if (p->pNext) p->pNext->pPrev = p->pPrev;
#endif

  sqlite3_free(p);
  return 0;
}

/*
** Change the limit on the number of pages allowed in the cache.
**
** The maximum number of cache pages is set to the absolute
** value of mxPage.  If mxPage is negative, the pager will
** operate asynchronously - it will not stop to do fsync()s
** to insure data is written to the disk surface before
** continuing.  Transactions still work if synchronous is off,
** and the database cannot be corrupted if this program
** crashes.  But if the operating system crashes or there is
** an abrupt power failure when synchronous is off, the database
** could be left in an inconsistent and unrecoverable state.
** Synchronous is on by default so database corruption is not
** normally a worry.
*/
int sqlite3BtreeSetCacheSize(Btree *p, int mxPage){
  /* do nothing, there is no cache yet */
  DTREELOG("btree %p mxPage %d", p, mxPage);
  return 0;
}

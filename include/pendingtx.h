//
// pendingtx.h
//
// In-memory information about pending transactions
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

#ifndef _PENDINGTX_H
#define _PENDINGTX_H

#include <map>
#include <list>
#include "tmalloc.h"

#include "debug.h"
#include "gaiatypes.h"
#include "util.h"

#include "supervalue.h"
#include "datastruct.h"
#include "gaiarpcauxfunc.h"
#include "record.h"
#include "datastructmt.h"

using namespace std;

class SingleLogEntryInMemory;

// information about a single write of a value
class TxWriteItem {
public:
  COid coid;
  int len;
  char *buf;        // pointer to buffer with data
  char *rpcrequest; // pointer to rpc request buffer; here so that it can be freed afterwards if alloctype=0
  int alloctype;    // way request buffer was allocated (so it can be freed properly later).
                    // 0=via UDP layer, 1=via malloc
  TxWriteItem(){ buf = rpcrequest = 0; }
  ~TxWriteItem(){
    switch(alloctype){
    case 0:
      free(rpcrequest);
      break;
    case 1:
      //delete [] buf;
      free((void*)buf);
      break;
    default: assert(0);
    }
  }
};

// information about a single write of a supervalue
class TxWriteSVItem {
            // anything from here to attrs (exclusive) gets saved to file literally by DiskStorage::writeCOidToFile
            // careful putting things here. A previous bug had temporary buffers here, which got saved
            // to disk and later got reloaded with random stuff, which overwrote the zero values
            // they were supposed to have. The destructor later tried to free those random buffers,
            // corrupting memory
public:
  COid coid;
  u16 nattrs;      // number of 64-bit attribute values
  u8  celltype;    // type of cells: 0=int, 1=nKey+pKey
                   // -------- end of fixed part -----------
                   // stuff from attrs below are serialized independently
                   // when reading/writing on disk

  u64 *attrs;      // value of attributes
  SkipListBK<ListCellPlus,int> cells; // parsed cells
                                  // note: the value in this skiplist is not used
  GKeyInfo *pki;   // all cells in above skiplist will point to this pki.
                   // If non-null, it is freed with free() in the destructor.
                   // This pki is initially empty, but we learn it later when we
                   // compare against a given ListCellPlus that comes in a ListAdd or
                   // ListDelRange RPC.
                   // The reason we do it this way is because sqlite does not include
                   // the KeyInfoPtr in the pnkey; it is supplied externally whenever
                   // it needs to compare cells. So, we must also supply it externally
                   // later.
                   // This field is the mechanism to do it: all cells have a pointer to
                   // this field, and we set this field later. Once we set it (function
                   // SetPkiSticky(), it remains with the value. That method will clone
                   // the KeyInfo (and so the object will own it) and will not change it
                   // again in subsequent calls).

  TxWriteSVItem(){ nattrs = 0; attrs = 0; pki = 0; ncelloids = lencelloids = 0; celloids = 0; }
  TxWriteSVItem(const TxWriteSVItem &r);
  ~TxWriteSVItem();
  TxWriteSVItem &operator=(const TxWriteSVItem &r){ assert(0); return *this; }

  void SetPkiSticky(GKeyInfo *k);

  char *GetCelloids(int &retncelloids, int &retlencelloids);

  // converts from a single interval type for both start and end of the interval to
  // two interval types, one for the start and one for the end
  static void convertOneIntervalTypeToTwoIntervalType(int intervaltype1, int &intervaltype2start, int &intervaltype2end);

  void printShort(COid coidtomatch);

private:
  // Cached celloid information. This is populated by PopulateCelloids(), and it should be
  // called only after the TxWriteSVItem has become immutable.
  int ncelloids;
  int lencelloids;
  char *celloids;
};

class TxListItem {
  COid coid;
public:
  TxListItem(const COid &coidinit) : coid(coidinit) {}
  int type;  // 0=ListAdd, 1=ListDelRange
  virtual ~TxListItem();
  void printShort(COid expectedcoid);
};

// information about a list add item
class TxListAddItem : public TxListItem {
private:
public:
  ListCellPlus item;
  TxListAddItem(const COid &coidinit, GKeyInfo *ki, const ListCell &c) :
    TxListItem(coidinit),
    item(c, ki) // copy cell content
  { 
    type = 0;
  }

  ~TxListAddItem();
};

// information about a list del range item
class TxListDelRangeItem : public TxListItem {
private:
public:
  u8 intervalType;
  ListCellPlus itemstart; // first item in interval to delete
  ListCellPlus itemend;   // list item in interval to delete
  TxListDelRangeItem(const COid &coidinit, GKeyInfo *ki, u8 it, const ListCell &is, const ListCell &ie) :
    TxListItem(coidinit),
    intervalType(it),
    itemstart(is, ki),
    itemend(ie, ki)
  {
    type = 1;
  }
  ~TxListDelRangeItem();
};


struct TxReadItem {
  COid coid;
};

// Information about updates of a single coid in pending transaction.
//
// A TxInfoCoid object (or ticoid, in short) expresses all the updates done on a
// single coid by a transaction. Those updates might consist of overwriting the
// entire coid (in which case Writevalue or WriteSV get set), setting individual
// attributes (in which case SetAttrs and Attrs get set), adding or removing items/cells
// (in which case Litems get set).
//
// We combine several ticoids of different coids into PendingTxInfo, which
// records all updates done by the transaction.

class TxInfoCoid {
  friend class Ptr<TxInfoCoid>;
private:
  void CommonConstructor();
  SkipListBK<ListCellPlus,int> SLAddItems;  // to be used in hasConflicts
                                            // will be precomputed once and then used later
                                            // note: (1) the value in this skiplist is not used
                                            // note: (2) the ListCellPlus* items added do not belong to SLAddItems
  bool SLpopulated;
  void PopulateSLAddItems();
  Align4 int refcount;         // for Ptr<> smart pointers

public:
  TxInfoCoid();
  TxInfoCoid(TxWriteItem *twi);
  TxInfoCoid(TxWriteSVItem *twsvi);
  ~TxInfoCoid();

  // clear all the updates (called when there is an overwrite).
  // If justfree is true, then just free entries (do not zero them out)
  void ClearUpdates(bool justfree=false);

  u8 SetAttrs[GAIA_MAX_ATTRS];// which attributes have been set. 
  u64 Attrs[GAIA_MAX_ATTRS];  // to what values they have been set. These Attr changes are on top
                              // of any writes that have occurred
  TxWriteItem *Writevalue;    // if there has been a write, the latest one
  TxWriteSVItem *WriteSV;     // if there has been a write of a supervalue, the latest one
  list<TxListItem*> Litems;   // list item operations, on top of any writes that have occurred

  SingleLogEntryInMemory *pendingentriesSleim; // if item is in pendingupdates log, a pointer to
                                 // the first entry there.
                                 // This is set in the PREPARERPC and used in COMMITRPC to
                                 // to able to quickly find the entry in the pendingupdates log without
                                 // having to scan the log. The item pointed to by pendingUpdateSleim
                                 // is not owned by the TxInfoCoid object, but rather belongs to the log.

  // Returns whether this TxInfoCoid has a conflict with another TxInfoCoid of the same coid.
  // This method should be called only after no more modifications will be done to
  // this or the given TxInfoCoid (in other words, both TxInfoCoid objects have reached
  // their final state). This is because this function will precompute data once and
  // will reuse the precomputed data in subsequent calls. Precomputed data is stored
  // in SLAddItems.
  bool hasConflicts(Ptr<TxInfoCoid> ticoid, SingleLogEntryInMemory *sleim);

  // print contents of object (for debugging)
  void print();
  void printdetail(COid coid);

  static void del(TxInfoCoid *ticoid){ delete ticoid; }
};

#define PTISTATUS_INPROGRESS   0
#define PTISTATUS_VOTEDYES     1 // transaction prepared and vote was yes
#define PTISTATUS_VOTEDNO      2 // transaction prepared and vote was no
#define PTISTATUS_CLEAREDABORT 3 // transaction aborted


// information for a single pending transaction; holds the writeset of the transaction
class PendingTxInfo {
private:
  friend Ptr<PendingTxInfo>;
  //RWLock txInfo_l;
  Align4 int refcount;

public:
  SkipList<COid,Ptr<TxInfoCoid> > coidinfo; // for each coid, what updates were done to it
  bool updatesCachable; // whether tx updates cachable data
  int status;    // see status codes PTISTATUS_...

  // Delete all ticoid items in coidinfo.
  // This is called when transaction aborts.
  void clear(){
    status = PTISTATUS_CLEAREDABORT;
    coidinfo.clear(0,0);
  }

  //void unlock(void){ txInfo_l.unlock(); }
  //void lock(void){ txInfo_l.lock(); }

  PendingTxInfo(){
    status = PTISTATUS_INPROGRESS;
    refcount = 0;
    updatesCachable = false;
  }
  ~PendingTxInfo(){ }
};

class PendingTx {
private:
  HashTableMT<Tid,Ptr<PendingTxInfo> > cTxList;
  static int getInfoLockaux(Tid &tid, Ptr<PendingTxInfo> *pti, int status, SkipList<Tid,Ptr<PendingTxInfo>> *b, u64 parm);
public:
  PendingTx();

  // gets info structure for a given tid. If it does not exist, create it.
  // Returns the entry locked in ret. Returns 0 if entry was found, 1 if it was created.
  int getInfo(Tid &tid, Ptr<PendingTxInfo> &retpti);

  // Gets info structure for a given tid.
  // Returns the entry locked in retpti. Returns 0 if entry was found, non-zero if it was not found.
  // If not found, retpti is not changed.
  int getInfoNoCreate(Tid &tid, Ptr<PendingTxInfo> &retpti);

  // returns 0 if item removed, -1 if item not found
  int removeInfo(Tid &tid);
};

#endif

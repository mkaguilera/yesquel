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
class TxWriteSVItem;
class TxUpdateCoid;


struct TxListItem {
  COid coid;
  i16 type;  // 0=ListAdd, 1=ListDelRange, 2=TxWriteItem, 3=TxWriteSVItem,
             // 4=SetAttr, 5=TxReadItem
  i16 level;
  i16 where; // where item is **!**
  TxListItem *next, *prev; // so items can be included in LinkList<>

  TxListItem() : type(-1), level(-1), where(-1) {
  }
  TxListItem(const COid &coidinit, int typ, int l)
    : coid(coidinit),
      type(typ),
      level(l),
      where(-1)
  {
  }
  virtual ~TxListItem();
  void printShort(COid expectedcoid);
  // Apply an item to a TxWriteSVItem. cancapture indicates whether item
  // can be captured by this function (meaning the function will own it).
  // Returns true if item was captured and so it should not be deleted
  // afterwards, false otherwise. Always returns false if cancapture==false
  bool applyItemToTucoid(Ptr<TxUpdateCoid> tucoid, bool cancapture);
};

// information about a single write of a value
struct TxWriteItem : public TxListItem {
  int len;
  char *buf;        // pointer to buffer with data
  char *rpcrequest; // pointer to rpc request buffer; here so that it can be
                    //   freed afterwards if alloctype=0
  int alloctype;    // way request buffer was allocated (so it can be freed
                    //   properly later).
                    // 0=via UDP layer, 1=via malloc
  TxWriteItem(const COid &coidinit, int l) : TxListItem(coidinit, 2, l)
  { buf = rpcrequest = 0; }
  TxWriteItem(const TxWriteItem &r);
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
  void printShort(COid coidtomatch);
  bool applyItemToTucoid(Ptr<TxUpdateCoid> tucoid, bool cancapture);
};

// information about a single write of a supervalue
struct TxWriteSVItem : public TxListItem {
    // Warning: anything from nattrs to attrs (exclusive) gets saved to
    // a file literally by DiskStorage::writeCOidToFile, so careful putting
    // things here. A previous bug had temporary buffers here, which got saved
    // to disk and later got reloaded with random stuff, which overwrote the
    // zero values they were supposed to have. The destructor later tried to
    // free those random buffers, corrupting memory
  u16 nattrs;      // number of 64-bit attribute values
  u8  celltype;    // type of cells: 0=int, 1=nKey+pKey
                   // -------- end of fixed part -----------
                   // stuff from attrs below are serialized independently
                   // when reading/writing on disk

  u64 *attrs;      // value of attributes
  SkipListBK<ListCellPlus,int> cells; // parsed cells
                            // note: the value in this skiplist is not used
  Ptr<RcKeyInfo> prki;
    // all cells in above skiplist will point to this prki.
    // If non-null, it is freed with free() in the destructor.
    // This prki is initially empty, but we learn it later when we
    // compare against a given ListCellPlus that comes in a ListAdd or
    // ListDelRange RPC.
    // The reason we do it this way is because sqlite does not include
    // the KeyInfoPtr in the pnkey; it is supplied externally whenever
    // it needs to compare cells. So, we must also supply it externally
    // later.
    // This field is the mechanism to do it: all cells have a pointer to
    // this field, and we set this field later. Once we set it (function
    // SetPrkiSticky(), it remains with the value. That method will clone
    // the KeyInfo (and so the object will own it) and will not change it
    // again in subsequent calls).

  TxWriteSVItem(const COid &coidinit, int l) : TxListItem(coidinit, 3, l) {
    nattrs = 0; attrs = 0; prki = 0; ncelloids = lencelloids = 0;
    celloids = 0; }
  TxWriteSVItem(const TxWriteSVItem &r);
  ~TxWriteSVItem();
  void clear(bool reset); // clears object and optionally reset to empty state
  TxWriteSVItem &operator=(const TxWriteSVItem &r){ assert(0); return *this; }

  void setPrkiSticky(Ptr<RcKeyInfo> prki_arg);

  char *getCelloids(int &retncelloids, int &retlencelloids);

  // converts from a single interval type for both start and end of the
  // interval to two interval types, one for the start and one for the end
  static void convertOneIntervalTypeToTwoIntervalType(int intervaltype1,
                  int &intervaltype2start, int &intervaltype2end);

  void printShort(COid coidtomatch);
  bool applyItemToTucoid(Ptr<TxUpdateCoid> tucoid, bool cancapture);

private:
  // Cached celloid information. This is populated by PopulateCelloids(), and
  // it should be called only after the TxWriteSVItem has become immutable.
  int ncelloids;
  int lencelloids;
  char *celloids;
};


// information about a list add item
struct TxListAddItem : public TxListItem {
  ListCellPlus item;
  Ptr<RcKeyInfo> prki;
  
  TxListAddItem(const COid &coidinit, Ptr<RcKeyInfo> prki_arg,
                const ListCell &c, int l) :
    TxListItem(coidinit, 0, l),
    item(c, prki_arg) // copy cell content
  {
    prki = prki_arg;
  }
  TxListAddItem(TxListAddItem &r);
  ~TxListAddItem();
  void printShort(COid coidtomatch);
  bool applyItemToTucoid(Ptr<TxUpdateCoid> tucoid, bool cancapture);
};

// information about a list del range item
struct TxListDelRangeItem : public TxListItem {
  u8 intervalType;
  ListCellPlus itemstart; // first item in interval to delete
  ListCellPlus itemend;   // list item in interval to delete
  Ptr<RcKeyInfo> prki;
  TxListDelRangeItem(const COid &coidinit, Ptr<RcKeyInfo> prki_arg, u8 it,
                     const ListCell &is, const ListCell &ie, int l) :
    TxListItem(coidinit, 1, l),
    intervalType(it),
    itemstart(is, prki_arg),
    itemend(ie, prki_arg)
  {
    prki = prki_arg;
  }
  TxListDelRangeItem(TxListDelRangeItem &r);
  
  ~TxListDelRangeItem();
  void printShort(COid coidtomatch);
  bool applyItemToTucoid(Ptr<TxUpdateCoid> tucoid, bool cancapture);
};

// information about a setattr item
struct TxSetAttrItem : public TxListItem {
  u32 attrid;
  u64 attrvalue;
  TxSetAttrItem(const COid &coidinit, u32 aid, u64 av, int l) :
    TxListItem(coidinit, 4, l)
  { attrid = aid; attrvalue = av; }
  void printShort(COid coidtomatch);
  bool applyItemToTucoid(Ptr<TxUpdateCoid> tucoid, bool cancapture);
};

// information about a transaction read item
struct TxReadItem : public TxListItem {
  TxReadItem(const COid &coidinit, int l) :
    TxListItem(coidinit, 5, l)
  { }
  void printShort(COid coidtomatch);
  bool applyItemToTucoid(Ptr<TxUpdateCoid> tucoid, bool cancapture);
};

// Uncompressed information about operations of a single coid in pending
// transaction.
class TxRawCoid {
private:
  Ptr<TxUpdateCoid> cachedTucoid; // cached tucoid from getTucoid
  LinkList<TxListItem> items;
  int refcount;
  friend class Ptr<TxRawCoid>;
public:
  TxRawCoid(){ refcount = 0; }
  ~TxRawCoid(){ items.clear(true);  }
  int abortLevel(int level); // remove any items with > level. Returns non-0
                             // if items became empty, 0 otherwise
  void releaseLevel(int level); // change any items with > level to level
  // add item to list. The item will be owned.
  void add(TxListItem *toadd){ toadd->where = 1; items.pushTail(toadd); }
  void remove(TxListItem *todel){ todel->where = -2; items.remove(todel); }

  TxListItem *getFirst(){ return items.getFirst(); }
  TxListItem *getNext(TxListItem *tli){ return items.getNext(tli); }
  TxListItem *getLast(){ return items.getLast(); }
  
  // Change TxRawCoid to TxUpdateCoid. Returns result and cache it
  // internally for later use. This function will modify TxRawCoid,
  // by moving some of its elements to the tucoid.
  // This function should be called only when transaction has no further
  // updates (e.g., when it is preparing to commit).
  Ptr<TxUpdateCoid> getTucoid(const COid &coid);
};


// Compressed information about updates of a single coid in pending
// transaction.
//
// A TxUpdateCoid object (or tucoid, in short) expresses all the updates done
// on a single coid by a transaction. Those updates might consist of
// overwriting the entire coid (in which case Writevalue or WriteSV get set),
// setting individual attributes (in which case SetAttrs and Attrs get set),
// adding or removing items/cells (in which case Litems get set).
//
// We combine several tucoids of different coids into PendingTxInfo, which
// records all updates done by the transaction.

class TxUpdateCoid {
  friend class Ptr<TxUpdateCoid>;
private:
  void commonConstructor();
  SkipListBK<ListCellPlus,int> SLAddItems;  // to be used in hasConflicts
      // will be precomputed once and then used later
      // note: (1) the value in this skiplist is not used
      // note: (2) the ListCellPlus* items added do not belong to SLAddItems
  bool SLpopulated;
  void populateSLAddItems();
  Align4 int refcount;         // for Ptr<> smart pointers

public:
  TxUpdateCoid();
  TxUpdateCoid(TxWriteItem *twi);
  TxUpdateCoid(TxWriteSVItem *twsvi);
  ~TxUpdateCoid();

  // clear all the updates (called when there is an overwrite).
  // If justfree is true, then just free entries (do not zero them out)
  void clearUpdates(bool justfree=false);

  u8 SetAttrs[GAIA_MAX_ATTRS];// which attributes have been set. 
  u64 Attrs[GAIA_MAX_ATTRS];  // to what values they have been set. These Attr
                              // changes are on top
                              // of any writes that have occurred
  TxWriteItem *Writevalue;    // if there has been a write, the latest one
  TxWriteSVItem *WriteSV;     // if there has been a write of a supervalue,
                              //      the latest one
  LinkList<TxListItem> Litems;// list item operations (only listadd and
                              // listdelrange), on top of any writes

  SingleLogEntryInMemory *pendingentriesSleim; // if item is in pendingupdates
     // log, a pointer to the first entry there.
     // This is set in the PREPARERPC and used in COMMITRPC to
     // to able to quickly find the entry in the pendingupdates log without
     // having to scan the log. The item pointed to by pendingUpdateSleim
     // is not owned by the TxUpdateCoid object, but rather belongs to the log.

  // Returns whether this TxUpdateCoid has a conflict with another
  // TxUpdateCoid of the same coid.
  // This method should be called only after no more modifications will be
  // done to this or the given TxUpdateCoid (in other words, both TxUpdateCoid
  // objects have reached their final state). This is because this function
  // will precompute data once and will reuse the precomputed data in
  // subsequent calls. Precomputed data is stored in SLAddItems.
  bool hasConflicts(Ptr<TxUpdateCoid> tucoid, SingleLogEntryInMemory *sleim);

  // print contents of object (for debugging)
  void print();
  void printdetail(COid coid);

  static void del(TxUpdateCoid *tucoid){ delete tucoid; }
};

#define PTISTATUS_INPROGRESS   0
#define PTISTATUS_VOTEDYES     1 // transaction prepared and vote was yes
#define PTISTATUS_VOTEDNO      2 // transaction prepared and vote was no
#define PTISTATUS_CLEAREDABORT 3 // transaction aborted


// information for a single pending transaction; holds the writeset of the
// transaction
class PendingTxInfo {
private:
  friend Ptr<PendingTxInfo>;
  //RWLock txInfo_l;
  Align4 int refcount;

public:
  SkipList<COid,Ptr<TxRawCoid> > coidinfo; // for each coid, what updates
                                              // were done to it
  bool updatesCachable; // whether tx updates cachable data
  int status;    // see status codes PTISTATUS_...

  // Delete all tucoid items in coidinfo.
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
  static int getInfoLockaux(Tid &tid, Ptr<PendingTxInfo> *pti, int status,
                            SkipList<Tid,Ptr<PendingTxInfo>> *b, u64 parm);
public:
  PendingTx();

  // gets info structure for a given tid. If it does not exist, create it.
  // Returns the entry locked in ret. Returns 0 if entry was found, 1 if it
  // was created.
  int getInfo(Tid &tid, Ptr<PendingTxInfo> &retpti);

  // Gets info structure for a given tid.
  // Returns the entry locked in retpti. Returns 0 if entry was found,
  //     non-zero if it was not found.
  // If not found, retpti is not changed.
  int getInfoNoCreate(Tid &tid, Ptr<PendingTxInfo> &retpti);

  // returns 0 if item removed, -1 if item not found
  int removeInfo(Tid &tid);
};

#endif

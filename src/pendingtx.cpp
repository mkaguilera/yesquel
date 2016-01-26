//
// pendingtx.cpp
//
// In-memory information about pending transactions.
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
#include <ctype.h>
#include <stddef.h>

#include <map>
#include <list>
#include <set>

#include "tmalloc.h"
#include "options.h"
#include "debug.h"
#include "pendingtx.h"
#include "logmem.h"

TxWriteSVItem::TxWriteSVItem(const TxWriteSVItem &r) 
  : cells(r.cells,0)
{
  pki = CloneGKeyInfo(r.pki);
  coid = r.coid;
  nattrs = r.nattrs;
  celltype = r.celltype;
  //ncelloids = r.ncelloids;
  attrs = new u64[nattrs];
  memcpy(attrs, r.attrs, nattrs * sizeof(u64));
  ncelloids = lencelloids = 0;
  celloids = 0;
}

TxWriteSVItem::~TxWriteSVItem(){
  if (attrs) delete [] attrs;
  cells.clear(ListCellPlus::del,0);
  if (pki) free(pki);
  if (celloids) delete [] celloids;
}

void TxWriteSVItem::SetPkiSticky(GKeyInfo *k){ 
  if (!pki) pki = CloneGKeyInfo(k); 
}

char *TxWriteSVItem::GetCelloids(int &retncelloids, int &retlencelloids){
  if (!celloids) celloids = ListCellsToCelloids(cells, ncelloids, lencelloids);
  retncelloids = ncelloids;
  retlencelloids = lencelloids;
  return celloids;
}

void TxWriteSVItem::convertOneIntervalTypeToTwoIntervalType(int intervaltype1, int &intervaltype2start, int &intervaltype2end){
  if (intervaltype1 < 3) intervaltype2start = 0; // open left interval
  else if (intervaltype1 < 6) intervaltype2start = 1; // closed left interval
  else intervaltype2start = 2; // infinite left interval
  switch(intervaltype1 % 3){
  case 0: intervaltype2end = 0; break; // open right interval
  case 1: intervaltype2end = 1; break; // closed right interval
  default: // to silence static checker
  case 2: intervaltype2end = 2; break; // infinite right interval
  }
}

void TxWriteSVItem::printShort(COid coidtomatch){
  if (COid::cmp(coid, coidtomatch) != 0){ 
    printf("*BADCOID*"); 
  }
  printf("celltype %d", celltype);
  printf(" attrs ");
  for (int i=0; i < nattrs; ++i) putchar(attrs[i] ? 'S' : '0');
  printf(" ki "); pki->printShort();
  printf(" cells ");
  SkipListNodeBK<ListCellPlus,int> *cellptr;
  for (cellptr = cells.getFirst(); cellptr != cells.getLast(); cellptr = cells.getNext(cellptr))
    cellptr->key->printShort();
}



TxListItem::~TxListItem(){
}

void TxListItem::printShort(COid expectedcoid){
  if (COid::cmp(coid,expectedcoid) != 0) 
    printf("*BADCOID*");
  if (type == 0){ // add item
    TxListAddItem *tlai = dynamic_cast<TxListAddItem*>(this);
    putchar('+');
    tlai->item.printShort(false,false);
  } else if (type == 1){
    TxListDelRangeItem *tldr = dynamic_cast<TxListDelRangeItem*>(this);
    putchar('-');
    switch(tldr->intervalType/3){
    case 0: putchar('('); break;
    case 1: putchar('['); break;
    case 2: putchar('{'); break;
    default: putchar('#'); break; // something is wrong
    }
    tldr->itemstart.printShort(false,false);
    putchar(',');
    tldr->itemend.printShort(false,false);
    switch(tldr->intervalType%3){
    case 0: putchar(')'); break;
    case 1: putchar(']'); break;
    case 2: putchar('}'); break;
    default: putchar('#'); break; // something is wrong
    }
  } else printf("type %d?", type);
}

TxListAddItem::~TxListAddItem()
{
  item.Free();
}

TxListDelRangeItem::~TxListDelRangeItem(){
  itemstart.Free();
  itemend.Free();
}

// int nticoids = 0;

void TxInfoCoid::CommonConstructor(){
  //++nticoids; 
  refcount = 0;
  SLpopulated = false;
  memset(SetAttrs, 0, sizeof(u8)*GAIA_MAX_ATTRS);
  memset(Attrs, 0, sizeof(u64)*GAIA_MAX_ATTRS);
  Writevalue = 0;
  WriteSV = 0;
  pendingentriesSleim = 0;
}

TxInfoCoid::TxInfoCoid(){
  CommonConstructor();
}

TxInfoCoid::TxInfoCoid(TxWriteItem *twi){
  CommonConstructor();
  Writevalue = twi;
}

TxInfoCoid::TxInfoCoid(TxWriteSVItem *twsvi){
  CommonConstructor();
  WriteSV = twsvi;
}

TxInfoCoid::~TxInfoCoid(){
  //--nticoids;
  ClearUpdates(true);
#ifdef GAIA_DESTROY_MARK
  memset(SetAttrs, 0xdd, sizeof(u8)*GAIA_MAX_ATTRS);
  memset(Attrs, 0xdd, sizeof(u64)*GAIA_MAX_ATTRS);
  memset(&Writevalue, 0xdd, sizeof(char*));
  memset(&WriteSV, 0xdd, sizeof(char*));
#endif
}

void TxInfoCoid::PopulateSLAddItems(){
  if (SLpopulated) return;
  SLpopulated = true;
  // put each TxListAddItem's ListCellPlus into SLAddItems
  for (list<TxListItem*>::iterator it = Litems.begin(); it != Litems.end(); ++it){
    TxListItem *tli = *it;
    if (tli->type == 0){ // add item
      TxListAddItem *tlai = dynamic_cast<TxListAddItem*>(tli);
      SLAddItems.insert(&tlai->item, 0);
    }
  }
}

void TxInfoCoid::ClearUpdates(bool justfree){
  if (!justfree){
    memset(SetAttrs, 0, sizeof(u8)*GAIA_MAX_ATTRS);
    memset(Attrs, 0, sizeof(u64)*GAIA_MAX_ATTRS);
  }
  if (Writevalue){ delete Writevalue; Writevalue=0; }
  if (WriteSV){ delete WriteSV; WriteSV=0; }
  list<TxListItem*>::iterator it, itnext;
  it = Litems.begin();
  while (it != Litems.end()){
    itnext = it; ++itnext;
    delete *it;
    Litems.erase(it);
    it = itnext;
  }
}

// Returns whether this TxInfoCoid has a conflict with another TxInfoCoid of the same coid.
// This method should be called only after no more modifications will be done to
// this or the given TxInfoCoid (in other words, both TxInfoCoid objects have reached
// their final state). This is because this function will precompute data once and
// will reuse the precomputed data in subsequent calls.
#ifdef GAIA_NONCOMMUTATIVE
bool TxInfoCoid::hasConflicts(Ptr<TxInfoCoid> ticoid, SingleLogEntryInMemory *sleim){
  return true; // two updates on same coid always conflict
}
#else
bool TxInfoCoid::hasConflicts(Ptr<TxInfoCoid> ticoid, SingleLogEntryInMemory *sleim){
  int i;
  bool hasdelete1, hasdelete2;
  if (Writevalue || WriteSV || ticoid->Writevalue || ticoid->WriteSV){
    dprintf(2, "  vote no because %llx wrote a value or supervalue", (long long)sleim->ts.getd1());
    return true; // write the value or supervalue
  }
  for (i=0; i < GAIA_MAX_ATTRS; ++i)
    if (SetAttrs[i] && ticoid->SetAttrs[i]){
      dprintf(2, "  vote no because %llx set the same attributes",(long long)sleim->ts.getd1());
      return true; // modify the same attributes
    }
  // now check Litems
  if (Litems.size() == 0 || ticoid->Litems.size() == 0) return false; // no Litems

  if (Litems.size() == 1 && ticoid->Litems.size() == 1){ // special case of 1 Litem each
    TxListItem *left = *Litems.begin();
    TxListItem *right = *ticoid->Litems.begin();
    if (left->type == 0){ // left is add item
      TxListAddItem *left2 = dynamic_cast<TxListAddItem *>(left);
      if (right->type == 0){ // right is add item
        TxListAddItem *right2 = dynamic_cast<TxListAddItem *>(right);
        int cmpres = ListCellPlus::cmp(left2->item, right2->item);
        if (cmpres==0){
          dprintf(2, "  vote no because %llx added the same item", (long long)sleim->ts.getd1());
        }
        return cmpres==0; // conflict iff they are the same item
      } else { // right is delrange item
        TxListDelRangeItem *right2 = dynamic_cast<TxListDelRangeItem *>(right);
        // conflict iff left item is in iterval of right item
        // interval types
        //    0 = (a,b),    1=(a,b],     2=(a,inf),
        //    3 = [a,b),     4=[a,b],    5=[a,inf),
        //    6 = (-inf,b),  7=(-inf,b], 8=(-inf,inf)
        bool test1, test2;
        if (right2->intervalType < 3)
          test1 = ListCellPlus::cmp(right2->itemstart, left2->item) < 0;
        else if (right2->intervalType < 6)
          test1 = ListCellPlus::cmp(right2->itemstart, left2->item) <= 0;
        else test1 = true;
        switch(right2->intervalType % 3){
        case 0: test2 = ListCellPlus::cmp(left2->item, right2->itemend) < 0; break;
        case 1: test2 = ListCellPlus::cmp(left2->item, right2->itemend) <= 0; break;
        case 2: test2 = true; break;
        }
        if (test1 && test2){
          dprintf(2, "  vote no because we are deleting, %llx added", (long long)sleim->ts.getd1());
        }
        return test1 && test2; // conflict iff both tests are met
      }
    } else { // left is delrange item
      TxListDelRangeItem *left2 = dynamic_cast<TxListDelRangeItem *>(left);
      if (right->type == 0){ // right is add item
        TxListAddItem *right2 = dynamic_cast<TxListAddItem *>(right);
        // conflict iff right item is in iterval of left item
        bool test1, test2;
        if (left2->intervalType < 3)
          test1 = ListCellPlus::cmp(left2->itemstart, right2->item) < 0;
        else if (left2->intervalType < 6)
          test1 = ListCellPlus::cmp(left2->itemstart, right2->item) <= 0;
        else test1 = true;
        switch(left2->intervalType % 3){
        case 0: test2 = ListCellPlus::cmp(right2->item, left2->itemend) < 0; break;
        case 1: test2 = ListCellPlus::cmp(right2->item, left2->itemend) <= 0; break;
        case 2: test2 = true; break;
        }
        if (test1 && test2){
          dprintf(2, "  vote no because we are adding, %llx deleted", (long long)sleim->ts.getd1());
        }
        return test1 && test2; // conflict iff both tests are met
      } else { // right is delrange item
#ifndef DISABLE_DELRANGE_DELRANGE_CONFLICTS
        dprintf(2, "  vote no because we are deleting, %llx deleted", (long long)sleim->ts.getd1());
        return true; // delrange always conflicts with delrange
#else
        return false;
#endif
      }
    }
  }
  // one of the lists has length > 1
  if (!SLpopulated) PopulateSLAddItems();
  if (!ticoid->SLpopulated) ticoid->PopulateSLAddItems();

  // check additem1 vs additem2
  SkipListNodeBK<ListCellPlus,int> *ptr1;
  for (ptr1 = SLAddItems.getFirst(); ptr1 != SLAddItems.getLast(); ptr1 = SLAddItems.getNext(ptr1)){
    if (ticoid->SLAddItems.belongs(ptr1->key)){
      dprintf(2, "  vote no because %llx added the same item", (long long)sleim->ts.getd1());
      return true; // additem in common
    }
  }

  hasdelete1 = hasdelete2 = false;

  // check additem1 vs delrangeitem2
  for (list<TxListItem*>::iterator it2 = ticoid->Litems.begin(); it2 != ticoid->Litems.end(); ++it2){
    TxListItem *tli2 = *it2;
    if (tli2->type == 1){ // delrange item
      hasdelete2 = true;
      TxListDelRangeItem *tldr2 = dynamic_cast<TxListDelRangeItem*>(tli2);
      if (SLAddItems.keyInInterval(&tldr2->itemstart, &tldr2->itemend, tldr2->intervalType)) 
      {
        dprintf(2, "  vote no because we are deleting, %llx added", (long long)sleim->ts.getd1());
        return true; // some key1 inside delrange interval2
      }
    }
  }

  // check delrangeitem1 vs additem2
  for (list<TxListItem*>::iterator it = Litems.begin(); it != Litems.end(); ++it){
    TxListItem *tli1 = *it;
    if (tli1->type == 1){ // delrange item
      hasdelete1 = true;
      TxListDelRangeItem *tldr1 = dynamic_cast<TxListDelRangeItem*>(tli1);
      if (ticoid->SLAddItems.keyInInterval(&tldr1->itemstart, &tldr1->itemend, tldr1->intervalType)) 
      {
        dprintf(2, "  vote no because we are adding, %llx deleted", (long long)sleim->ts.getd1());
        return true; // some key2 inside delrange interval1
      }
    }
  }
#ifndef DISABLE_DELRANGE_DELRANGE_CONFLICTS
  if (hasdelete1 && hasdelete2){
    dprintf(2, "  vote no because we are deleting, %llx deleted", (long long)sleim->ts.getd1());
    return true;
  }
#endif

  return false;
}
#endif

void TxInfoCoid::print(){
  int additems, delrangeitems;
  printf("Attrs ");
  for (int i=0; i < GAIA_MAX_ATTRS; ++i) putchar(Attrs[i] ? 'S' : '0');
  printf(" Write %s WriteSV %s", Writevalue ? "yes" : "no", WriteSV ? "yes" : "no");
  additems = delrangeitems = 0;
  for (list<TxListItem*>::iterator it=Litems.begin(); it != Litems.end(); ++it){
    if ((*it)->type==0) ++additems;
    else ++delrangeitems;
  }
  printf(" #additems %d #delrangeitems %d\n", additems, delrangeitems);
}

void TxInfoCoid::printdetail(COid coid){ // to check
  bool someattr = false;
  printf("Attrs ");
  for (int i=0; i < GAIA_MAX_ATTRS; ++i)
    if (Attrs[i]) someattr=true; // some attribute is set
  if (someattr){
    for (int i=0; i < GAIA_MAX_ATTRS; ++i){
      printf("%d(%llx) ", i, (long long)Attrs[i]);
    }
  } else printf("0");
  printf(" Write(");
  if (Writevalue){
    if (COid::cmp(Writevalue->coid, coid) != 0){ 
      printf("*BADCOID*"); 
    }
    DumpDataShort(Writevalue->buf, Writevalue->len < 8 ? Writevalue->len : 8);
  }
  printf(") WriteSV(");
  if (WriteSV) WriteSV->printShort(coid);
  printf(") ops (");
  for (list<TxListItem*>::iterator it = Litems.begin(); it != Litems.end(); ++it)
    (*it)->printShort(coid);
  printf(") sleim %s", pendingentriesSleim ? "yes" : "no");
}


PendingTx::PendingTx() : cTxList(PENDINGTX_HASHTABLE_SIZE){}

int PendingTx::getInfoLockaux(Tid &tid, Ptr<PendingTxInfo> *pti, int status, SkipList<Tid,Ptr<PendingTxInfo>> *b, u64 parm){
  Ptr<PendingTxInfo> *retpti = (Ptr<PendingTxInfo> *) parm; // pti to return
  if (status==0){
    *retpti = *pti; // entry was found, so use it
    return 0;
  }
  else { // entry not found
    *retpti = new PendingTxInfo; // not found, so create it
    b->insert(tid, *retpti); // insert into the skiplist of appropriate bucket (passed as parameter)
    return 1;
  }
}

// gets info structure for a given tid. If it does not exist, create it.
// Returns the entry locked in ret. Returns 0 if entry was found, 1 if it was created.
// Note: no longer acquires lock since the new RPC system ensures that the RPC
//    is always sent to the same thread for a given tid, so there'll never be two
//    threads trying to manipulate the same tid at once
int PendingTx::getInfo(Tid &tid, Ptr<PendingTxInfo> &retpti){
  int res;
  res = cTxList.lookupApply(tid, getInfoLockaux, (u64) &retpti);
  //retpti->lock();
  return res;
}

// Gets info structure for a given tid.
// Returns the entry locked in retpti. Returns 0 if entry was found, non-zero if it was not found.
// If not found, retpti is not changed.
// Caller must release
// the entry's lock before calling any PendingTx functions, otherwise
// a deadlock might occur.
int PendingTx::getInfoNoCreate(Tid &tid, Ptr<PendingTxInfo> &retpti){
  int res;
  res = cTxList.lookup(tid, retpti);
  //if (!res) retpti->lock();
  return res;
}

// returns 0 if item removed, non-zero if item not found
int PendingTx::removeInfo(Tid &tid){
  int res;

  res = cTxList.remove(tid, 0);
  return res;
}

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

TxListItem::~TxListItem(){
}

// This is a virtual method, implemented manually.
// We do this to decrease the size of a TxListItem
void TxListItem::printShort(COid expectedcoid){
  if (COid::cmp(coid,expectedcoid) != 0) 
    printf("*BADCOID*");
  switch(type){
  case 0: // listadd
    dynamic_cast<TxListAddItem*>(this)->printShort(expectedcoid);
    break;
  case 1: // listdelrange
    dynamic_cast<TxListDelRangeItem*>(this)->printShort(expectedcoid);
    break;
  case 2: // write item
    dynamic_cast<TxWriteItem*>(this)->printShort(expectedcoid);
    break;
  case 3: // write sv items
    dynamic_cast<TxWriteSVItem*>(this)->printShort(expectedcoid);
    break;
  case 4: // setattr
    dynamic_cast<TxSetAttrItem*>(this)->printShort(expectedcoid);
    break;
  case 5: // read item
    dynamic_cast<TxReadItem*>(this)->printShort(expectedcoid);
    break;
  default:
    printf("*BADTYPE*\n");
    break;
  }
}

// Apply an item to a TxWriteSVItem. cancapture indicates whether item
// can be captured by this function (meaning the function will own it).
// Returns true if item was captured and so it should not be deleted
// afterwards, false otherwise. Always returns false if cancapture==false
// This is a virtual method, implemented manually.
// We do this to decrease the size of a TxListItem
bool TxListItem::applyItemToTucoid(Ptr<TxUpdateCoid> tucoid, bool cancapture){
  switch(type){
  case 0: // listadd
    return dynamic_cast<TxListAddItem*>(this)->applyItemToTucoid(tucoid,
                                                                 cancapture);
  case 1: // listdelrange
    return dynamic_cast<TxListDelRangeItem*>(this)->applyItemToTucoid(tucoid,
                                                                 cancapture);
  case 2: // write item
    return dynamic_cast<TxWriteItem*>(this)->applyItemToTucoid(tucoid,
                                                               cancapture);
  case 3: // write sv items
    return dynamic_cast<TxWriteSVItem*>(this)->applyItemToTucoid(tucoid,
                                                                 cancapture);
  case 4: // setattr
    return dynamic_cast<TxSetAttrItem*>(this)->applyItemToTucoid(tucoid,
                                                                 cancapture);
  case 5: // read item
    return dynamic_cast<TxReadItem*>(this)->applyItemToTucoid(tucoid,
                                                              cancapture);
  default:
    assert(0);
  }
  return false;
}

TxWriteItem::TxWriteItem(const TxWriteItem &r) :
  TxListItem(r.coid, 2, r.level)
{
  len = r.len;
  buf = (char*) malloc(len);
  memcpy(buf, r.buf, len);
  rpcrequest = 0;
  alloctype = 1;
}

void TxWriteItem::printShort(COid coidtomatch){
  putchar('W');
}

bool TxWriteItem::applyItemToTucoid(Ptr<TxUpdateCoid> tucoid, bool cancapture){
  tucoid->clearUpdates();
  if (cancapture){ // capture it
    tucoid->Writevalue = this;
    return true;
  } else { // make a copy of it
    tucoid->Writevalue = new TxWriteItem(*this);
    return false;
  }
}

TxWriteSVItem::TxWriteSVItem(const TxWriteSVItem &r) :
  TxListItem(r.coid, 3, r.level),
  cells(r.cells, 0)
{
  prki = r.prki;
  nattrs = r.nattrs;
  celltype = r.celltype;
  attrs = new u64[nattrs];
  memcpy(attrs, r.attrs, nattrs * sizeof(u64));
  ncelloids = lencelloids = 0;
  celloids = 0;
}

TxWriteSVItem::~TxWriteSVItem(){
  clear(false);
}

void TxWriteSVItem::clear(bool reset){
  if (attrs) delete [] attrs;
  cells.clear(ListCellPlus::del,0);
  prki = 0;
  if (celloids) delete [] celloids;
  if (reset){
    nattrs = 0;
    attrs = 0;
    prki = 0;
    ncelloids = lencelloids = 0;
    celloids = 0;
  }
}

void TxWriteSVItem::setPrkiSticky(Ptr<RcKeyInfo> prki_arg){
  if (!prki.isset()) prki = prki_arg;
}

char *TxWriteSVItem::getCelloids(int &retncelloids, int &retlencelloids){
  if (!celloids) celloids = ListCellsToCelloids(cells, ncelloids, lencelloids);
  retncelloids = ncelloids;
  retlencelloids = lencelloids;
  return celloids;
}

void TxWriteSVItem::convertOneIntervalTypeToTwoIntervalType(int intervaltype1,
     int &intervaltype2start, int &intervaltype2end){
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
  printf("celltype %d", celltype);
  printf(" attrs ");
  for (int i=0; i < nattrs; ++i) putchar(attrs[i] ? 'S' : '0');
  printf(" rki "); prki->printShort();
  printf(" cells ");
  SkipListNodeBK<ListCellPlus,int> *cellptr;
  for (cellptr = cells.getFirst(); cellptr != cells.getLast();
       cellptr = cells.getNext(cellptr))
    cellptr->key->printShort();
}

bool TxWriteSVItem::applyItemToTucoid(Ptr<TxUpdateCoid> tucoid,
                                      bool cancapture){
  tucoid->clearUpdates(false);
  if (cancapture){ // capture it
    tucoid->WriteSV = this;
    return true;
  } else { // make copy of it
    tucoid->WriteSV = new TxWriteSVItem(*this);
    return false;
  }
}

TxListAddItem::TxListAddItem(TxListAddItem &r) :
      TxListItem(r.coid, 0, r.level),
      item(r.item, r.item.pprki.getprki()),
      prki(r.prki)
{
}

TxListAddItem::~TxListAddItem()
{
  item.Free();
  prki = 0;
}

void TxListAddItem::printShort(COid expectedcoid){
  assert(type == 0); // add item
  putchar('+');
  item.printShort(false,false);
}

bool TxListAddItem::applyItemToTucoid(Ptr<TxUpdateCoid> tucoid,
                                      bool cancapture){
  TxWriteSVItem *twsvi;
  twsvi = tucoid->WriteSV;
  if (twsvi){ // if there is already a supervalue, change it
    if (prki.isset()) twsvi->setPrkiSticky(prki);
    twsvi->cells.insertOrReplace(new ListCellPlus(item, &twsvi->prki),
                                 0, ListCellPlus::del, 0);
    return false;
  }
  else { // otherwise add a listadd item to transaction
    if (tucoid->Writevalue){ assert(0); return false; }
    if (cancapture){
      tucoid->Litems.pushTail(this);
      return true;
    } else {
      tucoid->Litems.pushTail(new TxListAddItem(*this));
      return false;
    }
  }
}

TxListDelRangeItem::TxListDelRangeItem(TxListDelRangeItem &r) :
      TxListItem(r.coid, 1, r.level),
      itemstart(r.itemstart, r.itemstart.pprki.getprki()),
      itemend(r.itemend, r.itemend.pprki.getprki()),
      prki(r.prki)
{
  intervalType = r.intervalType;
}

TxListDelRangeItem::~TxListDelRangeItem(){
  itemstart.Free();
  itemend.Free();
}

void TxListDelRangeItem::printShort(COid expectedcoid){
  assert(type==1);
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
}

bool TxListDelRangeItem::applyItemToTucoid(Ptr<TxUpdateCoid> tucoid,
                                           bool cancapture){
  TxWriteSVItem *twsvi;
  twsvi = tucoid->WriteSV;
  if (twsvi){ // if there is already a supervalue, change it
    if (prki.isset()) twsvi->setPrkiSticky(prki);

    int type1, type2;
    ListCellPlus rangestart(itemstart, prki);
    ListCellPlus rangeend(itemend, prki);
    TxWriteSVItem::convertOneIntervalTypeToTwoIntervalType(intervalType,
                                                           type1, type2);
    twsvi->cells.delRange(&rangestart, type1, &rangeend, type2,
                          ListCellPlus::del, 0);
    return false;
  } else { // otherwise add a delrange item to transaction
    if (cancapture){
      tucoid->Litems.pushTail(this);
      return true;
    } else {
      tucoid->Litems.pushTail(new TxListDelRangeItem(*this));
      return false;
    }
  }
}

void TxSetAttrItem::printShort(COid expectedcoid){
  putchar('S');
}

bool TxSetAttrItem::applyItemToTucoid(Ptr<TxUpdateCoid> tucoid,
                                      bool cancapture){
  TxWriteSVItem *twsvi;
  if (tucoid->Writevalue){ assert(0); return false; }
  assert(attrid < GAIA_MAX_ATTRS);
  twsvi = tucoid->WriteSV;
  if (twsvi) // if there is already a supervalue, change it
    twsvi->attrs[attrid] = attrvalue;
  else {
    tucoid->SetAttrs[attrid]=1; // mark attribute as set
    tucoid->Attrs[attrid] = attrvalue; // record attr value
  }
  return false;
}

void TxReadItem::printShort(COid expectedcoid){
  putchar('R');
}

bool TxReadItem::applyItemToTucoid(Ptr<TxUpdateCoid> tucoid, bool cancapture){
  return false;
}

// remove any items with > level. Returns non-0
// if items became empty, 0 otherwise
int TxRawCoid::abortLevel(int level){
  TxListItem *curr, *next;
  for (curr = items.getFirst(); curr != items.getLast();
       curr = next){
    next = items.getNext(curr);
    if (curr->level > level){
      remove(curr);
      delete curr;
    }
  }
  return items.getNitems() == 0;
}

// change any items with > level to level
void TxRawCoid::releaseLevel(int level){
  for (TxListItem *curr = items.getFirst(); curr != items.getLast();
       curr = items.getNext(curr)){
    if (curr->level > level) curr->level = level;
  }
}

// change TxRawCoid to TxUpdateCoid. This is used when transaction
// commits to store transaction updates in memory log
Ptr<TxUpdateCoid> TxRawCoid::getTucoid(const COid &coid){
  TxListItem *lastwrite = 0;
  TxListItem *curr, *next;
  
  if (cachedTucoid.isset()) return cachedTucoid; // return cached version
  
  // scan first to find last write or writeSV, if any
  for (curr = items.getFirst(); curr != items.getLast();
       curr = items.getNext(curr)){
    if (curr->type == 2 || curr->type==3) lastwrite=curr;
  }
  // check last write or writeSV
  if (lastwrite){
    if (lastwrite->type == 2){ // write
      // make sure there is nothing else
      assert(items.getNext(lastwrite) == items.getLast());
      // produce a tucoid with write and return it
      TxWriteItem *twi = dynamic_cast<TxWriteItem*>(lastwrite); assert(twi);
      remove(lastwrite); // remove since items in list
                               // will be deleted
      cachedTucoid = new TxUpdateCoid(twi);
      return cachedTucoid;
    }
    // writeSV
    assert(lastwrite->type == 3);
    curr = items.getNext(lastwrite); // start after write
    remove(lastwrite); // remove since items in list will be deleted
    TxWriteSVItem *twsvi = dynamic_cast<TxWriteSVItem*>(lastwrite);
    cachedTucoid = new TxUpdateCoid(twsvi);
  } else {
    curr = items.getFirst(); // start at first item
    cachedTucoid = new TxUpdateCoid(); // start with empty tucoid
  }
  // now, we have a tucoid to start with, and the rest of items to
  // apply to it
  bool itemcaptured;
  while (curr != items.getLast()){
    next = items.getNext(curr);
    remove(curr); // remove from list
    itemcaptured = curr->applyItemToTucoid(cachedTucoid, true);
    if (!itemcaptured){
      items.pushHead(curr); // put it back at beginning
    }
    curr = next;
  }
  return cachedTucoid;
}

void TxUpdateCoid::commonConstructor(){
  refcount = 0;
  SLpopulated = false;
  memset(SetAttrs, 0, sizeof(u8)*GAIA_MAX_ATTRS);
  memset(Attrs, 0, sizeof(u64)*GAIA_MAX_ATTRS);
  Writevalue = 0;
  WriteSV = 0;
  pendingentriesSleim = 0;
}

TxUpdateCoid::TxUpdateCoid(){
  commonConstructor();
}

TxUpdateCoid::TxUpdateCoid(TxWriteItem *twi){
  commonConstructor();
  Writevalue = twi;
}

TxUpdateCoid::TxUpdateCoid(TxWriteSVItem *twsvi){
  commonConstructor();
  WriteSV = twsvi;
}

TxUpdateCoid::~TxUpdateCoid(){
  clearUpdates(true);
#ifdef GAIA_DESTROY_MARK
  memset(SetAttrs, 0xdd, sizeof(u8)*GAIA_MAX_ATTRS);
  memset(Attrs, 0xdd, sizeof(u64)*GAIA_MAX_ATTRS);
  memset(&Writevalue, 0xdd, sizeof(char*));
  memset(&WriteSV, 0xdd, sizeof(char*));
#endif
}

void TxUpdateCoid::populateSLAddItems(){
  if (SLpopulated) return;
  SLpopulated = true;
  // put each TxListAddItem's ListCellPlus into SLAddItems
  for (TxListItem *tli = Litems.getFirst(); tli != Litems.getLast();
       tli = Litems.getNext(tli)){
    if (tli->type == 0){ // add item
      TxListAddItem *tlai = dynamic_cast<TxListAddItem*>(tli);
      SLAddItems.insert(&tlai->item, 0);
    }
  }
}

void TxUpdateCoid::clearUpdates(bool justfree){
  if (!justfree){
    memset(SetAttrs, 0, sizeof(u8)*GAIA_MAX_ATTRS);
    memset(Attrs, 0, sizeof(u64)*GAIA_MAX_ATTRS);
  }
  if (Writevalue){ delete Writevalue; Writevalue=0; }
  if (WriteSV){ delete WriteSV; WriteSV=0; }
  TxListItem *curr, *next;
  curr = Litems.getFirst();
  while (curr != Litems.getLast()){
    next = Litems.getNext(curr);
    Litems.remove(curr);
    delete curr;
    curr = next;
  }
}

// Returns whether this TxUpdateCoid has a conflict with another
// TxUpdateCoid of the same coid.
// This method should be called only after no more modifications will be
// done to this or the given TxUpdateCoid (in other words, both TxUpdateCoid
// objects have reached their final state). This is because this function
// will precompute data once and will reuse the precomputed data in
// subsequent calls. Precomputed data is stored in SLAddItems.
#ifdef GAIA_NONCOMMUTATIVE
bool TxUpdateCoid::hasConflicts(Ptr<TxUpdateCoid> tucoid,
                                SingleLogEntryInMemory *sleim){
  return true; // two updates on same coid always conflict
}
#else
bool TxUpdateCoid::hasConflicts(Ptr<TxUpdateCoid> tucoid,
                                SingleLogEntryInMemory *sleim){
  int i;
  bool hasdelete1, hasdelete2;
  if (Writevalue || WriteSV || tucoid->Writevalue || tucoid->WriteSV){
    dprintf(2, "  vote no because %llx wrote a value or supervalue",
            (long long)sleim->ts.getd1());
    return true; // write the value or supervalue
  }
  for (i=0; i < GAIA_MAX_ATTRS; ++i)
    if (SetAttrs[i] && tucoid->SetAttrs[i]){
      dprintf(2, "  vote no because %llx set the same attributes",
              (long long)sleim->ts.getd1());
      return true; // modify the same attributes
    }
  // now check Litems
  if (Litems.getNitems() == 0 || tucoid->Litems.getNitems() == 0)
    return false; // no Litems

  if (Litems.getNitems() == 1 && tucoid->Litems.getNitems() == 1){
    // special case of 1 Litem each
    TxListItem *left = Litems.getFirst();
    TxListItem *right = tucoid->Litems.getFirst();
    if (left->type == 0){ // left is add item
      TxListAddItem *left2 = dynamic_cast<TxListAddItem *>(left);
      if (right->type == 0){ // right is add item
        TxListAddItem *right2 = dynamic_cast<TxListAddItem *>(right);
        int cmpres = ListCellPlus::cmp(left2->item, right2->item);
        if (cmpres==0){
          dprintf(2, "  vote no because %llx added the same item",
                  (long long)sleim->ts.getd1());
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
        case 0: test2 = ListCellPlus::cmp(left2->item, right2->itemend) < 0;
                break;
        case 1: test2 = ListCellPlus::cmp(left2->item, right2->itemend) <= 0;
                break;
        case 2: test2 = true;
                break;
        }
        if (test1 && test2){
          dprintf(2, "  vote no because we are deleting, %llx added",
                  (long long)sleim->ts.getd1());
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
        case 0: test2 = ListCellPlus::cmp(right2->item, left2->itemend) < 0;
                break;
        case 1: test2 = ListCellPlus::cmp(right2->item, left2->itemend) <= 0;
                break;
        case 2: test2 = true;
                break;
        }
        if (test1 && test2){
          dprintf(2, "  vote no because we are adding, %llx deleted",
                  (long long)sleim->ts.getd1());
        }
        return test1 && test2; // conflict iff both tests are met
      } else { // right is delrange item
#ifndef DISABLE_DELRANGE_DELRANGE_CONFLICTS
        dprintf(2, "  vote no because we are deleting, %llx deleted",
                (long long)sleim->ts.getd1());
        return true; // delrange always conflicts with delrange
#else
        return false;
#endif
      }
    }
  }
  // one of the lists has length > 1
  if (!SLpopulated) populateSLAddItems();
  if (!tucoid->SLpopulated) tucoid->populateSLAddItems();

  // check additem1 vs additem2
  SkipListNodeBK<ListCellPlus,int> *ptr1;
  for (ptr1 = SLAddItems.getFirst(); ptr1 != SLAddItems.getLast();
       ptr1 = SLAddItems.getNext(ptr1)){
    if (tucoid->SLAddItems.belongs(ptr1->key)){
      dprintf(2, "  vote no because %llx added the same item",
              (long long)sleim->ts.getd1());
      return true; // additem in common
    }
  }

  hasdelete1 = hasdelete2 = false;

  // check additem1 vs delrangeitem2
  for (TxListItem *tli2 = tucoid->Litems.getFirst();
       tli2 != tucoid->Litems.getLast();
       tli2 = tucoid->Litems.getNext(tli2)){
    if (tli2->type == 1){ // delrange item
      hasdelete2 = true;
      TxListDelRangeItem *tldr2 = dynamic_cast<TxListDelRangeItem*>(tli2);
      if (SLAddItems.keyInInterval(&tldr2->itemstart, &tldr2->itemend,
                                   tldr2->intervalType)) 
      {
        dprintf(2, "  vote no because we are deleting, %llx added",
                (long long)sleim->ts.getd1());
        return true; // some key1 inside delrange interval2
      }
    }
  }

  // check delrangeitem1 vs additem2
  for (TxListItem *tli1 = Litems.getFirst(); tli1 != Litems.getLast();
       tli1 = Litems.getNext(tli1)){
    if (tli1->type == 1){ // delrange item
      hasdelete1 = true;
      TxListDelRangeItem *tldr1 = dynamic_cast<TxListDelRangeItem*>(tli1);
      if (tucoid->SLAddItems.keyInInterval(&tldr1->itemstart, &tldr1->itemend,
                                           tldr1->intervalType)) 
      {
        dprintf(2, "  vote no because we are adding, %llx deleted",
                (long long)sleim->ts.getd1());
        return true; // some key2 inside delrange interval1
      }
    }
  }
#ifndef DISABLE_DELRANGE_DELRANGE_CONFLICTS
  if (hasdelete1 && hasdelete2){
    dprintf(2, "  vote no because we are deleting, %llx deleted",
            (long long)sleim->ts.getd1());
    return true;
  }
#endif

  return false;
}
#endif

void TxUpdateCoid::print(){
  int additems, delrangeitems;
  printf("Attrs ");
  for (int i=0; i < GAIA_MAX_ATTRS; ++i) putchar(Attrs[i] ? 'S' : '0');
  printf(" Write %s WriteSV %s", Writevalue ? "yes" : "no",
         WriteSV ? "yes" : "no");
  additems = delrangeitems = 0;
  for (TxListItem *tli = Litems.getFirst(); tli != Litems.getLast();
       tli = Litems.getNext(tli)){
    if (tli->type==0) ++additems;
    else ++delrangeitems;
  }
  printf(" #additems %d #delrangeitems %d\n", additems, delrangeitems);
}

void TxUpdateCoid::printdetail(COid coid){ // to check
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
  for (TxListItem *tli = Litems.getFirst(); tli != Litems.getLast();
       tli = Litems.getNext(tli)){
    tli->printShort(coid);
  }
  printf(") sleim %s", pendingentriesSleim ? "yes" : "no");
}


PendingTx::PendingTx() : cTxList(PENDINGTX_HASHTABLE_SIZE){}

int PendingTx::getInfoLockaux(Tid &tid, Ptr<PendingTxInfo> *pti, int status,
                              SkipList<Tid,Ptr<PendingTxInfo>> *b, u64 parm){
  Ptr<PendingTxInfo> *retpti = (Ptr<PendingTxInfo> *) parm; // pti to return
  if (status==0){
    *retpti = *pti; // entry was found, so use it
    return 0;
  }
  else { // entry not found
    *retpti = new PendingTxInfo; // not found, so create it
    b->insert(tid, *retpti); // insert into the skiplist of appropriate bucket
                             // (passed as parameter)
    return 1;
  }
}

// gets info structure for a given tid. If it does not exist, create it.
// Returns the entry locked in ret. Returns 0 if entry was found, 1 if it was
// created.
// Note: no longer acquires lock since the new RPC system ensures that the RPC
//   is always sent to the same thread for a given tid, so there'll never be two
//   threads trying to manipulate the same tid at once
int PendingTx::getInfo(Tid &tid, Ptr<PendingTxInfo> &retpti){
  int res;
  res = cTxList.lookupApply(tid, getInfoLockaux, (u64) &retpti);
  //retpti->lock();
  return res;
}

// Gets info structure for a given tid.
// Returns the entry locked in retpti. Returns 0 if entry was found, non-zero
//    if it was not found.
// If not found, retpti is not changed.
// Caller must release the entry's lock before calling any PendingTx functions,
// otherwise a deadlock might occur.
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

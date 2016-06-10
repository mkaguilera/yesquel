//
// supervalue.h
//
// Definitions for supervalues. A supervalue is a value in a key-value
// pair with additional structure (rather than an opaque value): a list of
// cells, and a bunch of attributes that can be individually set.
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

#ifndef _SUPERVALUE_H
#define _SUPERVALUE_H

#include <string.h>
#include "tmalloc.h"
#include "util.h"
#include "inttypes.h"
#include "datastruct.h"
#include "record.h"
#include "gaiatypes.h"
#include "valbuf.h"

#define GAIA_MAX_ATTRS 6 // max number of attributes in a supervalue

struct UnpackedRecord;

class ListCell {
private:
public:
  i64 nKey;
  char *pKey;
  u64 value;
  ListCell(){ pKey = 0; }
  ListCell(const ListCell& c){ copy(c); }
  ListCell &operator=(const ListCell &c){ copy(c); return *this; }
  static bool equal(const ListCell &l, const ListCell &r){ 
    return &l==&r || (l.nKey == r.nKey && l.value == r.value &&
                      (l.pKey == 0)==(r.pKey == 0) &&
                      (l.pKey==0 || memcmp(l.pKey,r.pKey,(int)l.nKey)==0));
  }
  
  int size(){ return myVarintLen(nKey) + sizeof(u64) + (int)(pKey ? nKey : 0); }
  void copy(const ListCell &c){
    nKey = c.nKey;
    value = c.value;
    if (c.pKey){ 
      assert(nKey >= 0);
      pKey = (char*) malloc((int)nKey); 
      memcpy(pKey, c.pKey, (int)nKey);
    } else pKey=0;
  }

  void Free(){
    if (pKey) free(pKey);
    pKey = 0;
  }
};

// A cell with space for a UnpackedRecord.
// There are two types of ListCellPlus objects:
//  - those that are part of a supervalue (TxWriteSVItem) object.
//    They will all share the value of pprki, which will point
//    to a RcKeyInfoPtr object stored in the TxWriteSVItem. This
//    object will be modified as we compare the ListCellPlus
//     with a standalone ListCellPlus.
// - those that are standalone and arrive in ListAdd and ListDelRange
//   RPCs. They will have their own private pprki.

// ListCellsPlus in a TxWriteSVItem will have freeki = false, since
// they all point to the KeyInfo of the enclosing TxWriteSVItem
// (which is owned by TxWriteSVItem).
// Standalone ListCellsPlus will have freeki = true
class RcKeyInfoPtr {
private:
  Ptr<RcKeyInfo> *pprki;
  bool ownpprki;
  
public:
  // requires k to be non-null. If there is no keyinfo,
  // k should point to a Ptr<RcKeyInfo> set to 0.
  RcKeyInfoPtr(Ptr<RcKeyInfo> *k, bool own){
    assert(k);
    pprki = k; ownpprki = own; 
  }
  bool hasprki(){ return pprki->isset(); }
  // might return a Ptr<> set to 0
  Ptr<RcKeyInfo> getprki(){
    return *pprki;
  }
  ~RcKeyInfoPtr(){ if (ownpprki){ assert(pprki); delete pprki; } }
};

class ListCellPlus : public ListCell {
private:
  char aspace[150];
protected:
  void UnpackRecord(){
    if (!pIdxKey)
      pIdxKey = myVdbeRecordUnpack(&*pprki.getprki(), (int)nKey, pKey, aspace,
                                   sizeof(aspace));
  }
public:
  UnpackedRecord *pIdxKey;
  RcKeyInfoPtr pprki;

  // Fresh ListCell, but use a given pprki.
  // Intended to be used when creating a new ListCellPlus
  // to be added into a SV (in this case, the suplied pprki will be the
  // one of the SV)
  ListCellPlus(Ptr<RcKeyInfo> *pprki_arg) :
    ListCell(), pprki(pprki_arg, false)   // do not free the RcKeyInfo
  {
    pIdxKey = 0;
  }

  // Copy from another ListCell or ListCellPlus, but use a given pprki.
  // Intended to be used when adding a standalone ListCellPlus
  // into a SV (in this case, the suplied pprki will be the
  // one of the SV)
  ListCellPlus(const ListCell &r, Ptr<RcKeyInfo> *pprki_arg) :
    ListCell(r), pprki(pprki_arg, false)   // do not free the RcKeyInfo
  {
    pIdxKey = 0;
  }

  // create with a private RcKeyInfo, copying from a ListCell.
  // Used when we deserialize a ListAdd or ListDelRange, to
  // create a standalone ListCellPlus from a ListCell.
  ListCellPlus(const ListCell &r, Ptr<RcKeyInfo> srcprki)
    : ListCell(r),
      pprki(new Ptr<RcKeyInfo>(srcprki), true)
  { 
    pIdxKey = 0;
  }

  ListCellPlus operator=(const ListCellPlus &r){ assert(0); return *this; }


  void Free(){
    if (pIdxKey){ myVdbeDeleteUnpackedRecord(pIdxKey); pIdxKey = 0; }
    ListCell::Free();
  }

  ~ListCellPlus(){ Free(); }

  static int cmp(ListCellPlus &left, ListCellPlus &right){
    if (left.pKey==0 && right.pKey==0){
      if (left.nKey < right.nKey) return -1;
      if (left.nKey > right.nKey) return +1;
      return 0;
    }
    if (!right.pIdxKey) right.UnpackRecord();
    return myVdbeRecordCompare((int)left.nKey, left.pKey, right.pIdxKey);
  }

  static void del(ListCellPlus *lc){ delete lc; }

  bool isanyprint(char *buf, int len){
    if (!buf) return false;
    bool retval = false;
    for (int i=0; i < len; ++i){
      if (isprint((u8)*buf++)){
        retval = true;
        break;
      }
    }
    return retval;
  }

  void printShort(bool showparenthesis=true, bool showvalue=true){
    int len;
    if (showparenthesis) putchar('(');
    printf("%llx", (long long)nKey);
    len = nKey < 8 ? (int)nKey : 8;
    if (pKey){
      putchar(',');
      if (isanyprint(pKey, len))
        DumpDataShort(pKey, nKey < 8 ? (int)nKey : 8);
      else putchar('.');
    }
    if (showvalue && value != 0xabcdabcdabcdabcdLL)
      printf(",%llx", (long long)value);
    if (showparenthesis) putchar(')');
  }
};

class SuperValue {
public:
  i16 Nattrs;       // number of 64-bit attribute values
  u8  CellType;     // 0=int, 1=nKey+pKey
  i32 Ncells;       // number of (cell,oid) pairs in list
  i32 CellsSize;    // size of cells combined
  u64 *Attrs;       // value of attributes
  ListCell *Cells;  // contents of cells, owned by DTreeNode
  Ptr<RcKeyInfo> prki; // keyinfo if available

  SuperValue(){ Nattrs = 0; CellType = 0; Ncells = 0; CellsSize = 0;
                Attrs = 0; Cells = 0; prki = 0; }
  void copy(const SuperValue& c);
  SuperValue(const SuperValue& c){ copy(c); }
  SuperValue& operator=(const SuperValue& c){ copy(c); return *this; }
  ~SuperValue(){ Free(); }
  void Free(void);

  // Insert a new cell at position pos.
  // pos must be between 0 and Ncells. If pos==Ncells, insert at the end.
  // After method is done, Cells[pos] is the new inserted cell
  void InsertCell(int pos);

  // Delete cell at position pos.
  // pos must be between 0 and Ncells-1
  void DeleteCell(int pos);

  // Delete cells in positions startpos..endpos-1 (a total of endpos-startpos
  // cells).
  // startpos must be between 0 and Ncells-1
  // endpos must be between startpos and Ncells
  void DeleteCellRange(int startpos, int endpos);
};
#endif

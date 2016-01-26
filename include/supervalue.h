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
#include "gaiatypes.h"
#include "valbuf.h"
#include "record.h"

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
    return &l==&r || (l.nKey == r.nKey && l.value == r.value && (l.pKey == 0)==(r.pKey == 0) && (l.pKey==0 || memcmp(l.pKey,r.pKey,(int)l.nKey)==0));
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
//    They will all share the value of ppki, which will point
//    to a GKeyInfoPtr object stored in the TxWriteSVItem. This
//    object will be modified as we compare the ListCellPlus
//     with a standalone ListCellPlus.
// - those that are standalone and arrive in ListAdd and ListDelRange
//   RPCs. They will have their own private ppki.

// ListCellsPlus in a TxWriteSVItem will have freeki = false, since
// they all point to the KeyInfo of the enclosing TxWriteSVItem
// (which is owned by TxWriteSVItem).
// Standalone ListCellsPlus will have freeki = true
class GKeyInfoPtr {
public:
  // This is for a private G
  GKeyInfoPtr(GKeyInfo **k, bool fk){ 
    ki = k; freeki = fk; 
  }
  ~GKeyInfoPtr(){ if (freeki && ki){ free(*ki); delete ki; } }
  GKeyInfo **ki;
  bool freeki;
};


class ListCellPlus : public ListCell {
private:
  char aspace[150];
protected:
  void UnpackRecord(){
    if (!pIdxKey)
      pIdxKey = myVdbeRecordUnpack(*ppki.ki, (int)nKey, pKey, aspace, sizeof(aspace));
  }
public:
  UnpackedRecord *pIdxKey;
  GKeyInfoPtr ppki;

  //ListCellPlus(const ListCell &c) : ListCell(c)
  //{
  //  pIdxKey = 0;
  //}

  //ListCellPlus() : ListCell()
  //{
  //  pIdxKey = 0;
  //}

  // Copy from another ListCellPlus.
  // Used when we copy a supervalue (TxWriteSVItem).
  // Not intended to be used with standalone ListCellPlus
  // (eg, those that come from ListAdd or ListDelRange)
  ListCellPlus(const ListCellPlus &r) :
    ListCell(r), ppki(r.ppki.ki, false)  // do not free the GKeyInfo
  {
    pIdxKey = 0;
  }

  // Fresh ListCell, but use a given ppki.
  // Intended to be used when creating a new ListCellPlus
  // to be added into a SV (in this case, the suplied ppki will be the
  // one of the SV)
  ListCellPlus(GKeyInfo **ki) :
    ListCell(), ppki(ki, false)   // do not free the GKeyInfo
  {
    pIdxKey = 0;
  }

  // Copy from another ListCell or ListCellPlus, but use a given ppki.
  // Intended to be used when adding a standalone ListCellPlus
  // into a SV (in this case, the suplied ppki will be the
  // one of the SV)
  ListCellPlus(const ListCell &r, GKeyInfo **ki) :
    ListCell(r), ppki(ki, false)   // do not free the GKeyInfo
  {
    pIdxKey = 0;
  }

  // create with a private GKeyInfo, copying from a ListCell.
  // Used when we deserialize a ListAdd or ListDelRange, to
  // create a standalone ListCellPlus from a ListCell.
  ListCellPlus(const ListCell &r, GKeyInfo *srcpki)
    : ListCell(r), ppki(srcpki ? new GKeyInfo *(CloneGKeyInfo(srcpki)) : 0, true)   // free the GKeyInfo when we delete the ListCellPlus
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
    for (int i=0; i < len; ++i){ if (isprint((u8)*buf++)){ retval = true; break; }}
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
  GKeyInfo *pki;    // keyinfo if available

  SuperValue(){ Nattrs = 0; CellType = 0; Ncells = 0; CellsSize = 0; Attrs = 0; Cells = 0; pki = 0; }
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

  // Delete cells in positions startpos..endpos-1 (a total of endpos-startpos cells).
  // startpos must be between 0 and Ncells-1
  // endpos must be between startpos and Ncells
  void DeleteCellRange(int startpos, int endpos);
};
#endif

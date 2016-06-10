//
// dtreeaux.h
//
// Auxiliary definitions of distributed b-tree
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

#ifndef _DTREEAUX_H_
#define _DTREEAUX_H_

#include "options.h"
#include "inttypes.h"
#include "cellbuf.h"
#include "kvinterface.h"
#include "util.h"
#include "supervalue.h"
#include "datastruct.h"
#include "scheduler.h"
#include "debug.h"

#ifdef DEBUGLOG
#define DTREELOG(format,...) dprintf(2, "%llx:%s:%d:" format, (long long)Time::now(), __FUNCTION__, __LINE__, ##__VA_ARGS__)
#else
#define DTREELOG(format,...)
#endif


#define DTREENODE_FLAG_INTKEY    0x0001 // node holds integer keys only
#define DTREENODE_FLAG_LEAF      0x0002 // node is a leaf

#define DTREENODE_NATTRIBS           5  // number of attribs in each node
// attribute numbers
#define DTREENODE_ATTRIB_FLAGS       0 // flags
#define DTREENODE_ATTRIB_HEIGHT      1 // height
#define DTREENODE_ATTRIB_LASTPTR     2 // last pointer in node
#define DTREENODE_ATTRIB_LEFTPTR     3 // pointer to left neighbor (0 if none)
#define DTREENODE_ATTRIB_RIGHTPTR    4 // pointer to right neighbor (0 if none)

// used to store a node of the DTree
class DTreeNode {
public:
  Ptr<Valbuf> raw;    // has flags, height, ncells, cells, etc
  Oid &NodeOid(){ return raw->coid.oid; }
  u64 &Flags(){ return raw->u.raw->Attrs[DTREENODE_ATTRIB_FLAGS]; }
  u64 &Height(){ return raw->u.raw->Attrs[DTREENODE_ATTRIB_HEIGHT]; }
  Oid &LastPtr(){ return raw->u.raw->Attrs[DTREENODE_ATTRIB_LASTPTR]; }
  Oid &LeftPtr(){ return raw->u.raw->Attrs[DTREENODE_ATTRIB_LEFTPTR]; }
  Oid &RightPtr(){ return raw->u.raw->Attrs[DTREENODE_ATTRIB_RIGHTPTR]; }
  int &Ncells(){ return raw->u.raw->Ncells; }
  int &CellsSize(){ return raw->u.raw->CellsSize; }
  ListCell *Cells(){ return raw->u.raw->Cells; }
  u8 &CellType(){ return raw->u.raw->CellType; }
  Ptr<RcKeyInfo> Prki(){ return raw->u.raw->prki; }
  Oid &GetPtr(int index){
    if (index==raw->u.raw->Ncells) return LastPtr();
    else return raw->u.raw->Cells[index].value;
  }

  DTreeNode(){ raw = 0; }
  //DTreeNode(const DTreeNode& c);

   
  bool isRoot(){ return raw->coid.oid==0; } // root is oid 0
  bool isLeaf(){ return (Flags() & DTREENODE_FLAG_LEAF) != 0; }
  bool isInner(){ return !isLeaf(); }
  bool isIntKey(){ 
    assert(((Flags() & DTREENODE_FLAG_INTKEY) != 0) ==
           (raw->u.raw->CellType==0));
    return (Flags() & DTREENODE_FLAG_INTKEY) != 0;
  }

  //void newEmpty(COid coid, bool intKey);   // create new empty node
  //                                   // intKey==true iff node stores integers
  //int read(KVTransaction *tx, COid coid); // read node from KV store
  //int write(KVTransaction *tx, COid coid); // write node to KV store

  static void InitSuperValue(SuperValue *sv, u8 celltype);
};

// prototype definitions
int auxReadReal(KVTransaction *tx, COid coid, DTreeNode &outptr,
                ListCell *cell, Ptr<RcKeyInfo> prki);
int auxReadCache(COid coid, DTreeNode &outptr);
void auxRemoveCache(COid coid);
int auxReadCacheOrReal(KVTransaction *tx, COid coid, DTreeNode &outptr,
                       int &real, ListCell *cell, Ptr<RcKeyInfo> prki);

#endif

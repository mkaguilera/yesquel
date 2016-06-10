//
// dtreesplit.cpp
//
// Implements splitting of the distributed B-tree. This code can run at the
// server or at the client, depending on how Yesquel is configured.
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
#include <float.h>

#include <map>
#include <list>

#include "tmalloc.h"
#include "options.h"
#include "gaiatypes.h"
#include "datastruct.h"
#include "util.h"
#include "dtreeaux.h"
#include "dtreesplit.h"
#include "debug.h"
#include "coid.h"

#ifdef _DTREE_C
struct KeyInfo;
static int CellSearchNode(DTreeNode &node, i64 nkey, void *pkey,
                          KeyInfo *pKeyInfo, int biasRight);
inline int GCellSearchNode(DTreeNode &node, i64 nkey, void *pkey,
                           Ptr<RcKeyInfo> prki, int biasRight){
  // call the function defined in dtree.cpp that uses KeyInfo
  // instead of RcKeyInfo
  return CellSearchNode(node, nkey, pkey, (KeyInfo*) &*prki, biasRight);
}
#else
#include "splitter-standalone.h"
#endif

// finds the parent of a node given a targetcoid and a cell within targetcoid.
// Returns 0 if found, non-zero if error. If found, the oid of the parent
// is placed in result.
int FindParentReal(KVTransaction *tx, COid targetcoid, ListCell &cell,
                   Ptr<RcKeyInfo> prki, Oid &result){
  DTreeNode node;
  int res;
  COid coid, nextcoid;
  Ptr<Valbuf> buf;
  int index;
  int nsearches;

  if (targetcoid.oid == 0){ dprintf(1,"A "); return -1; } // root has no parent

  coid.cid = targetcoid.cid;
  coid.oid = 0;  // start with root
  nextcoid.cid = targetcoid.cid;
  nextcoid.oid = 0; 
  nsearches = 0;

  do {
    ++nsearches;
    coid.oid = nextcoid.oid;
    res = auxReadReal(tx, coid, node, 0, 0);
    if (res){
      dprintf(1,"Aa%d,%d,%llx ", res, nsearches, (long long)coid.oid);
      return res;
    }

    index = GCellSearchNode(node, cell.nKey, cell.pKey, prki, 0);
    assert(0 <= index && index <= node.Ncells()+1);
    nextcoid.oid = node.GetPtr(index);
  } while (nextcoid.oid != targetcoid.oid &&
           nsearches < DTREE_MAX_LEVELS && node.isInner());

  if (node.isLeaf() || nextcoid.oid != targetcoid.oid){
    dprintf(1,"Ab%d,%d,%llx ", -1, nsearches, (long long)coid.oid);
    return -1; // could not find it
  }
  result = coid.oid;
  return 0;
}

// Using cached information, finds the parent of a node given a targetcoid
// and a cell within targetcoid.
// Returns 0 if found, non-zero if error. If found, the oid of the parent is
// placed in result.
// If parent is found, the function confirms its accuracy by reading the real
// node if necessary.
// In other words, this function never returns an incorrect parent. However,
// it may return non-zero (not found) even if parent exists because cached
// information may be wrong.
int FindParentCache(KVTransaction *tx, COid targetcoid, ListCell &cell,
                    Ptr<RcKeyInfo> prki, Oid &result){
  DTreeNode node;
  int res;
  COid coid, nextcoid;
  Ptr<Valbuf> buf;
  int index;
  int nsearches;
  int real;

  if (targetcoid.oid == 0){ dprintf(1,"A "); return -1; } // root has no parent

  coid.cid = targetcoid.cid;
  coid.oid = 0;  // start with root
  nextcoid.cid = targetcoid.cid;
  nextcoid.oid = 0; 
  nsearches = 0;

  do {
    ++nsearches;
    coid.oid = nextcoid.oid;
    res = auxReadCacheOrReal(tx, coid, node, real, 0, 0);
    if (res){
      dprintf(1,"Ba%d,%d,%llx ", res, nsearches, (long long)coid.oid);
      return res;
    }

    index = GCellSearchNode(node, cell.nKey, cell.pKey, prki, 0);
    assert(0 <= index && index <= node.Ncells()+1);
    nextcoid.oid = node.GetPtr(index);
  } while (nextcoid.oid != targetcoid.oid &&
           nsearches < DTREE_MAX_LEVELS && node.isInner());

  if (node.isLeaf() || nsearches >= DTREE_MAX_LEVELS){
    // reached leaf without finding
    dprintf(1, "Bb%d,%d,%llx ", -1, nsearches, (long long)coid.oid);
    return -1;
  }

  if (!real){ // confirm parent is correct
    res = auxReadReal(tx, coid, node, 0, 0);
    if (res){ 
      dprintf(1,"Bc%d,%d,%llx ", res, nsearches, (long long)coid.oid); 
      return res; 
    }
    index = GCellSearchNode(node, cell.nKey, cell.pKey, prki, 0);
    assert(0 <= index && index <= node.Ncells()+1);
    nextcoid.oid = node.GetPtr(index);
  }

  if (nextcoid.oid != targetcoid.oid){
    dprintf(1,"Bd%d,%d,%llx ", -1, nsearches, (long long)coid.oid); 
    return -1; // could not find it
  }
  result = coid.oid;
  return 0;
}

// checks that a node matches what is in node.
int chknode(COid coid, DTreeNode node, bool remote){
  KVTransaction *tx;
  DTreeNode r;
  int res;
  int i;
  beginTx(&tx, remote);
  res = KVreadSuperValue(tx, coid, r.raw, 0, 0); assert(!res);
  freeTx(tx);
  assert(node.NodeOid() == r.NodeOid());
  assert(node.Flags() == r.Flags());
  assert(node.Height() == r.Height());
  assert(node.LastPtr() == r.LastPtr());
  assert(node.LeftPtr() == r.LeftPtr());
  assert(node.RightPtr() == r.RightPtr());
  assert(node.Ncells() == r.Ncells());
  assert(node.CellsSize() == r.CellsSize());
  assert(node.CellType() == r.CellType());
  for (i=0; i <= node.Ncells(); ++i) 
    assert(node.GetPtr(i) == r.GetPtr(i));
  for (i=0; i < node.Ncells(); ++i)
    assert(ListCellPlus::equal(node.Cells()[i], r.Cells()[i])==0);
  dprintf(1, "Chknode %llx %llx ok", (long long)coid.cid, (long long)coid.oid);
  return 1;
}

// Splits a node.
// toSplit: node to split
// cell: where to split.
//   If cell==0, the split is done in the middle of the node if the node is
//               too large.
//   If cell!=0, the split is done at the indicated cell, which becomes the
//               first cell of the split node. This cell should not be the
//               first cell in the node.
// remote: type of transaction to use (normally set to true)
// enqueueMoreSplit: optional function to enqueue more nodes to be split.
//     The function will invoke this function (if non-null) for any node that
//     requires further splits
// enqueueMoreSplitParm: parameter to pass enqueueMoreSplit
int DtSplit(COid toSplit, ListCellPlus *cell, bool remote,
            int (*enqueueMoreSplit)(COid, int, void*, int), 
            void *enqueueMoreSplitParm){
  // start a new transaction
  // read real toSplit node
  // read real parent
  // check if parent does not point to toSplit
  // if not, call function to find real parent by doing a traversal using
  //      toSplit's leftmost cell
  // splitindex = find midpoint of toSplit node (if cell==0) or location of
  //      cell (if cell != 0)
  // obtain new oid for left node
  // copy splitindex cell, and set its pointer to the left node
  // listAdd this new cell to parent
  // create left node with cells first..splitindex-1, with rightmost
  //      pointer = pointer of old splitindex cell
  // set left pointer to be toSplit's left pointer, right pointer to be toSplit
  // writeSV left node
  // attrSet left pointer of toSplit to be the left cell
  // attrSet the right pointer of the node to the left of toSplit (if not 0)
  //      to be the left cell
  // DelRange of first..splitindex from toSplit node (the right node)
  // commit transaction
  
  int res, i, splitindex;
  int cellsInNodesplit;
  int cellSizeInNodesplit;

  KVTransaction *tx;
  COid parentcoid, leftcoid, oldleftcoid;
  DTreeNode nodesplit, nodeparent;
  Ptr<RcKeyInfo> prki;
  Timestamp committs;

  parentcoid.cid = toSplit.cid;
  leftcoid.cid = toSplit.cid;
  oldleftcoid.cid=toSplit.cid;
  parentcoid.oid = 0;
  bool splitroot;

  // start a new transaction
#ifndef DTREE_SPLIT_DEFER_TS
  beginTx(&tx, remote);
#else
  beginTx(&tx, remote, true);
#endif

  // read real toSplit node
  res = auxReadReal(tx, toSplit, nodesplit, 0, 0);
  if (res){ dprintf(1,"a%d ", res); return res; }
  assert(nodesplit.raw->type==1); // must be supervalue

  prki = nodesplit.Prki();

  // check if cell==0 and node is too large (node has been split already)
  //       or cell!=0 and node smaller than minimum splittable size (no
  //       split possible)
  if (!cell && nodesplit.Ncells() <= DTREE_SPLIT_SIZE &&
      nodesplit.CellsSize() <= DTREE_SPLIT_SIZE_BYTES ||
      cell && nodesplit.Ncells() < DTREE_SPLIT_MINSIZE){ // do not split
    dputchar(1,'_');
    freeTx(tx);
    return 0;
  }

  // splitindex = find midpoint of toSplit node
  if (!cell) splitindex = nodesplit.Ncells()/2;
  else {
    splitindex = GCellSearchNode(nodesplit, cell->nKey, cell->pKey,
                                 cell->pprki.getprki(),  0);
    if (splitindex==0) ++splitindex;
  }
  cellsInNodesplit = nodesplit.Ncells()-splitindex-1; // # cells that will be
                                       // left in node being split after split
  cellSizeInNodesplit = 0; // compute size of cells that will be left in node
                           // being split after split
  for (i = splitindex+1; i < nodesplit.Ncells(); ++i)
    cellSizeInNodesplit += nodesplit.Cells()[i].size();

  // obtain new coid for left node
  leftcoid.oid = NewOid(remote);
  setRandomServerid(&leftcoid.oid); // random serverid policy

  // copy splitindex cell, and set its pointer to the left node
  ListCell lc(nodesplit.Cells()[splitindex]);
  lc.value = leftcoid.oid;

  // create left node with cells first..splitindex-1, with last
  //     pointer = pointer of old splitindex cell,
  // and with flags and height matching the node to be split
  SuperValue leftnode;
  memset(&leftnode, 0, sizeof(SuperValue));
  leftnode.Nattrs = DTREENODE_NATTRIBS;
  leftnode.CellType = nodesplit.CellType();
  leftnode.prki = nodesplit.Prki();
  leftnode.Attrs = new u64[DTREENODE_NATTRIBS];
  // copy flags and height from right node (toSplit node)
  leftnode.Attrs[DTREENODE_ATTRIB_FLAGS] = nodesplit.Flags();
  leftnode.Attrs[DTREENODE_ATTRIB_HEIGHT] = nodesplit.Height();

  //DTreeNode::InitSuperValue(&leftnode, 1);
  leftnode.Ncells = splitindex;
  if (nodesplit.Flags() & DTREENODE_FLAG_LEAF)
    ++leftnode.Ncells; // if splitting a leaf, left node should contain
                        // splitindex
  leftnode.Cells = new ListCell[leftnode.Ncells];
  for (i=0; i < leftnode.Ncells; ++i){
    leftnode.Cells[i].copy(nodesplit.Cells()[i]); // copy cell from node
                                       // to split
    leftnode.CellsSize += leftnode.Cells[i].size();
  }
  leftnode.Attrs[DTREENODE_ATTRIB_LASTPTR] =
    nodesplit.Cells()[splitindex].value; // set last pointer

  oldleftcoid.oid = nodesplit.LeftPtr(); // save leftptr (if any) before
      // changing the node to split. Note that oldleftcoid.oid will be 0
      // if there is not left pointer in node to be split

  splitroot = toSplit.oid == 0;

  if (splitroot){
    // change oid of node to be split
    nodesplit.raw->coid.oid = NewOid(remote);
    setRandomServerid(&nodesplit.raw->coid.oid); // random serverid policy
    parentcoid.oid = 0; //root is parent

    SuperValue newroot;
    memset(&newroot, 0, sizeof(SuperValue));
    newroot.Nattrs = DTREENODE_NATTRIBS;
    newroot.CellType = nodesplit.CellType();
    newroot.prki = nodesplit.Prki();
    newroot.Attrs = new u64[DTREENODE_NATTRIBS];
    // copy flags from right node (toSplit node), and set height to be 1 greater
    newroot.Attrs[DTREENODE_ATTRIB_FLAGS] = nodesplit.Flags() &
      ~DTREENODE_FLAG_LEAF; // not leaf
    newroot.Attrs[DTREENODE_ATTRIB_HEIGHT] = nodesplit.Height()+1;
    newroot.Attrs[DTREENODE_ATTRIB_LASTPTR] = nodesplit.raw->coid.oid; // right
                                         // pointer of root is node being split
    newroot.Attrs[DTREENODE_ATTRIB_LEFTPTR] = 0;
    newroot.Attrs[DTREENODE_ATTRIB_RIGHTPTR] = 0;

    // set left node's left pointer to be toSplit's left pointer, right
    // pointer to be toSplit
    leftnode.Attrs[DTREENODE_ATTRIB_LEFTPTR] = nodesplit.LeftPtr();
    leftnode.Attrs[DTREENODE_ATTRIB_RIGHTPTR] = nodesplit.raw->coid.oid;

    // set right node's (toSplit's) left pointer to be left node
    nodesplit.raw->u.raw->Attrs[DTREENODE_ATTRIB_LEFTPTR] = leftcoid.oid;

    // add the new cell to the root
    newroot.Ncells = 1;
    newroot.Cells = new ListCell[1];
    newroot.Cells[0].copy(lc);
    newroot.CellsSize = newroot.Cells[0].size();

    // remove cells 0..splitindex from toSplit node (the right node)
    nodesplit.raw->u.raw->DeleteCellRange(0, splitindex+1);

    // writeSV left node, right node, root
    res = KVwriteSuperValue(tx, leftcoid, &leftnode);
    if (res){ dprintf(1,"b%d ", res); goto end; }
    res = KVwriteSuperValue(tx, nodesplit.raw->coid, nodesplit.raw->u.raw);
    if (res){ dprintf(1, "c%d ", res); goto end; }
    res = KVwriteSuperValue(tx, parentcoid, &newroot);
    if (res){ dprintf(1, "d%d ", res); goto end; }

    // attrSet the right pointer of the node to the left of toSplit (if not 0)
    // to be the left cell
    if (oldleftcoid.oid){
      res = KVattrset(tx, oldleftcoid, DTREENODE_ATTRIB_RIGHTPTR, leftcoid.oid);
      if (res){ dprintf(1, "e%d ", res);  goto end; }
    }
    res = commitTx(tx, &committs);
    // commit transaction
    freeTx(tx);
    if (res){ dprintf(1, "m%d ", res); goto end; }
    else {
      // fix cached entries
      DTreeNode tofix;
      // fix newroot (parentcoid)
      if (!(newroot.Attrs[DTREENODE_ATTRIB_FLAGS] & DTREENODE_FLAG_LEAF)){
        // only need to fix if inner node (leafs not cached)
        tofix.raw = new Valbuf(newroot, parentcoid, true, &committs);
        GCache.remove(parentcoid);
        GCache.refresh(tofix.raw);
        //assert(chknode(parentcoid, tofix, remote));
      }

      // fix nodesplit
      if (nodesplit.isInner()){
        // only need to fix if inner node (leafs not cached)
        tofix.raw = new Valbuf(*nodesplit.raw);
        tofix.raw->commitTs = committs; // update timestamps
        tofix.raw->readTs = committs;
        GCache.remove(nodesplit.raw->coid);
        GCache.refresh(tofix.raw);
        //assert(chknode(nodesplit.raw->coid, tofix, remote));
      }

      // fix leftnode (leftcoid)
      if (!(leftnode.Attrs[DTREENODE_ATTRIB_FLAGS] & DTREENODE_FLAG_LEAF)){
        // only need to fix if inner node (leafs not cached)
        tofix.raw = new Valbuf(leftnode, leftcoid, true, &committs);
        GCache.remove(leftcoid);
        GCache.refresh(tofix.raw);
        //assert(chknode(leftcoid, tofix, remote));
      }

      // fix oldleftcoid (if present)
      if (oldleftcoid.oid){
        res = auxReadCache(oldleftcoid, tofix);
        if (res==0){
#if (DTREE_SPLIT_LOCATION==1)  // splitting at client; cache is shared
                              // so we should not alter entries directly
          tofix.raw = new Valbuf(*tofix.raw); // make a copy
#endif
          tofix.raw->commitTs = committs; // update timestamps
          tofix.raw->readTs = committs;
          tofix.RightPtr() = leftcoid.oid;
#if (DTREE_SPLIT_LOCATION==1)
          GCache.remove(oldleftcoid);
          GCache.refresh(tofix.raw);
#endif
          //assert(chknode(oldleftcoid, tofix, remote));
        }
      }
    }
  }
  else { // splitting non-root
    // find real parent by doing a traversal using toSplit's leftmost cell
    res = FindParentCache(tx, toSplit, nodesplit.Cells()[0], prki,
                          parentcoid.oid);
    //if (!res) dprintf(1, "Found parent of %llx %llx using cache",
    //                  (long long)toSplit.cid, (long long)toSplit.oid);
    if (res){
      dprintf(1, "Cannot find parent of %llx %llx using cache: %d",
              (long long)toSplit.cid, (long long)toSplit.oid, res);
      res = FindParentReal(tx, toSplit, nodesplit.Cells()[0], prki,
                           parentcoid.oid);
      if (res){ dprintf(1, "g%d ", res); goto end; }
    }

    // set left node's left pointer to be toSplit's left pointer,
    // right pointer to be toSplit
    leftnode.Attrs[DTREENODE_ATTRIB_LEFTPTR] = nodesplit.LeftPtr();
    leftnode.Attrs[DTREENODE_ATTRIB_RIGHTPTR] = nodesplit.NodeOid();

    // listAdd the new cell to parent
#if DTREE_SPLIT_LOCATION != 1
    res = KVlistadd(tx, parentcoid, &lc, prki, 2);  // flags&2 means
                                                           // bypass throttling
#else
    res = KVlistadd(tx, parentcoid, &lc, prki, 2, 0, 0); // flags&2
                                                   // means bypass throttling
#endif
    if (res){ dprintf(1, "h%d ", res); goto end; }

    // writeSV left node
    res = KVwriteSuperValue(tx, leftcoid, &leftnode);
    if (res){ dprintf(1, "i%d ", res);  goto end; }

    // attrSet left pointer of toSplit to be the left cell
    res = KVattrset(tx, toSplit, DTREENODE_ATTRIB_LEFTPTR, leftcoid.oid);
    if (res){ dprintf(1, "j%d ", res); goto end; }

    // attrSet the right pointer of the node to the left of toSplit (if not 0)
    // to be the left cell
    if (oldleftcoid.oid){
      res = KVattrset(tx, oldleftcoid, DTREENODE_ATTRIB_RIGHTPTR, leftcoid.oid);
      if (res){ dprintf(1, "k%d ", res);  goto end; }
    }
    
    // DelRange of cells (-inf..splitindex+1) from toSplit node (the right node)
    res = KVlistdelrange(tx, toSplit, 6, &nodesplit.Cells()[0],
                         &nodesplit.Cells()[splitindex+1], prki);
    if (res){ dprintf(1, "l%d ", res);  goto end; }

    res = commitTx(tx, &committs);
    // commit transaction
    freeTx(tx);
    if (res){ dprintf(1, "m%d ", res);  goto end; }
    else { // fix cached entries for modified objects
      // fix parentcoid: insert new element
      DTreeNode tofix;
      int index;
      res = auxReadCache(parentcoid, tofix);
      if (!res){ // found
#if (DTREE_SPLIT_LOCATION==1)  // splitting at client; cache is shared so
                               // we should not alter entries directly
        tofix.raw = new Valbuf(*tofix.raw); // make a copy
#endif
        tofix.raw->commitTs = committs; // update timestamps
        tofix.raw->readTs = committs;
        index = GCellSearchNode(tofix, lc.nKey, lc.pKey, prki, 0);
        assert(0 <= index && index <= tofix.Ncells());
        tofix.raw->u.raw->InsertCell(index);
        tofix.raw->u.raw->CellsSize += lc.size();
        new(&tofix.raw->u.raw->Cells[index]) ListCell(lc); // placement
                                                           // constructor
#if (DTREE_SPLIT_LOCATION==1)
        GCache.remove(parentcoid);
        GCache.refresh(tofix.raw);
#endif
        //assert(chknode(parentcoid, tofix, remote));
      }

      // fix leftcoid: write new node
      if (!(leftnode.Attrs[DTREENODE_ATTRIB_FLAGS] & DTREENODE_FLAG_LEAF)){
        // only need to fix if inner node (leafs not cached)
        tofix.raw = new Valbuf(leftnode, leftcoid, true, &committs);
        GCache.remove(leftcoid);
        GCache.refresh(tofix.raw);
        //assert(chknode(leftcoid, tofix, remote));
      }

      // fix toSplit: set leftptr, delete range
      res = auxReadCache(toSplit, tofix);
      if (!res && splitindex+1 <= tofix.raw->u.raw->Ncells){
#if (DTREE_SPLIT_LOCATION==1)  // splitting at client; cache is shared so we
                               // should not alter entries directly
        tofix.raw = new Valbuf(*tofix.raw); // make a copy
#endif
        tofix.raw->commitTs = committs; // update timestamps
        tofix.raw->readTs = committs;
        tofix.LeftPtr() = leftcoid.oid;
        tofix.raw->u.raw->DeleteCellRange(0, splitindex+1);
#if (DTREE_SPLIT_LOCATION==1)
        GCache.remove(toSplit);
        GCache.refresh(tofix.raw);
#endif
        //assert(chknode(toSplit, tofix, remote));
      }

      // fix oldleftcoid (if present): set rightptr
      if (oldleftcoid.oid){
        res = auxReadCache(oldleftcoid, tofix);
        if (!res){
#if (DTREE_SPLIT_LOCATION==1)  // splitting at client; cache is shared so we
                               // should not alter entries directly
          tofix.raw = new Valbuf(*tofix.raw); // make a copy
#endif
          tofix.raw->commitTs = committs; // update timestamps
          tofix.raw->readTs = committs;
          tofix.RightPtr() = leftcoid.oid;
#if (DTREE_SPLIT_LOCATION==1)
          GCache.remove(oldleftcoid);
          GCache.refresh(tofix.raw);        
#endif
          //assert(chknode(oldleftcoid, tofix, remote));
        }
      }
    }
  }

  if (enqueueMoreSplit){
    // see if we need to split parent
    if (!splitroot){ // if we just split root, then we do not need to split
                     // parent since it was just created with 1 cell
#ifndef DTREE_SPLIT_DEFER_TS
      beginTx(&tx, remote);
#else
      beginTx(&tx, remote, true);
#endif

      res = KVreadSuperValue(tx, parentcoid, nodeparent.raw, 0, 0);
      if (res){ dprintf(1, "n%d ", res); goto end; }
      freeTx(tx);
      if (nodeparent.Ncells() > DTREE_SPLIT_SIZE ||
          nodeparent.CellsSize() > DTREE_SPLIT_SIZE_BYTES){
        //dprintf(1, "Need to further split parent %llx %llx\n",
        //          (long long)parentcoid.cid, p(long long)arentcoid.oid);
        enqueueMoreSplit(parentcoid, 0, enqueueMoreSplitParm, 0); // enqueue
                                                   // request to split parent
      }
    }

    // see if we need to further split left node
    if (leftnode.Ncells > DTREE_SPLIT_SIZE ||
        leftnode.CellsSize > DTREE_SPLIT_SIZE_BYTES){
      //dprintf(1, "Need to further split left node %llx %llx\n",
      //            (long long)leftcoid.cid, (long long)leftcoid.oid);
      enqueueMoreSplit(leftcoid, 1, enqueueMoreSplitParm,
         leftnode.Attrs[DTREENODE_ATTRIB_FLAGS] & DTREENODE_FLAG_LEAF ? 1 : 0);
    }

    // see if we need to further split right node
    if (cellsInNodesplit > DTREE_SPLIT_SIZE ||
        cellSizeInNodesplit > DTREE_SPLIT_SIZE_BYTES){
      //dprintf(1, "Need to further split right node %llx %llx\n",
      //(long long)nodesplit.raw->coid.cid, (long long)nodesplit.raw->coid.oid);
      enqueueMoreSplit(nodesplit.raw->coid, 1, enqueueMoreSplitParm,
                       nodesplit.isLeaf() ? 1 : 0);
    }
  }
  res = 0;
 end:
  lc.Free();
  return res;
}

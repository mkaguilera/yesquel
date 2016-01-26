//
// showdtree.cpp
//
// Prints out a distributed B-tree
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

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <malloc.h>
#include <assert.h>
#include <stdarg.h>
#include <values.h>

#include "tmalloc.h"
#include "gaiatypes.h"
#include "util.h"
#include "datastruct.h"
#include "clientlib.h"
#include "clientdir.h"
#include "dtreeaux.h"

#define SHOWREALOIDS

#define MAXCOLLEN 3

extern StorageConfig *SC;

int OptShowReal = 0;    // show real oids instead of small numbers
int OptSummaryOnly = 0; // show only summary
int OptCheck = 0;  // check integrity

struct COidQueueElement {
  COidQueueElement(){ coid.cid = coid.oid = 0; }
  COidQueueElement(COid c){ coid=c; }
  COid coid;
  i64 fencemin; // exclusive (node is not supposed to have this element)
  i64 fencemax; // inclusive (node could have this element)
  COidQueueElement *prev, *next;
};

SkipList<COid, int> COidMap;
int nextId = 0;

int getid(COid coid){
  int res, *retid;
  int id;
  res = COidMap.lookup(coid, retid); // try to find coid
  if (!res) id = *retid;
  else { // not found
    id = nextId;
    COidMap.insert(coid, id);
    ++nextId;
  }
  return id;
}

int c8(char *data){
  unsigned char *buf = (unsigned char*) data;
  return (signed char)buf[0];
}

int c16(char *data){
  unsigned char *buf = (unsigned char*) data;
  return (((signed char)buf[0])<<8) + buf[1];
}

int c24(char *data){
  unsigned char *buf = (unsigned char*) data;
  return (((signed char)buf[0])<<16) + (buf[1]<<8) + buf[2];
}

int c32(char *data){
  unsigned char *buf = (unsigned char*) data;
  return (buf[0]<<24) + (buf[1]<<16) + (buf[2]<<8) + buf[3];
}

i64 c48(char *data){
  unsigned char *buf = (unsigned char*) data;
  u64 x = (((signed char)buf[0])<<8) | buf[1];
  u32 y = (buf[2]<<24) | (buf[3]<<16) | (buf[4]<<8) | buf[5];
  x = (x<<32) | y;
  return *(i64*)&x;
}

i64 c64(char *data){
  unsigned char *buf = (unsigned char*) data;
  u64 x;
  u32 y;

  x = (buf[0]<<24) | (buf[1]<<16) | (buf[2]<<8) | buf[3];
  y = (buf[4]<<24) | (buf[5]<<16) | (buf[6]<<8) | buf[7];
  x = (x<<32) | y;
  return *(i64*)&x;
}

double cfloat(char *data){
  unsigned char *buf = (unsigned char*) data;
  u64 x;
  u32 y;

  x = (buf[0]<<24) | (buf[1]<<16) | (buf[2]<<8) | buf[3];
  y = (buf[4]<<24) | (buf[5]<<16) | (buf[6]<<8) | buf[7];
  x = (x<<32) | y;
  return *(double*)&x;
}

char *coidtostr(COid coid, bool showroot=false){
  static char str[256];
  int i;
  if (!OptShowReal){
    i = getid(coid);
    if (i == 0){
      if (showroot) sprintf(str, "A");
      else sprintf(str, "-");
    }
    else if (i < 26) sprintf(str, "%c", i+'A');
    else sprintf(str, "#%d", i-26);
  }
  else
    sprintf(str, "%llx", (long long)coid.oid);
  return str;
}

char *coidtostraux(Cid cid, Oid oid, bool showroot=false){
  COid coid;
  coid.cid = cid;
  coid.oid = oid;
  return coidtostr(coid, showroot);
}

char *pnkeytostr(int nkey, char *pkey){
  static char str[256];
  u64 headerlen;
  u64 col1;
  int i;
  int len;
  char *ptr = pkey;
  ptr += myGetVarint((unsigned char*) ptr, &headerlen);
  ptr += myGetVarint((unsigned char*) ptr, &col1);
  char *data = pkey + headerlen;
  switch(col1){
  case 0: return strcpy(str, "nil"); break;
  case 1: sprintf(str, "%d", *data); break;
  case 2: sprintf(str, "%d", c16(data)); break;
  case 3: sprintf(str, "%d", c24(data)); break;
  case 4: sprintf(str, "%d", c32(data)); break;
  case 5: sprintf(str, "%lld", (long long)c48(data)); break;
  case 6: sprintf(str, "%lld", (long long)c64(data)); break;
  case 7: sprintf(str, "%f", cfloat(data)); break;
  case 8: sprintf(str, "0"); break;
  case 9: sprintf(str, "1"); break;
  case 10:
  case 11: sprintf(str, "?"); break;
  default:
    if (col1%2 == 0) len = ((int)col1-12)/2;
    else len = ((int)col1-13)/2;
    if (len >= MAXCOLLEN) len = MAXCOLLEN;
    for (i=0; i < len; ++i){
      if (isprint((u8)data[i])) str[i] = data[i];
      else str[i]='.';
    }
    str[len] = 0;
  }
  return str;
}

void ShowLeaf(COid coid, SuperValue *sv){
  int i;
  COid left, right;

  left.cid = coid.cid;
  left.oid = sv->Attrs[DTREENODE_ATTRIB_LEFTPTR];
  right.cid = coid.cid;
  right.oid = sv->Attrs[DTREENODE_ATTRIB_RIGHTPTR];
  printf("Node [%s]", coidtostr(coid, true));
  printf(" hgt %d%s%s", (int) sv->Attrs[DTREENODE_ATTRIB_HEIGHT], 
    sv->Attrs[DTREENODE_ATTRIB_FLAGS] & DTREENODE_FLAG_LEAF ? "(leaf)" : "", 
    sv->Attrs[DTREENODE_ATTRIB_FLAGS] & DTREENODE_FLAG_INTKEY ? "(int)" : "");
  printf(" left [%s]", coidtostr(left, false));
  printf(" right [%s]\n      ", coidtostr(right, false));
  for (i=0; i < sv->Ncells; ++i){
    if (sv->CellType == 0) printf("%llx ", (long long)sv->Cells[i].nKey);
    else printf("%s ", pnkeytostr((int)sv->Cells[i].nKey, sv->Cells[i].pKey));
  }
  putchar('\n');
}

void ShowInner(COid coid, SuperValue *sv){
  int i;
  COid child, left, right;
  child.cid = coid.cid;
  left.cid = coid.cid;
  left.oid = sv->Attrs[DTREENODE_ATTRIB_LEFTPTR];
  right.cid = coid.cid;
  right.oid = sv->Attrs[DTREENODE_ATTRIB_RIGHTPTR];
  printf("Node [%s]", coidtostr(coid, true));
  printf(" hgt %d%s%s", (int) sv->Attrs[DTREENODE_ATTRIB_HEIGHT], 
    sv->Attrs[DTREENODE_ATTRIB_FLAGS] & DTREENODE_FLAG_LEAF ? "(leaf)" : "", 
    sv->Attrs[DTREENODE_ATTRIB_FLAGS] & DTREENODE_FLAG_INTKEY ? "(int)" : "");
  printf(" left [%s]", coidtostr(left, false));
  printf(" right [%s]\n      ", coidtostr(right, false));
  //printf("Node %d hgt %d%s%s left %d right %d:\n ", getid(coid), (int) sv->Attrs[DTREENODE_ATTRIB_HEIGHT], 
  //  sv->Attrs[DTREENODE_ATTRIB_FLAGS] & DTREENODE_FLAG_LEAF ? "(leaf)" : "", 
  //  sv->Attrs[DTREENODE_ATTRIB_FLAGS] & DTREENODE_FLAG_INTKEY ? "(int)" : "", 
  //  getid(left), getid(right));
  for (i=0; i < sv->Ncells; ++i){
    child.oid = sv->Cells[i].value;
    printf("[%s] ", coidtostr(child, false));
    if (sv->CellType == 0) printf("%llx ", (long long)sv->Cells[i].nKey);
    else printf("%s ", pnkeytostr((int)sv->Cells[i].nKey, sv->Cells[i].pKey));
  }
  child.oid = sv->Attrs[DTREENODE_ATTRIB_LASTPTR];  
  printf("[%s]\n", coidtostr(child,false));
}

// returns 0 if ok, -1 if error
int CheckNode(COid coid, SuperValue *sv, i64 fencemin, i64 fencemax){
  int status = 0;
  if (sv->CellType != 0) return 0; // checking for non-intkey is not supported
  
  for (int i=0; i < sv->Ncells; ++i){
    // check that fencemin < sv->Cells[i].nKey <= fencemax (except that if fencemin is LLONG_MIN, no check
    //                                                      is made against it)
    if (fencemin != LLONG_MIN && sv->Cells[i].nKey  <= fencemin || fencemax < sv->Cells[i].nKey){
      printf("Error %016llx:%016llx cell %llx outside range (%llx,%llx]\n",
             (long long)coid.cid, (long long)coid.oid, (long long) sv->Cells[i].nKey,
             (long long)fencemin, (long long)fencemax);
      status = -1;
    }
  }
  return status;
}

int checkNodeMonot(SuperValue *sv){
  assert(sv->CellType == 0);  // checking for non-intkey is not supported
  i64 prevkey = LLONG_MIN;
  
  for (int i=0; i < sv->Ncells; ++i){
    if (prevkey > sv->Cells[i].nKey) return -1;
    prevkey = sv->Cells[i].nKey;
  }
  return 0;
}

// check that following right pointers lead to siblings
// direction = 0 for left, 1 for right
int checkHorizontal(Transaction *tx, COid coid, int direction){
  int res;
  Ptr<Valbuf> buf, buf2;
  COid coid2;
  SuperValue *sv, *sv2;
  int nextattr, prevattr;
  int ret = 0;
  
  res = tx->vsuperget(coid, buf, 0, 0); assert(res==0);
  assert(buf->type!=0);
  sv = buf->u.raw;
  res = checkNodeMonot(sv);
  if (res){
    printf("Error [%s] cells not monotonic\n", coidtostr(coid));
    ++ret;
  }
  if (direction == 0){ // left
    nextattr = DTREENODE_ATTRIB_LEFTPTR;
    prevattr = DTREENODE_ATTRIB_RIGHTPTR;
  }
  else { // right
    nextattr = DTREENODE_ATTRIB_RIGHTPTR;
    prevattr = DTREENODE_ATTRIB_LEFTPTR;
  }
  
  // if right pointer is set
  if (sv->Attrs[nextattr]){
    // read it
    coid2.cid = coid.cid;
    coid2.oid = sv->Attrs[nextattr];
    res = tx->vsuperget(coid2, buf2, 0, 0); assert(res==0);
    assert(buf2->type!=0);
    sv2 = buf2->u.raw;

    res = checkNodeMonot(sv2);
    if (res){
      printf("Error [%s] cells not monotonic\n", coidtostr(coid2));
      ++ret;
    }
    
    // check that it points back to us
    if (sv2->Attrs[prevattr] != coid.oid){
      printf("Error [%s] neighbor points to ", coidtostr(coid));
      printf("[%s] not back to us\n", coidtostraux(coid.cid, (Oid)sv2->Attrs[prevattr]));
      ++ret;
    }
    
    // check that level is the same
    if (sv2->Attrs[DTREENODE_ATTRIB_HEIGHT] != sv->Attrs[DTREENODE_ATTRIB_HEIGHT]){
      printf("Error [%s] height %lld ",
             coidtostr(coid),
             (long long)sv->Attrs[DTREENODE_ATTRIB_HEIGHT]);
      printf("mismatch neighbor [%s] height %lld\n",
             coidtostr(coid2),
             (long long)sv2->Attrs[DTREENODE_ATTRIB_HEIGHT]);
      ++ret;
    }
    
    // check that leaf status is the same
    if ((sv2->Attrs[DTREENODE_ATTRIB_FLAGS] & DTREENODE_FLAG_LEAF) !=
        (sv->Attrs[DTREENODE_ATTRIB_FLAGS] & DTREENODE_FLAG_LEAF)){
      printf("Error [%s] leaf %lld ",
             coidtostr(coid),
             (long long)sv->Attrs[DTREENODE_ATTRIB_FLAGS] & DTREENODE_FLAG_LEAF);
      printf("mismatch neighbor [%s] leaf %lld\n",
             coidtostr(coid2),
             (long long)sv2->Attrs[DTREENODE_ATTRIB_FLAGS] & DTREENODE_FLAG_LEAF);
      ++ret;
    }

    // check that int status is the same
    if ((sv2->Attrs[DTREENODE_ATTRIB_FLAGS] & DTREENODE_FLAG_INTKEY) !=
        (sv->Attrs[DTREENODE_ATTRIB_FLAGS] & DTREENODE_FLAG_INTKEY)){
      printf("Error [%s] int %lld ",
             coidtostr(coid),
             (long long)sv->Attrs[DTREENODE_ATTRIB_FLAGS] & DTREENODE_FLAG_INTKEY);
      printf("mismatch neighbor [%s] int %lld\n",
             coidtostr(coid2),
             (long long)sv2->Attrs[DTREENODE_ATTRIB_FLAGS] & DTREENODE_FLAG_INTKEY);
      ++ret;
    }

    // check that largest key in first node is < smallest key in second
    if (sv->Ncells && sv2->Ncells){
      if (direction == 0){
        if (sv->Cells[0].nKey <= sv2->Cells[sv2->Ncells-1].nKey){
          printf("Error [%s] first key %llx ",
                 coidtostr(coid),
                 (long long)sv->Cells[0].nKey);
          printf("smaller than left neighbor [%s] last key %llx\n",
                 coidtostr(coid2),
                 (long long)sv2->Cells[sv2->Ncells-1].nKey);

          ++ret;
        }
      } else {
        if (sv->Cells[sv->Ncells-1].nKey >= sv2->Cells[0].nKey){
          printf("Error [%s] last key %llx ", 
                 coidtostr(coid),
                 (long long)sv->Cells[sv->Ncells-1].nKey);
          printf("greater than right neighbor [%s] first key %llx\n",
                 coidtostr(coid2),
                 (long long)sv2->Cells[0].nKey);
          ++ret;
        }
      }
    }
    
    // recursively call on right pointer
    ret += checkHorizontal(tx, coid2, direction);
  }
  return ret;
}


// statistics computed by printCoid
int nelemsLeaf = 0;   // number of cells at leafs
int nelemsTotal = 0;  // total number of cells
int nleafs = 0;       // number of leaf nodes
int ninner = 0;       // number of inner nodes
int largest = 0;      // largest number of cells in a node
int smallest = MAXINT;// smallest number of cells in a node excluding the first one (root)
int largestSize = 0;  // size of largest node
int smallestSize = MAXINT; // size of smallest node excluding the first one
int depth = 0;  // depth

int bignodes = 0;

#define BIGNODE_THRESHOLD 50  // number of cells above which a node is considered to be big

int printCoid(COid startcoid){
  Transaction *tx;
  tx = new Transaction(SC);
  LinkList<COidQueueElement> coidqueue;
  Set<COid> pastcoids;
  COidQueueElement *el, *elchild;
  COid coid;
  COid child;
  int res;
  Ptr<Valbuf> buf;
  i64 fencemin, fencemax;

  SuperValue *sv;

  bool first=true;

  el = new COidQueueElement(startcoid);
  el->fencemin = LLONG_MIN;
  el->fencemax = LLONG_MAX;
  coidqueue.pushTail(el);

  while (!coidqueue.empty()){
    el = coidqueue.popHead();
    coid = el->coid;
    fencemin = el->fencemin;
    fencemax = el->fencemax;
    delete el;

    if (OptCheck){ // check if coid was seen before
      if (pastcoids.belongs(coid)){
        printf("Error COid %016llx referenced more than once\n", (long long)coid.oid);
      } else pastcoids.insert(coid);
      checkHorizontal(tx, coid, 0);
      checkHorizontal(tx, coid, 1);
    }
    
    // read coid
    res = tx->vsuperget(coid, buf, 0, 0); if (res) return res;

    if (buf->type==0){
      printf("COid %llx (%d) not a supervalue\n", (long long)coid.oid, getid(coid));
      continue;
    }

    // update stats
    sv = buf->u.raw;
    nelemsTotal += sv->Ncells;
    if (depth < (int)sv->Attrs[DTREENODE_ATTRIB_HEIGHT]) 
      depth = (int)sv->Attrs[DTREENODE_ATTRIB_HEIGHT];
    if (first) first=false;
    else {
      if (sv->Ncells > largest) largest = sv->Ncells;
      if (sv->Ncells < smallest) smallest = sv->Ncells;
      if (sv->Ncells > BIGNODE_THRESHOLD) ++bignodes;
      if (sv->CellsSize > largestSize) largestSize = sv->CellsSize;
      if (sv->CellsSize < smallestSize) smallestSize = sv->CellsSize;
    }

    if (OptCheck)
      CheckNode(coid, sv, fencemin, fencemax);
    
    if (sv->Attrs[DTREENODE_ATTRIB_FLAGS] & DTREENODE_FLAG_LEAF){ // this is a leaf node
      ++nleafs;
      nelemsLeaf += sv->Ncells;
      if (!OptSummaryOnly)
        ShowLeaf(coid, sv);
      //else { putchar('.'); fflush(stdout); }
    } else {  // this is an inner node
      ++ninner;
      if (!OptSummaryOnly)
        ShowInner(coid, sv);
      //else { putchar('.'); fflush(stdout); }
    }

    if (!(sv->Attrs[DTREENODE_ATTRIB_FLAGS] & DTREENODE_FLAG_LEAF)){
      // add children to queue
      child.cid = coid.cid;
      for (int i=0; i < sv->Ncells; ++i){
        child.oid = sv->Cells[i].value;
        elchild = new COidQueueElement(child);

        if (sv->CellType == 0){ // intkey
          if (i==0) elchild->fencemin = fencemin;
          else elchild->fencemin = sv->Cells[i-1].nKey;
          elchild->fencemax = sv->Cells[i].nKey;
        } else {
          // checking functionality is disabled for non-intkey trees, so just set fences to 0
          elchild->fencemin = elchild->fencemax = 0;
        }
        coidqueue.pushTail(elchild);
      }
      // now add the last pointer
      child.oid = sv->Attrs[DTREENODE_ATTRIB_LASTPTR];
      elchild = new COidQueueElement(child);
      if (sv->CellType == 0){ // intkey
        elchild->fencemin = sv->Cells[sv->Ncells-1].nKey;
        elchild->fencemax = fencemax;
      } else {
        elchild->fencemin = elchild->fencemax = 0;
      }
      coidqueue.pushTail(elchild);
    }
  }
  //if (OptSummaryOnly) putchar('\n');
  delete tx;
  return 0;
}

int main(int argc, char **argv)
{
  char *argv0;
  COid coid;
  int res;
  int badargs;
  int c;

  //setvbuf(stdout, 0, _IONBF, 0);
  //setvbuf(stderr, 0, _IONBF, 0);

  // remove path from argv[0]
  argv0 = strrchr(argv[0], '\\');
  if (!argv0) argv0 = argv[0];
  else ++argv0;

  badargs=0;
  while ((c = getopt(argc,argv, "crs")) != -1){
    switch(c){
    case 'c':
      OptCheck = 1;
      break;
    case 'r':
      OptShowReal = 1;
      break;
    case 's':
      OptSummaryOnly = 1;
      break;
    default:
      ++badargs;
    }
  }
  argc = argc - optind;
  
  if (argc != 1 && argc != 2 || badargs){
    fprintf(stderr, "usage: %s [-crs] containerid [objectid]\n", argv0);
    fprintf(stderr, "          (both parameters in hex)\n");
    fprintf(stderr, "  -c check integrity (intkey trees only)\n");
    fprintf(stderr, "  -r show real node ids\n");
    fprintf(stderr, "  -s show summary only\n");
    exit(1);
  }

  SC = InitGaia();

  sscanf(argv[optind], "%llx", (long long*)&coid.cid);
  if (argc == 2) sscanf(argv[optind+1], "%llx", (long long*)&coid.oid);
  else coid.oid = 0;
  printf("Showing tree rooted at %llx %llx\n", (long long)coid.cid, (long long)coid.oid);
  res = printCoid(coid);
  if (res){
    fprintf(stderr, "Error %d printing tree\n", res);
  }
  printf("\nStatistics\n");
  printf("  Depth              %d\n", depth);
  printf("  Total leaf cells   %d\n", nelemsLeaf);
  printf("  Total cells        %d\n", nelemsTotal);
  printf("  Leaf nodes         %d\n", nleafs);
  printf("  Inner nodes        %d\n", ninner);
  printf("  Largest #cells     %d\n", largest);
  printf("  Smallest #cells    %d\n", smallest != MAXINT ? smallest : -1);
  printf("  Largest cell size  %d\n", largestSize);
  printf("  Smallest cell size %d\n", smallestSize != MAXINT ? smallestSize : -1);
  printf("  Big nodes          %d\n", bignodes);

  UninitGaia(SC);
  exit(res);
}


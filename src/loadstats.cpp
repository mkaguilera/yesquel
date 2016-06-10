//
// loadstats.cpp
//
// Keeps statistics about access to coid's and cells within coids.
// We keep track of all read accesses within the last StatIntervalMs ms.
// Then, once this interval is past, we look at the statistics to find
// coids with a large number of accesses. For those coids, we look at
// the cell accesses to determine where they should be split.
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


#ifdef TEST_LOADSTATS
#include <stdio.h>
#include <unistd.h>
#endif

#include "loadstats.h"

// function to be called to split a node
void SplitNode(COid &coid, ListCellPlus *cell);

static void dellcp(ListCellPlus *lc){ if (lc) delete lc; }
static void delcoidstatkey(COidStat *cs){ if (cs) delete cs; }

COidStat::~COidStat(){
  CellStat.clear(dellcp, 0);
}

void LoadStats::report(COid &coid, ListCellPlus *cell){ // reports an access
  COidStat **csptr, *cs;
  int res;
  res = Stats.lookupInsert(coid, csptr);
  if (res){ // item was created
    *csptr = 0; // do not record exact cell for the first access. This is
                // because we expect lots of items with a single access only
    delete cell;
    return;
  }
  if (!*csptr){
    // this is the second time this coid is accessed; create a COidStat for it
    *csptr = new COidStat;
  }
  cs = *csptr;

  int *countptr;
  ++cs->Hits;
  res = cs->CellStat.lookupInsert(cell, countptr);
  if (res){ // item was just created
    *countptr = 0;
  }
  ++*countptr; // bump counter for cell
}

// check if period is done. If so, find heavy hitters, call the splitter and
// start new period.
// Returns 0 if period continues, non-zero if new period started
int LoadStats::check(void){
  u64 now;
  ListCellPlus *cell;
  now = Time::now();
  if (now-PeriodStart < StatIntervalMs) return 0;

  // iterate over Stats finding heavy hitters
  SkipListNode<COid,COidStat*> *ptr;
  COidStat *cs;
    
  for (ptr = Stats.getFirst(); ptr != Stats.getLast();
       ptr = Stats.getNext(ptr)){
    cs = ptr->value;
    if (cs && cs->Hits > HeavyHitterThreshold){
      SkipListNodeBK<ListCellPlus,int> *nptr;
      int hitthres = cs->Hits / 2;
      int count = 0;
      for (nptr = cs->CellStat.getFirst(); nptr != cs->CellStat.getLast();
           nptr = cs->CellStat.getNext(nptr)){
	count += nptr->value;
	if (count >= hitthres)
          break; // stop when we are past half the number of hits
      }
      assert(count >= hitthres);
      // nptr points to ListCellPlus where split should happen. The cell
      // pointed to is the first cell of the second half of the split.

      // split coid ptr->key at cell nptr->key
#ifndef DISABLE_NODESPLITS
      // clone the listcellplus and its RcKeyInfo
      cell = new ListCellPlus(*nptr->key, nptr->key->pprki.getprki());
      SplitNode(ptr->key, cell);
#endif      
    }
  }

  // clear up everything for new period
  Stats.clear(0, delcoidstatkey);
  PeriodStart = now;
  return -1;
}

// prints all stats
void LoadStats::print(void){
  u64 now = Time::now();
  printf("Age %lld\n", (long long) (now-PeriodStart));
    
  // iterate over Stats
  SkipListNode<COid,COidStat*> *ptr;
  COidStat *cs;
  COid coid;
    
  for (ptr = Stats.getFirst(); ptr != Stats.getLast();
       ptr = Stats.getNext(ptr)){
    coid = ptr->key;
    printf("%08llx:%08llx", (long long)coid.cid, (long long)coid.oid);
    cs = ptr->value;
    if (!cs){ printf("\n"); continue; } // no further data
    printf(" %d [", cs->Hits);
    SkipListNodeBK<ListCellPlus,int> *nptr;
    int notfirst=0;
    for (nptr = cs->CellStat.getFirst(); nptr != cs->CellStat.getLast();
         nptr = cs->CellStat.getNext(nptr)){
      if (notfirst) printf(", ");
      else notfirst = 1;
      nptr->key->printShort(false, false);
      printf(":%d", nptr->value);
    }
    printf("]\n");
  }
}

#ifdef TEST_LOADSTATS

int main(){
  int i, j, k, l;
  LoadStats ls;
  ListCell lc;
  ListCellPlus *lcptr;
  COid coid;
  int res;
  
  coid.cid = 0;

  k = 0;
  l = 0;
  
  for (i=0; i < 10000; ++i){
    coid.oid = k;
    ++l;
    if (l > k){
      l = 0;
      ++k;
    }
    
    for (j=1; j < 10; ++j){
      lc.nKey = Time::now() % j;
      lc.pKey = 0;
      lc.value = 0;
      lcptr = new ListCellPlus(lc,(RcKeyInfo*)0);
      ls.report(coid, lcptr);
    }
  }

  ls.print();

  do {
    res = ls.check();
    sleep(1);
  } while (!res);
  
  return 0;
}
#endif

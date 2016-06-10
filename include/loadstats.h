//
// loadstats.h
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

#ifndef _LOADSTATS_H
#define _LOADSTATS_H

const int StatIntervalMs=1000;
const int HeavyHitterThreshold=5000; // hits above which a coid is considered
                                     // a heavy hitter


#include <stdio.h>
#include <stdlib.h>
#include <malloc.h>

#include "os.h"
#include "tmalloc.h"
#include "datastruct.h"
#include "gaiatypes.h"
#include "util.h"
#include "supervalue.h"


// statistics kept for a given coid
struct COidStat {
  int Hits;
  SkipListBK<ListCellPlus,int> CellStat; // statistics for each accessed cell
  COidStat(){ Hits=0; }
  ~COidStat();
};

class LoadStats {
 private:
  SkipList<COid,COidStat*> Stats;
  u64 PeriodStart; // time when the period started
  
 public:
  LoadStats(){
    PeriodStart = Time::now();
  }
  
  void report(COid &coid, ListCellPlus *cell); // reports an access. cell will
      // be owned by LoadStats and should have its own RcKeyInfo (or RcKeyInfo
      // should be guaranteed to remain valid until the end of the interval)
  int check(void); // check if period is done. If so, find heavy hitters, call
      // the splitter and start new period. Returns 0 if period continues,
      // non-zero if new period started
  void print(void); // prints all stats
};

#endif

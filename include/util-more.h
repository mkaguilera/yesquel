//
// util-more.h
//
// More utilities
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

#ifndef _UTIL_MORE_H
#define _UTIL_MORE_H

#include <list>
#include <set>
using namespace std;

#include "util.h"
#include "datastruct.h"

// This class is used for getting unique integers.
// After geting an integer, the user can return it so that it can be
// reused later. Performance is good if only few integers are gotten at a time.
// If most integers have been gotten, performance will suffer a lot,
// so this class should not be used for such cases.
class UniqueInt {
  int Counter;
  RWLock UsedInts_l;
  set<int> UsedInts;
public:
  UniqueInt(){ Counter=0; }
  // get a unique non-zero integer. Returns 0 if no more integers available
  int getUniqueInt(void);

  // return integer to pool of unique integers
  void returnUniqueInt(int i);
};

class Stat {
  int nitems;
  double sum;
  double sumsquare;
  double min;
  double max;
  multiset<double> values;
public:
  Stat(){ reset(); }
  void reset(){ nitems=0; sum=sumsquare=min=max=0.0; values.clear(); }
  void put(double item);
  double getMin(void){ return min; }
  double getMax(void){ return max; }
  double getAvg(void){ return sum/nitems; }
  double getVariance(void){
    double ave=getAvg();
    return sumsquare/nitems - ave*ave;
  }
  double getStdDev(void){ return sqrt(getVariance()); }
  double getMedian(void);
};

class MovingAverage {
  int nitems;
  int windowsize;
  SimpleLinkList<double> values;
  double sum;
  double sumsquare;
public:
  MovingAverage(int ws);
  void reset();
  void put(double item);
  double getAvg(void){ if (!nitems) return 0; return sum/nitems; }
  double getVariance(void){
    if (!nitems) return 0;
    double ave=getAvg();
    return sumsquare/nitems - ave*ave;
  }
  double getStdDev(void){ return sqrt(getVariance()); }
};

#endif

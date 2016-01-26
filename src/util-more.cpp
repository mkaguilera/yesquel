//
// util-more.cpp
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


#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <map>
#include <list>

#include "tmalloc.h"

#include "debug.h"
#include "util-more.h"

// get a unique non-zero integer. Returns 0 if no more integers available
int UniqueInt::getUniqueInt(void){
  int i, count;
  count=0;
  UsedInts_l.lock();
  do {
    i = ++Counter;
    ++count;
  } while (UsedInts.find(i) != UsedInts.end() && count != 0);
  if (count) UsedInts.insert(i);
  UsedInts_l.unlock();
  if (count==0) return 0;  // overflow, so no integers available
  else return i;
}

// return integer to pool of unique integers
void UniqueInt::returnUniqueInt(int i){
  UsedInts.erase(i);
}

void Stat::put(double item){
  if (nitems==0) min=max=item;
  else {
    if (item < min) min = item;
    else if (item > max) max = item;
  }
  ++nitems;
  sum += item;
  sumsquare += item*item;
  values.insert(item);
}

double Stat::getMedian(void){
  int i;
  int n = (int) values.size();
  int half=(n-1)/2;
  multiset<double>::iterator it;
  double res;
  if (n==0) return 0;
  for (i=0, it=values.begin(); i < half; ++i, ++it) ;
  if (n%2 == 0){ // even number of elements
    res = *it;
    ++it;
    res = (res + *it)/2;
  } else res = *it; // odd number of elements
  return res;
}

MovingAverage::MovingAverage(int ws){ 
  reset(); 
  windowsize=ws; 
}

void MovingAverage::reset(){
  nitems=0;
  sum=sumsquare=0.0;
  values.clear();
}

void MovingAverage::put(double item){
  if (nitems >= windowsize){
    assert(!values.empty());
    double d = values.popHead();
    sum -= d;
    sumsquare -= d*d;
    --nitems;
  }
  values.pushTail(item);
  sum += item;
  sumsquare += item*item;
  ++nitems;
}

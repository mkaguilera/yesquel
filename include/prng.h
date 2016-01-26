//
// prng.h
//
// Pseudo-random number generation
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


#ifndef _PRNG_H
#define _PRNG_H

#include <time.h>
#include <stdint.h>

class SimplePrng { // Simple generator used to seed the better generator
private:
  unsigned long n;
public:
  SimplePrng(){ n = (unsigned long) time(0); }
  void SetSeed(long s){ n=s; }
  SimplePrng(int seed){ n = seed; }

  // returns 15-bit random number
  unsigned long next(void){
    n = n * 1103515245 + 12345;
    return (unsigned int)(n/65536) % 32768;
  }

  // returns 32-bit random number
  uint32_t next32(void){
    return ((uint32_t) next() << 17) ^ ((uint32_t) next() << 1) ^
      (uint32_t) next();
  }
};

class Prng {
private:
  SimplePrng seeder;
  uint64_t Y[55];
  int j,k;
  void Init(unsigned long seed=0){ // uses simpler generator to produce a seed
    if (seed!=0) seeder.SetSeed(seed);
    else seeder.SetSeed((unsigned long) time(0));
    uint64_t v;
    for (int i=0; i < 55; ++i){
      // generate 64-bit random number of 7-bit seeder
      v = 0;
      for (int h=0; h < 10; ++h) v = (v << 7) | seeder.next();
      Y[i] = v;
    }
    j=23;
    k=54;
  }
public:
  Prng() : seeder() { Init(); }
  Prng(unsigned long seed) : seeder((int)seed){ Init(seed); }
  uint64_t next(void){
    uint64_t retval;
    Y[k] = Y[k] + Y[j];
    retval = Y[k];
    --j;
    if (j<0) j=54;
    --k;
    if (k<0) k=54;
    return retval;
  }
};

#endif

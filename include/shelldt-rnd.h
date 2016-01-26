//
// shelldt-rnd.h
//
// Auxiliary functions to unique random numbers for shelldt
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

#ifndef _SHELLDT_RND_H
#define _SHELLDT_RND_H

#include "util.h"
#include "datastruct.h"
#include "prng.h"

// A class to generate/delete unique random integers within an interval.
// Not exactly a uniform distribution, but probably random enough.
// Not advisable to use if the number of random integers to generate is
// near the size of the interval (it will be very inefficient).


class RandomUnique {
private:
  int Nbuckets;
  int MaxElement;
  SimpleLinkList<int> *Buckets;
  RWLock *Bucket_l;
  Prng prng;

  int hash(unsigned v){ return v % Nbuckets; }


public:
  Align4 int nitems;
  RandomUnique(int nbuckets, int max){
    Nbuckets = nbuckets;
    MaxElement = max;
    Buckets = new SimpleLinkList<int>[nbuckets];
    Bucket_l = new RWLock[nbuckets];
    nitems = 0;
  }

  ~RandomUnique(){ delete [] Buckets; delete [] Bucket_l; }

  // clear HashTable. If deleteitem=1 then call delete on each element
  void Clear(int deleteitem=1){
    int bucket;
    SimpleLinkList<int> *b;

    for (bucket=0; bucket < Nbuckets; ++bucket){
      b = Buckets + bucket;
      Bucket_l[bucket].lock();
      while (!b->Empty()){
        b->PopHead();
      }
      Bucket_l[bucket].unlock();
    }
  }

  // generates a new unique random element
  unsigned newRandom(void){
    SimpleLinkListItem<int> *ptr;
    int bucket = (unsigned)prng.next() % Nbuckets;
    SimpleLinkList<int> *b;
    bool done;

    unsigned candidate;
    done = false;
    while (!done){
      candidate = ((unsigned) prng.next() % MaxElement);
      bucket = hash(candidate);
      b = Buckets + bucket;
      Bucket_l[bucket].lock();
      for (ptr = b->GetFirst(); ptr != b->GetLast(); ptr = b->GetNext(ptr)){
        if (candidate == b->peek(ptr)) break;
      }
      if (ptr == b->GetLast()){
        // found new element
        b->PushTail(candidate);
        done = true;
      }
      Bucket_l[bucket].unlock();
    }
    AtomicInc32(&nitems);
    return candidate;
  }

  //int *Lookup(int key){
  //  int *res;
  //  int bucket = hash(key) % Nbuckets;
  //  SimpleLinkList<int> *b = Buckets + bucket;
  //  Bucket_l[bucket].lockRead();
  //  res = b->Find(key);
  //  Bucket_l[bucket].unlockRead();
  //  return res;
  //}

  //int *operator[](int key){ return Lookup(key); }

  // removes a random element and returns it
  unsigned removeRandom(void){
    unsigned retval;
    SimpleLinkListItem<int> *ptr;
    unsigned bucket = hash((unsigned)prng.next()); // pick a random bucket
    SimpleLinkList<int> *b = Buckets + bucket;
    int n;

    n=0;
    // search for a bucket with elements
    do {
      Bucket_l[bucket].lock();
      if (b->nitems > 0) break;
      Bucket_l[bucket].unlock();
      bucket = (bucket+1) % Nbuckets;
      b = Buckets + bucket;
      ++n;
      if (n==Nbuckets) return -1; // no more elements
    } while (1);

    n = (unsigned)prng.next() % b->nitems;
    ptr = b->GetFirst();
    while (n){
      --n;
      ptr = b->GetNext(ptr);
      assert(ptr != b->GetLast());
    }
    retval = b->peek(ptr);
    b->remove(ptr);
    AtomicDec32(&nitems);
    Bucket_l[bucket].unlock();
    return retval;
  }
};

#endif

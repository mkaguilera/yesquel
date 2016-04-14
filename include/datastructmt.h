//
// datastructmt.h
//
// General-purpose multithread-safe data structures
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
#ifndef _DATASTRUCTMT_H
#define _DATASTRUCTMT_H

#include "os.h"
#include "datastruct.h"

// Multithread-safe hash table.
// Assumes that type T has a hash function and a comparison function
//   static unsigned hash(const T &l);
//   static int cmp(const T &l, const T &r);

template<class T, class U>
class HashTableMT {
private:
  int Nbuckets;
  SkipList<T,U> *Buckets;
  RWLock *Bucket_l;

public:
  HashTableMT(int nbuckets){
    Nbuckets = nbuckets;
    Buckets = new SkipList<T,U>[nbuckets];
    Bucket_l = new RWLock[nbuckets];
    //nitems = 0;
  }

  ~HashTableMT(){ delete [] Buckets; delete [] Bucket_l; }

  int GetNbuckets(){ return Nbuckets; }
  SkipList<T,U> *GetBucket(int i){
    assert(0 <= i && i < Nbuckets);
    return Buckets+i;
  }

  // clear HashTable. If delkey!=0 then invoke it for each key deleted.
  // If delvalue!= then invoke it for each value deleted.
  void clear(void (*delkey)(T&), void (*delvalue)(U)){
    int bucket;
    SkipList<T,U> *b;

    for (bucket=0; bucket < Nbuckets; ++bucket){
      b = Buckets + bucket;
      Bucket_l[bucket].lock();
      b->clear(delkey, delvalue);
      Bucket_l[bucket].unlock();
    }
    //nitems = 0;
  }

  // adds an element. Does not check if there is already another element with
  // the same key so element may be in table multiple times
  void insert(T &key,U value){
    int bucket = key.hash(key) % Nbuckets;
    Bucket_l[bucket].lock();
    Buckets[bucket].insert(key, value);
    //AtomicInc32(&nitems);
    Bucket_l[bucket].unlock();
  }

  // returns 0 if found, non-zero if not found. If found, sets retvalue to
  // value.
  // This version of lookup() returns the value itself, not a pointer to the
  // value (hence it cannot be used to modify the value). This is done on
  // purpose: when the value is a smart pointer, the value gets copied
  // (and hence the refcount is incremented) before releasing the lock to the
  // bucket. If we were to return a pointer to the value (ie, to the smart
  // pointer), then its refcount would not be incremented, and the object
  // could be GC'ed after we release the bucket lock and before we
  // dereference the pointer to obtain the smart pointer.
  int lookup(T &key, U &retval){
    int res;
    U *retvalue;
    int bucket = T::hash(key) % Nbuckets;
    SkipList<T,U> *b = Buckets + bucket;
    Bucket_l[bucket].lockRead();
    res = b->lookup(key, retvalue);
    if (res==0) retval = *retvalue; // found it
    Bucket_l[bucket].unlockRead();
    return res;
  }

  // finds key; executes f(key, &value, status, bucket-linked-list, parm)
  // where status==0 iff found, returning the result
  int lookupApply(T &key, int (*f)(T&, U*, int, SkipList<T,U> *, u64),
                  u64 parm){
    U *value=0;
    int status;
    int retval;
    int bucket = T::hash(key) % Nbuckets;
    SkipList<T,U> *b = Buckets + bucket;
    Bucket_l[bucket].lock();
    status = b->lookup(key, value);
    retval = f(key, value, status, b, parm);
    Bucket_l[bucket].unlock();
    return retval;
  }

  // lookup a key. If found, return 0 and pointer to value in retval.
  // If not found, create it and return non-zero and pointer to newly created
  // value in retval.
  // If f!=0 then invoke f with found status (0=found, non-zero=not found)
  // and retval, where retval is set to existing value or newly created value
  int lookupInsert(T &key, U *&retval, void (*f)(int, U*)){
    int res;
    U *retvalue;
    int bucket = T::hash(key) % Nbuckets;
    SkipList<T,U> *b = Buckets + bucket;
    Bucket_l[bucket].lock();
    res = b->lookupInsert(key, retvalue);
    if (f) f(res,retvalue);
    retval = retvalue;
    Bucket_l[bucket].unlock();
    return res;
  }

  // remove element with the given key. If there are multiple elements with
  // that key, remove only one of them. Returns 0 if element was removed,
  // non-zero if there were no elements to remove.
  // If delkey is non-null, invoke it on key being deleted

  int remove(T &key, void (*delkey)(T&)){ 
    int retval;
    U removedU;
    int bucket = T::hash(key) % Nbuckets;
    SkipList<T,U> *b = Buckets + bucket;

    Bucket_l[bucket].lock();
    retval = b->lookupRemove(key, delkey, removedU);
    Bucket_l[bucket].unlock();
    //if (!retval) AtomicDec32(&nitems);
    return retval;
  }

  // lookup element with the given key, remove it, and return a copy of its
  // value. If there are multiple elements with that key,
  // do this only for one of them. Returns 0 if element was removed, non-zero
  // if there were no elements to remove.
  // If delkey is non-null, invoke it on key being deleted
  int lookupRemove(T &key, void (*delkey)(T&), U &value){ 
    int retval;
    int bucket = T::hash(key) % Nbuckets;
    SkipList<T,U> *b = Buckets + bucket;

    Bucket_l[bucket].lock();
    retval = b->lookupRemove(key, delkey, value);
    Bucket_l[bucket].unlock();
    //if (!retval) AtomicDec32(&nitems);
    return retval;
  }
};

// a bounded concurrent queue (multi-thread safe)
template<class T>
class BoundedQueue {
private:
  int QueueSize;
  Semaphore SemItems, SemSpaces;
  T *Buffer;
  RWLock Buffer_l;
  int NextWrite; // offset in Buffer for next item to be enqueued
  int NextRead;  // offten in Buffer for next item to be dequeued
public:
  BoundedQueue(int queuesize) : 
    QueueSize(queuesize),
    SemItems(0),
    SemSpaces(queuesize)
  {
    Buffer = new T[queuesize];
    NextWrite = 0;
    NextRead = 0;
  }

  ~BoundedQueue(){
    delete [] Buffer;
  }

  void enqueue(T item){
    SemSpaces.wait(INFINITE); // wait for space to be available
    Buffer_l.lock();
    Buffer[NextWrite] = item;
    if (++NextWrite == QueueSize) NextWrite = 0;
    Buffer_l.unlock();
    SemItems.signal(); // signal that item is available
  }

  T dequeue(void){
    T retval;
    SemItems.wait(INFINITE);  // wait for item to be available
    Buffer_l.lock();
    retval = Buffer[NextRead];
    if (++NextRead == QueueSize) NextRead = 0;
    Buffer_l.unlock();
    SemSpaces.signal(); // signal that space is available
    return retval;
  }

  bool empty(void){
    bool res;
    Buffer_l.lock();
    res = NextRead == NextWrite;
    Buffer_l.unlock();
    return res;
  }
};


// Channel is a queue where a single thread can enqueue (the sender) and
// a single thread can dequeue (the receiver)
// Assumes T has two methods:
//   setinvalid() - sets the object to an invalid value
//   bool isinvalid() - indicates whether object has an invalid value
#define DEFAULT_CHANNEL_SIZE 2048
template <class T, int SIZE=DEFAULT_CHANNEL_SIZE>
class Channel {
private:
  Align64 u32 SendPos;     // position where next element sent will be placed
  Align64 u32 ReceivePos;  // position of next element to be received
  Align64 T Elements[SIZE];
public:
  // size should be a power of 2
  Channel(){
    assert((SIZE & (SIZE-1))==0); // ensure size is a power of two
    //Elements = new T[SIZE];
    //memset(Elements, 0, sizeof(T) * size);
    SendPos = 0;
    ReceivePos = 0;
    for (int i=0; i < SIZE; ++i){
      Elements[i].setinvalid();
    }
  }

  ~Channel(){
  }

  // Receiver can call to check if queue is empty
  bool empty(){
    int receivepos = ReceivePos & (SIZE-1);
    return Elements[receivepos].isinvalid();
  }

  // Sender can call to check if queue is full
  bool full(){
    int sendpos = (SendPos+1) & (SIZE-1);
    return !Elements[sendpos].isinvalid();
  }

  // returns 0 if successful, non-zero if queue is full
  int enqueue(T& element){
    assert(!element.isinvalid());
    int sendpos = SendPos & (SIZE-1);
    if (!Elements[sendpos].isinvalid())
      return 1;
    Elements[sendpos] = element;
    ++SendPos;
    //__sync_synchronize();
    return 0;
  }

  // return 0 if successful (setting element), non-zero if queue is empty
  int dequeue(T& element){
    int receivepos = ReceivePos & (SIZE-1);
    if (Elements[receivepos].isinvalid()) return 1;
    element = Elements[receivepos];
    Elements[receivepos].setinvalid();
    ++ReceivePos;
    //__sync_synchronize();
    return 0;
  }
};

// old version of Channel, using shared head and tail pointers
template <class T, int SIZE=DEFAULT_CHANNEL_SIZE>
class OldChannel {
private:
  Align64 u32 SendPos;     // position where next element sent
                                    // will be placed
  Align4 u32 ReceivePos;  // position of next element to be received
  Align4 T Elements[SIZE];
public:
  // size should be a power of 2
  OldChannel(){
    assert((SIZE & (SIZE-1))==0); // ensure size is a power of two
    //Elements = new T[SIZE];
    //memset(Elements, 0, sizeof(T) * size);
    SendPos = 0;
    ReceivePos = 0;
  }

  ~OldChannel(){
  }

  // Sender can call to check if queue is empty
  bool empty(){
    int sendpos = SendPos & (SIZE-1);
    int receivepos = ReceivePos & (SIZE-1);
    return sendpos == receivepos;
  }

  // Receiver can call to check if queue is full
  bool full(){
    int sendpos = (SendPos+1) & (SIZE-1);
    int receivepos = ReceivePos & (SIZE-1);
    return sendpos == receivepos;
  }

  // returns 0 if successful, non-zero if queue is full
  int enqueue(T& element){
    int sendpos = SendPos & (SIZE-1);
    int receivepos = ReceivePos & (SIZE-1);
    if (((sendpos+1)&(SIZE-1)) == receivepos)
      return 1;
    Elements[sendpos] = element;
    AtomicInc32(&SendPos);
    return 0;
  }

  // return 0 if successful (setting element), non-zero if queue is empty
  int dequeue(T& element){
    int sendpos = SendPos & (SIZE-1);
    int receivepos = ReceivePos & (SIZE-1);
    if (sendpos == receivepos) return 1;
    element = Elements[receivepos];
    AtomicInc32(&ReceivePos);
    return 0;
  }

  // return number of elements waiting in queue
  unsigned waiting(){
    return SendPos-ReceivePos;
  }

  // return number of slots available in queue
  unsigned available(){
    return SIZE-(unsigned)(SendPos-ReceivePos);
  }
};

#endif

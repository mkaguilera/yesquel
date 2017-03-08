//
// datastruct.h
//
// General-purpose data structures, templated. These are not
// multithread safe.
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


#ifndef _DATASTRUCT_H
#define _DATASTRUCT_H

#include <new>
#include <assert.h>
#include <string.h>

#include "inttypes.h"
#include "prng.h"
#include "os.h"
#ifndef _NO_TMALLOC
#include "tmalloc.h"
#endif

class DefaultAllocator {
public:
  static void *alloc(size_t size){ return malloc(size); }
  static void free(void *ptr){ ::free(ptr); }
};
#define ANEW(TYPE) new(Alloc::alloc(sizeof(TYPE))) TYPE
#define ADELETE(VAR,TYPE) VAR->~TYPE(),Alloc::free((void*)VAR)
#define AMALLOC(size) Alloc::alloc(size)
#define AFREE(ptr) Alloc::free(ptr)

template<class T>
struct SimpleLinkListItem {
  T item;
  SimpleLinkListItem<T> *next, *prev;
};


// This is a linked list with elements of type T, where T
// should be a small type that can be efficiently copied
// (eg, int, long, etc).
//
// Destructor deletes all elements in linked list.
template<class T, class Alloc=DefaultAllocator>
class SimpleLinkList {
private:
  // Head and Tail always points to two empty dummy nodes  
  SimpleLinkListItem<T> *Head, *Tail;
  int nitems;

public:
  SimpleLinkList(){
    Head = ANEW(SimpleLinkListItem<T>);
    Tail = ANEW(SimpleLinkListItem<T>);
    Head->prev = 0;
    Head->next = Tail;
    Tail->prev = Head;
    Tail->next = 0;
    nitems=0;
  }

  ~SimpleLinkList(){
    clear();
    ADELETE(Head,SimpleLinkListItem<T>);
    ADELETE(Tail,SimpleLinkListItem<T>);
  }

  void clear(){
    SimpleLinkListItem<T> *item, *next;
    item = Head->next;
    while (item != Tail){
      next = item->next;
      ADELETE(item,SimpleLinkListItem<T>);
      item = next;
    }
    Head->next = Tail;
    Tail->prev = Head;
    nitems = 0;
  }

  void pushTail(T toadd){
    SimpleLinkListItem<T> *newitem = ANEW(SimpleLinkListItem<T>);
    newitem->item = toadd;
    newitem->prev = Tail->prev;
    newitem->next = Tail;
    Tail->prev->next = newitem;
    Tail->prev = newitem;
    ++nitems;
  }

  T popTail(void){
    assert(Tail->prev != Head);
    SimpleLinkListItem<T> *prev = Tail->prev;
    SimpleLinkListItem<T> *prevprev = prev->prev;
    Tail->prev = prevprev;
    prevprev->next = Tail;
    T retval = prev->item;
    ADELETE(prev,SimpleLinkListItem<T>);
    --nitems;
    return retval;
  }

  void pushHead(T toadd){
    SimpleLinkListItem<T> *newitem = ANEW(SimpleLinkListItem<T>);
    newitem->item = toadd;
    newitem->prev = Head;
    newitem->next = Head->next;
    Head->next->prev = newitem;
    Head->next = newitem;
    ++nitems;
  }

  T popHead(void){
    assert(Head->next != Tail);
    SimpleLinkListItem<T> *next = Head->next;
    SimpleLinkListItem<T> *nextnext = next->next;
    Head->next = nextnext;
    nextnext->prev = Head;
    T retval = next->item;
    ADELETE(next,SimpleLinkListItem<T>);
    --nitems;
    return retval;
  }

  bool empty(void){ return Head->next == Tail; }

  // methods for iterating forward
  SimpleLinkListItem<T> *getFirst(void){ return Head->next; }
  SimpleLinkListItem<T> *getLast(void){ return Tail; }
  SimpleLinkListItem<T> *getNext(SimpleLinkListItem<T> *ptr){return ptr->next;}

  // methods for iterating backward
  SimpleLinkListItem<T> *rGetFirst(void){ return Tail->prev; }
  SimpleLinkListItem<T> *rGetLast(void){ return Head; }
  SimpleLinkListItem<T> *rGetNext(SimpleLinkListItem<T> *ptr){return ptr->prev;}

  T peek(SimpleLinkListItem<T> *ptr){ return ptr->item; }
  void remove(SimpleLinkListItem<T> *ptr){
    ptr->prev->next = ptr->next;
    ptr->next->prev = ptr->prev;
    --nitems;
    ADELETE(ptr,SimpleLinkListItem<T>);
  }
  int getNitems(){ return nitems; }
};


// This is a linked list for types U.
// Requirements:
// 1. U has public fields next and prev of type U*
//
// In this class, when an element is added, it will belong
// to the linked list, so caller should not free element.
//
// When an element is returned by popTail or popHead, it will be given to the
// caller, who can then free the element if appropriate.
//
// Destructor may or may not free elements in the list, depending
// on the constructor's parameter toclean. If toclean==true, the destructor
// frees the elements.
// Otherwise, the caller is responsible for freeing it before destroying
// LinkList.
//

template<class U, class Alloc=DefaultAllocator>
class LinkList {
private:
  U *Head, *Tail; // Head and Tail always points to two empty dummy nodes
  bool ToClean;  // if set, delete items on destructor
  int nitems;

public:
  LinkList(bool toclean = false){
    Head = ANEW(U);
    Tail = ANEW(U);
    Head->prev = 0;
    Head->next = Tail;
    Tail->prev = Head;
    Tail->next = 0;
    nitems = 0;
    ToClean = toclean;
  }

  ~LinkList(){
    if (ToClean) clear(true);
    ADELETE(Head,U);
    ADELETE(Tail,U);
  }

  void addBefore(U *toadd, U *where){
    U *ptrprev = where->prev;
    toadd->prev = ptrprev;
    toadd->next = where;
    ptrprev->next = toadd;
    where->prev = toadd;
    ++nitems;
  }

  void addAfter(U *toadd, U *where){
    U *ptrnext = where->next;
    toadd->next = ptrnext;
    toadd->prev = where;
    ptrnext->prev = toadd;
    where->next = toadd;
    ++nitems;
  }

  void clear(bool del){  // empty the linklist. If del is set to true, delete
                         // nodes as we clear them
    if (del){
      U *ptr, *next;
      ptr = Head->next;
      while (ptr != Tail){
        next = ptr->next;
        ADELETE(ptr,U);
        ptr = next;
      }
    }
    Head->next = Tail;
    Tail->prev = Head;
    nitems = 0;
  }

  void pushTail(U *toadd){ addBefore(toadd, Tail); }

  void pushHead(U *toadd){ addAfter(toadd, Head); }

  void remove(U *ptr){
    U *ptrnext, *ptrprev;
    ptrnext = ptr->next;
    ptrprev = ptr->prev;
    ptrnext->prev = ptrprev;
    ptrprev->next = ptrnext;
    --nitems;
  }

  U *popTail(void){
    assert(Tail->prev != Head);
    U *ptr = Tail->prev;
    remove(ptr);
    return ptr;
  }

  U *popHead(void){
    assert(Head->next != Tail);
    U *ptr = Head->next;
    remove(ptr);
    return ptr;
  }

  U *peekTail(void){
    assert(Tail->prev != Head);
    return Tail->prev;
  }

  U *peekHead(void){
    assert(Head->next != Tail);
    return Head->next;
  }

  bool empty(void){ return Head->next == Tail; }

  // methods for forward iteration
  U *getFirst(void){ return Head->next; }
  U *getLast(void){ return Tail; }
  U *getNext(U *ptr){ return ptr->next; }

  // methods for backward iteration
  U *rGetFirst(void){ return Tail->prev; }
  U *rGetLast(void){ return Head; }
  U *rGetNext(U *ptr){ return ptr->prev; }
  
  int getNitems(){ return nitems; }
};

// This is a singly linked list for types U.
// Requirements:
// 1. U has public fields next of type U*
//
// In this class, when an element is added, it will belong
// to the linked list, so caller should not free element.
//
// When an element is returned by popTail or popHead, it will be given to the
// caller, who can then free the element if appropriate.
//
// Note: destructor does not free elements in the list.
// Caller should enumerate list, pop its elements, and free them.

template<class U>
class SLinkList {
private:
  U *Head, *Tail;
  int nitems;

public:
  SLinkList(){
    Head = Tail = 0;
    nitems = 0;
  }
  
  void pushTail(U *toadd){
    if (Tail) Tail->next = toadd;
    toadd->next = 0;
    Tail = toadd;
    if (!Head) Head=toadd;
    ++nitems;
  }

  void pushHead(U *toadd){
    toadd->next = Head;
    Head = toadd;
    if (!Tail) Tail = toadd;
    ++nitems;
  }

  U *popHead(void){
    U *retval;
    assert(Head != 0);
    retval = Head;
    Head = Head->next;
    if (!Head) Tail=0;
    --nitems;
    return retval;
  }

  // Remove rest of linked list starting at item after given item,
  // unless item==0, in which case removes entire linked list
  // invokes delitem on each item delete (presumably to free the item)
  void removeRest(U *item, void (*delitem)(U*)){
    U *ptr, *next;
    // invoke del function for each item to be deleted
    if (item) ptr = item->next;
    else ptr = Head;
    while (ptr){
      next = ptr->next;
      delitem(ptr);
      --nitems;
      ptr = next;
    }
    if (item){
      item->next = 0;
      Tail=item;
    } else {
      Head = Tail = 0;
    }
  }

  U *peekTail(void){
    return Tail;
  }

  U *peekHead(void){
    return Head;
  }

  bool empty(){ return Head==0; }

  // methods for forward iteration
  U *getFirst(){ return Head; }
  U *getLast(){ return 0; }
  U *getNext(U *ptr){ return ptr->next; }

  int getNitems(){ return nitems; }
};


// This is a linked list for larger types U with key T.
// Requirements:
// 1. U has public fields snext and sprev of type U*
// 2. U has method T GetKey which returns key of itself
// 3. U has static function CompareKey(T,T) which compares a given key
//
// In this class, when an element is added, it will belong
// to the linked list, so caller should not free element.
//
// When an element is returned, by popHead/popTail it will be given to the
// caller, who can then free the element if appropriate.
//
// Note: destructor does not free elements in the list.
// Caller should enumerate list, pop its elements, and free them.
//

template<class T, class U, class Alloc=DefaultAllocator>
class SortedLinkList {
protected:
  U *Head, *Tail; // Head and Tail always points to two empty dummy nodes

public:
  SortedLinkList(){
    Head = ANEW(U);
    Tail = ANEW(U);
    Head->sprev = 0;
    Head->snext = Tail;
    Tail->sprev = Head;
    Tail->snext = 0;
  }

  ~SortedLinkList(){
    ADELETE(Head,U);
    ADELETE(Tail,U);
  }

  // If exact=0: finds first entry with key >= given key
  // If exact=1: finds entry with first key maching given key; returns 0 if none
  U *lookup(T key, int exact=1){
    U *ptr;
    int cmp=0;
    ptr = getFirst();
    while (ptr != Tail && (cmp = U::CompareKey(ptr->GetKey(), key)) < 0)
      ptr=ptr->snext;
    if (ptr == Tail) return 0; // not found
    if (!exact) return ptr;
    else if (cmp) return 0; // not found
    else return ptr;
  }

  void insert(U *toadd){
    U *ptr = lookup(toadd->GetKey(), 0);
    if (!ptr) ptr = Tail;
    U *ptrprev = ptr->sprev;
    toadd->sprev = ptrprev;
    toadd->snext = ptr;
    ptrprev->snext = toadd;
    ptr->sprev = toadd;
  }

  static void removeDirect(U *ptr){
    U *ptrnext, *ptrprev;
    ptrnext = ptr->snext;
    ptrprev = ptr->sprev;
    ptrnext->sprev = ptrprev;
    ptrprev->snext = ptrnext;
  }

  void remove(U *ptr){
    SortedLinkList::removeDirect(ptr);
  }

  U *popTail(void){
    assert(Tail->sprev != Head);
    U *sprev = Tail->sprev;
    U *prevprev = sprev->sprev;
    Tail->sprev = prevprev;
    prevprev->snext = Tail;
    return sprev;
  }

  U *popHead(void){
    assert(Head->snext != Tail);
    U *snext = Head->snext;
    U *nextnext = snext->snext;
    Head->snext = nextnext;
    nextnext->sprev = Head;
    return snext;
  }

  bool empty(void){ return Head->snext == Tail; }

  U *getFirst(void){ return Head->snext; }
  U *getLast(void){ return Tail; }
  U *getNext(U *ptr){ return ptr->snext; }
};

// Same as SortedLinkList but for big keys.
// Assumptions
//   1. U has method T *GetKeyPtr() instead of T GetKey()
//   2. U has static functions CompareKey(T*,T*) instead of CompareKey(T,T)
//
template<class T, class U, class Alloc=DefaultAllocator>
class SortedLinkListBK : public SortedLinkList<T,U,Alloc>
{
public:
  SortedLinkListBK() : SortedLinkList<T,U,Alloc>() { }
  ~SortedLinkListBK(){}

  // If exact=0: finds first entry with key >= given key; if none, return 0
  // If exact=1: finds entry with first key maching given key; returns
  // getLast() if none
  U *lookup(T *key, int exact=1){
    U *ptr;
    int cmp=0;
    ptr = SortedLinkList<T,U,Alloc>::getFirst();
    while (ptr != SortedLinkList<T,U,Alloc>::Tail &&
           (cmp = U::CompareKey(ptr->GetKeyPtr(),key)) < 0)
      ptr=ptr->snext;
    if (ptr == SortedLinkList<T,U,Alloc>::Tail) return 0;
    if (!exact) return ptr;
    else if (cmp) return 0; // not found
    else return ptr;
  }

  void insert(U *toadd){
    U *ptr = lookup(toadd->GetKeyPtr(), 0);
    if (!ptr) ptr = SortedLinkList<T,U,Alloc>::Tail;
    U *ptrprev = ptr->sprev;
    toadd->sprev = ptrprev;
    toadd->snext = ptr;
    ptrprev->snext = toadd;
    ptr->sprev = toadd;
  }
};

// Auxiliary type for SkipList class below
template<class T, class U, class Alloc=DefaultAllocator>
struct SkipListNode {
  T key;
  U value;
  int nlevels; // this field is used for some of the more sophisticated
               // operations (copy skiplist, delete range)
  SkipListNode *next[1];
  SkipListNode(){}
  ~SkipListNode(){}
  static SkipListNode<T,U,Alloc> *newNode(int n){
    if (n <= 0) n=1;
    SkipListNode<T,U,Alloc> *ret = (SkipListNode<T,U,Alloc>*)
      AMALLOC(sizeof(SkipListNode<T,U,Alloc>) + (n-1) *
              sizeof(SkipListNode<T,U,Alloc>*));
    rtchk(ret);
    new(ret) SkipListNode<T,U,Alloc>;
    ret->nlevels = n;
    return ret;
  }

  static void freeNode(SkipListNode<T,U,Alloc> *node){
    node->~SkipListNode<T,U,Alloc>();
    AFREE(node);
  }
};

// SkipList
// Requires T to have static function cmp(T &left, T &right) which
// returns -1, 0, +1  if left < right, left==right, left > right, respectively
template<class T, class U, class Alloc=DefaultAllocator>
class SkipList {
private:
  SkipListNode<T,U,Alloc> *Head, *Tail;
  SimplePrng prng;
  int maxlevels;
  int nitems;
  void expandLevels(int highern){
    int i;
    if (highern <= maxlevels) return;
    SkipListNode<T,U,Alloc> *newHead=SkipListNode<T,U,Alloc>::newNode(highern);
    for (i=0; i < maxlevels; ++i) newHead->next[i] = Head->next[i];
    for (i=maxlevels; i < highern; ++i) newHead->next[i] = Tail;
    SkipListNode<T,U,Alloc>::freeNode(Head);
    Head = newHead;
    maxlevels = highern;
  }

  // seek to position <= key
  SkipListNode<T,U,Alloc> *seek(T &key){
    int i;
    SkipListNode<T,U,Alloc> *ptr;
    ptr = Head;
    for (i = maxlevels-1; i >= 0; --i)
    {
      while (ptr->next[i] != Tail && T::cmp(ptr->next[i]->key, key) <= 0){
        if (T::cmp(ptr->next[i]->key, key)==0) return ptr->next[i];
        ptr = ptr->next[i];
      }
    }
    return ptr;
  }

  // seek to position < k
  SkipListNode<T,U,Alloc> *seekL(T &key){
    int i;
    SkipListNode<T,U,Alloc> *ptr;
    ptr = Head;
    for (i = maxlevels-1; i >= 0; --i)
      while (ptr->next[i] != Tail && T::cmp(ptr->next[i]->key, key) < 0)
        ptr = ptr->next[i];
    return ptr;
  }

public:
  SkipList(){
    Head = SkipListNode<T,U,Alloc>::newNode(1);
    Tail = SkipListNode<T,U,Alloc>::newNode(1);
    Head->next[0] = Tail;
    Tail->next[0] = 0;
    maxlevels = 1;
    nitems = 0;
  }

  ~SkipList(){
    clear(0,0);
    SkipListNode<T,U,Alloc>::freeNode(Head);
    SkipListNode<T,U,Alloc>::freeNode(Tail);
  }

  // copy skiplist
  void copy(const SkipList &r){
    int i;
    SkipListNode<T,U,Alloc> **missingprev, *ptr, *newnode;

    // missingprev has previous nodes that are missing a next pointer,
    // for each level
    nitems = r.nitems;
    maxlevels = r.maxlevels;
    missingprev = (SkipListNode<T,U,Alloc> **)
      AMALLOC(sizeof(SkipListNode<T,U,Alloc>*) * maxlevels);
    Head = SkipListNode<T,U,Alloc>::newNode(maxlevels);
    for (i=0; i < r.maxlevels; ++i) missingprev[i] = Head;
    ptr = r.Head->next[0];
    while (ptr != r.Tail){
      newnode = SkipListNode<T,U,Alloc>::newNode(ptr->nlevels);
      newnode->key = ptr->key;
      if (ptr->value) newnode->value = ptr->value;
      else newnode->value = 0;
 
      for (i=0; i < ptr->nlevels; ++i){
        missingprev[i]->next[i] = newnode;
        missingprev[i] = newnode;
      }
      ptr = ptr->next[0];
    }
    Tail = SkipListNode<T,U,Alloc>::newNode(1);
    for (i=0; i < maxlevels; ++i)
      missingprev[i]->next[i] = Tail;
    AFREE(missingprev);
  }

  // copy constructor. Calls copy constructors for the key and value
  // of each item in the skiplist to be copied
  SkipList(const SkipList &r){ copy(r); }
  SkipList &operator=(const SkipList &r){ copy(r); return *this; }

  // Clear all items. 
  // delkey and delvalue are functions that will be called to delete key/value.
  // These functions can be 0, in which case they will not be called.
  void clear(void (*delkey)(T&), void (*delvalue)(U)){
    SkipListNode<T,U,Alloc> *ptr, *nextptr;
    nitems = 0;
    ptr = Head->next[0];
    while (ptr != Tail){
      nextptr = ptr->next[0];
      if (delkey) delkey(ptr->key);
      if (delvalue) delvalue(ptr->value);
      SkipListNode<T,U,Alloc>::freeNode(ptr);
      ptr = nextptr;
    }
    for (int i=0; i < maxlevels; ++i) Head->next[i] = Tail;
  }

  // returns 0 if found, non-zero if not found. If found, makes retvalue point
  // to value, in which case caller can read the value with *retvalue, and it
  // can change the value with *retvalue = ...
  int lookup(T &key, U *&retvalue){
    SkipListNode<T,U,Alloc> *ptr = seek(key);
    if (ptr == Head) return -1;
    if (T::cmp(ptr->key, key)==0){ // found
      retvalue = &ptr->value;
      return 0;
    }
    else return -1;
  }

  bool belongs(T &key){
    SkipListNode<T,U,Alloc> *ptr = seek(key);
    if (ptr == Head) return false; // does not belong
    if (T::cmp(ptr->key, key)==0) return true; // belongs
    else return false; // does not belong
  }

  // try to find key; if not found then create it.
  // Returns 0 if found, non-0 if not found and therefore new item was created.
  // Sets valueptr to point to place containing value of found or created item.
  // For example, if item was created, the caller can set its value with
  //    *valueptr = ...
  // If item was found, the caller can retrieve the value with *valueptr.
  int lookupInsert(T &key, U *&valueptr){
    SkipListNode<T,U,Alloc> *newptr;
    int i;
    SkipListNode<T,U,Alloc> *ptr;
    SkipListNode<T,U,Alloc> **prevptrs;
    SkipListNode<T,U,Alloc> *oldhead;

    // store previous ptrs at each level
    prevptrs = (SkipListNode<T,U,Alloc> **)
      AMALLOC(sizeof(SkipListNode<T,U,Alloc>*) * maxlevels);

    oldhead = ptr = Head;
    for (i = maxlevels-1; i >= 0; --i){
      while (ptr->next[i] != Tail && T::cmp(ptr->next[i]->key, key) < 0)
        ptr = ptr->next[i];
      prevptrs[i] = ptr; // remember ptr at this level in case we want
                         // to insert later
    }

    if (ptr->next[0] != Tail && T::cmp(ptr->next[0]->key, key)==0){  // found it
      AFREE(prevptrs); 
      valueptr = &ptr->next[0]->value;
      return 0;
    }

    // did not find it, so insert new item
    ++nitems;
    // choose random height with exponentially decreasing probability
    int height = 0;
    int countbit=0, bit, rnd;
    rnd = (int)prng.next();
    do {
      ++height;
      bit = rnd & 1;  // extra a random bit
      rnd >>= 1;      // set up to get next random bits
      if (++countbit == 15){ // prng returns only 15 bits
        rnd = (int) prng.next(); // get next number
        countbit = 0;
      }
    } while (bit); // continue with 50% probability

    int oldheight = maxlevels;
    if (height > maxlevels) expandLevels(height);
    newptr = SkipListNode<T,U,Alloc>::newNode(height);
    newptr->key = key;
    valueptr = &newptr->value;

    // insert item
    for (i=0; i < height; ++i){
      if (i < oldheight){
        if (prevptrs[i] == oldhead) ptr = Head; // head may have been resized
        else ptr = prevptrs[i];
      }
      else ptr = Head;
      newptr->next[i] = ptr->next[i];
      ptr->next[i] = newptr;
    }

    AFREE(prevptrs);
    return 1;
  }

  // Insert a new key if it is not in the list, or replace an existing one if
  // it already exists.
  // If there are multiple existing keys, only the first one is replaced.
  // If replacing, delkey and delvalue are functions that will be called to
  // delete key/value.
  // These functions can be 0, in which case they will not be called.
  // Returns 0 if replaced, 1 if inserted.
  int insertOrReplace(T &key, U value, void (*delkey)(T&), void (*delvalue)(U)){
    SkipListNode<T,U,Alloc> *newptr;
    int i;
    SkipListNode<T,U,Alloc> *ptr;
    SkipListNode<T,U,Alloc> **prevptrs;
    SkipListNode<T,U,Alloc> *oldhead;

    // store previous ptrs at each level    
    prevptrs = (SkipListNode<T,U,Alloc> **)
      AMALLOC(sizeof(SkipListNode<T,U,Alloc>*) * maxlevels); 

    oldhead = ptr = Head;
    for (i = maxlevels-1; i >= 0; --i){
      while (ptr->next[i] != Tail && T::cmp(ptr->next[i]->key, key)<0)
        ptr = ptr->next[i];
      prevptrs[i] = ptr; // remember ptr at this level to maybe insert later
    }

    if (ptr->next[0] != Tail && T::cmp(ptr->next[0]->key,key)==0){  // found it
      AFREE(prevptrs);
      ptr = ptr->next[0];
      if (delkey) delkey(ptr->key);
      if (delvalue) delvalue(ptr->value);
      ptr->key = key;
      ptr->value = value;
      return 0;
    }

    // did not find it, so insert new item
    ++nitems;
    // choose random height with exponentially decreasing probability
    int height = 0;
    int countbit=0, bit, rnd;
    rnd = (int)prng.next();
    do {
      ++height;
      bit = rnd & 1;  // extra a random bit
      rnd >>= 1;      // set up to get next random bits
      if (++countbit == 15){ // prng returns only 15 bits
        rnd = (int)prng.next(); // get next number
        countbit = 0;
      }
    } while (bit); // continue with 50% probability

    int oldheight = maxlevels;
    if (height > maxlevels) expandLevels(height);
    newptr = SkipListNode<T,U,Alloc>::newNode(height);
    newptr->key = key;
    newptr->value = value;

    // insert item
    for (i=0; i < height; ++i){
      if (i < oldheight){
        if (prevptrs[i] == oldhead) ptr = Head; // head may have been resized
        else ptr = prevptrs[i];
      }
      else ptr = Head;
      newptr->next[i] = ptr->next[i];
      ptr->next[i] = newptr;
    }

    AFREE(prevptrs);
    return 1;
  }

  // Try to find key. If found then delete it and return 0 and a copy of its
  // value in retvalue. If not found, return non-zero.
  // If there are multiple copies of the key, deletes only the first one.
  // If delkey is non-null, invoke it on key being deleted
  int lookupRemove(T &key, void (*delkey)(T&), U& retvalue){
    int i;
    SkipListNode<T,U,Alloc> *ptr;
    SkipListNode<T,U,Alloc> **prevptrs;

    // store previous ptrs at each level
    prevptrs = (SkipListNode<T,U,Alloc> **)
      AMALLOC(sizeof(SkipListNode<T,U,Alloc>*) * maxlevels);

    ptr = Head;
    for (i = maxlevels-1; i >= 0; --i){
      while (ptr->next[i] != Tail && T::cmp(ptr->next[i]->key, key) < 0)
        ptr = ptr->next[i];
      prevptrs[i] = ptr; // remember ptr at this level to maybe insert later
    }

    if (ptr->next[0] == Tail || T::cmp(ptr->next[0]->key, key) != 0){
      // did not find it
      AFREE(prevptrs); 
      return -1;
    }
    // found it, so delete item
    --nitems;
    ptr = ptr->next[0];
    retvalue = ptr->value; // value being returned
    if (delkey) delkey(ptr->key);

    int height = ptr->nlevels;
    for (i=0; i < height; ++i)
      prevptrs[i]->next[i] = ptr->next[i];
    SkipListNode<T,U,Alloc>::freeNode(ptr);

    AFREE(prevptrs);
    return 0;
  }

  // Finds first key that lies within a key interval.
  // The intervals can be of 9 types:
  //    0 = (a,b),    1=(a,b],     2=(a,inf),
  //    3 = [a,b),     4=[a,b],    5=[a,inf),
  //    6 = (-inf,b),  7=(-inf,b], 8=(-inf,inf)
  // If key found, returns 1 and sets retkey to it
  // If not found, returns 0 and leaves retkey untouched
  int keyInInterval(T &startkey, T &endkey, int intervalType, T &retkey){
    SkipListNode<T,U,Alloc> *ptr1;
    if (Head == Tail) return 0;
    if (intervalType < 3) 
      ptr1 = seek(startkey); // seek to position right before or at startkey
    else if (intervalType < 6)
      ptr1 = seekL(startkey); // seek to position right before startkey
    else ptr1 = Head;
    ptr1 = ptr1->next[0];
    if (ptr1 == Tail) return 0; // if end, no match
    switch(intervalType % 3){
      case 0:
        if (T::cmp(ptr1->key, endkey) < 0){
          retkey = ptr1->key; // if we are before endkey, match
          return 1;
        }
        else return 0; // else no match
      case 1:
        if (T::cmp(ptr1->key, endkey) <= 0){
          retkey = ptr1->key; // if we are before or at endkey, match
          return 1;
        }
        else return 0; // else no match
      case 2:
        retkey = ptr1->key;
        return 1;
    }
    return 0; // control should never reach here
  }

  void insert(T &key, U value){
    SkipListNode<T,U,Alloc> *newptr;
    int i;
    SkipListNode<T,U,Alloc> *ptr;

    ++nitems;
    // choose random height with exponentially decreasing probability
    int height = 0;
    int countbit=0, bit, rnd;
    rnd = (int)prng.next();
    do {
      ++height;
      bit = rnd & 1;  // extra a random bit
      rnd >>= 1;      // set up to get next random bits
      if (++countbit == 15){ // prng returns only 15 bits
        rnd = (int)prng.next(); // get next number
        countbit = 0;
      }
    } while (bit); // continue with 50% probability

    if (height > maxlevels) expandLevels(height);
    newptr = SkipListNode<T,U,Alloc>::newNode(height);
    newptr->key = key;
    newptr->value = value;

    ptr = Head;
    for (i = maxlevels-1; i >= 0; --i){
      while (ptr->next[i] != Tail && T::cmp(ptr->next[i]->key, key) <= 0)
        ptr = ptr->next[i];
      if (i < height){
        newptr->next[i] = ptr->next[i];
        ptr->next[i] = newptr;
      }
    }
  }
  // Delete a range of keys, from key1 to key2 inclusive.
  // deleteitem indicates if the keys and values themselves are to be deleted
  //   0=delete only node, not keys nor values
  //   1=delete only key
  //   2=delete only value
  //   3=delete both key and value
  // The values of type1 and type2 indicate whether the interval is opened,
  // closed, or infinite on the left and right, respectively. That is:
  //    type1==0 means (key1..
  //    type1==1 means [key1..
  //    type1==2 means (-inf... (key1 is ignored)
  //    type2==0 means ..key2)
  //    type2==1 means ..key2]
  //    type2==2 means ..inf)
  // For example, type1==1 and type2==0 means an interval of form [key1,key2)
  // Returns number of deleted keys
  int delRange(T &key1, int type1, T &key2, int type2, void (*delkey)(T&),
               void (*delvalue)(U)){
    int i;
    SkipListNode<T,U,Alloc> *ptr, *nextptr;
    SkipListNode<T,U,Alloc> **prevptrs;
    int ndeleted=0;

    // store previous ptrs at each level    
    prevptrs = (SkipListNode<T,U,Alloc> **)
      AMALLOC(sizeof(SkipListNode<T,U,Alloc>*) * maxlevels); 

    // first traverse to key1 and record previous pointers
    // for each previous pointer, record their next pointer at their level
    ptr = Head;
    for (i = maxlevels-1; i >= 0; --i){
      switch(type1){
      case 0: // find point until the last key matching key1
        while (ptr->next[i] != Tail && T::cmp(ptr->next[i]->key, key1) <= 0)
          ptr = ptr->next[i];
        break;
      case 1: // find point until right before key1
        while (ptr->next[i] != Tail && T::cmp(ptr->next[i]->key, key1) < 0)
          ptr = ptr->next[i];
        break;
      case 2: // stay at the head
        break;
      }
      prevptrs[i] = ptr; // remember ptr at this level to maybe insert later
    }

    ptr = ptr->next[0];  // from now on, we must delete...

    // now traverse from key1 until key2, while deleting nodes
    // while traversing, see if node being deleted was blocking one of
    // the previous pointers
    while (ptr != Tail){
      switch(type2){
      case 0:
        if (T::cmp(ptr->key,key2) >= 0) goto breakwhile;
        break;
      case 1:
        if  (T::cmp(ptr->key, key2) > 0) goto breakwhile;
        break;
      case 2:
        break;
      }

      ++ndeleted;
      for (i=0; i < ptr->nlevels; ++i){
        // update next pointer of previous pointers        
        prevptrs[i]->next[i] = ptr->next[i];
      }
      nextptr = ptr->next[0];
      if (delkey) delkey(ptr->key);
      if (delvalue) delvalue(ptr->value);
      SkipListNode<T,U,Alloc>::freeNode(ptr);
      ptr = nextptr;
    }
    breakwhile:

    AFREE(prevptrs);

    nitems -= ndeleted;
    return ndeleted;
  }

  SkipListNode<T,U,Alloc> *getFirst(){
    return Head->next[0];
  }

  SkipListNode<T,U,Alloc> *getLast(){
    return Tail;
  }

  SkipListNode<T,U,Alloc> *getNext(SkipListNode<T,U,Alloc> *ptr){
    return ptr->next[0];
  }
  
  int getNitems(){ return nitems; }
};

// Auxiliary class for SkipList BK
template<class T, class U, class Alloc=DefaultAllocator>
struct SkipListNodeBK {
  T *key;
  U value;
  int nlevels; // this field is used for some of the more sophisticated
               // operations (copy skiplist, delete range)
  SkipListNodeBK *next[1];
  SkipListNodeBK(){}
  ~SkipListNodeBK(){}
  static SkipListNodeBK<T,U,Alloc> *newNode(int n){
    if (n <= 0) n=1;
    SkipListNodeBK<T,U,Alloc> *ret = (SkipListNodeBK<T,U,Alloc>*)
      AMALLOC(sizeof(SkipListNodeBK<T,U,Alloc>) +
              (n-1) * sizeof(SkipListNodeBK<T,U,Alloc>*));
    rtchk(ret);
    new(ret) SkipListNodeBK<T,U,Alloc>;
    ret->nlevels = n;
    return ret;
  }
  static void freeNode(SkipListNodeBK<T,U,Alloc> *node){
    node->~SkipListNodeBK<T,U,Alloc>();
    AFREE(node);
  }
};

// Same as SkipList, but for big keys (functions take U* instead of U).
// Functions will not generally copy the key, but rather just copy the pointer.
// Therefore, once inserted, the key being pointed to will be owned by the
// SkipListBK.
// When copying a SkipList with the copy() method, the key pointed to will be
// copied using its copy constructor and the caller can pass a function to
// copy the value.
template<class T, class U, class Alloc=DefaultAllocator>
class SkipListBK {
private:
  int maxlevels;
  int nitems;
  SkipListNodeBK<T,U,Alloc> *Head, *Tail;
  
  SkipListBK &operator=(const SkipListBK &r){ rtchk(0); } // forbid assignment
  SkipListBK(const SkipListBK &r){ rtchk(0); } //forbid default copy constructor

  // Copy skiplist
  // This is private because if skiplist is not empty, there will be a memory
  // leak.
  // Use instead the other copy method below
  // This is intended to be called only from the public copy constructor below
  void copy(const SkipListBK &r, void (*copyvalue)(U, U&)){
    int i;
    SkipListNodeBK<T,U,Alloc> **missingprev, *ptr, *newnode;
    
    // missingprev has previous nodes that are missing a next pointer,
    // for each level
    nitems = r.nitems;
    maxlevels = r.maxlevels;
    missingprev = (SkipListNodeBK<T,U,Alloc> **)
      AMALLOC(sizeof(SkipListNodeBK<T,U,Alloc>*) * maxlevels);
    Head = SkipListNodeBK<T,U,Alloc>::newNode(maxlevels);
    for (i=0; i < r.maxlevels; ++i) missingprev[i] = Head;
    ptr = r.Head->next[0];
    while (ptr != r.Tail){
      newnode = SkipListNodeBK<T,U,Alloc>::newNode(ptr->nlevels);
      newnode->key = (T*) AMALLOC(sizeof(T));
      new(newnode->key) T(*ptr->key);
      if (copyvalue) copyvalue(ptr->value, newnode->value);
      else newnode->value = ptr->value;
      
      for (i=0; i < ptr->nlevels; ++i){
        missingprev[i]->next[i] = newnode;
        missingprev[i] = newnode;
      }
      ptr = ptr->next[0];
    }
    Tail = SkipListNodeBK<T,U,Alloc>::newNode(1);
    for (i=0; i < maxlevels; ++i)
      missingprev[i]->next[i] = Tail;
    AFREE(missingprev);
  }
  
  SimplePrng prng;
  void expandLevels(int highern){
    int i;
    if (highern <= maxlevels) return;
    SkipListNodeBK<T,U,Alloc> *newHead =
      SkipListNodeBK<T,U,Alloc>::newNode(highern);
    for (i=0; i < maxlevels; ++i) newHead->next[i] = Head->next[i];
    for (i=maxlevels; i < highern; ++i) newHead->next[i] = Tail;
    SkipListNodeBK<T,U,Alloc>::freeNode(Head);
    Head = newHead;
    maxlevels = highern;
  }

  // seek to position <= key
  SkipListNodeBK<T,U,Alloc> *seek(T *key){
    int i;
    SkipListNodeBK<T,U,Alloc> *ptr;
    ptr = Head;
    for (i = maxlevels-1; i >= 0; --i)
    {
      while (ptr->next[i] != Tail && T::cmp(*ptr->next[i]->key, *key) <= 0){
        if (T::cmp(*ptr->next[i]->key, *key)==0) return ptr->next[i];
        ptr = ptr->next[i];
      }
    }
    return ptr;
  }

  // seek to position < k
  SkipListNodeBK<T,U,Alloc> *seekL(T *key){
    int i;
    SkipListNodeBK<T,U,Alloc> *ptr;
    ptr = Head;
    for (i = maxlevels-1; i >= 0; --i)
      while (ptr->next[i] != Tail && T::cmp(*ptr->next[i]->key, *key)<0)
        ptr = ptr->next[i];
    return ptr;
  }

public:
  SkipListBK(){
    Head = SkipListNodeBK<T,U,Alloc>::newNode(1);
    Tail = SkipListNodeBK<T,U,Alloc>::newNode(1);
    Head->next[0] = Tail;
    Tail->next[0] = 0;
    maxlevels = 1;
    nitems = 0;
  }

  ~SkipListBK(){
    clear(0,0);
    SkipListNodeBK<T,U,Alloc>::freeNode(Head);
    SkipListNodeBK<T,U,Alloc>::freeNode(Tail);
  }

  // copy constructor. Calls the copy constructor for the key and a
  // user-supplied function to copy the value of each item in the skiplist
  SkipListBK(const SkipListBK &r, void (*copyvalue)(U,U&)){
    copy(r, copyvalue);
  }
  
  // Clear all items. If deleteitem == 1 then call delete on each key,
  // if deleteitem == 2, call delete on each value
  // if deleteitem == 3, call delete on both key and value
  void clear(void (*delkey)(T*), void (*delvalue)(U)){
    SkipListNodeBK<T,U,Alloc> *ptr, *nextptr;
    nitems = 0;
    ptr = Head->next[0];
    while (ptr != Tail){
      nextptr = ptr->next[0];
      if (delkey) delkey(ptr->key);
      if (delvalue) delvalue(ptr->value);
      SkipListNodeBK<T,U,Alloc>::freeNode(ptr);
      ptr = nextptr;
    }
    for (int i=0; i < maxlevels; ++i) Head->next[i] = Tail;
  }

  // Clears skiplist and then copies it from another skiplist
  // copyvalue is a method to copy the value. If 0, just use assignment
  // delkey is a method to free the key. If 0, key is not freed
  // delvalue is a method to free the value. If 0, value is not freed
  void copy(const SkipListBK &r, void (*copyvalue)(U, U&),
            void (*delkey)(T*), void (*delvalue)(U)){
    clear(delkey, delvalue);
    copy(r, copyvalue);
  }

  // returns 0 if found, non-zero if not found. If found, makes retvalue point
  // to value, in which case caller can read the value with *retvalue, and it
  // can change the value with *retvalue = ...
  int lookup(T *key, U *&retvalue){
    SkipListNodeBK<T,U,Alloc> *ptr = seek(key);
    if (ptr == Head) return -1;
    if (T::cmp(*ptr->key, *key)==0){
      retvalue = &ptr->value;
      return 0;
    }
    else return -1;
  }

  bool belongs(T *key){
    SkipListNodeBK<T,U,Alloc> *ptr = seek(key);
    if (ptr == Head) return false; // does not belong
    if (T::cmp(*ptr->key, *key)==0) return true; // belongs
    else return false; // does not belong
  }

  // try to find key; if not found then create it.
  // Returns 0 if found, non-zero if not found and therefore new item was
  // created.
  // Sets valueptr to point to location containing pointer to value of found
  // or created item.
  // For example, if item was created, the caller can set its value with
  //    *valueptr = ...
  // If item was found, the caller can retrieve the value with *valueptr.
  // If key was not found, this method will own the key; otherwise, it will
  // not (so caller should free key)
  int lookupInsert(T *key, U *& valueptr){
    SkipListNodeBK<T,U,Alloc> *newptr;
    int i;
    SkipListNodeBK<T,U,Alloc> *ptr;
    SkipListNodeBK<T,U,Alloc> **prevptrs;
    SkipListNodeBK<T,U,Alloc> *oldhead;

    // store previous ptrs at each level
    prevptrs = (SkipListNodeBK<T,U,Alloc> **)
      AMALLOC(sizeof(SkipListNodeBK<T,U,Alloc>*) * maxlevels);

    oldhead = ptr = Head;
    for (i = maxlevels-1; i >= 0; --i){
      while (ptr->next[i] != Tail && T::cmp(*ptr->next[i]->key, *key)<0)
        ptr = ptr->next[i];
      prevptrs[i] = ptr; // remember ptr at this level to maybe insert later
    }

    if (ptr->next[0] != Tail && T::cmp(*ptr->next[0]->key, *key)==0){ //found it
      AFREE(prevptrs); 
      valueptr = &ptr->next[0]->value;
      return 0;
    }

    // did not find it, so insert new item
    ++nitems;
    // choose random height with exponentially decreasing probability
    int height = 0;
    int countbit=0, bit, rnd;
    rnd = (int)prng.next();
    do {
      ++height;
      bit = rnd & 1;  // extra a random bit
      rnd >>= 1;      // set up to get next random bits
      if (++countbit == 15){ // prng returns only 15 bits
        rnd = (int)prng.next(); // get next number
        countbit = 0;
      }
    } while (bit); // continue with 50% probability

    int oldheight = maxlevels;
    if (height > maxlevels) expandLevels(height);
    newptr = SkipListNodeBK<T,U,Alloc>::newNode(height);
    newptr->key = key;
    valueptr = &newptr->value;

    // insert item
    for (i=0; i < height; ++i){
      if (i < oldheight){
        if (prevptrs[i] == oldhead) ptr = Head; // head may have been resized
        else ptr = prevptrs[i];
      }
      else ptr = Head;
      newptr->next[i] = ptr->next[i];
      ptr->next[i] = newptr;
    }

    AFREE(prevptrs);
    return 1;
  }

  // Insert a new key if it is not in the list, or replace an existing one if
  // it already exists.
  // If there are multiple existing keys, only the first one is replaced.
  // If replacing, deleteitem controls whether the old key and value should be
  // deleted:
  //   deleteitem == 1: delete old key
  //   deleteitem == 2: delete old value
  //   deleteitem == 3: delete both old key and value
  // This method will own key regardless of whether an existing key was
  // replaced or not.
  // Returns 0 if replaced, 1 if inserted.
  int insertOrReplace(T *key, U value, void (*delkey)(T*),
                      void (*delvalue)(U)){
    SkipListNodeBK<T,U,Alloc> *newptr;
    int i;
    SkipListNodeBK<T,U,Alloc> *ptr;
    SkipListNodeBK<T,U,Alloc> **prevptrs;
    SkipListNodeBK<T,U,Alloc> *oldhead;

    // store previous ptrs at each level
    prevptrs = (SkipListNodeBK<T,U,Alloc> **)
      AMALLOC(sizeof(SkipListNodeBK<T,U,Alloc>*) * maxlevels);

    oldhead = ptr = Head;
    for (i = maxlevels-1; i >= 0; --i){
      while (ptr->next[i] != Tail && T::cmp(*ptr->next[i]->key, *key)<0)
        ptr = ptr->next[i];
      prevptrs[i] = ptr; // remember ptr at this level to maybe insert later
    }

    if (ptr->next[0] != Tail && T::cmp(*ptr->next[0]->key, *key)==0){ //found it
      AFREE(prevptrs);
      ptr = ptr->next[0];
      if (delkey) delkey(ptr->key);
      if (delvalue) delvalue(ptr->value);
      ptr->key = key;
      ptr->value = value;
      return 0;
    }

    // did not find it, so insert new item
    ++nitems;
    // choose random height with exponentially decreasing probability
    int height = 0;
    int countbit=0, bit, rnd;
    rnd = (int)prng.next();
    do {
      ++height;
      bit = rnd & 1;  // extra a random bit
      rnd >>= 1;      // set up to get next random bits
      if (++countbit == 15){ // prng returns only 15 bits
        rnd = (int)prng.next(); // get next number
        countbit = 0;
      }
    } while (bit); // continue with 50% probability

    int oldheight = maxlevels;
    if (height > maxlevels) expandLevels(height);
    newptr = SkipListNodeBK<T,U,Alloc>::newNode(height);
    newptr->key = key;
    newptr->value = value;

    // insert item
    for (i=0; i < height; ++i){
      if (i < oldheight){
        if (prevptrs[i] == oldhead) ptr = Head; // head may have been resized
        else ptr = prevptrs[i];
      }
      else ptr = Head;
      newptr->next[i] = ptr->next[i];
      ptr->next[i] = newptr;
    }

    AFREE(prevptrs);
    return 1;
  }

  // Try to find key. If found then delete it and return 0 and its value in
  // retvalue. If not found, return non-zero.
  // If there are multiple copies of the key, deletes only the first one.
  // If delkey is non-null, invoke it on key being deleted
  int lookupRemove(T *key, void (*delkey)(T*), U& retvalue){
    int i;
    SkipListNodeBK<T,U,Alloc> *ptr;
    SkipListNodeBK<T,U,Alloc> **prevptrs;

 // store previous ptrs at each level
    prevptrs = (SkipListNodeBK<T,U,Alloc> **)
      AMALLOC(sizeof(SkipListNodeBK<T,U,Alloc>*) * maxlevels);

    ptr = Head;
    for (i = maxlevels-1; i >= 0; --i){
      while (ptr->next[i] != Tail && T::cmp(*ptr->next[i]->key, *key)<0)
        ptr = ptr->next[i];
      prevptrs[i] = ptr; // remember ptr at this level to maybe insert later
    }

    if (ptr->next[0] == Tail || T::cmp(*ptr->next[0]->key, *key) != 0){
      // did not find it
      AFREE(prevptrs); 
      return -1;
    }
    // found it, so delete item
    --nitems;
    ptr = ptr->next[0];
    retvalue = ptr->value; // remember value
    if (delkey) delkey(ptr->key);

    int height = ptr->nlevels;
    for (i=0; i < height; ++i)
      prevptrs[i]->next[i] = ptr->next[i];
    SkipListNodeBK<T,U,Alloc>::freeNode(ptr);

    AFREE(prevptrs);
    return 0;
  }

  // Returns first key that lies within a key interval, or 0 if none.
  // The intervals can be of 9 types:
  //    0 = (a,b),    1=(a,b],     2=(a,inf),
  //    3 = [a,b),     4=[a,b],    5=[a,inf),
  //    6 = (-inf,b),  7=(-inf,b], 8=(-inf,inf)
  T *keyInInterval(T *startkey, T *endkey, int intervalType){
    SkipListNodeBK<T,U,Alloc> *ptr1;
    if (Head == Tail) return 0;
    if (intervalType < 3) 
      ptr1 = seek(startkey); // seek to position right before or at startkey
    else if (intervalType < 6)
      ptr1 = seekL(startkey); // seek to position right before startkey
    else ptr1 = Head;
    ptr1 = ptr1->next[0];
    if (ptr1 == Tail) return 0; // if end, no match
    switch(intervalType % 3){
    case 0: 
      if (T::cmp(*ptr1->key, *endkey)<0) return ptr1->key; // if we are before
                                                           // endkey, match
      else return 0; // else no match
    case 1:
      if (T::cmp(*ptr1->key, *endkey)<=0) return ptr1->key; // if we are before
                                                         // or at endkey, match
      else return 0; // else no match
    case 2: return ptr1->key;
    }
    return 0; // control should never reach here
  }

  // Inserts a new key,value.
  // This method will own key.
  void insert(T *key, U value){
    SkipListNodeBK<T,U,Alloc> *newptr;
    int i;
    SkipListNodeBK<T,U,Alloc> *ptr;

    ++nitems;
    // choose random height with exponentially decreasing probability
    int height = 0;
    int countbit=0, bit, rnd;
    rnd = (int)prng.next();
    do {
      ++height;
      bit = rnd & 1;  // extra a random bit
      rnd >>= 1;      // set up to get next random bits
      if (++countbit == 15){ // prng returns only 15 bits
        rnd = (int)prng.next(); // get next number
        countbit = 0;
      }
    } while (bit); // continue with 50% probability

    if (height > maxlevels) expandLevels(height);
    newptr = SkipListNodeBK<T,U,Alloc>::newNode(height);
    newptr->key = key;
    newptr->value = value;

    ptr = Head;
    for (i = maxlevels-1; i >= 0; --i){
      while (ptr->next[i] != Tail && T::cmp(*ptr->next[i]->key, *key) <= 0)
        ptr = ptr->next[i];
      if (i < height){
        newptr->next[i] = ptr->next[i];
        ptr->next[i] = newptr;
      }
    }
  }

  // Delete a range of keys, from key1 to key2 inclusive.
  // deleteitem indicates if the keys and values themselves are to be deleted
  //   0=delete only node, not keys nor values
  //   1=delete only key
  //   2=delete only value
  //   3=delete both key and value
  // The values of type1 and type2 indicate whether the interval is opened,
  // closed, or infinite on the left and right, respectively. That is:
  //    type1==0 means (key1..
  //    type1==1 means [key1..
  //    type1==2 means (-inf... (key1 is ignored)
  //    type2==0 means ..key2)
  //    type2==1 means ..key2]
  //    type2==2 means ..inf)
  // For example, type1==1 and type2==0 means an interval of form [key1,key2)
  // Returns number of deleted keys
  int delRange(T *key1, int type1, T *key2, int type2,
               void (*delkey)(T*), void (*delvalue)(U)){
    int i;
    SkipListNodeBK<T,U,Alloc> *ptr, *nextptr;
    SkipListNodeBK<T,U,Alloc> **prevptrs;
    int ndeleted=0;

    // store previous ptrs at each level
    prevptrs = (SkipListNodeBK<T,U,Alloc> **)
      AMALLOC(sizeof(SkipListNodeBK<T,U,Alloc>*) * maxlevels);

    // first traverse to key1 and record previous pointers
    // for each previous pointer, record their next pointer at their level
    ptr = Head;
    for (i = maxlevels-1; i >= 0; --i){
      switch(type1){
      case 0: // find point until the last key matching key1
        while (ptr->next[i] != Tail && T::cmp(*ptr->next[i]->key, *key1) <= 0)
          ptr = ptr->next[i];
        break;
      case 1: // find point until right before key1
        while (ptr->next[i] != Tail && T::cmp(*ptr->next[i]->key, *key1) < 0)
          ptr = ptr->next[i];
        break;
      case 2: // stay at the head
        break;
      }
      prevptrs[i] = ptr; // remember ptr at this level to maybe insert later
    }

    ptr = ptr->next[0];  // from now on, we must delete...

    // now traverse from key1 until key2, while deleting nodes
    // while traversing, see if node being deleted was blocking one
    // of the previous pointers
    while (ptr != Tail){
      switch(type2){
      case 0:
        if (T::cmp(*ptr->key, *key2) >= 0) goto breakwhile;
        break;
      case 1:
        if  (T::cmp(*ptr->key, *key2) > 0) goto breakwhile;
        break;
      case 2:
        break;
      }

      ++ndeleted;
      for (i=0; i < ptr->nlevels; ++i){
        // update next pointer of previous pointers
        prevptrs[i]->next[i] = ptr->next[i];
      }
      nextptr = ptr->next[0];
      if (delkey) delkey(ptr->key);
      if (delvalue) delvalue(ptr->value);
      SkipListNodeBK<T,U,Alloc>::freeNode(ptr);
      ptr = nextptr;
    }
    breakwhile:

    AFREE(prevptrs);

    nitems -= ndeleted;
    return ndeleted;
  }

  SkipListNodeBK<T,U,Alloc> *getFirst(void){
    return Head->next[0];
  }

  SkipListNodeBK<T,U,Alloc> *getLast(void){
    return Tail;
  }

  SkipListNodeBK<T,U,Alloc> *getNext(SkipListNodeBK<T,U,Alloc> *ptr){
    return ptr->next[0];
  }
  
  int getNitems(){ return nitems; }
};


// A Hash table with elements of type U and key of type T
// Requirements:
// 1. U has a field of type T
// 2. U has public fields next, prev, snext, sprev of type U*
// 3. U has method T GetKey() which returns key of U
// 4. U has static functions HashKey(T) and CompareKey(T,T) which hashes and
//    compares a given key
template<class T, class U, class Alloc=DefaultAllocator>
class HashTable {
private:
  int Nbuckets;
  SortedLinkList<T,U,Alloc> *Buckets;
  LinkList<U,Alloc> AllElements;
  int nitems; // number of elements so far

public:
  HashTable(int nbuckets){
    int i;
    Nbuckets = nbuckets;
    Buckets = (SortedLinkList<T,U,Alloc> *)
      AMALLOC(sizeof(SortedLinkList<T,U,Alloc>) * nbuckets);
    for (i=0; i < nbuckets; ++i){
      new(Buckets+i) SortedLinkList<T,U,Alloc>();
    }
    nitems = 0;
  }

  ~HashTable(){
    for (int i=0; i < Nbuckets; ++i){
      Buckets[i].~SortedLinkList<T,U,Alloc>();
    }
    AFREE(Buckets);
  }

  void insert(U *toadd){
    int bucket = (unsigned)U::HashKey(toadd->GetKey()) % Nbuckets;
    ++nitems;
    Buckets[bucket].insert(toadd);
    AllElements.pushTail(toadd);
  }

  U *lookup(T key){
    int bucket = (unsigned)U::HashKey(key) % Nbuckets;
    SortedLinkList<T,U,Alloc> *b = Buckets + bucket;
    return b->lookup(key);
  }

  U *operator[](T key){ return lookup(key); }

  void remove(U *ptr){ SortedLinkList<T,U,Alloc>::removeDirect(ptr);
                       AllElements.remove(ptr); --nitems; }

  U *getFirst(){ return AllElements.getFirst(); }
  U *getNext(U *ptr){ return AllElements.getNext(ptr); }
  U *getLast(){ return AllElements.getLast(); }
  int getNitems(){ return nitems; }
};

// Same as HashTable but for big keys. Assumes:
// 1. U has a field of type T
// 2. U has public fields next, prev, snext, sprev of type U*
// 3. U has method T GetKeyPtr which returns pointer to key of itself
// 4. U has static functions HashKey(T*) and CompareKey(T*,T*) which hashes
//    and compares a given key

template<class T, class U, class Alloc=DefaultAllocator>
class HashTableBK {
private:
  int Nbuckets;
  SortedLinkListBK<T,U,Alloc> *Buckets;
  LinkList<U,Alloc> AllElements;
  int nitems;

public:
  HashTableBK(int nbuckets){
    int i;
    Nbuckets = nbuckets;
    Buckets = (SortedLinkListBK<T,U,Alloc> *)
      AMALLOC(sizeof(SortedLinkListBK<T,U,Alloc>) * nbuckets);
    for (i=0; i < nbuckets; ++i){
      new(Buckets+i) SortedLinkListBK<T,U,Alloc>;
    }
    nitems = 0;
  }

  ~HashTableBK(){
    for (int i=0; i < Nbuckets; ++i){
      Buckets[i].~SortedLinkListBK<T,U,Alloc>();
    }
    AFREE(Buckets);
  }

  void insert(U *toadd){
    int bucket = (unsigned)U::HashKey(toadd->GetKeyPtr()) % Nbuckets;
    ++nitems;
    Buckets[bucket].insert(toadd);
    AllElements.pushTail(toadd);
  }

  U *lookup(T *key){
    int bucket = (unsigned)U::HashKey(key) % Nbuckets;
    SortedLinkListBK<T,U,Alloc> *b = Buckets + bucket;
    return b->lookup(key);
  }

  U *operator[](T *key){ return lookup(key); }
  void remove(U *ptr){
    SortedLinkList<T,U,Alloc>::removeDirect(ptr);
    AllElements.remove(ptr);
    --nitems;
  }

  U *getFirst(){ return AllElements.getFirst(); }
  U *getNext(U *ptr){ return AllElements.getNext(ptr); }
  U *getLast(){ return AllElements.getLast(); }
  int getNitems(){ return nitems; }
};

// A stack based on arrays. Intended for small item types T.
// The destructor of T will not be called as elements get popped, only
// when the StackArray is deleted or an element gets pushed over it.
// So, it is not advisable to use this class with types with destructors.
template<class T,class Alloc=DefaultAllocator>
class StackArray {
private:
  T *ElementArray;    // array holding items
  int CurrItem;       // position of current item
  int CurrArraySize;  // current size
  double GrowArrayFactor; // growth factor

  void freeArray(){
    int i;
    for (i=0; i < CurrArraySize; ++i){
      ElementArray[i].~T();
    }
    AFREE(ElementArray);
  }

  // allocate new array of given size and copy elements to it
  // newsize is actually allowed to be smaller than CurrArraySize, as long as
  // it is not smaller than CurrItem (otherwise, there is no space to copy
  // current items)
  void grow(int newsize){
    assert(newsize >= CurrItem);
    int i;
    T *newarray = (T*) AMALLOC(sizeof(T) * newsize);
    for (i=0; i < newsize; ++i)
      new(newarray+i) T;
    if (ElementArray){
      memcpy(newarray, ElementArray, sizeof(T) * CurrItem);
      freeArray();
    }
    ElementArray = newarray;
    CurrArraySize = newsize;
  }

public:
  // initarraysize is the initial size of the array. Items will be placed in
  // array until it reaches the array size, at which point a new larger array
  // is allocated (of size bigger by a factor of growarrayfactor) and items
  // are copied to the new larger array
  StackArray(int initarraysize, double growarrayfactor){
    ElementArray = 0;
    CurrItem = 0;
    GrowArrayFactor = growarrayfactor;
    grow(initarraysize); // sets ElementArray and CurrArraySize
  }

  ~StackArray(){
    freeArray();
  }

  // returns true iff stack is empty
  bool empty(){ return CurrItem == 0; }

  T pop(){
    assert(CurrItem >= 1);
    --CurrItem;
    return ElementArray[CurrItem];
  }

  void push(T item){
    if (CurrItem >= CurrArraySize)
      grow((int)(CurrArraySize * GrowArrayFactor));
    assert(CurrItem < CurrArraySize);
    ElementArray[CurrItem] = item;
    ++CurrItem;
  }
  
  int getNitems(){ return CurrItem; }
};

// smart pointer class
// Assumes that T contains an 32-bit attribute refcount that is 32-bit aligned
// and initialized to zero in all constructors and that T declares Ptr<T> as a
// class friend.
template<class T>
class Ptr {
private:
  T *ptr;
public:
  void init(){ ptr = 0; } // This function is intended to initialize
                          // the smart pointer when it is pointing to
                          // garbage (e.g., uninitialized memory). This
                          // does not happen normally since the constructor
                          // initializes things, but it may happen if one
                          // uses a placement new or similar.
                          // Do NOT use init() when the pointer is
                          // already set to something valid; doing so will
                          // fail to decrease the refcount of the old value,
                          // leading to a memory leak.
  Ptr(){ init(); }
  Ptr(T *p){ ptr = p; if (p) AtomicInc32(&p->refcount); }
  Ptr(Ptr const &ptr2) { ptr = ptr2.ptr; if (ptr) AtomicInc32(&ptr->refcount); }
  ~Ptr(){ if (ptr && AtomicDec32(&ptr->refcount)==0) delete ptr; }
  Ptr &operator=(Ptr const &ptr2){
    T *const oldp = ptr;
    ptr = ptr2.ptr;
    if (ptr) AtomicInc32(&ptr->refcount);
    if (oldp && AtomicDec32(&oldp->refcount)==0) delete oldp;
    return *this;
  }
  T &operator* (){ return *ptr; }
  T *operator->(){ return ptr; }
  bool isset(){ return ptr!=0; }
  int refcount(){ return ptr->refcount; }
};

// Set, implemented as a skiplist.
// Requires T to have static function cmp(T &left, T &right) which
// returns -1, 0, +1 if left < right, left==right, left > right, respectively

template<class T, class Alloc=DefaultAllocator>
class SetNode : public SkipListNode<T,int,Alloc> { };

template<class T, class Alloc=DefaultAllocator>
class Set {
private:
  SkipList<T,int,Alloc> Elements; // int value is dummy
public:
  // insert an item. Returns 0 if item was inserted, non-zero if item
  // previously existed
  int insert(T key){
    int *dummy;
    int res;
    res = Elements.lookupInsert(key, dummy);
    *dummy = 0xbafabafa;
    return res==0;
  }

  // delete an element. Returns 0 if item was deleted, non-zero if item
  // did not belong
  int remove(T key){
    int res;
    int retval;
    res = Elements.lookupRemove(key, 0, retval);
    return res;
  }

  void clear(){
    Elements.clear(0,0);
  }
  
  int belongs(T key){
    return Elements.belongs(key);
  }

  SetNode<T,Alloc> *getFirst(){ return (SetNode<T,Alloc>*) Elements.getFirst(); }
  SetNode<T,Alloc> *getLast(){ return (SetNode<T,Alloc>*) Elements.getLast(); }
  SetNode<T,Alloc> *getNext(SetNode<T,Alloc> *ptr){
    return (SetNode<T,Alloc>*) Elements.getNext(ptr);
  }
  int getNitems(){ return Elements.getNitems(); }
  bool empty(){ return getNitems() == 0; }
};

// ---------------------------------------------------------------------------
// class version of some primitive types, to be used with data structures above
struct U32 {
  u32 data;
  static unsigned hash(const U32 &l){ return (unsigned) l.data; }
  static int cmp(const U32 &l, const U32 &r){ 
    if (l.data < r.data) return -1;
    else if (l.data > r.data) return +1;
    else return 0;
  }
  U32(){ data = 0; }
  U32(u32 d){ data = d; }
};

struct I32 {
  i32 data;
  static unsigned hash(const I32 &l){ return (unsigned) l.data; }
  static int cmp(const I32 &l, const I32 &r){ 
    if (l.data < r.data) return -1;
    else if (l.data > r.data) return +1;
    else return 0;
  }
  I32(){ data = 0; }
  I32(i32 d){ data = d; }
};

struct U64 {
  u64 data;
  static unsigned hash(const U64 &l){ return (unsigned) l.data; }
  static int cmp(const U64 &l, const U64 &r){ 
    if (l.data < r.data) return -1;
    else if (l.data > r.data) return +1;
    else return 0;
  }
  U64(){ data = 0; }
  U64(u64 d){ data = d; }
};

struct I64 {
  i64 data;
  static unsigned hash(const I64 &l){ return (unsigned) l.data; }
  static int cmp(const I64 &l, const I64 &r){ 
    if (l.data < r.data) return -1;
    else if (l.data > r.data) return +1;
    else return 0;
  }
  I64(){ data = 0; }
  I64(i64 d){ data = d; }
};

#endif

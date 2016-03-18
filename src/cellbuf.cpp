//
// cellbuf.cpp
//
// Maintains a buffer with variable length items
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

#include "cellbuf.h"

// parse the buffer. Set CellPtrs and UsedBuf
void CellBuffer::parseBuffer(void){
  int i;
  u8 *ptr;
  u64 value;
  int len;

  assert(Ncells <= MaxCells);
  ptr = BufPtr;
  for (i=0; i < Ncells; ++i){
    CellPtrs[i] = ptr;
    len = getVarint((unsigned char*) ptr, &value); // read value 
    assert(len < 100000);
    ptr +=  len;
    if (!IntKey){
      assert(value < 100000);
      ptr += value;  // if non-integer key then add its size
    }
  }
#ifdef DEBUG
  for (; i < MaxCells; ++i) CellPtrs[i]=0;
#endif

  CellPtrs[i] = ptr; // fill one more beyond last cell
  UsedBuf = (int)(ptr - BufPtr);
  assert(UsedBuf <= TotalBuf);
}

// performs a sanity check on cells; generates an assert() error if there
// is a problem
void CellBuffer::validate(void){
  int i;
  u8 *ptr;
  u64 value;
  int len;

  assert(Ncells <= MaxCells);
  ptr = BufPtr;
  for (i=0; i < Ncells; ++i){
    assert(CellPtrs[i] == ptr);
    if (CellPtrs[i+1] == CellPtrs[i]) continue; // empty cell
    len = getVarint((unsigned char*) ptr, &value); // read value 
    assert(len < 100000);
    ptr +=  len;
    if (!IntKey){
      assert(value < 100000);
      ptr += value;  // if non-integer key then add its size
    }
  }

  assert(CellPtrs[i] == ptr); // fill one more beyond last cell
  assert(UsedBuf == ptr - BufPtr);
  assert(UsedBuf <= TotalBuf);
}

// increase size of buffer, adjust pointers
void CellBuffer::GrowBuffer(int growsize){
  u8 *newbuf, *oldbuf;
  int i;

  assert(growsize >= 0 && growsize + TotalBuf >= 0);

  newbuf = new u8[TotalBuf + growsize];
  oldbuf = BufPtr;
  memcpy(newbuf, BufPtr, UsedBuf); // copy contents to new buffer

  // adjust pointers to new buffer
  for (i=0; i <= Ncells; ++i) CellPtrs[i] = (CellPtrs[i]-oldbuf)+newbuf;

  // change to new buffer
  switch(HowFreeBufPtr){ // but first free old buffer if necessary
  case 1: delete [] oldbuf; break;
  case 2: free((void*) oldbuf); break;
  }
  BufPtr = newbuf;
  HowFreeBufPtr = 1; // use delete[] to free it next time
  TotalBuf += growsize; // adjust size
  //validate();
}

// change the size of a cell. Contents of cell are destroyed
void CellBuffer::ResizeCell(int cellno, int newsize){
  int i;
  int currsize = (int) CellBufferCELLSIZE(cellno);

  if (newsize == currsize) return; // nothing to do

  // grow buffer if necessary
  if (UsedBuf - currsize + newsize > TotalBuf)
    GrowBuffer(UsedBuf-currsize+newsize+CellBufferGROWSIZE);

  // move content
  if (cellno+1 < Ncells)
    memmove(CellPtrs[cellno+1] + newsize - currsize, CellPtrs[cellno+1],
            CellPtrs[Ncells]-CellPtrs[cellno+1]);

  UsedBuf = UsedBuf - currsize + newsize;

  // adjust pointers
  for (i=cellno+1; i <= Ncells; ++i)
    CellPtrs[i] = CellPtrs[i] + newsize - currsize;
}

// can be used past the last cell to insert new cell
void CellBuffer::InsertEmptyCell(int cellno){
  assert(cellno <= Ncells && Ncells < MaxCells);
  // insert space in vector of pointers
  memmove(&CellPtrs[cellno+1], &CellPtrs[cellno], sizeof(u8*) *
          (Ncells+1-cellno));
  // remember, there is always an extra pointer at the end we need to keep
  ++Ncells;
}

void CellBuffer::SetCell(int cellno, i64 nkey, u8 *pkey){
  int newsize;
  u8 tmpbuf[9];
  u8 *ptr;
  assert(cellno <= Ncells && cellno < MaxCells);

  if (cellno == Ncells) InsertEmptyCell(cellno);

  // compute size of new cell
  newsize = putVarint(tmpbuf, nkey);
  if (!IntKey) newsize += (int)nkey;

  ResizeCell(cellno, newsize);
  ptr = CellPtrs[cellno];
  ptr += putVarint(ptr, nkey);
  assert(IntKey && !pkey || !IntKey && (pkey || !nkey));
  if (!IntKey) memcpy(ptr, pkey, (int) nkey);
  //validate();
}

// Insert a new cell into the buffer.
// New cell nmber can be after last cell, but not beyond
void CellBuffer::InsertCell(int cellno, i64 nkey, u8 *pkey){
  assert(cellno <= Ncells && cellno < MaxCells);
  InsertEmptyCell(cellno);
  SetCell(cellno, nkey, pkey);
  //validate();
}

void CellBuffer::DeleteCell(int cellno){
  assert(Ncells>0);
  ResizeCell(cellno, 0);
  memmove(&CellPtrs[cellno], &CellPtrs[cellno+1], sizeof(u8*) *
          (Ncells-cellno));
  // remember, there is always an extra pointer at the end we need to keep
  --Ncells;
  //validate();
}

void CellBuffer::truncBelow(int cellno){
  int newsize,i,offset;
  assert(0 <= cellno && cellno <= Ncells);

  offset = (int)(CellPtrs[cellno]-CellPtrs[0]);
  // move content
  newsize = (int)(CellPtrs[Ncells]-CellPtrs[cellno]);
  memmove(BufPtr, CellPtrs[cellno], newsize);

  // move pointers
  memmove(&CellPtrs[0], &CellPtrs[cellno], sizeof(u8*) * (Ncells+1-cellno));

  // set Ncell
  Ncells = Ncells-cellno;

  // fix pointers up to Ncell
  for (i=0; i <= Ncells; ++i)
    CellPtrs[i] = CellPtrs[i] - offset;

  // set UsedBuf;
  UsedBuf = newsize;
  //validate();
}

void CellBuffer::truncAfter(int cellno){
  int newsize,i;
  assert(0 <= cellno && cellno < Ncells);

  newsize = (int)(CellPtrs[cellno]-CellPtrs[0]);

  // fix pointers after cellno
  for (i=cellno+1; i <= Ncells; ++i)
    CellPtrs[i] = 0;

  // fix Ncell
  Ncells = cellno;

  // set UsedBuf;
  UsedBuf = newsize;
  //validate();
}



CellBuffer::CellBuffer(u8 *bufptr, int totalbuf, int ncells, int maxcells,
                       int intkey, int howfree){
  assert(totalbuf >= 0 && ncells >= 0 && maxcells >= 0);
  BufPtr = bufptr;  // initial buffer
  TotalBuf = totalbuf;
  Ncells = ncells;
  MaxCells = maxcells;
  IntKey = intkey;
  CellPtrs = new u8*[maxcells+1];
  HowFreeBufPtr = howfree;
  parseBuffer();
}

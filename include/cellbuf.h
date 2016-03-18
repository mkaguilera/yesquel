//
// cellbuf.h
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

#ifndef _CELLBUF_H
#define _CELLBUF_H

#include "tmalloc.h"

#define CellBufferGROWSIZE 512 // how many extra bytes to allocate when expanding buffer

#define CellBufferCELLSIZE(i) (CellPtrs[i+1]-CellPtrs[i])

class CellBuffer
{
private:
  int Ncells;        // number of cells
  int MaxCells;      // maximum number of cells
  u8  *BufPtr;       // pointer to the buffer beginning
  int HowFreeBufPtr; // how to free bufptr when it is no longer needed.
                     // 0=don't free, 1=use delete, 2=use free
  int IntKey;        // whether cell has just one Varint (intCell=true) or
                     // Varint followed by data (intCell=false)

  u8 **CellPtrs;   // pointer to each cell. Last pointer is to the first
                   // byte after last cell
  int UsedBuf;     // number of bytes used in buffer
  int TotalBuf;    // number of bytes allocated in buffer

  // parse the buffer. Set CellPtrs and UsedBuf
  void parseBuffer(void);

  // increase size of buffer, adjust pointers
  void GrowBuffer(int growsize);

  // change the size of a cell. Contents of cell are destroyed
  void ResizeCell(int cellno, int newsize);

  // can be used past the last cell to insert new cell
  void InsertEmptyCell(int cellno);

public:
  void SetCell(int cellno, i64 nkey, u8 *pkey);

  // Insert a new cell into the buffer.
  // New cell nmber can be after last cell, but not beyond
  void InsertCell(int cellno, i64 nkey, u8 *pkey);

  void DeleteCell(int cellno);
  void validate(void);

  // truncates all entries < cellno
  void truncBelow(int cellno);

  // truncates all entries >= cellno
  void truncAfter(int cellno);

  u8 *getCell(int cellno){ assert(cellno <= Ncells && cellno >= 0);
                           return CellPtrs[cellno]; }
  u8 *operator [](int cellno){ return getCell(cellno); }
  int getBufSize(void){ return UsedBuf; }
  int getNcells(void){ return Ncells; }


  // howfree is how the bufptr should be freed when it is no longer needed
  //   howfree=0: do not free bufptr, belongs to caller
  //   howfree=1: free bufptr by calling delete [] bufptr
  //   howfree=2: free bufptr by calling free(bufptr)
  CellBuffer(u8 *bufptr, int totalbuf, int ncells, int maxcells, int intkey,
             int howfree=0);

  ~CellBuffer(){
    delete [] CellPtrs;
    switch(HowFreeBufPtr){
    case 1:
      delete [] BufPtr;
      break;
    case 2:
      free((void*) BufPtr);
      break;
    }
  }
};

#endif

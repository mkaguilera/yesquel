//
// supervalue.cpp
//
// Definitions for supervalues. A supervalue is a value in a key-value
// pair with additional structure (rather than an opaque value): a list of
// cells, and a bunch of attributes that can be individually set.
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
#include <malloc.h>
#include <signal.h>
#include <list>
#include <map>
#include <set>

#include "tmalloc.h"
#include "supervalue.h"
#include "clientlib.h"

void SuperValue::copy(const SuperValue& c){
  memcpy(this, &c, sizeof(SuperValue));
  if (Nattrs){
    Attrs = new u64[Nattrs];
    memcpy(Attrs, c.Attrs, Nattrs * sizeof(u64));
  } else assert(Attrs==0);

  if (Ncells){
    Cells = new ListCell[Ncells];
    for (int i=0; i < Ncells; ++i){ 
      Cells[i] = c.Cells[i]; 
    }
  } else Cells = 0;
  prki.init();
  prki = c.prki;
}

// Insert a new cell at position pos.
// pos must be between 0 and Ncells. If pos==Ncells, insert at the end.
// After method is done, Cells[pos] is the new inserted cell
// Caller should fix CellsSize after setting Cells[pos]
// (eg, CellsSize += Cells[pos].size())
void SuperValue::InsertCell(int pos){
  ListCell *newcells;
  assert(0 <= pos && pos <= Ncells);
  newcells = new ListCell[Ncells+1];
  memcpy(newcells, Cells, pos * sizeof(ListCell));
  memcpy(newcells+pos+1, Cells+pos, (Ncells-pos)*sizeof(ListCell));
  delete [] Cells;
  Cells = newcells;
  ++Ncells;
}

// Delete cell at position pos.
// pos must be between 0 and Ncells-1
void SuperValue::DeleteCell(int pos){
  assert(0 <= pos && pos < Ncells);
  CellsSize -= Cells[pos].size();
  Cells[pos].Free();
  memmove(Cells+pos, Cells+pos+1, (Ncells-pos-1)*sizeof(ListCell));
  --Ncells;
}

// Delete cells in positions startpos..endpos-1 (a total of endpos-startpos
// cells).
// startpos must be between 0 and Ncells-1
// endpos must be between startpos and Ncells
void SuperValue::DeleteCellRange(int startpos, int endpos){
  int pos;
  assert(0 <= startpos && startpos < Ncells);
  assert(startpos <= endpos && endpos <= Ncells);
  for (pos = startpos; pos < endpos; ++pos){
    CellsSize -= Cells[pos].size();
    Cells[pos].Free();
  }
  memmove(Cells+startpos, Cells+endpos, (Ncells-endpos)*sizeof(ListCell));
  // zero out tail so desstructor won't try to delete pkey
  memset(Cells+(Ncells-(endpos-startpos)), 0,
         (endpos-startpos)*sizeof(ListCell)); 
  Ncells -= (endpos-startpos);
}

void SuperValue::Free(void){
  if (Cells){ 
    for (int i=0; i < Ncells; ++i) Cells[i].Free();
    delete [] Cells;
  }
  if (Attrs) delete [] Attrs;
  prki = 0;
  Ncells = 0;
  CellsSize = 0;
  Nattrs = 0;
  Cells = 0;
  Attrs = 0;
}


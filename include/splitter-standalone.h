//
// splitter-standalone.h
//
// Auxiliary functions for standalone splitter
//

/*
  Original code: Copyright (c) 2014 Microsoft Corporation
  Modified code: Copyright (c) 2015-2016 VMware, Inc
  All rights reserved. 

  Written by Marcos K. Aguilera

  MIT License

  Permission is hereby granted, free of charge, to any person
  obtaining a copy of this software and associated documentation files
  (the ""Software""), to deal in the Software without restriction,
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


#ifndef _SPLITTER_STANDALONE_H
#define _SPLITTER_STANDALONE_H

#include "record.h"
#include "dtreeaux.h"

// prototype definitions
int compareNpKeyWithKey(i64 nKey1, char *pKey1, i64 nKey2,
                        UnpackedRecord *pIdxKey2);
int CellSearchNodeUnpacked(DTreeNode &node, UnpackedRecord *pIdxKey, i64 nkey,
                           int biasRight, int *matches=0);
int GCellSearchNode(DTreeNode &node, i64 nkey, void *pkey, Ptr<RcKeyInfo> prki,
                    int biasRight);

#endif

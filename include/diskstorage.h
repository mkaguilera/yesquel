//
// diskstorage.h
//
// Disk storage of server objects
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

#ifndef _DISKSTORAGE_H
#define _DISKSTORAGE_H

#include "gaiatypes.h"
#include "pendingtx.h"


class DiskStorage {
private:
  int DiskStoragePathLen;
  char *DiskStoragePath;

  // returns the name of the file that holds an oid. The returned value is
  // a new allocated buffer that should be freed by the caller.
  char *getFilename(const COid& coid);

public:
  DiskStorage(char *diskstoragepath);
  static char *searchseparator(char *name);
  // creates directories in path if they do not exist
  static int Makepath(char *dirname); 
  char *getDiskStoragePath(){ return DiskStoragePath; }

  // converts a filename to a COid
  static COid FilenameToCOid(char *filename);

  // aux function to read a Coid from the current position in a file
  int readCOidFromFile(FILE *f, const COid &coid, Ptr<TxUpdateCoid> &tucoid);

  // aux function to write coid at current position of file f
  int writeCOidToFile(FILE *f, Ptr<TxUpdateCoid> tucoid);

  // Read an object id into memory. A length of -1 means reading as much as
  // available.
  // Set offset to 0 and length to -1 to read entire object.
  // Returns number of bytes read, or -1 if error (e.g., object does not exist)
  int readCOid(const COid& coid, int len, Ptr<TxUpdateCoid> &tucoid,
               Timestamp& version);

  // Write an object id to disk.
  int writeCOid(const COid& coid, Ptr<TxUpdateCoid> tucoid, Timestamp version);

  // returns size of a given oid
  int getCOidSize(const COid& coid);
};

#endif

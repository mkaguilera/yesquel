//
// gaiarpcaux.cpp
//
// Data structures and marshalling/demarshalling of RPCs
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
#include <sys/uio.h>

#include <map>
#include <list>

#include "tmalloc.h"
#include "debug.h"
#include "gaiarpcaux.h"
#include "pendingtx.h"

// ---------------------------------- WRITE RPC ----------------------------------

int WriteRPCData::marshall(iovec *bufs, int maxbufs){ 
  assert(maxbufs >= 1+niovs);
  bufs[0].iov_base = (char*) data;
  bufs[0].iov_len = sizeof(WriteRPCParm);
  memcpy(bufs+1, iov, sizeof(iovec) * niovs);
  return niovs+1;
}
    
void WriteRPCData::demarshall(char *buf){
  data = (WriteRPCParm*) buf;

  data->buf = buf + sizeof(WriteRPCParm);

  // the following is not used by server, but we might fill them anyways
  niovs=1;
  iov = new iovec;
  iov->iov_base = data->buf;
  iov->iov_len = data->len;
}

int WriteRPCRespData::marshall(iovec *bufs, int maxbufs){ 
  assert(maxbufs >= 1);
  bufs[0].iov_base = (char*) data;
  bufs[0].iov_len = sizeof(WriteRPCResp);
  return 1;
}
    
void WriteRPCRespData::demarshall(char *buf){
  data = (WriteRPCResp*) buf;
}

// ---------------------------------- READ RPC ----------------------------------

int ReadRPCData::marshall(iovec *bufs, int maxbufs){ 
  assert(maxbufs >= 1);
  bufs[0].iov_base = (char*) data;
  bufs[0].iov_len = sizeof(ReadRPCParm);
  return 1;
}
    
void ReadRPCData::demarshall(char *buf){
  data = (ReadRPCParm*) buf;
}

int ReadRPCRespData::marshall(iovec *bufs, int maxbufs){ 
  assert(maxbufs >= 2);
  bufs[0].iov_base = (char*) data;
  bufs[0].iov_len = sizeof(ReadRPCResp);
  bufs[1].iov_base = data->buf;
  bufs[1].iov_len = data->len;
  return 2;
}
    
void ReadRPCRespData::demarshall(char *buf){
  data = (ReadRPCResp*) buf;
  data->buf = buf + sizeof(ReadRPCResp); // Note: if changing this, change clientFreeReceiveBuffer below
}

// static
void ReadRPCRespData::clientFreeReceiveBuffer(char *data){
  // extract the incoming buffer from the data buffer and free it
  free(data-sizeof(ReadRPCResp));
}

char *ReadRPCRespData::clientAllocReceiveBuffer(int size){
  char *buf;
  buf = (char*) malloc(size + sizeof(ReadRPCResp));
  assert(buf);
  return buf + sizeof(ReadRPCResp);
}


// ---------------------------------- PREPARE RPC ----------------------------------

int PrepareRPCData::marshall(iovec *bufs, int maxbufs){ 
  assert(maxbufs >= 1);
  int nbufs=0;
  bufs[nbufs].iov_base = (char*) data;
  bufs[nbufs].iov_len = sizeof(PrepareRPCParm);
  ++nbufs;
  if (data->piggy_buf){
    bufs[nbufs].iov_base = (char*) data->piggy_buf;
    bufs[nbufs].iov_len = data->piggy_len;
    ++nbufs;
  }
  if (data->readset_len){
    bufs[nbufs].iov_base = (char*) data->readset;
    bufs[nbufs].iov_len = data->readset_len * sizeof(COid);
    ++nbufs;
  }
  return nbufs;
}

void PrepareRPCData::demarshall(char *buf){
  data = (PrepareRPCParm*) buf;
  data->piggy_buf = (char*)(buf + sizeof(PrepareRPCParm));
  data->readset = (COid*)(data->piggy_buf + data->piggy_len);
}

int PrepareRPCRespData::marshall(iovec *bufs, int maxbufs){ 
  assert(maxbufs >= 1);
  bufs[0].iov_base = (char*) data;
  bufs[0].iov_len = sizeof(PrepareRPCResp);
  return 1;
}
    
void PrepareRPCRespData::demarshall(char *buf){
  data = (PrepareRPCResp*) buf;
}

// ---------------------------------- COMMIT RPC ----------------------------------

int CommitRPCData::marshall(iovec *bufs, int maxbufs){ 
  assert(maxbufs >= 1);
  bufs[0].iov_base = (char*) data;
  bufs[0].iov_len = sizeof(CommitRPCParm);
  return 1;
}
    
void CommitRPCData::demarshall(char *buf){
  data = (CommitRPCParm*) buf;
}

int CommitRPCRespData::marshall(iovec *bufs, int maxbufs){ 
  assert(maxbufs >= 1);
  bufs[0].iov_base = (char*) data;
  bufs[0].iov_len = sizeof(CommitRPCResp);
  return 1;
}
    
void CommitRPCRespData::demarshall(char *buf){
  data = (CommitRPCResp*) buf;
}

// -----------------stuff for marshalling/demarshalling GKeyInfo objects ---------------

// converts a CollSeq into a byte
static u8 EncodeCollSeqAsByte(CollSeq *cs){
  if (strcmp(cs->zName, "BINARY")==0){
    switch(cs->enc){
    case SQLITE_UTF8: return 1;
    case SQLITE_UTF16BE: return 2;
    case SQLITE_UTF16LE: return 3;
    default: assert(0);
    }
  }
  if (strcmp(cs->zName, "RTRIM")==0){
    if (cs->enc == SQLITE_UTF8) return 4;
    else assert(0);
  }
  if (strcmp(cs->zName, "NOCASE")==0){
    if (cs->enc == SQLITE_UTF8) return 5;
    else assert(0);
  }
  assert(0);
  return 1;
}

// do the reverse conversion
static CollSeq *DecodeByteAsCollSeq(u8 b){
  static CollSeq CollSeqs[] = 
    {{(char*)"BINARY", SQLITE_UTF8,    SQLITE_COLL_BINARY, 0, binCollFunc, 0},
      {(char*)"BINARY", SQLITE_UTF16BE, SQLITE_COLL_BINARY, 0, binCollFunc, 0},
      {(char*)"BINARY", SQLITE_UTF16LE, SQLITE_COLL_BINARY, 0, binCollFunc, 0},
      {(char*)"RTRIM", SQLITE_UTF8, SQLITE_COLL_USER, (void*)1, binCollFunc, 0},
      {(char*)"NOCASE", SQLITE_UTF8, SQLITE_COLL_NOCASE, 0, nocaseCollatingFunc, 0}};
 
  assert(1 <= b && b <= 5);
  return &CollSeqs[b-1];
}

struct GKeyInfoSerialize {
  u8 enc;
  u8 nsortorder; // number of sort order values
  u8 ncoll;      // number of collating sequence values
  // must have nsortorder==0 or nsortorder == ncoll
};

// serialize a GKeyInfo object.
// Returns the number of iovecs used, and sets *retbuf to a newly allocated buffer (using malloc)
// that the caller should later free() after the serialized data is no
// longer needed.
// The GKeyInfo is serialized as follows:
//   [haskey]    whether there is a key. If 0, this indicates a null GKeyInfo object
//               and the serialization ends here.
//   [KeyInfoSerialize struct] 
//   [sortorder byte array with KeyInfoSerialize.nsortorder entries, possibly 0]
//   [coll byte array with KeyInfoSerialize.ncoll entries, possibly 0]
// The ncoll byte array has a byte per collating sequence value. The map is
// obtained from EncodeCollSeqAsByte().
int marshall_keyinfo(GKeyInfo *pki, iovec *bufs, int maxbufs, char **retbuf){
  GKeyInfoSerialize *kis;
  char *buf;
  char *ptr;
  int i;

  assert(maxbufs >= 1);
  if (!pki){ // null pki
    buf = (char*) malloc(sizeof(int)); assert(buf);
    *(int*)buf = 0;
    bufs[0].iov_base = buf;
    bufs[0].iov_len = sizeof(int);
    *retbuf = buf;
    return 1; // 1 iovec used
  }
  // nField*2 reserves space for aSortOrder and aColl. This is conservative,
  // since aSortOrder may be null (meaning it needs no space)
  ptr = buf = (char*) malloc(sizeof(int) + sizeof(GKeyInfoSerialize) + pki->nField*2);
  assert(ptr);
  *(int*)ptr = 1;
  ptr += sizeof(int);
  
  // marshall KeyInfoSerialize part
  kis = (GKeyInfoSerialize*) ptr;
  kis->enc = pki->enc;
  // if aSortOrder is non-null, nField indicates its size
  kis->nsortorder = pki->aSortOrder ? pki->nField : 0; 
  kis->ncoll = (u8)pki->nField;
  // append entries in aSortOrder (if it is non-null)
  ptr += sizeof(GKeyInfoSerialize);

  // marshall nsortorder
  if (kis->nsortorder){
    assert(kis->nsortorder <= pki->nField);
    memcpy(ptr, pki->aSortOrder, kis->nsortorder);
    ptr += kis->nsortorder;
  }

  // marshall collating sequences
  for (i=0; i < pki->nField; ++i)
    ptr[i] = EncodeCollSeqAsByte(pki->aColl[i]);
  ptr += pki->nField;
  // add entry to iovecs
  bufs[0].iov_base = buf;
  bufs[0].iov_len = (int)(ptr - buf);

  *retbuf = buf;
  return 1; // only 1 iovec used
}


static int Ioveclen(iovec *bufs, int nbufs){
  int len=0;
  for (int i=0; i < nbufs; ++i){ len += bufs[i].iov_len; }
  return len;
}

static void Iovecmemcpy(char *dest, iovec *bufs, int nbufs){
  for (int i=0; i < nbufs; ++i){
    memcpy((void*) dest, (void*) bufs[i].iov_base, bufs[i].iov_len);
    dest += bufs[i].iov_len;
  }
}

// marshalls a keyinfo into a single buffer, which is returned.
// Caller should later free buffer with free()
char *marshall_keyinfo_onebuf(GKeyInfo *pki, int &retlen){
  iovec bufs[10];
  int nbuf;
  int len;
  char *tmpbuf, *retval;
  nbuf = marshall_keyinfo(pki, bufs, sizeof(bufs)/sizeof(iovec), &tmpbuf);
  len = Ioveclen(bufs, nbuf);
  retval = (char*) malloc(len);
  Iovecmemcpy(retval, bufs, nbuf);
  free(tmpbuf);
  retlen = len;
  return retval;
}

// Demarshall a GKeyInfo object. *buf is a pointer to the serialized buffer.
// Returns a pointer to the demarshalled object, and modifies *buf to point
// after the demarshalled buffer. Caller is responsible for calling free()
// on the returned pointer.
GKeyInfo *demarshall_keyinfo(char **buf){
  char *ptr;
  GKeyInfo *pki;
  GKeyInfoSerialize *kis;
  int i;
  CollSeq **collseqs;
  int haskeyinfo;

  ptr = *buf;
  haskeyinfo = *(int*)ptr;
  ptr += sizeof(int);
  if (!haskeyinfo){ // null GKeyInfo
    *buf = ptr;
    return 0;
  }

  kis = (GKeyInfoSerialize*) ptr;

  // allocate space for CollSeq pointers too. This malloc reserves one more pointer than
  // needed since KeyInfo already has space for 1 CollSeq pointer. I didn't want to subtract 1
  // because otherwise we need to special case ncoll==0 (in which case we mustn't subtract 1)
  pki = (GKeyInfo*) malloc(sizeof(GKeyInfo) + kis->ncoll*sizeof(CollSeq*) + kis->nsortorder); assert(pki);

  // fill out KeyInfo fields stored in KeyInfoSerialize
  pki->db = 0;
  pki->enc = kis->enc;
  pki->nField = kis->ncoll;
  ptr += sizeof(GKeyInfoSerialize);

  // fill out aSortOrder if present
  if (kis->nsortorder){ 
    // copy sortorder array
    // where to copy: after GKeyInfo and its  array of CollSeq pointers
    char *copydest = (char*)pki + sizeof(GKeyInfo) + kis->ncoll*sizeof(CollSeq*);
    memcpy(copydest, ptr, kis->nsortorder);
    pki->aSortOrder = (u8*) copydest; // point to where we copied
    ptr += kis->nsortorder;
  }
  else pki->aSortOrder = 0;

  // fill out collseq pointers
  collseqs = &pki->aColl[0];
  for (i=0; i < kis->ncoll; ++i) collseqs[i] = DecodeByteAsCollSeq(ptr[i]);
  ptr += kis->ncoll;

  *buf = ptr;
  return pki;
}

// ---------------------------------- LISTADD RPC ----------------------------------

int ListAddRPCData::marshall(iovec *bufs, int maxbufs){
  int nbufs=0;

  bufs[0].iov_base = (char*) data;
  bufs[0].iov_len = sizeof(ListAddRPCParm);
  ++nbufs; // nbufs==1

  // serialize GKeyInfo
  if (serializeKeyinfoBuf) free(serializeKeyinfoBuf);
  nbufs += marshall_keyinfo(data->pKeyInfo, bufs+1, maxbufs-1, &serializeKeyinfoBuf);

  // serialize the data in pKey (if any)
  if (data->cell.pKey){
    bufs[nbufs].iov_base = data->cell.pKey;
    bufs[nbufs].iov_len = (unsigned) data->cell.nKey;
    ++nbufs;
  }
  return nbufs;
}

void ListAddRPCData::demarshall(char *buf){
  char *ptr;

  data = (ListAddRPCParm*) buf;
  ptr = buf + sizeof(ListAddRPCParm);

  // demarshall GKeyInfo
  data->pKeyInfo = demarshall_keyinfo(&ptr);
  freekeyinfo = data->pKeyInfo;

  // deserialize the data in pKey (if any)
  if (data->cell.pKey) // reading pointer from another machine; we only care if it is zero or not;
                  // zero indicates no pKey data (key is integer), non-zero indicates data
  {
    data->cell.pKey = ptr; // actual data is at the end
    ptr += data->cell.nKey;
  }
}

int ListAddRPCRespData::marshall(iovec *bufs, int maxbufs){
  assert(maxbufs >= 1);
  bufs[0].iov_base = (char*) data;
  bufs[0].iov_len = sizeof(ListAddRPCResp);
  return 1;
}

void ListAddRPCRespData::demarshall(char *buf){
  data = (ListAddRPCResp*) buf;
}


// ---------------------------------- LISTDELRANGE RPC ----------------------------------


int ListDelRangeRPCData::marshall(iovec *bufs, int maxbufs){
  int nbufs=0;

  bufs[0].iov_base = (char*) data;
  bufs[0].iov_len = sizeof(ListDelRangeRPCParm);
  ++nbufs; // nbufs==1

  // serialize GKeyInfo
  if (serializeKeyinfoBuf) free(serializeKeyinfoBuf);
  nbufs += marshall_keyinfo(data->pKeyInfo, bufs+1, maxbufs-1, &serializeKeyinfoBuf);

  // serialize the data in pKey1 (if any)
  if (data->cell1.pKey){
    bufs[nbufs].iov_base = data->cell1.pKey;
    bufs[nbufs].iov_len = (unsigned) data->cell1.nKey;
    ++nbufs;
  }
  if (data->cell2.pKey){
    bufs[nbufs].iov_base = data->cell2.pKey;
    bufs[nbufs].iov_len = (unsigned) data->cell2.nKey;
    ++nbufs;
  }
  return nbufs;
}

void ListDelRangeRPCData::demarshall(char *buf){
  char *ptr;

  data = (ListDelRangeRPCParm*) buf;
  ptr = buf + sizeof(ListDelRangeRPCParm);

  // demarshall GKeyInfo
  data->pKeyInfo = demarshall_keyinfo(&ptr);
  freekeyinfo = data->pKeyInfo;

  // deserialize the data in pKey1 (if any)
  if (data->cell1.pKey){
    data->cell1.pKey = ptr;
    ptr += data->cell1.nKey;
  }
  // deserialize the data in pKey2 (if any)
  if (data->cell2.pKey){
    data->cell2.pKey = ptr;
    ptr += data->cell2.nKey;
  }
}

int ListDelRangeRPCRespData::marshall(iovec *bufs, int maxbufs){
  assert(maxbufs >= 1);
  bufs[0].iov_base = (char*) data;
  bufs[0].iov_len = sizeof(ListDelRangeRPCResp);
  return 1;
}

void ListDelRangeRPCRespData::demarshall(char *buf){
  data = (ListDelRangeRPCResp*) buf;
}

// ---------------------------------- ATTRSET RPC ----------------------------------

int AttrSetRPCData::marshall(iovec *bufs, int maxbufs){
  assert(maxbufs >= 1);
  bufs[0].iov_base = (char*) data;
  bufs[0].iov_len = sizeof(AttrSetRPCParm);
  return 1;
}

void AttrSetRPCData::demarshall(char *buf){
  data = (AttrSetRPCParm*) buf;
}

int AttrSetRPCRespData::marshall(iovec *bufs, int maxbufs){
  assert(maxbufs >= 1);
  bufs[0].iov_base = (char*) data;
  bufs[0].iov_len = sizeof(AttrSetRPCResp);
  return 1;
}

void AttrSetRPCRespData::demarshall(char *buf){
  data = (AttrSetRPCResp*) buf;
}


// ---------------------------------- ATTRGET RPC ----------------------------------

int AttrGetRPCData::marshall(iovec *bufs, int maxbufs){
  assert(maxbufs >= 1);
  bufs[0].iov_base = (char*) data;
  bufs[0].iov_len = sizeof(AttrGetRPCParm);
  return 1;
}

void AttrGetRPCData::demarshall(char *buf){
  data = (AttrGetRPCParm*) buf;
}

int AttrGetRPCRespData::marshall(iovec *bufs, int maxbufs){
  assert(maxbufs >= 1);
  bufs[0].iov_base = (char*) data;
  bufs[0].iov_len = sizeof(AttrGetRPCResp);
  return 1;
}

void AttrGetRPCRespData::demarshall(char *buf){
  data = (AttrGetRPCResp*) buf;
}

// ---------------------------------- FULLREAD RPC ----------------------------------

int FullReadRPCData::marshall(iovec *bufs, int maxbufs){
  int nbufs=0;

  bufs[0].iov_base = (char*) data;
  bufs[0].iov_len = sizeof(FullReadRPCParm);
  ++nbufs; // nbufs==1

  // serialize GKeyInfo
  if (serializeKeyinfoBuf) free(serializeKeyinfoBuf);
  nbufs += marshall_keyinfo(data->pKeyInfo, bufs+1, maxbufs-1, &serializeKeyinfoBuf);
  assert(nbufs < maxbufs);

  // serialize the data in pKey (if any)
  if (data->cell.pKey){
    bufs[nbufs].iov_base = data->cell.pKey;
    bufs[nbufs].iov_len = (unsigned) data->cell.nKey;
    ++nbufs;
  }
  return nbufs;
  
}

void FullReadRPCData::demarshall(char *buf){
  char *ptr;
  data = (FullReadRPCParm*) buf;
  ptr = buf + sizeof(FullReadRPCParm);

  // demarshall GKeyInfo
  data->pKeyInfo = demarshall_keyinfo(&ptr);
  freekeyinfo = data->pKeyInfo;

  // deserialize the data in pKey (if any)
  if (data->cell.pKey) // reading pointer from another machine; we only care if it is zero or not;
                  // zero indicates no pKey data (key is integer), non-zero indicates data
  {
    data->cell.pKey = ptr; // actual data is at the end
    ptr += data->cell.nKey;
  }
}

FullReadRPCRespData::~FullReadRPCRespData(){
  if (deletecelloids) delete [] deletecelloids;
  if (freedatapki) free(freedatapki);
  if (freedata && data) delete data; 
  if (twsvi) delete twsvi;
  if (tmppkiserializebuf) free(tmppkiserializebuf);
}


int FullReadRPCRespData::marshall(iovec *bufs, int maxbufs){
  assert(maxbufs >= 3);
  int nbufs=0;
  char *tofree;
  bufs[nbufs].iov_base = (char*) data;
  bufs[nbufs++].iov_len = sizeof(FullReadRPCResp);
  bufs[nbufs].iov_base = (char*) data->attrs;
  bufs[nbufs++].iov_len = sizeof(u64) * data->nattrs;
  bufs[nbufs].iov_base = (char*) data->celloids;
  bufs[nbufs++].iov_len = data->lencelloids;
  nbufs += marshall_keyinfo(data->pki, bufs+nbufs, maxbufs-nbufs, &tofree);
  if (tmppkiserializebuf)
    free(tmppkiserializebuf);
  tmppkiserializebuf = tofree;
  return nbufs;
}

void FullReadRPCRespData::demarshall(char *buf){
  data = (FullReadRPCResp*) buf;
  buf += sizeof(FullReadRPCResp);
  data->attrs = (u64*) buf; // attrs follows buffer
  buf += data->nattrs * sizeof(u64);
  data->celloids = buf; // celloids follows attrs
  buf += data->lencelloids;
  data->pki = demarshall_keyinfo(&buf);
  freedatapki = data->pki;
}

// ---------------------------------- FULLWRITE RPC ----------------------------------

//FullWriteRPCParm *CloneFullWriteRPCParm(FullWriteRPCParm *orig){
//  FullWriteRPCParm *clone;
//  clone = (FullWriteRPCParm*) malloc(sizeof(FullWriteRPCParm) + orig->nattrs*sizeof(u64)
//    + orig->lencelloids);
//  memcpy(clone, orig, sizeof(FullWriteRPCParm));
//  clone->attrs = (u64*)((char*) clone + sizeof(FullWriteRPCParm));
//  clone->celloids = (char*) clone + sizeof(FullWriteRPCParm) + orig->nattrs*sizeof(u64);
//  memcpy(clone->attrs, orig->attrs, orig->nattrs*sizeof(u64));
//  memcpy(clone->celloids, orig->celloids, orig->lencelloids);
//  return clone;
//}

// converts serialized celloids into items put inside skiplist of cells
void CelloidsToListCells(char *celloids, int ncelloids, int celltype, SkipListBK<ListCellPlus,int> &cells,
                         GKeyInfo **pki){
  char *ptr = celloids;
  ListCellPlus *lc;
  for (int i=0; i < ncelloids; ++i){
    lc = new ListCellPlus(pki);
    // extract nkey
    u64 nkey;
    ptr += myGetVarint((unsigned char*) ptr, &nkey);
    lc->nKey = nkey;
    if (celltype == 0) lc->pKey = 0; // integer cell, set pKey=0
    else { // non-integer key, so extract pKey (nkey has its length)
      lc->pKey = new char[(unsigned) nkey];
      memcpy(lc->pKey, ptr, (size_t)nkey);
      ptr += nkey;
    }
    // extract childOid
    lc->value = *(Oid*)ptr;
    ptr += sizeof(u64); // space for 64-bit value in cell

    // add ListCell to cells
    cells.insert(lc,0);
  }
}

int CellSize(ListCellPlus *lc){
  int len = myVarintLen(lc->nKey);
  if (lc->pKey == 0) ; // integer key (no pkey)
  else len += (int) lc->nKey; // space for pkey
  len += sizeof(u64); // spave for 64-bit value in cell
  return len;
}

int ListCellsSize(SkipListBK<ListCellPlus,int> &cells){
  SkipListNodeBK<ListCellPlus,int> *ptr;
  int len = 0;
  // iterate to calculate length
  for (ptr = cells.getFirst(); ptr != cells.getLast(); ptr = cells.getNext(ptr)){
    len += CellSize(ptr->key);
  }
  return len;
}

// converts a skiplist of cells into a buffer with celloids.
// Returns:
// - a pointer to an allocated buffer (allocated with new),
// - the number of celloids in variable ncelloids
// - the length of the buffer in variable lencelloids
char *ListCellsToCelloids(SkipListBK<ListCellPlus,int> &cells, int &ncelloids, int &lencelloids){
  SkipListNodeBK<ListCellPlus,int> *ptr;
  int len;
  char *buf, *p;
  int ncells=0;
  
  // first find length of listcells to determine length of buffer to allocate
  len = ListCellsSize(cells);
  ncells = cells.getNitems();
  
  p = buf = new char[len];
  // now iterate to serialize
  for (ptr = cells.getFirst(); ptr != cells.getLast(); ptr = cells.getNext(ptr)){
    p += myPutVarint((unsigned char *)p, ptr->key->nKey);
    if (ptr->key->pKey == 0) ; // integer key
    else {
      memcpy(p, ptr->key->pKey, (int)ptr->key->nKey);
      p += ptr->key->nKey;
    }
    memcpy(p, &ptr->key->value, sizeof(u64));
    p += sizeof(u64);
  }
  assert(p-buf == len);
  lencelloids = len;
  ncelloids = ncells;
  return buf;
}


TxWriteSVItem *FullWriteRPCParmToTxWriteSVItem(FullWriteRPCParm *data){
  TxWriteSVItem *twsvi;
  twsvi = new TxWriteSVItem;
  twsvi->coid.cid = data->cid;
  twsvi->coid.oid = data->oid;
  twsvi->nattrs = GAIA_MAX_ATTRS;
  twsvi->celltype = data->celltype;
  //twsvi->ncelloids = data->ncelloids;
  twsvi->attrs = new u64[GAIA_MAX_ATTRS];
  assert(twsvi->nattrs <= GAIA_MAX_ATTRS);
  memcpy(twsvi->attrs, data->attrs, data->nattrs * sizeof(u64));
  // fill any missing attributes with 0
  memset(twsvi->attrs + data->nattrs, 0, (GAIA_MAX_ATTRS-data->nattrs) * sizeof(u64));
  twsvi->pki = CloneGKeyInfo(data->pKeyInfo); // clone since data->pKeyInfo will be destroyed
  CelloidsToListCells(data->celloids, data->ncelloids, data->celltype, twsvi->cells,
    &twsvi->pki);
  return twsvi;
}

int FullWriteRPCData::marshall(iovec *bufs, int maxbufs){
  assert(maxbufs >= 1);
  int nbufs=0;
  bufs[nbufs].iov_base = (char*) data;
  bufs[nbufs++].iov_len = sizeof(FullWriteRPCParm);
  bufs[nbufs].iov_base = (char*) data->attrs;
  bufs[nbufs++].iov_len = sizeof(u64) * data->nattrs;
  bufs[nbufs].iov_base = (char*) data->celloids;
  bufs[nbufs++].iov_len = data->lencelloids;
  if (serializeKeyinfoBuf) free(serializeKeyinfoBuf);
  nbufs += marshall_keyinfo(data->pKeyInfo, bufs+nbufs, maxbufs-nbufs, &serializeKeyinfoBuf);
  return nbufs;
}

void FullWriteRPCData::demarshall(char *buf){
  char *ptr = buf;
  data = (FullWriteRPCParm*) ptr;
  ptr += sizeof(FullWriteRPCParm);
  data->attrs = (u64*) ptr; // attrs follows buffer
  ptr += data->nattrs * sizeof(u64);
  data->celloids = ptr; // celloids follows attrs
  ptr += data->lencelloids;
  data->pKeyInfo = demarshall_keyinfo(&ptr);
  freekeyinfo = data->pKeyInfo;
}

int FullWriteRPCRespData::marshall(iovec *bufs, int maxbufs){
  assert(maxbufs >= 1);
  bufs[0].iov_base = (char*) data;
  bufs[0].iov_len = sizeof(FullWriteRPCResp);
  return 1;
}

void FullWriteRPCRespData::demarshall(char *buf){
  data = (FullWriteRPCResp*) buf;
}

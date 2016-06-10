//
// gaiarpcaux.h
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

#ifndef _GAIARPCAUX_H
#define _GAIARPCAUX_H

#include <sys/uio.h>

#include "tmalloc.h"
#include "options.h"
#include "ipmisc.h"
#include "gaiatypes.h"
#include "record.h"
#include "supervalue.h"
#include "pendingtx.h"
#include "datastruct.h"

class TxWriteItem;
class TxWriteSVItem;

// If changing these numbers, also change main.cpp and splitter-client.h
// (the latter for SS_GETWORID_RPCNO)
const int NULL_RPCNO = 0,
          GETSTATUS_RPCNO = 1,
          WRITE_RPCNO = 2,
          READ_RPCNO  = 3,
          FULLWRITE_RPCNO = 4,
          FULLREAD_RPCNO = 5,
          LISTADD_RPCNO = 6,
          LISTDELRANGE_RPCNO = 7,
          ATTRSET_RPCNO = 8,
          //ATTRGET_RPCNO = *!*,
          PREPARE_RPCNO = 9,
          COMMIT_RPCNO = 10,
          SUBTRANS_RPCNO = 11,
          SHUTDOWN_RPCNO = 12,
          STARTSPLITTER_RPCNO = 13,
          FLUSHFILE_RPCNO = 14,
          LOADFILE_RPCNO = 15;
          // RPC 16 is used by storageserver-splitter.h when STORAGESERVER_SPLITTER is defined (see also splitter-client.h)

// error codes
#define GAIAERR_GENERIC         -1 // generic error code
#define GAIAERR_TOO_OLD_VERSION -2 // trying to read data that is too old and
                                   // that is no longer in the log
#define GAIAERR_PENDING_DATA    -3 // trying to read pending data, whose
                                   // transaction is prepared but not committed
#define GAIAERR_CORRUPTED_LOG   -4 // in-memory log is corrupted
#define GAIAERR_DEFER_RPC       -5 // RPC has been deferred; this error should
                                   // not be returned to client
#define GAIAERR_INVALID_TID     -6 // tid is invalid
#define GAIAERR_CLEARED_TID     -7 // tid is cleared and about to be deleted
#define GAIAERR_TX_ENDED        -9 // trying to operate on a transaction that
                                   // has ended
#define GAIAERR_SERVER_TIMEOUT -10 // timeout trying to contact server
#define GAIAERR_NOT_IMPL       -11 // operation not implemented
#define GAIAERR_NO_MEMORY      -12 // insufficient memory
#define GAIAERR_CELL_OUTRANGE  -13 // cell does not belong to this coid
#define GAIAERR_ATTR_OUTRANGE  -14 // attribute id out of range
#define GAIAERR_WRONG_TYPE     -99 // trying to read value but got supervalue,
                                   // or vice-versa

// --------------------------------- NULL RPC ----------------------------------

struct NullRPCParm {
  int reserved;
};

class NullRPCData : public Marshallable {
public:
  NullRPCParm *data;
  int freedata;
  NullRPCData()  { freedata = 0; }
  ~NullRPCData(){ if (freedata){ delete data; } }
  int marshall(iovec *bufs, int maxbufs){
    assert(maxbufs >= 1);
    bufs[0].iov_base = (char*) data;
    bufs[0].iov_len = sizeof(NullRPCParm);
    return 1;
  }
  void demarshall(char *buf){
     data = (NullRPCParm*) buf;
  }
};

struct NullRPCResp {
  int reserved;
};

class NullRPCRespData : public Marshallable {
public:
  NullRPCResp *data;
  int freedata;
  NullRPCRespData(){ freedata = 0; }
  ~NullRPCRespData(){ if (freedata){ delete data; } }
  int marshall(iovec *bufs, int maxbufs){
    assert(maxbufs >= 1);
    bufs[0].iov_base = (char*) data;
    bufs[0].iov_len = sizeof(NullRPCResp);
    return 1;
  }
  void demarshall(char *buf){ data = (NullRPCResp*) buf; }
};

// ------------------------------ GETSTATUS RPC --------------------------------

struct GetStatusRPCParm {
  int reserved;
};

class GetStatusRPCData : public Marshallable {
public:
  GetStatusRPCParm *data;
  int freedata;
  GetStatusRPCData()  { freedata = 0; }
  ~GetStatusRPCData(){ if (freedata){ delete data; } }
  int marshall(iovec *bufs, int maxbufs){
    assert(maxbufs >= 1);
    bufs[0].iov_base = (char*) data;
    bufs[0].iov_len = sizeof(GetStatusRPCParm);
    return 1;
  }
  void demarshall(char *buf){
     data = (GetStatusRPCParm*) buf;
  }
};

struct GetStatusRPCResp {
  int reserved;
};

class GetStatusRPCRespData : public Marshallable {
public:
  GetStatusRPCResp *data;
  int freedata;
  GetStatusRPCRespData(){ freedata = 0; }
  ~GetStatusRPCRespData(){ if (freedata){ delete data; } }
  int marshall(iovec *bufs, int maxbufs){
    assert(maxbufs >= 1);
    bufs[0].iov_base = (char*) data;
    bufs[0].iov_len = sizeof(GetStatusRPCResp);
    return 1;
  }
  void demarshall(char *buf){ data = (GetStatusRPCResp*) buf; }
};


// --------------------------------- WRITE RPC ---------------------------------

struct WriteRPCParm {
  Tid tid;   // transaction id
  Cid cid;   // container id
  Oid oid;   // object id
  int level; // subtransaction level
  int len;   // buffer length
  char *buf; // buffer
};

class WriteRPCData : public Marshallable {
public:
  WriteRPCParm *data;
  int niovs;         // intended to be used by client only not server
  iovec *iov;   // ditto
  int freedata;
  char *freedatabuf;
  WriteRPCData()  { freedata = 0; freedatabuf = 0; iov = 0; }
  ~WriteRPCData(){ 
    if (iov) delete iov;
    if (freedatabuf) delete freedatabuf;
    if (freedata) delete data;
  }
  int marshall(iovec *bufs, int maxbufs);
  void demarshall(char *buf);
};

struct WriteRPCResp {
  int status;                  // operation status
  u64 versionNoForCache;       // version number for cache
  Timestamp tsForCache;        // timestamp for cache
  Timestamp reserveTsForCache; // reserve timestamp for cache
};

class WriteRPCRespData : public Marshallable {
public:
  WriteRPCResp *data;
  int freedata;
  WriteRPCRespData(){ freedata = 0; }
  ~WriteRPCRespData(){ if (freedata){ delete data; } }
  int marshall(iovec *bufs, int maxbufs); 
  void demarshall(char *buf);
};

// --------------------------------- READ RPC ----------------------------------

struct ReadRPCParm {
  Tid tid;       // transaction id
  Timestamp ts;  // timestamp
  Cid cid;       // container id
  Oid oid;       // object id
  int len;       // length
};

class ReadRPCData : public Marshallable {
public:
  ReadRPCParm *data;
  int freedata;
  ReadRPCData()  { freedata = 0; }
  ~ReadRPCData(){ if (freedata){ delete data; } }
  int marshall(iovec *bufs, int maxbufs);
  void demarshall(char *buf);
};

struct ReadRPCResp {
  int status;                    // operation status
  Timestamp readts;              // timestamp of data
  int len;                       // length
  char *buf;                     // buffer
  u64 versionNoForCache;         // version number for cache
  Timestamp tsForCache;          // timestamp for cache
  Timestamp reserveTsForCache;   // reserve timestamp for cache
};

class ReadRPCRespData : public Marshallable {
public:
  ReadRPCResp *data;
  int freedata;
  char *freedatabuf;
  Ptr<TxUpdateCoid> tucoid; // this is here to decrement the refcount of
       // tucoid when this object is deleted. This is used at the server only,
       // which creates a tucoid in LogInMemory::readCOid holding
       // the data of the object being read

  // Constructor sets freedata=0 by default.
  //
  // At the client, the transport layer allocates a buffer
  // for the entire packet, and ReadRPCResp->data and ReadRPCResp->data.buf are
  // just pointers inside that buffer that should not be freed via delete.
  // Rather, the buffer should be freed as described below under
  // clientFreeReceiveBuffer.
  //
  // At the server, the remote procedure will allocate
  // data and data->buf, and will set freedata to true, so that it is freed
  // below after the RPC layer sends back the response.
  ReadRPCRespData(){ freedata = 0; freedatabuf = 0;}
  ~ReadRPCRespData(){ if (freedatabuf) free(freedatabuf); 
                      if (freedata) delete data; }
  int marshall(iovec *bufs, int maxbufs); 
  void demarshall(char *buf);
  static void clientFreeReceiveBuffer(char *data);
     // A client having data->buf can call this function
     // to free the buffer containing the data. This
     // should be used only by client not server, because
     // at client the data buffer itself is never allocated, as
     // it is just a pointer into the buffer
  static char *clientAllocReceiveBuffer(int len);
     // Allocates a buffer that can be freed by
     // clientFreeReceiveBuffer. Normally such a buffer
     // comes from the RPC layer, but when reading client cached
     // data we need to produce such a buffer so that the client
     // can free it in the same way
};


// ------------------------------ PREPARE RPC ----------------------------------

struct PrepareRPCParm {
  Tid tid;                // transaction id
  Timestamp startts;      // start timestamp
  int onephasecommit;     // whether to commit as well as prepare
                          // (used when transaction spans just one server)

  // stuff for piggyback write optimization (if GAIA_WRITE_ON_PREPARE enabled)
  Cid piggy_cid;          // piggyback container id
  Oid piggy_oid;          // piggyback oid
  int piggy_len;          // piggyback buffer length
  char *piggy_buf;        // piggyback buffer
  
  int  readset_len;       // size of readset array below. Used in GAIA_OCC only
  COid *readset;          // used in GAIA_OCC only

};

class PrepareRPCData : public Marshallable {
public:
  PrepareRPCParm *data;
  int deletedata;
  int deletereadset;
  char *freedatabuf;
  PrepareRPCData()  { deletedata = 0; deletereadset = 0; freedatabuf = 0; }
  ~PrepareRPCData(){ 
    if (deletereadset) delete [] data->readset;
    if (deletedata){ delete data; }
    if (freedatabuf) delete freedatabuf;
  }
  int marshall(iovec *bufs, int maxbufs);
  void demarshall(char *buf);
};

struct PrepareRPCResp {
  int status;                  // status of operation
  int vote;                    // 0=commit, 1=abort
  Timestamp mincommitts;       // if vote==0, the min possible commit timestamp 
                               //   (commit timestamp must be strictly
                               //   greater than this value)
  u64 versionNoForCache;       // version number for cache
  Timestamp tsForCache;        // timestamp for cache
  Timestamp reserveTsForCache; // reserve timestamp for cache
};

class PrepareRPCRespData : public Marshallable {
public:
  PrepareRPCResp *data;
  int freedata;
  PrepareRPCRespData(){ freedata = 0; }
  ~PrepareRPCRespData(){ if (freedata){ delete data; } }
  int marshall(iovec *bufs, int maxbufs); 
  void demarshall(char *buf);
};

// ------------------------------- COMMIT RPC ----------------------------------

struct CommitRPCParm {
  Tid tid;               // transaction id
  Timestamp committs;    // commit timestamp
  int commit;            // 0=commit, 1=abort, 2=abort without having prepared
};

class CommitRPCData : public Marshallable {
public:
  CommitRPCParm *data;
  int freedata;
  CommitRPCData()  { freedata = 0; }
  ~CommitRPCData(){ if (freedata){ delete data; } }
  int marshall(iovec *bufs, int maxbufs);
  void demarshall(char *buf);
};

struct CommitRPCResp {
  int status;           // should always be zero
  Timestamp waitingts;  // largest timestamp of a waiting read on some
                        //   item of the transaction
};

class CommitRPCRespData : public Marshallable {
public:
  CommitRPCResp *data;
  int freedata;
  CommitRPCRespData(){ freedata = 0; }
  ~CommitRPCRespData(){ if (freedata){ delete data; } }
  int marshall(iovec *bufs, int maxbufs); 
  void demarshall(char *buf);
};

// ----------------------------- SUBTRANS RPC ----------------------------------

struct SubtransRPCParm {
  Tid tid;               // transaction id
  int level;             // level to apply action
  int action;            // 0=discard updates with >= level,
                         // 1=change updates with >= level to level-1
};

class SubtransRPCData : public Marshallable {
public:
  SubtransRPCParm *data;
  int freedata;
  SubtransRPCData()  { freedata = 0; }
  ~SubtransRPCData(){ if (freedata){ delete data; } }
  int marshall(iovec *bufs, int maxbufs);
  void demarshall(char *buf);
};

struct SubtransRPCResp {
  int status;           // status of operation
};

class SubtransRPCRespData : public Marshallable {
public:
  SubtransRPCResp *data;
  int freedata;
  SubtransRPCRespData(){ freedata = 0; }
  ~SubtransRPCRespData(){ if (freedata){ delete data; } }
  int marshall(iovec *bufs, int maxbufs); 
  void demarshall(char *buf);
};

// ---------------------------- SHUTDOWN RPC ----------------------------------
struct ShutdownRPCParm {
  int reserved;  // reserved for future use
  int level;     // 0=stop splitter, 1=stop entire server
};

class ShutdownRPCData : public Marshallable {
public:
  ShutdownRPCParm *data;
  int freedata;
  ShutdownRPCData()  { freedata = 0; }
  ~ShutdownRPCData(){ if (freedata){ delete data; } }
  int marshall(iovec *bufs, int maxbufs){
    assert(maxbufs >= 1);
    bufs[0].iov_base = (char*) data;
    bufs[0].iov_len = sizeof(ShutdownRPCParm);
    return 1;
  }
  void demarshall(char *buf){
     data = (ShutdownRPCParm*) buf;
  }
};

struct ShutdownRPCResp {
  int status;    // status of operation
  int reserved;  // reserved for future use
};

class ShutdownRPCRespData : public Marshallable {
public:
  ShutdownRPCResp *data;
  int freedata;
  ShutdownRPCRespData(){ freedata = 0; }
  ~ShutdownRPCRespData(){ if (freedata){ delete data; } }
  int marshall(iovec *bufs, int maxbufs){
    assert(maxbufs >= 1);
    bufs[0].iov_base = (char*) data;
    bufs[0].iov_len = sizeof(ShutdownRPCResp);
    return 1;
  }
  void demarshall(char *buf){ data = (ShutdownRPCResp*) buf; }
};

// ----------------------- STARTSPLITTERRPC RPC -------------------------------
struct StartSplitterRPCParm {
  int reserved;   // reserved for future use
};

class StartSplitterRPCData : public Marshallable {
public:
  StartSplitterRPCParm *data;
  int freedata;
  StartSplitterRPCData()  { freedata = 0; }
  ~StartSplitterRPCData(){ if (freedata){ delete data; } }
  int marshall(iovec *bufs, int maxbufs){
    assert(maxbufs >= 1);
    bufs[0].iov_base = (char*) data;
    bufs[0].iov_len = sizeof(StartSplitterRPCParm);
    return 1;
  }
  void demarshall(char *buf){
     data = (StartSplitterRPCParm*) buf;
  }
};

struct StartSplitterRPCResp {
  int reserved;   // reserved for future use
};

class StartSplitterRPCRespData : public Marshallable {
public:
  StartSplitterRPCResp *data;
  int freedata;
  StartSplitterRPCRespData(){ freedata = 0; }
  ~StartSplitterRPCRespData(){ if (freedata){ delete data; } }
  int marshall(iovec *bufs, int maxbufs){
    assert(maxbufs >= 1);
    bufs[0].iov_base = (char*) data;
    bufs[0].iov_len = sizeof(StartSplitterRPCResp);
    return 1;
  }
  void demarshall(char *buf){ data = (StartSplitterRPCResp*) buf; }
};

// -------------------------- FLUSHFILERPC RPC -------------------------------
struct FlushFileRPCParm {
  int filenamelen;   // length of filename
  char *filename;    // name of file
};

class FlushFileRPCData : public Marshallable {
public:
  FlushFileRPCParm *data;
  int freedata;
  char *freefilenamebuf;
  FlushFileRPCData()  { freedata = 0; freefilenamebuf = 0; }
  ~FlushFileRPCData(){
    if (freedata) delete data;
    if (freefilenamebuf) delete [] freefilenamebuf;
  }
  int marshall(iovec *bufs, int maxbufs){
    assert(maxbufs >= 2);
    bufs[0].iov_base = (char*) data;
    bufs[0].iov_len = sizeof(FlushFileRPCParm);
    bufs[1].iov_base = data->filename;
    bufs[1].iov_len = data->filenamelen;
    return 2;
  }
  void demarshall(char *buf){
    data = (FlushFileRPCParm*) buf;
    data->filename = buf + sizeof(FlushFileRPCParm);
  }
};

struct FlushFileRPCResp {
  int status;       // status of operation
  int reserved;     // reserved for future use
};

class FlushFileRPCRespData : public Marshallable {
public:
  FlushFileRPCResp *data;
  int freedata;
  FlushFileRPCRespData(){ freedata = 0; }
  ~FlushFileRPCRespData(){ if (freedata){ delete data; } }
  int marshall(iovec *bufs, int maxbufs){
    assert(maxbufs >= 1);
    bufs[0].iov_base = (char*) data;
    bufs[0].iov_len = sizeof(FlushFileRPCResp);
    return 1;
  }
  void demarshall(char *buf){ data = (FlushFileRPCResp*) buf; }
};

// --------------------------- LOADFILERPC RPC -------------------------------
struct LoadFileRPCParm {
  int filenamelen;   // length of filename
  char *filename;    // name of file
};

class LoadFileRPCData : public Marshallable {
public:
  LoadFileRPCParm *data;
  int freedata;
  char *freefilenamebuf;
  LoadFileRPCData()  { freedata = 0; freefilenamebuf = 0; }
  ~LoadFileRPCData(){
    if (freedata) delete data;
    if (freefilenamebuf) delete [] freefilenamebuf;
  }
  int marshall(iovec *bufs, int maxbufs){
    assert(maxbufs >= 2);
    bufs[0].iov_base = (char*) data;
    bufs[0].iov_len = sizeof(LoadFileRPCParm);
    bufs[1].iov_base = data->filename;
    bufs[1].iov_len = data->filenamelen;
    return 2;
  }
  void demarshall(char *buf){
    data = (LoadFileRPCParm*) buf;
    data->filename = buf + sizeof(LoadFileRPCParm);
  }
};

struct LoadFileRPCResp {
  int status;    // status of operation
  int reserved;  // reserved for future use
};

class LoadFileRPCRespData : public Marshallable {
public:
  LoadFileRPCResp *data;
  int freedata;
  LoadFileRPCRespData(){ freedata = 0; }
  ~LoadFileRPCRespData(){ if (freedata){ delete data; } }
  int marshall(iovec *bufs, int maxbufs){
    assert(maxbufs >= 1);
    bufs[0].iov_base = (char*) data;
    bufs[0].iov_len = sizeof(LoadFileRPCResp);
    return 1;
  }
  void demarshall(char *buf){ data = (LoadFileRPCResp*) buf; }
};


// ------------------------------- LISTADD RPC ---------------------------------
// RPC to add an item to a list of a Value

struct ListAddRPCParm {
  Tid tid;             // transaction id
  Cid cid;             // container id
  Oid oid;             // object id
  int level;           // subtransaction level
  u32 flags;           // if flags&1, check cell being added before adding
                       // if flags&2, bypass throttle
  Timestamp ts;        // start timestamp of transaction (used for reading,
                       //   when check >= 1)
  ListCell cell;       // cell to add
  Ptr<RcKeyInfo> prki; // information about the record format
  ~ListAddRPCParm(){ cell.Free(); }
};

class ListAddRPCData : public Marshallable {
private:
  char *serializeKeyinfoBuf;  // intended to be used by client only.
                             // this is a buffer allocated to serialize RcKeyInfo

public:
  ListAddRPCParm *data;
  int freedata;  // caller should set if data should be deleted in destructor
  ListAddRPCData()  { serializeKeyinfoBuf = 0; freedata = 0; }
  ~ListAddRPCData(){ 
    if (serializeKeyinfoBuf) free(serializeKeyinfoBuf);
    if (freedata) delete data;
  }
  int marshall(iovec *bufs, int maxbufs);
  void demarshall(char *buf);
};

struct ListAddRPCResp {
  int status;            // status of operation
#if DTREE_SPLIT_LOCATION == 1  // client splitter needs this information when
                               // items are added
  int ncells;           // approx # of cells in node (used for client splitter)
  int size;             // approx size of node
#endif  
  u64 versionNoForCache;       // version number for cache
  Timestamp tsForCache;        // timestamp for cache
  Timestamp reserveTsForCache; // reserve timestamp for cache
};

class ListAddRPCRespData : public Marshallable {
public:
  ListAddRPCResp *data;
  int freedata;
  ListAddRPCRespData(){ freedata = 0; }
  ~ListAddRPCRespData(){ if (freedata){ delete data; } }
  int marshall(iovec *bufs, int maxbufs); 
  void demarshall(char *buf);
};


// ---------------------------- LISTDELRANGE RPC ------------------------------
// RPC to add delete a range [A,B] of items from a list of a Value

struct ListDelRangeRPCParm {
  Tid tid;            // transaction id
  Cid cid;            // container id
  Oid oid;            // object id
  int level;          // subtransaction level
  Ptr<RcKeyInfo> prki;// information about the record format
  u8 intervalType;    // 0 = (key1,key2), 1 = (key1,key2],
                      // 2=[key1,key2), 3=[key1,key2]
  ListCell cell1;     // starting key in range
  ListCell cell2;     // ending key in range
  ~ListDelRangeRPCParm(){ cell1.Free(); cell2.Free(); }
};

class ListDelRangeRPCData : public Marshallable {
private:
  char *serializeKeyinfoBuf;  // intended to be used by client only.
                             // this is a buffer allocated to serialize RcKeyInfo

public:
  ListDelRangeRPCParm *data;
  int freedata;  // caller should set if data should be deleted in destructor
  ListDelRangeRPCData()  { serializeKeyinfoBuf = 0;
                           freedata = 0; }
  ~ListDelRangeRPCData(){ 
    if (serializeKeyinfoBuf) free(serializeKeyinfoBuf);
    if (freedata) delete data;
  }
  int marshall(iovec *bufs, int maxbufs);
  void demarshall(char *buf);
};

struct ListDelRangeRPCResp {
  int status;                   // status of operation
  u64 versionNoForCache;        // version number for cache
  Timestamp tsForCache;         // timestamp for cache
  Timestamp reserveTsForCache;  // reserve timestamp for cache
};

class ListDelRangeRPCRespData : public Marshallable {
public:
  ListDelRangeRPCResp *data;
  int freedata;
  ListDelRangeRPCRespData(){ freedata = 0; }
  ~ListDelRangeRPCRespData(){ if (freedata){ delete data; } }
  int marshall(iovec *bufs, int maxbufs); 
  void demarshall(char *buf);
};

// ------------------------------- ATTRSET RPC -------------------------------
// RPC to set the value of an attribute of a Value

struct AttrSetRPCParm {
  Tid tid;        // transaction id
  Cid cid;        // container id
  Oid oid;        // object id
  int level;      // subtransaction level
  u32 attrid;     // attribute id
  u64 attrvalue;  // attribute value
};

class AttrSetRPCData : public Marshallable {
public:
  AttrSetRPCParm *data;
  int freedata;  // caller should set if data should be deleted in destructor
  AttrSetRPCData()  {  freedata = 0; }
  ~AttrSetRPCData(){ 
    if (freedata) delete data;
  }
  int marshall(iovec *bufs, int maxbufs);
  void demarshall(char *buf);
};

struct AttrSetRPCResp {
  int status;   // status of operation
};

class AttrSetRPCRespData : public Marshallable {
public:
  AttrSetRPCResp *data;
  int freedata;
  AttrSetRPCRespData(){ freedata = 0; }
  ~AttrSetRPCRespData(){ if (freedata){ delete data; } }
  int marshall(iovec *bufs, int maxbufs); 
  void demarshall(char *buf);
};


// ------------------------------ ATTRGET RPC ----------------------------------
// RPC to get the value of an attribute of a Value

struct AttrGetRPCParm {
  Tid tid;        // transaction id
  Timestamp ts;   // timestamp
  Cid cid;        // container id
  Oid oid;        // object id
  u32 attrid;     // attribute id
};

class AttrGetRPCData : public Marshallable {
public:
  AttrGetRPCParm *data;
  int freedata;  // caller should set if data should be deleted in destructor
  AttrGetRPCData()  {  freedata = 0; }
  ~AttrGetRPCData(){ 
    if (freedata) delete data;
  }
  int marshall(iovec *bufs, int maxbufs);
  void demarshall(char *buf);
};

struct AttrGetRPCResp {
  int status;     // status of operation
  u64 attrvalue;  // value of attribute
};

class AttrGetRPCRespData : public Marshallable {
public:
  AttrGetRPCResp *data;
  int freedata;
  AttrGetRPCRespData(){ freedata = 0; }
  ~AttrGetRPCRespData(){ if (freedata){ delete data; } }
  int marshall(iovec *bufs, int maxbufs); 
  void demarshall(char *buf);
};

// ----------------------------- FULLREAD RPC ----------------------------------
// RPC to return the Value, including all lists and attributes

struct FullReadRPCParm {
  Tid tid;            // transaction id
  Timestamp ts;       // timestamp
  Cid cid;            // container id
  Oid oid;            // object id
  int cellPresent;    // whether cell information is present
  ListCell cell;      // if cellPresent: desired cell. This is used only to
                      // keep stats of which cell caused the read, to be used
                      // for load splits
  Ptr<RcKeyInfo> prki;// cell type
  ~FullReadRPCParm(){ cell.Free(); }
};

class FullReadRPCData : public Marshallable {
private:
  char *serializeKeyinfoBuf;  // intended to be used by client only.
                             // this is a buffer allocated to serialize RcKeyInfo
  
public:
  FullReadRPCParm *data;
  int freedata;  // caller should set if data should be deleted in destructor
  FullReadRPCData(){ serializeKeyinfoBuf = 0; freedata = 0; }
  ~FullReadRPCData(){ 
    if (serializeKeyinfoBuf) free(serializeKeyinfoBuf);
    if (freedata) delete data;
  }
  int marshall(iovec *bufs, int maxbufs);
  void demarshall(char *buf);
};

struct FullReadRPCResp {
  int status;                  // operation status, -99 if not supervalue
  int dummy;                   // dummy parameter
  Timestamp readts;            // timestamp of value
  u16 nattrs;                  // number of 64-bit attribute values
  u8  celltype;                // type of cells: 0=int, 1=nKey+pKey
  u32 ncelloids;               // number of (cell,oid) pairs in list
  u32 lencelloids;             // length in bytes of (cell,oid) pairs
  u64 *attrs;                  // value of attributes
  char *celloids;              // list with celloids
  Ptr<RcKeyInfo> prki;         // keyinfo if available
  u64 versionNoForCache;       // version number for cache
  Timestamp tsForCache;        // timestamp for cache
  Timestamp reserveTsForCache; // reserve timestamp for cache
};

class FullReadRPCRespData : public Marshallable {
public:
  FullReadRPCResp *data;
  TxWriteSVItem *twsvi; // used by server only. Set to TxWriteSVItem to delete
                        // (if any) after sending response
  char *tmpprkiserializebuf;   // used by server only. Set to temporary prki
                            // serialize buffer to delete after sending response
  int freedata;
  char *deletecelloids; // used by server only. If true, delete data->celloids
                        // after sending response
  Ptr<TxUpdateCoid> tucoid; // this is here to decrement the refcount of tucoid
         // when this object is deleted. This is used at the server only,
         // which creates a tucoid in LogInMemory::readCOid holding
         // the data of the object being read
  FullReadRPCRespData(){
    freedata = 0; twsvi = 0; deletecelloids = 0;
    tmpprkiserializebuf = 0;
  }
  ~FullReadRPCRespData();
  int marshall(iovec *bufs, int maxbufs); 
  void demarshall(char *buf);
};

// ------------------------------ FULLWRITE RPC -------------------------------
// RPC to return the Value, including all lists and attributes

struct FullWriteRPCParm {
  Tid tid;            // transaction id
  Cid cid;            // container id
  Oid oid;            // object id
  int level;          // subtransaction level
  u16 nattrs;         // number of 64-bit attribute values
  u8  celltype;       // type of cells: 0=int, 1=nKey+pKey
  u32 ncelloids;      // number of (cell,oid) pairs in list
  u32 lencelloids;    // length in bytes of (cell,oid) pairs
  u64 *attrs;         // value of attributes
  char *celloids;     // list with celloids
  Ptr<RcKeyInfo> prki;// key info; can be null if there are no cells or if
                      // celltype==0. Otherwise should not be null
};

// converts a FullWriteRPCParm to a newly allocated TxWriteSVItem
TxWriteSVItem *fullWriteRPCParmToTxWriteSVItem(FullWriteRPCParm *data);


// clones a FullWriteRPCParm. Returns a pointer that should be freed with free()
//FullWriteRPCParm *CloneFullWriteRPCParm(FullWriteRPCParm *orig);

class FullWriteRPCData : public Marshallable {
private:
  char *serializeKeyinfoBuf;  // intended to be used by client only.
                              // this is a buffer allocated at the client's
                              // marshall() to serialize RcKeyInfo
public:
  FullWriteRPCParm *data;
  int freedata;  // if set, delete data in destructor; set by client
  char *deletecelloids; // if non-null, free it in destructor; set by client
  FullWriteRPCData() { serializeKeyinfoBuf = 0; freedata = 0;
                       deletecelloids = 0; }
  ~FullWriteRPCData(){ 
    if (serializeKeyinfoBuf) free(serializeKeyinfoBuf);
    if (deletecelloids) delete [] deletecelloids;
    if (freedata) delete data;
  }
  int marshall(iovec *bufs, int maxbufs);
  void demarshall(char *buf);
};

struct FullWriteRPCResp {
  int status;                   // status of operation
  u64 versionNoForCache;        // version number for cache
  Timestamp tsForCache;         // timestamp for cache
  Timestamp reserveTsForCache;  // reserve timestamp for cache
};

class FullWriteRPCRespData : public Marshallable {
public:
  FullWriteRPCResp *data;
  int freedata;
  FullWriteRPCRespData(){ freedata = 0; }
  ~FullWriteRPCRespData(){ if (freedata){ delete data; } }
  int marshall(iovec *bufs, int maxbufs); 
  void demarshall(char *buf);
};

#endif

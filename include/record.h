//
// record.h
//
// Definitions of SQLite's keyinfo, which keeps track of the
// type of a data buffer. This file repeats and adapts some of the definitions
// in SQLite to be used in the storage server, without having to
// include all of SQLite in the storage server.
//

#ifndef _RECORD_H
#define _RECORD_H

#include "inttypes.h"

#if !defined(SQLITE_CORE) && !defined(_PARTIAL_RECORD_H)
/*
** A "Collating Sequence" is defined by an instance of the following
** structure. Conceptually, a collating sequence consists of a name and
** a comparison routine that defines the order of that sequence.
**
** There may two separate implementations of the collation function, one
** that processes text in UTF-8 encoding (CollSeq.xCmp) and another that
** processes text encoded in UTF-16 (CollSeq.xCmp16), using the machine
** native byte order. When a collation sequence is invoked, SQLite selects
** the version that will require the least expensive encoding
** translations, if any.
**
** The CollSeq.pUser member variable is an extra parameter that passed in
** as the first argument to the UTF-8 comparison function, xCmp.
** CollSeq.pUser16 is the equivalent for the UTF-16 comparison function,
** xCmp16.
**
** If both CollSeq.xCmp and CollSeq.xCmp16 are NULL, it means that the
** collating sequence is undefined.  Indices built on an undefined
** collating sequence may not be read or written.
*/
struct CollSeq {
  char *zName;          /* Name of the collating sequence, UTF-8 encoded */
  u8 enc;               /* Text encoding handled by xCmp() */
  u8 type;              /* One of the SQLITE_COLL_... values below */
  void *pUser;          /* First argument to xCmp() */
  int (*xCmp)(void*,int, const void*, int, const void*);
  void (*xDel)(void*);  /* Destructor for pUser */
};

/*
** Allowed values of CollSeq.type:
*/
#define SQLITE_COLL_BINARY  1  /* The default memcmp() collating sequence */
#define SQLITE_COLL_NOCASE  2  /* The built-in NOCASE collating sequence */
#define SQLITE_COLL_REVERSE 3  /* The built-in REVERSE collating sequence */
#define SQLITE_COLL_USER    0  /* Any other user-defined collating sequence */
#endif

struct CollSeq;
/*
** An instance of the following structure is passed as the first
** argument to sqlite3VdbeKeyCompare and is used to control the 
** comparison of the two index keys.
*/
struct RcKeyInfo {  // modified from KeyInfo to change sqlite3* to void*
  int refcount;       // to use smart pointers
  void *db;           /* The database connection */
  u8 enc;             /* Text encoding - one of the SQLITE_UTF* values */
  u16 nField;         /* Number of entries in aColl[] */
  u8 *aSortOrder;     /* Sort order for each column.  May be NULL */
  CollSeq *aColl[1];  /* Collating sequence for each term of the key */
  RcKeyInfo(){ refcount = 0; }

  static void *operator new(std::size_t sz, int ncoll, int nsort);
  static void operator delete(void *ptr, std::size_t sz);
  void printShort();
};

/*********************** Prototypes ******************/
struct UnpackedRecord;

int myVarintLen(u64 v);
int myGetVarint(const unsigned char *p, u64 *v);
int myPutVarint(unsigned char *p, u64 v);
// functions to expose
int myVdbeRecordCompare(int nKey1, const void *pKey1, UnpackedRecord *pPKey2);
UnpackedRecord *myVdbeRecordUnpack(RcKeyInfo *pKeyInfo, int nKey,
                                   const void *pKey, char *pSpace, 
                   int szSpace);
void myVdbeDeleteUnpackedRecord(UnpackedRecord *p);

// Clone a RcKeyInfo, returning a newly allocated pointer.
// The pointer should be freed with free()
RcKeyInfo *CloneKeyInfo(RcKeyInfo *ki);

// Returns a pKey from a pIdxKey. The value returned is a dynamically
// allocated buffer that should be freed by the called with free().
char *myVdbeRecordPack(UnpackedRecord *pIdxKey, int file_format, int &length);

// test whether myVdbeRecordPack is working. Returns true if ok, false
// otherwise
int testRecordPack(UnpackedRecord *pIdxKey, int file_format);

#ifndef SQLITE_CORE

int binCollFunc(void *padFlag, int nKey1, const void *pKey1,
                int nKey2, const void *pKey2);
int nocaseCollatingFunc(void *NotUsed, int nKey1, const void *pKey1,
                        int nKey2, const void *pKey2);

#ifndef _PARTIAL_RECORD_H

/************************ Compile-time options **********************/

#define SQLITE_ASCII 1

/*********************** Visual Studio specific defines ******************/

#if defined __linux__
#define malloc_size(ptr) malloc_usable_size(ptr) // returns size of an
                                                 // allocated chunk
#elif defined _WIN32
#define malloc_size(ptr) _msize(ptr) // returns the size of an allocated chunk
#else
#error Must define certain compiler-specific functions (see source file)
#endif



/************************* MISC defines ******************************/
#define SQLITE_API

#define testcase(X)

#define SQLITE_MAX_U32  ((((u64)1)<<32)-1)
#define ROUND8(x)     (((x)+7)&~7)
#define ROUNDDOWN8(x) ((x)&~7)

#if defined(__PTRDIFF_TYPE__)  /* This case should work for GCC */
# define SQLITE_INT_TO_PTR(X)  ((void*)(__PTRDIFF_TYPE__)(X))
# define SQLITE_PTR_TO_INT(X)  ((int)(__PTRDIFF_TYPE__)(X))
#elif !defined(__GNUC__)       /* Works for compilers other than LLVM */
# define SQLITE_INT_TO_PTR(X)  ((void*)&((char*)0)[X])
# define SQLITE_PTR_TO_INT(X)  ((int)(((char*)X)-(char*)0))
#elif defined(HAVE_STDINT_H)   /* Use this case if we have ANSI headers */
# define SQLITE_INT_TO_PTR(X)  ((void*)(intptr_t)(X))
# define SQLITE_PTR_TO_INT(X)  ((int)(intptr_t)(X))
#else                          /* Generates a warning - but it always works */
# define SQLITE_INT_TO_PTR(X)  ((void*)(X))
# define SQLITE_PTR_TO_INT(X)  ((int)(X))
#endif


#ifdef SQLITE_4_BYTE_ALIGNED_MALLOC
# define EIGHT_BYTE_ALIGNMENT(X)   ((((char*)(X) - (char*)0)&3)==0)
#else
# define EIGHT_BYTE_ALIGNMENT(X)   ((((char*)(X) - (char*)0)&7)==0)
#endif

#if defined(SQLITE_COVERAGE_TEST)
# define ALWAYS(X)      (1)
# define NEVER(X)       (0)
#elif !defined(NDEBUG)
# define ALWAYS(X)      ((X)?1:(assert(0),0))
# define NEVER(X)       ((X)?(assert(0),1):0)
#else
# define ALWAYS(X)      (X)
# define NEVER(X)       (X)
#endif

static int sqlite3PutVarint(unsigned char *p, u64 v);
static int sqlite3PutVarint32(unsigned char *p, u32 v);
static u8 sqlite3GetVarint(const unsigned char *p, u64 *v);
static u8 sqlite3GetVarint32(const unsigned char *p, u32 *v);
static int sqlite3VarintLen(u64 v);


#define getVarint32(A,B)  (u8)((*(A)<(u8)0x80) ? ((B) = (u32)*(A)),1 : sqlite3GetVarint32((A), (u32 *)&(B)))
#define putVarint32(A,B)  (u8)(((u32)(B)<(u32)0x80) ? (*(A) = (unsigned char)(B)),1 : sqlite3PutVarint32((A), (B)))
#define getVarint    sqlite3GetVarint
#define putVarint    sqlite3PutVarint
#define varintLen    sqlite3VarintLen



/*************************** Aliases ******************************/
#define sqlite3StrNICmp sqlite3_strnicmp
#define UpperToLower sqlite3UpperToLower
#define sqlite3_snprintf(count, buf, format, ...) snprintf(buf, count, format, ##__VA_ARGS__)


/******************************* RETURN CODES *********************************/

#define SQLITE_OK           0   /* Successful result */
#define SQLITE_NOMEM        7   /* A malloc() failed */


#ifndef NDEBUG
# define DEBUG_ONLY(X)  X
#else
# define DEBUG_ONLY(X)
#endif


/*
** Internally, the vdbe manipulates nearly all SQL values as Mem
** structures. Each Mem struct may cache multiple representations (string,
** integer etc.) of the same value.
*/
struct Mem {
  void *db;        /* The associated database connection */
  char *z;            /* String or BLOB value */
  double r;           /* Real value */
  union {
    i64 i;              /* Integer value used when MEM_Int is set in flags */
    int nZero;          /* Used when bit MEM_Zero is set in flags */
    //FuncDef *pDef;      /* Used only when flags==MEM_Agg */
    //RowSet *pRowSet;    /* Used only when flags==MEM_RowSet */
    //VdbeFrame *pFrame;  /* Used when flags==MEM_Frame */
  } u;
  int n;              /* Number of characters in string value, excluding '\0' */
  u16 flags;          /* Some combination of MEM_Null, MEM_Str, MEM_Dyn, etc. */
  u8  type;           /* One of SQLITE_NULL, SQLITE_TEXT, SQLITE_INTEGER, etc */
  u8  enc;            /* SQLITE_UTF8, SQLITE_UTF16BE, SQLITE_UTF16LE */
#ifdef SQLITE_DEBUG
  Mem *pScopyFrom;    /* This Mem is a shallow copy of pScopyFrom */
  void *pFiller;      /* So that sizeof(Mem) is a multiple of 8 */
#endif
  void (*xDel)(void *);  /* If not null, call this function to delete Mem.z */
  char *zMalloc;      /* Dynamic buffer allocated by sqlite3_malloc() */
};

/* One or more of the following flags are set to indicate the validOK
** representations of the value stored in the Mem struct.
**
** If the MEM_Null flag is set, then the value is an SQL NULL value.
** No other flags may be set in this case.
**
** If the MEM_Str flag is set then Mem.z points at a string representation.
** Usually this is encoded in the same unicode encoding as the main
** database (see below for exceptions). If the MEM_Term flag is also
** set, then the string is nul terminated. The MEM_Int and MEM_Real 
** flags may coexist with the MEM_Str flag.
*/
#define MEM_Null      0x0001   /* Value is NULL */
#define MEM_Str       0x0002   /* Value is a string */
#define MEM_Int       0x0004   /* Value is an integer */
#define MEM_Real      0x0008   /* Value is a real number */
#define MEM_Blob      0x0010   /* Value is a BLOB */
#define MEM_RowSet    0x0020   /* Value is a RowSet object */
#define MEM_Frame     0x0040   /* Value is a VdbeFrame object */
#define MEM_Invalid   0x0080   /* Value is undefined */
#define MEM_TypeMask  0x00ff   /* Mask of type bits */

/* Whenever Mem contains a valid string or blob representation, one of
** the following flags must be set to determine the memory management
** policy for Mem.z.  The MEM_Term flag tells us whether or not the
** string is \000 or \u0000 terminated
*/
#define MEM_Term      0x0200   /* String rep is nul terminated */
#define MEM_Dyn       0x0400   /* Need to call sqliteFree() on Mem.z */
#define MEM_Static    0x0800   /* Mem.z points to a static string */
#define MEM_Ephem     0x1000   /* Mem.z points to an ephemeral string */
#define MEM_Agg       0x2000   /* Mem.z points to an agg function context */
#define MEM_Zero      0x4000   /* Mem.i contains count of 0s appended to blob */
#ifdef SQLITE_OMIT_INCRBLOB
  #undef MEM_Zero
  #define MEM_Zero 0x0000
#endif

/*
** An instance of the following structure holds information about a
** single index record that has already been parsed out into individual
** values.
**
** A record is an object that contains one or more fields of data.
** Records are used to store the content of a table row and to store
** the key of an index.  A blob encoding of a record is created by
** the OP_MakeRecord opcode of the VDBE and is disassembled by the
** OP_Column opcode.
**
** This structure holds a record that has already been disassembled
** into its constituent fields.
*/
struct UnpackedRecord {
  RcKeyInfo *pKeyInfo;  /* Collation and sort-order information */
  u16 nField;         /* Number of entries in apMem[] */
  u16 flags;          /* Boolean settings.  UNPACKED_... below */
  i64 rowid;          /* Used by UNPACKED_PREFIX_SEARCH */
  Mem *aMem;          /* Values */
};

/*
** Allowed values of UnpackedRecord.flags
*/
#define UNPACKED_NEED_FREE     0x0001  /* Memory is from sqlite3Malloc() */
#define UNPACKED_NEED_DESTROY  0x0002  /* apMem[]s should all be destroyed */
#define UNPACKED_IGNORE_ROWID  0x0004  /* Ignore trailing rowid on key1 */
#define UNPACKED_INCRKEY       0x0008  /* Make this key an epsilon larger */
#define UNPACKED_PREFIX_MATCH  0x0010  /* A prefix match is considered OK */
#define UNPACKED_PREFIX_SEARCH 0x0020  /* A prefix match is considered OK */


/*
** Clear any existing type flags from a Mem and replace them with f
*/
#define MemSetTypeFlag(p, f) \
   ((p)->flags = ((p)->flags&~(MEM_TypeMask|MEM_Zero))|f)

/*
** Return true if a memory cell is not marked as invalid.  This macro
** is for use inside assert() statements only.
*/
#ifdef SQLITE_DEBUG
#define memIsValid(M)  ((M)->flags & MEM_Invalid)==0
#endif

#define SQLITE_UTF8           1
#define SQLITE_UTF16LE        2
#define SQLITE_UTF16BE        3
#define SQLITE_UTF16          4    /* Use native byte order */
#define SQLITE_ANY            5    /* sqlite3_create_function only */
#define SQLITE_UTF16_ALIGNED  8    /* sqlite3_create_collation only */

#endif
#endif
#endif

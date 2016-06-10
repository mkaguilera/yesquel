//
// record.cpp
//
// Definitions of SQLite's keyinfo, which keeps track of the
// type of a data buffer. This file repeats and adapts some of the definitions
// in SQLite to be used in the storage server, without having to
// include all of SQLite in the storage server.
//

#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <malloc.h>
#include <map>
#include <list>

#include "tmalloc.h"
#include "debug.h"
#define _RECORD_C
#include "record.h"


// Prototype definitions
static int sqlite3VdbeMemGrow(Mem *pMem, int n, int preserve);
static int sqlite3VdbeMemExpandBlob(Mem *pMem);


/***********************************************************************
 *                                                                     *
 *                       String functions                              *
 *                                                                     *
 ***********************************************************************/

/* An array to map all upper-case characters into their corresponding
** lower-case character. 
**
** SQLite only considers US-ASCII (or EBCDIC) characters.  We do not
** handle case conversions for the UTF character set since the tables
** involved are nearly as big or bigger than SQLite itself.
*/
static const unsigned char sqlite3UpperToLower[] = {
#ifdef SQLITE_ASCII
      0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15, 16, 17,
     18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35,
     36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53,
     54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 97, 98, 99,100,101,102,103,
    104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,
    122, 91, 92, 93, 94, 95, 96, 97, 98, 99,100,101,102,103,104,105,106,107,
    108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,
    126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,
    144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,
    162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,
    180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,
    198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,
    216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,
    234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,
    252,253,254,255
#endif
#ifdef SQLITE_EBCDIC
      0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15, /* 0x */
     16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, /* 1x */
     32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, /* 2x */
     48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, /* 3x */
     64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, /* 4x */
     80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, /* 5x */
     96, 97, 66, 67, 68, 69, 70, 71, 72, 73,106,107,108,109,110,111, /* 6x */
    112, 81, 82, 83, 84, 85, 86, 87, 88, 89,122,123,124,125,126,127, /* 7x */
    128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143, /* 8x */
    144,145,146,147,148,149,150,151,152,153,154,155,156,157,156,159, /* 9x */
    160,161,162,163,164,165,166,167,168,169,170,171,140,141,142,175, /* Ax */
    176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191, /* Bx */
    192,129,130,131,132,133,134,135,136,137,202,203,204,205,206,207, /* Cx */
    208,145,146,147,148,149,150,151,152,153,218,219,220,221,222,223, /* Dx */
    224,225,162,163,164,165,166,167,168,169,232,203,204,205,206,207, /* Ex */
    239,240,241,242,243,244,245,246,247,248,249,219,220,221,222,255, /* Fx */
#endif
};

static int sqlite3StrICmp(const char *zLeft, const char *zRight){
  register unsigned char *a, *b;
  a = (unsigned char *)zLeft;
  b = (unsigned char *)zRight;
  while (*a!=0 && UpperToLower[*a]==UpperToLower[*b]){ a++; b++; }
  return UpperToLower[*a] - UpperToLower[*b];
}
SQLITE_API int sqlite3_strnicmp(const char *zLeft, const char *zRight, int N){
  register unsigned char *a, *b;
  a = (unsigned char *)zLeft;
  b = (unsigned char *)zRight;
  while (N-- > 0 && *a!=0 && UpperToLower[*a]==UpperToLower[*b]){ a++; b++; }
  return N<0 ? 0 : UpperToLower[*a] - UpperToLower[*b];
}

/***********************************************************************
 *                                                                     *
 *                       Varint functions                              *
 *                                                                     *
 ***********************************************************************/

/*
** Write a 64-bit variable-length integer to memory starting at p[0].
** The length of data write will be between 1 and 9 bytes.  The number
** of bytes written is returned.
**
** A variable-length integer consists of the lower 7 bits of each byte
** for all bytes that have the 8th bit set and one byte with the 8th
** bit clear.  Except, if we get to the 9th byte, it stores the full
** 8 bits and is the last byte.
*/
static int sqlite3PutVarint(unsigned char *p, u64 v){
  int i, j, n;
  u8 buf[10];
  if (v & (((u64)0xff000000)<<32)){
    p[8] = (u8)v;
    v >>= 8;
    for(i=7; i>=0; i--){
      p[i] = (u8)((v & 0x7f) | 0x80);
      v >>= 7;
    }
    return 9;
  }    
  n = 0;
  do{
    buf[n++] = (u8)((v & 0x7f) | 0x80);
    v >>= 7;
  }while (v!=0);
  buf[0] &= 0x7f;
  assert(n<=9);
  for(i=0, j=n-1; j>=0; j--, i++){
    p[i] = buf[j];
  }
  return n;
}
int myPutVarint(unsigned char *p, u64 v){ return sqlite3PutVarint(p, v); }

/*
** This routine is a faster version of sqlite3PutVarint() that only
** works for 32-bit positive integers and which is optimized for
** the common case of small integers.  A MACRO version, putVarint32,
** is provided which inlines the single-byte case.  All code should use
** the MACRO version as this function assumes the single-byte case has
** already been handled.
*/
static int sqlite3PutVarint32(unsigned char *p, u32 v){
#ifndef putVarint32
  if ((v & ~0x7f)==0){
    p[0] = v;
    return 1;
  }
#endif
  if ((v & ~0x3fff)==0){
    p[0] = (u8)((v>>7) | 0x80);
    p[1] = (u8)(v & 0x7f);
    return 2;
  }
  return sqlite3PutVarint(p, v);
}

/*
** Bitmasks used by sqlite3GetVarint().  These precomputed constants
** are defined here rather than simply putting the constant expressions
** inline in order to work around bugs in the RVT compiler.
**
** SLOT_2_0     A mask for  (0x7f<<14) | 0x7f
**
** SLOT_4_2_0   A mask for  (0x7f<<28) | SLOT_2_0
*/
#define SLOT_2_0     0x001fc07f
#define SLOT_4_2_0   0xf01fc07f


/*
** Read a 64-bit variable-length integer from memory starting at p[0].
** Return the number of bytes read.  The value is stored in *v.
*/
static u8 sqlite3GetVarint(const unsigned char *p, u64 *v){
  u32 a,b,s;

  a = *p;
  /* a: p0 (unmasked) */
  if (!(a&0x80))
  {
    *v = a;
    return 1;
  }

  p++;
  b = *p;
  /* b: p1 (unmasked) */
  if (!(b&0x80))
  {
    a &= 0x7f;
    a = a<<7;
    a |= b;
    *v = a;
    return 2;
  }

  /* Verify that constants are precomputed correctly */
  assert(SLOT_2_0 == ((0x7f<<14) | (0x7f)));
  assert(SLOT_4_2_0 == ((0xfU<<28) | (0x7f<<14) | (0x7f)));

  p++;
  a = a<<14;
  a |= *p;
  /* a: p0<<14 | p2 (unmasked) */
  if (!(a&0x80))
  {
    a &= SLOT_2_0;
    b &= 0x7f;
    b = b<<7;
    a |= b;
    *v = a;
    return 3;
  }

  /* CSE1 from below */
  a &= SLOT_2_0;
  p++;
  b = b<<14;
  b |= *p;
  /* b: p1<<14 | p3 (unmasked) */
  if (!(b&0x80))
  {
    b &= SLOT_2_0;
    /* moved CSE1 up */
    /* a &= (0x7f<<14)|(0x7f); */
    a = a<<7;
    a |= b;
    *v = a;
    return 4;
  }

  /* a: p0<<14 | p2 (masked) */
  /* b: p1<<14 | p3 (unmasked) */
  /* 1:save off p0<<21 | p1<<14 | p2<<7 | p3 (masked) */
  /* moved CSE1 up */
  /* a &= (0x7f<<14)|(0x7f); */
  b &= SLOT_2_0;
  s = a;
  /* s: p0<<14 | p2 (masked) */

  p++;
  a = a<<14;
  a |= *p;
  /* a: p0<<28 | p2<<14 | p4 (unmasked) */
  if (!(a&0x80))
  {
    /* we can skip these cause they were (effectively) done above in calc'ing s */
    /* a &= (0x7f<<28)|(0x7f<<14)|(0x7f); */
    /* b &= (0x7f<<14)|(0x7f); */
    b = b<<7;
    a |= b;
    s = s>>18;
    *v = ((u64)s)<<32 | a;
    return 5;
  }

  /* 2:save off p0<<21 | p1<<14 | p2<<7 | p3 (masked) */
  s = s<<7;
  s |= b;
  /* s: p0<<21 | p1<<14 | p2<<7 | p3 (masked) */

  p++;
  b = b<<14;
  b |= *p;
  /* b: p1<<28 | p3<<14 | p5 (unmasked) */
  if (!(b&0x80))
  {
    /* we can skip this cause it was (effectively) done above in calc'ing s */
    /* b &= (0x7f<<28)|(0x7f<<14)|(0x7f); */
    a &= SLOT_2_0;
    a = a<<7;
    a |= b;
    s = s>>18;
    *v = ((u64)s)<<32 | a;
    return 6;
  }

  p++;
  a = a<<14;
  a |= *p;
  /* a: p2<<28 | p4<<14 | p6 (unmasked) */
  if (!(a&0x80))
  {
    a &= SLOT_4_2_0;
    b &= SLOT_2_0;
    b = b<<7;
    a |= b;
    s = s>>11;
    *v = ((u64)s)<<32 | a;
    return 7;
  }

  /* CSE2 from below */
  a &= SLOT_2_0;
  p++;
  b = b<<14;
  b |= *p;
  /* b: p3<<28 | p5<<14 | p7 (unmasked) */
  if (!(b&0x80))
  {
    b &= SLOT_4_2_0;
    /* moved CSE2 up */
    /* a &= (0x7f<<14)|(0x7f); */
    a = a<<7;
    a |= b;
    s = s>>4;
    *v = ((u64)s)<<32 | a;
    return 8;
  }

  p++;
  a = a<<15;
  a |= *p;
  /* a: p4<<29 | p6<<15 | p8 (unmasked) */

  /* moved CSE2 up */
  /* a &= (0x7f<<29)|(0x7f<<15)|(0xff); */
  b &= SLOT_2_0;
  b = b<<8;
  a |= b;

  s = s<<4;
  b = p[-4];
  b &= 0x7f;
  b = b>>3;
  s |= b;

  *v = ((u64)s)<<32 | a;

  return 9;
}

/*
** Read a 32-bit variable-length integer from memory starting at p[0].
** Return the number of bytes read.  The value is stored in *v.
**
** If the varint stored in p[0] is larger than can fit in a 32-bit unsigned
** integer, then set *v to 0xffffffff.
**
** A MACRO version, getVarint32, is provided which inlines the 
** single-byte case.  All code should use the MACRO version as 
** this function assumes the single-byte case has already been handled.
*/
static u8 sqlite3GetVarint32(const unsigned char *p, u32 *v){
  u32 a,b;

  /* The 1-byte case.  Overwhelmingly the most common.  Handled inline
  ** by the getVarin32() macro */
  a = *p;
  /* a: p0 (unmasked) */
#ifndef getVarint32
  if (!(a&0x80))
  {
    /* Values between 0 and 127 */
    *v = a;
    return 1;
  }
#endif

  /* The 2-byte case */
  p++;
  b = *p;
  /* b: p1 (unmasked) */
  if (!(b&0x80))
  {
    /* Values between 128 and 16383 */
    a &= 0x7f;
    a = a<<7;
    *v = a | b;
    return 2;
  }

  /* The 3-byte case */
  p++;
  a = a<<14;
  a |= *p;
  /* a: p0<<14 | p2 (unmasked) */
  if (!(a&0x80))
  {
    /* Values between 16384 and 2097151 */
    a &= (0x7f<<14)|(0x7f);
    b &= 0x7f;
    b = b<<7;
    *v = a | b;
    return 3;
  }

  /* A 32-bit varint is used to store size information in btrees.
  ** Objects are rarely larger than 2MiB limit of a 3-byte varint.
  ** A 3-byte varint is sufficient, for example, to record the size
  ** of a 1048569-byte BLOB or string.
  **
  ** We only unroll the first 1-, 2-, and 3- byte cases.  The very
  ** rare larger cases can be handled by the slower 64-bit varint
  ** routine.
  */
  {
    u64 v64;
    u8 n;

    p -= 2;
    n = sqlite3GetVarint(p, &v64);
    assert(n>3 && n<=9);
    if ((v64 & SQLITE_MAX_U32)!=v64){
      *v = 0xffffffff;
    }else{
      *v = (u32)v64;
    }
    return n;
  }
}

/*
** Return the number of bytes that will be needed to store the given
** 64-bit integer.
*/
static int sqlite3VarintLen(u64 v){
  int i = 0;
  do{
    i++;
    v >>= 7;
  }while (v!=0);
  if (i==10) --i;
  ALWAYS(i < 9 || (i==9 && v<2));
  return i;
}

int myVarintLen(u64 v){ return sqlite3VarintLen(v); }
int myGetVarint(const unsigned char *p, u64 *v){ return sqlite3GetVarint(p,v); }

/*
** Read or write a four-byte big-endian integer value.
*/
static u32 sqlite3Get4byte(const u8 *p){
  return (p[0]<<24) | (p[1]<<16) | (p[2]<<8) | p[3];
}
static void sqlite3Put4byte(unsigned char *p, u32 v){
  p[0] = (u8)(v>>24);
  p[1] = (u8)(v>>16);
  p[2] = (u8)(v>>8);
  p[3] = (u8)v;
}


/***********************************************************************
 *                                                                     *
 *                  UTF translation functions                          *
 *                                                                     *
 ***********************************************************************/

/*
** This lookup table is used to help decode the first byte of
** a multi-byte UTF8 character.
*/
static const unsigned char sqlite3Utf8Trans1[] = {
  0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
  0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
  0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
  0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
  0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
  0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
  0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
  0x00, 0x01, 0x02, 0x03, 0x00, 0x01, 0x00, 0x00,
};


#define WRITE_UTF8(zOut, c) {                          \
  if (c<0x00080){                                      \
    *zOut++ = (u8)(c&0xFF);                            \
  }                                                    \
  else if (c<0x00800){                                 \
    *zOut++ = 0xC0 + (u8)((c>>6)&0x1F);                \
    *zOut++ = 0x80 + (u8)(c & 0x3F);                   \
  }                                                    \
  else if (c<0x10000){                                 \
    *zOut++ = 0xE0 + (u8)((c>>12)&0x0F);               \
    *zOut++ = 0x80 + (u8)((c>>6) & 0x3F);              \
    *zOut++ = 0x80 + (u8)(c & 0x3F);                   \
  }else{                                               \
    *zOut++ = 0xF0 + (u8)((c>>18) & 0x07);             \
    *zOut++ = 0x80 + (u8)((c>>12) & 0x3F);             \
    *zOut++ = 0x80 + (u8)((c>>6) & 0x3F);              \
    *zOut++ = 0x80 + (u8)(c & 0x3F);                   \
  }                                                    \
}

#define WRITE_UTF16LE(zOut, c) {                                    \
  if (c<=0xFFFF){                                                   \
    *zOut++ = (u8)(c&0x00FF);                                       \
    *zOut++ = (u8)((c>>8)&0x00FF);                                  \
  }else{                                                            \
    *zOut++ = (u8)(((c>>10)&0x003F) + (((c-0x10000)>>10)&0x00C0));  \
    *zOut++ = (u8)(0x00D8 + (((c-0x10000)>>18)&0x03));              \
    *zOut++ = (u8)(c&0x00FF);                                       \
    *zOut++ = (u8)(0x00DC + ((c>>8)&0x03));                         \
  }                                                                 \
}

#define WRITE_UTF16BE(zOut, c) {                                    \
  if (c<=0xFFFF){                                                   \
    *zOut++ = (u8)((c>>8)&0x00FF);                                  \
    *zOut++ = (u8)(c&0x00FF);                                       \
  }else{                                                            \
    *zOut++ = (u8)(0x00D8 + (((c-0x10000)>>18)&0x03));              \
    *zOut++ = (u8)(((c>>10)&0x003F) + (((c-0x10000)>>10)&0x00C0));  \
    *zOut++ = (u8)(0x00DC + ((c>>8)&0x03));                         \
    *zOut++ = (u8)(c&0x00FF);                                       \
  }                                                                 \
}

#define READ_UTF16LE(zIn, TERM, c){                                   \
  c = (*zIn++);                                                       \
  c += ((*zIn++)<<8);                                                 \
  if (c>=0xD800 && c<0xE000 && TERM){                                 \
    int c2 = (*zIn++);                                                \
    c2 += ((*zIn++)<<8);                                              \
    c = (c2&0x03FF) + ((c&0x003F)<<10) + (((c&0x03C0)+0x0040)<<10);   \
  }                                                                   \
}

#define READ_UTF16BE(zIn, TERM, c){                                   \
  c = ((*zIn++)<<8);                                                  \
  c += (*zIn++);                                                      \
  if (c>=0xD800 && c<0xE000 && TERM){                                 \
    int c2 = ((*zIn++)<<8);                                           \
    c2 += (*zIn++);                                                   \
    c = (c2&0x03FF) + ((c&0x003F)<<10) + (((c&0x03C0)+0x0040)<<10);   \
  }                                                                   \
}

/*
** Translate a single UTF-8 character.  Return the unicode value.
**
** During translation, assume that the byte that zTerm points
** is a 0x00.
**
** Write a pointer to the next unread byte back into *pzNext.
**
** Notes On Invalid UTF-8:
**
**  *  This routine never allows a 7-bit character (0x00 through 0x7f) to
**     be encoded as a multi-byte character.  Any multi-byte character that
**     attempts to encode a value between 0x00 and 0x7f is rendered as 0xfffd.
**
**  *  This routine never allows a UTF16 surrogate value to be encoded.
**     If a multi-byte character attempts to encode a value between
**     0xd800 and 0xe000 then it is rendered as 0xfffd.
**
**  *  Bytes in the range of 0x80 through 0xbf which occur as the first
**     byte of a character are interpreted as single-byte characters
**     and rendered as themselves even though they are technically
**     invalid characters.
**
**  *  This routine accepts an infinite number of different UTF8 encodings
**     for unicode values 0x80 and greater.  It do not change over-length
**     encodings to 0xfffd as some systems recommend.
*/
#define READ_UTF8(zIn, zTerm, c)                           \
  c = *(zIn++);                                            \
  if (c>=0xc0){                                            \
    c = sqlite3Utf8Trans1[c-0xc0];                         \
    while(zIn!=zTerm && (*zIn & 0xc0)==0x80){              \
      c = (c<<6) + (0x3f & *(zIn++));                      \
    }                                                      \
    if (c<0x80                                             \
        || (c&0xFFFFF800)==0xD800                          \
        || (c&0xFFFFFFFE)==0xFFFE){ c = 0xFFFD; }          \
  }
static int sqlite3Utf8Read(
  const unsigned char *zIn,       /* First byte of UTF-8 character */
  const unsigned char **pzNext    /* Write first byte past UTF-8 char here */
){
  unsigned int c;

  /* Same as READ_UTF8() above but without the zTerm parameter.
  ** For this routine, we assume the UTF8 string is always zero-terminated.
  */
  c = *(zIn++);
  if (c>=0xc0){
    c = sqlite3Utf8Trans1[c-0xc0];
    while((*zIn & 0xc0)==0x80){
      c = (c<<6) + (0x3f & *(zIn++));
    }
    if (c<0x80
        || (c&0xFFFFF800)==0xD800
        || (c&0xFFFFFFFE)==0xFFFE){  c = 0xFFFD; }
  }
  *pzNext = zIn;
  return c;
}

#define expandBlob(P) (((P)->flags&MEM_Zero)?sqlite3VdbeMemExpandBlob(P):0)

/*
** Make the given Mem object MEM_Dyn.  In other words, make it so
** that any TEXT or BLOB content is stored in memory obtained from
** malloc().  In this way, we know that the memory is safe to be
** overwritten or altered.
**
** Return SQLITE_OK on success or SQLITE_NOMEM if malloc fails.
*/
static int sqlite3VdbeMemMakeWriteable(Mem *pMem){
  int f;
  //assert(pMem->db==0 || sqlite3_mutex_held(pMem->db->mutex));
  assert((pMem->flags&MEM_RowSet)==0);
  expandBlob(pMem);
  f = pMem->flags;
  if ((f&(MEM_Str|MEM_Blob)) && pMem->z!=pMem->zMalloc){
    if (sqlite3VdbeMemGrow(pMem, pMem->n + 2, 1)){
      return SQLITE_NOMEM;
    }
    pMem->z[pMem->n] = 0;
    pMem->z[pMem->n+1] = 0;
    pMem->flags |= MEM_Term;
#ifdef SQLITE_DEBUG
    pMem->pScopyFrom = 0;
#endif
  }

  return SQLITE_OK;
}



#ifndef SQLITE_OMIT_UTF16
/*
** This routine transforms the internal text encoding used by pMem to
** desiredEnc. It is an error if the string is already of the desired
** encoding, or if *pMem does not contain a string value.
*/
static int sqlite3VdbeMemTranslate(Mem *pMem, u8 desiredEnc){
  int len;                    /* Maximum length of output string in bytes */
  unsigned char *zOut;                  /* Output buffer */
  unsigned char *zIn;                   /* Input iterator */
  unsigned char *zTerm;                 /* End of input */
  unsigned char *z;                     /* Output iterator */
  unsigned int c;

  //assert(pMem->db==0 || sqlite3_mutex_held(pMem->db->mutex));
  assert(pMem->flags&MEM_Str);
  assert(pMem->enc!=desiredEnc);
  assert(pMem->enc!=0);
  assert(pMem->n>=0);

  /* If the translation is between UTF-16 little and big endian, then 
  ** all that is required is to swap the byte order. This case is handled
  ** differently from the others.
  */
  if (pMem->enc!=SQLITE_UTF8 && desiredEnc!=SQLITE_UTF8){
    u8 temp;
    int rc;
    rc = sqlite3VdbeMemMakeWriteable(pMem);
    if (rc!=SQLITE_OK){ assert(rc==SQLITE_NOMEM); return SQLITE_NOMEM; }
    zIn = (u8*)pMem->z;
    zTerm = &zIn[pMem->n&~1];
    while(zIn<zTerm){
      temp = *zIn;
      *zIn = *(zIn+1);
      zIn++;
      *zIn++ = temp;
    }
    pMem->enc = desiredEnc;
    goto translate_out;
  }

  /* Set len to the maximum number of bytes required in the output buffer. */
  if (desiredEnc==SQLITE_UTF8){
    pMem->n &= ~1;
    len = pMem->n * 2 + 1;
  } else len = pMem->n * 2 + 2;

  /* Set zIn to point at the start of the input buffer and zTerm to point 1
  ** byte past the end.
  **
  ** Variable zOut is set to point at the output buffer, space obtained
  ** from sqlite3_malloc().
  */
  zIn = (u8*)pMem->z;
  zTerm = &zIn[pMem->n];
  zOut = (unsigned char*)malloc(len);
  if (!zOut){
    return SQLITE_NOMEM;
  }
  z = zOut;

  if (pMem->enc==SQLITE_UTF8){
    if (desiredEnc==SQLITE_UTF16LE){
      /* UTF-8 -> UTF-16 Little-endian */
      while(zIn<zTerm){
        /* c = sqlite3Utf8Read(zIn, zTerm, (const u8**)&zIn); */
        READ_UTF8(zIn, zTerm, c);
        WRITE_UTF16LE(z, c);
      }
    }else{
      assert(desiredEnc==SQLITE_UTF16BE);
      /* UTF-8 -> UTF-16 Big-endian */
      while(zIn<zTerm){
        /* c = sqlite3Utf8Read(zIn, zTerm, (const u8**)&zIn); */
        READ_UTF8(zIn, zTerm, c);
        WRITE_UTF16BE(z, c);
      }
    }
    pMem->n = (int)(z - zOut);
    *z++ = 0;
  }else{
    assert(desiredEnc==SQLITE_UTF8);
    if (pMem->enc==SQLITE_UTF16LE){
      /* UTF-16 Little-endian -> UTF-8 */
      while(zIn<zTerm){
        READ_UTF16LE(zIn, zIn<zTerm, c); 
        WRITE_UTF8(z, c);
      }
    }else{
      /* UTF-16 Big-endian -> UTF-8 */
      while(zIn<zTerm){
        READ_UTF16BE(zIn, zIn<zTerm, c); 
        WRITE_UTF8(z, c);
      }
    }
    pMem->n = (int)(z - zOut);
  }
  *z = 0;
  assert((pMem->n+(desiredEnc==SQLITE_UTF8?1:2))<=len);

  free(pMem);
  pMem->flags &= ~(MEM_Static|MEM_Dyn|MEM_Ephem);
  pMem->enc = desiredEnc;
  pMem->flags |= (MEM_Term|MEM_Dyn);
  pMem->z = (char*)zOut;
  pMem->zMalloc = pMem->z;

translate_out:
  return SQLITE_OK;
}

/*
** This routine checks for a byte-order mark at the beginning of the 
** UTF-16 string stored in *pMem. If one is present, it is removed and
** the encoding of the Mem adjusted. This routine does not do any
** byte-swapping, it just sets Mem.enc appropriately.
**
** The allocation (static, dynamic etc.) and encoding of the Mem may be
** changed by this function.
*/
static int sqlite3VdbeMemHandleBom(Mem *pMem){
  int rc = SQLITE_OK;
  u8 bom = 0;

  assert(pMem->n>=0);
  if (pMem->n>1){
    u8 b1 = *(u8 *)pMem->z;
    u8 b2 = *(((u8 *)pMem->z) + 1);
    if (b1==0xFE && b2==0xFF){
      bom = SQLITE_UTF16BE;
    }
    if (b1==0xFF && b2==0xFE){
      bom = SQLITE_UTF16LE;
    }
  }
  
  if (bom){
    rc = sqlite3VdbeMemMakeWriteable(pMem);
    if (rc==SQLITE_OK){
      pMem->n -= 2;
      memmove(pMem->z, &pMem->z[2], pMem->n);
      pMem->z[pMem->n] = '\0';
      pMem->z[pMem->n+1] = '\0';
      pMem->flags |= MEM_Term;
      pMem->enc = bom;
    }
  }
  return rc;
}
#endif /* SQLITE_OMIT_UTF16 */


/*
** Make sure pMem->z points to a writable allocation of at least 
** n bytes.
**
** If the memory cell currently contains string or blob data
** and the third argument passed to this function is true, the 
** current content of the cell is preserved. Otherwise, it may
** be discarded.  
**
** This function sets the MEM_Dyn flag and clears any xDel callback.
** It also clears MEM_Ephem and MEM_Static. If the preserve flag is 
** not set, Mem.n is zeroed.
*/
static int sqlite3VdbeMemGrow(Mem *pMem, int n, int preserve){
  assert(1 >=
    ((pMem->zMalloc && pMem->zMalloc==pMem->z) ? 1 : 0) +
    (((pMem->flags&MEM_Dyn)&&pMem->xDel) ? 1 : 0) + 
    ((pMem->flags&MEM_Ephem) ? 1 : 0) + 
    ((pMem->flags&MEM_Static) ? 1 : 0));
  assert((pMem->flags&MEM_RowSet)==0);

  if (n<32) n = 32;
  if (malloc_size(pMem->zMalloc)<(unsigned)n){
    if (preserve && pMem->z==pMem->zMalloc){
      pMem->z = pMem->zMalloc = (char*)realloc(pMem->z, n);
      preserve = 0;
    }else{
      free(pMem->zMalloc);
      pMem->zMalloc = (char*)malloc(n);
    }
  }

  if (pMem->z && preserve && pMem->zMalloc && pMem->z!=pMem->zMalloc){
    memcpy(pMem->zMalloc, pMem->z, pMem->n);
  }
  if (pMem->flags&MEM_Dyn && pMem->xDel) pMem->xDel((void *)(pMem->z));


  pMem->z = pMem->zMalloc;
  if (pMem->z==0) pMem->flags = MEM_Null;
  else pMem->flags &= ~(MEM_Ephem|MEM_Static);
  pMem->xDel = 0;
  return (pMem->z ? SQLITE_OK : SQLITE_NOMEM);
}


/*
** If the given Mem* has a zero-filled tail, turn it into an ordinary
** blob stored in dynamically allocated space.
*/
#ifndef SQLITE_OMIT_INCRBLOB
static int sqlite3VdbeMemExpandBlob(Mem *pMem){
  if (pMem->flags & MEM_Zero){
    int nByte;
    assert(pMem->flags&MEM_Blob);
    assert((pMem->flags&MEM_RowSet)==0);
    // assert(pMem->db==0 || sqlite3_mutex_held(pMem->db->mutex));

    /* Set nByte to the number of bytes required to store the expanded blob. */
    nByte = pMem->n + pMem->u.nZero;
    if (nByte<=0) nByte = 1;
    if (sqlite3VdbeMemGrow(pMem, nByte, 1)) return SQLITE_NOMEM;

    memset(&pMem->z[pMem->n], 0, pMem->u.nZero);
    pMem->n += pMem->u.nZero;
    pMem->flags &= ~(MEM_Zero|MEM_Term);
  }
  return SQLITE_OK;
}
#endif


/*
** If pMem is an object with a valid string representation, this routine
** ensures the internal encoding for the string representation is
** 'desiredEnc', one of SQLITE_UTF8, SQLITE_UTF16LE or SQLITE_UTF16BE.
**
** If pMem is not a string object, or the encoding of the string
** representation is already stored using the requested encoding, then this
** routine is a no-op.
**
** SQLITE_OK is returned if the conversion is successful (or not required).
** SQLITE_NOMEM may be returned if a malloc() fails during conversion
** between formats.
*/
static int sqlite3VdbeChangeEncoding(Mem *pMem, int desiredEnc){
  int rc;
  assert((pMem->flags&MEM_RowSet)==0);
  assert(desiredEnc==SQLITE_UTF8 || desiredEnc==SQLITE_UTF16LE
           || desiredEnc==SQLITE_UTF16BE);
  if (!(pMem->flags&MEM_Str) || pMem->enc==desiredEnc){
    return SQLITE_OK;
  }
  //assert(pMem->db==0 || sqlite3_mutex_held(pMem->db->mutex));
#ifdef SQLITE_OMIT_UTF16
  return SQLITE_ERROR;
#else

  /* MemTranslate() may return SQLITE_OK or SQLITE_NOMEM. If NOMEM is returned,
  ** then the encoding of the value may not have changed.
  */
  rc = sqlite3VdbeMemTranslate(pMem, (u8)desiredEnc);
  assert(rc==SQLITE_OK    || rc==SQLITE_NOMEM);
  assert(rc==SQLITE_OK    || pMem->enc!=desiredEnc);
  assert(rc==SQLITE_NOMEM || pMem->enc==desiredEnc);
  return rc;
#endif
}

/*
** Make sure the given Mem is \u0000 terminated.
*/
static int sqlite3VdbeMemNulTerminate(Mem *pMem){
  //assert(pMem->db==0 || sqlite3_mutex_held(pMem->db->mutex));
  if ((pMem->flags & MEM_Term)!=0 || (pMem->flags & MEM_Str)==0){
    return SQLITE_OK;   /* Nothing to do */
  }
  if (sqlite3VdbeMemGrow(pMem, pMem->n+2, 1)){
    return SQLITE_NOMEM;
  }
  pMem->z[pMem->n] = 0;
  pMem->z[pMem->n+1] = 0;
  pMem->flags |= MEM_Term;
  return SQLITE_OK;
}

/*
** Compute a string length that is limited to what can be stored in
** lower 30 bits of a 32-bit signed integer.
**
** The value returned will never be negative.  Nor will it ever be greater
** than the actual length of the string.  For very long strings (greater
** than 1GiB) the value returned might be less than the true string length.
*/
static int sqlite3Strlen30(const char *z){
  const char *z2 = z;
  if (z==0) return 0;
  while (*z2){ z2++; }
  return 0x3fffffff & (int)(z2 - z);
}


/*
** Add MEM_Str to the set of representations for the given Mem.  Numbers
** are converted using sqlite3_snprintf().  Converting a BLOB to a string
** is a no-op.
**
** Existing representations MEM_Int and MEM_Real are *not* invalidated.
**
** A MEM_Null value will never be passed to this function. This function is
** used for converting values to text for returning to the user (i.e. via
** sqlite3_value_text()), or for ensuring that values to be used as btree
** keys are strings. In the former case a NULL pointer is returned the
** user and the later is an internal programming error.
*/
static int sqlite3VdbeMemStringify(Mem *pMem, int enc){
  int rc = SQLITE_OK;
  int fg = pMem->flags;
  const int nByte = 32;

  //assert(pMem->db==0 || sqlite3_mutex_held(pMem->db->mutex));
  assert(!(fg&MEM_Zero));
  assert(!(fg&(MEM_Str|MEM_Blob)));
  assert(fg&(MEM_Int|MEM_Real));
  assert((pMem->flags&MEM_RowSet)==0);
  assert(EIGHT_BYTE_ALIGNMENT(pMem));


  if (sqlite3VdbeMemGrow(pMem, nByte, 0)){
    return SQLITE_NOMEM;
  }

  /* For a Real or Integer, use sqlite3_mprintf() to produce the UTF-8
  ** string representation of the value. Then, if the required encoding
  ** is UTF-16le or UTF-16be do a translation.
  ** 
  ** FIX ME: It would be better if sqlite3_snprintf() could do UTF-16.
  */
  if (fg & MEM_Int){
    sqlite3_snprintf(nByte, pMem->z, "%lld", (long long) pMem->u.i);
  }else{
    assert(fg & MEM_Real);
    sqlite3_snprintf(nByte, pMem->z, "%.15g", pMem->r); // MKA: changed format %!.15g to %.15g
  }
  pMem->n = sqlite3Strlen30(pMem->z);
  pMem->enc = SQLITE_UTF8;
  pMem->flags |= MEM_Str|MEM_Term;
  sqlite3VdbeChangeEncoding(pMem, enc);
  return rc;
}

// Convert data to specified encoding indicated by second parameter,
// which must be one of SQLITE_UTF16BE, SQLITE_UTF16LE or
// SQLITE_UTF8.
// (2006-02-16:)  The enc value can be or-ed with SQLITE_UTF16_ALIGNED.
// If that is the case, then the result must be aligned on an even byte
// boundary.
static const void *sqlite3ValueText(Mem* pVal, u8 enc){
  if (!pVal) return 0;

  //assert(pVal->db==0 || sqlite3_mutex_held(pVal->db->mutex));
  assert((enc&3)==(enc&~SQLITE_UTF16_ALIGNED));
  assert((pVal->flags & MEM_RowSet)==0);

  if (pVal->flags&MEM_Null) return 0;
  assert((MEM_Blob>>3) == MEM_Str);
  pVal->flags |= (pVal->flags & MEM_Blob)>>3;
  expandBlob(pVal);
  if (pVal->flags&MEM_Str){
    sqlite3VdbeChangeEncoding(pVal, enc & ~SQLITE_UTF16_ALIGNED);
    if ((enc & SQLITE_UTF16_ALIGNED)!=0 && 1==(1&SQLITE_PTR_TO_INT(pVal->z))){
      assert((pVal->flags & (MEM_Ephem|MEM_Static))!=0);
      if (sqlite3VdbeMemMakeWriteable(pVal)!=SQLITE_OK) return 0;
    }
    sqlite3VdbeMemNulTerminate(pVal); /* IMP: R-59893-45467 */
  }else{
    assert((pVal->flags&MEM_Blob)==0);
    sqlite3VdbeMemStringify(pVal, enc);
    assert(0==(1&SQLITE_PTR_TO_INT(pVal->z)));
  }
  //assert(pVal->enc==(enc & ~SQLITE_UTF16_ALIGNED) || pVal->db==0
  //            || pVal->db->mallocFailed);
  if (pVal->enc==(enc & ~SQLITE_UTF16_ALIGNED)) return pVal->z;
  else return 0;
}


/***********************************************************************
 *                                                                     *
 *                       Collating  functions                          *
 *                                                                     *
 ***********************************************************************/


// Return true if the buffer z[0..n-1] contains all spaces.
static int allSpaces(const char *z, int n){
  while (n>0 && z[n-1]==' ') --n;
  return n==0;
}

// This is the default collating function named "BINARY" which is always
// available.
// If the padFlag argument is not NULL then space padding at the end
// of strings is ignored.  This implements the RTRIM collation.
int binCollFunc(void *padFlag, int nKey1, const void *pKey1,
                       int nKey2, const void *pKey2){
  int rc, n;
  n = nKey1<nKey2 ? nKey1 : nKey2;
  rc = memcmp(pKey1, pKey2, n);
  if (rc==0){
    if (padFlag && allSpaces(((char*)pKey1)+n, nKey1-n)
                && allSpaces(((char*)pKey2)+n, nKey2-n)) ; // leave rc=0
    else rc = nKey1 - nKey2;
  }
  return rc;
}

// Built-in collating sequence: NOCASE. 
// This collating sequence is intended to be used for "case independant
// comparison". SQLite's knowledge of upper and lower case equivalents
// extends only to the 26 characters used in the English language.
// At the moment there is only a UTF-8 implementation.
int nocaseCollatingFunc(void *NotUsed, int nKey1, const void *pKey1,
                               int nKey2, const void *pKey2){
  int r = sqlite3StrNICmp((const char *)pKey1,
                          (const char *)pKey2, (nKey1<nKey2)?nKey1:nKey2);
  //UNUSED_PARAMETER(NotUsed);
  if(r==0) r = nKey1-nKey2;
  return r;
}


// Return true if the floating point value is Not a Number (NaN).
static int sqlite3IsNaN(double x){
  int rc;   /* The value return */
  volatile double y = x;
  volatile double z = y;
  rc = (y!=z);
  return rc;
}

/*
** If the memory cell contains a string value that must be freed by
** invoking an external callback, free it now. Calling this function
** does not free any Mem.zMalloc buffer.
*/
static void sqlite3VdbeMemReleaseExternal(Mem *p){
  //assert(p->db==0 || sqlite3_mutex_held(p->db->mutex));
  testcase(p->flags & MEM_Agg);
  testcase(p->flags & MEM_Dyn);
  testcase(p->flags & MEM_RowSet);
  testcase(p->flags & MEM_Frame);
  if (p->flags&(MEM_Agg|MEM_Dyn|MEM_RowSet|MEM_Frame)){
    if (p->flags&MEM_Agg){
      assert(0);
      //sqlite3VdbeMemFinalize(p, p->u.pDef);
      //assert((p->flags & MEM_Agg)==0);
      //sqlite3VdbeMemRelease(p);
    }else if (p->flags&MEM_Dyn && p->xDel){
      assert((p->flags&MEM_RowSet)==0);
      p->xDel((void *)p->z);
      p->xDel = 0;
    }else if (p->flags&MEM_RowSet){
      assert(0);
      //sqlite3RowSetClear(p->u.pRowSet);
    }else if (p->flags&MEM_Frame){
      assert(0);
      //sqlite3VdbeMemSetNull(p);
    }
  }
}

/*
** Release any memory held by the Mem. This may leave the Mem in an
** inconsistent state, for example with (Mem.z==0) and
** (Mem.type==SQLITE_TEXT).
*/
static void sqlite3VdbeMemRelease(Mem *p){
  sqlite3VdbeMemReleaseExternal(p);
  free(p->zMalloc);
  p->z = 0;
  p->zMalloc = 0;
  p->xDel = 0;
}

/*
** Size of struct Mem not including the Mem.zMalloc member.
*/
#define MEMCELLSIZE (size_t)(&(((Mem *)0)->zMalloc))

/*
** Make an shallow copy of pFrom into pTo.  Prior contents of
** pTo are freed.  The pFrom->z field is not duplicated.  If
** pFrom->z is used, then pTo->z points to the same thing as pFrom->z
** and flags gets srcType (either MEM_Ephem or MEM_Static).
*/
static void sqlite3VdbeMemShallowCopy(Mem *pTo, const Mem *pFrom, int srcType){
  assert((pFrom->flags & MEM_RowSet)==0);
  sqlite3VdbeMemReleaseExternal(pTo);
  memcpy(pTo, pFrom, MEMCELLSIZE);
  pTo->xDel = 0;
  if ((pFrom->flags&MEM_Static)==0){
    pTo->flags &= ~(MEM_Dyn|MEM_Static|MEM_Ephem);
    assert(srcType==MEM_Ephem || srcType==MEM_Static);
    pTo->flags |= srcType;
  }
}


/*
** Compare the values contained by the two memory cells, returning
** negative, zero or positive if pMem1 is less than, equal to, or greater
** than pMem2. Sorting order is NULL's first, followed by numbers (integers
** and reals) sorted numerically, followed by text ordered by the collating
** sequence pColl and finally blob's ordered by memcmp().
**
** Two NULL values are considered equal by this function.
*/
static int sqlite3MemCompare(const Mem *pMem1, const Mem *pMem2, const CollSeq *pColl){
  int rc;
  int f1, f2;
  int combined_flags;

  f1 = pMem1->flags;
  f2 = pMem2->flags;
  combined_flags = f1|f2;
  assert((combined_flags & MEM_RowSet)==0);
 
  /* If one value is NULL, it is less than the other. If both values
  ** are NULL, return 0.
  */
  if (combined_flags&MEM_Null){
    return (f2&MEM_Null) - (f1&MEM_Null);
  }

  /* If one value is a number and the other is not, the number is less.
  ** If both are numbers, compare as reals if one is a real, or as integers
  ** if both values are integers.
  */
  if (combined_flags&(MEM_Int|MEM_Real)){
    if (!(f1&(MEM_Int|MEM_Real))) return 1;
    if (!(f2&(MEM_Int|MEM_Real))) return -1;
    if ((f1 & f2 & MEM_Int)==0){
      double r1, r2;
      if ((f1&MEM_Real)==0) r1 = (double)pMem1->u.i;
      else r1 = pMem1->r;
      if ((f2&MEM_Real)==0) r2 = (double)pMem2->u.i;
      else r2 = pMem2->r;
      if (r1<r2) return -1;
      if (r1>r2) return 1;
      return 0;
    }else{
      assert(f1&MEM_Int);
      assert(f2&MEM_Int);
      if (pMem1->u.i < pMem2->u.i) return -1;
      if (pMem1->u.i > pMem2->u.i) return 1;
      return 0;
    }
  }

  /* If one value is a string and the other is a blob, the string is less.
  ** If both are strings, compare using the collating functions.
  */
  if (combined_flags&MEM_Str){
    if ((f1 & MEM_Str)==0) return 1;
    if ((f2 & MEM_Str)==0) return -1;

    assert(pMem1->enc==pMem2->enc);
    assert(pMem1->enc==SQLITE_UTF8 || 
            pMem1->enc==SQLITE_UTF16LE || pMem1->enc==SQLITE_UTF16BE);

    /* The collation sequence must be defined at this point, even if
    ** the user deletes the collation sequence after the vdbe program is
    ** compiled (this was not always the case).
    */
    assert(!pColl || pColl->xCmp);

    if (pColl){
      if (pMem1->enc==pColl->enc){
        /* The strings are already in the correct encoding.  Call the
        ** comparison function directly */
        return pColl->xCmp(pColl->pUser,pMem1->n,pMem1->z,pMem2->n,pMem2->z);
      }else{
        const void *v1, *v2;
        int n1, n2;
        Mem c1;
        Mem c2;
        memset(&c1, 0, sizeof(c1));
        memset(&c2, 0, sizeof(c2));
        sqlite3VdbeMemShallowCopy(&c1, pMem1, MEM_Ephem);
        sqlite3VdbeMemShallowCopy(&c2, pMem2, MEM_Ephem);
        v1 = sqlite3ValueText(&c1, pColl->enc);
        n1 = v1==0 ? 0 : c1.n;
        v2 = sqlite3ValueText(&c2, pColl->enc);
        n2 = v2==0 ? 0 : c2.n;
        rc = pColl->xCmp(pColl->pUser, n1, v1, n2, v2);
        sqlite3VdbeMemRelease(&c1);
        sqlite3VdbeMemRelease(&c2);
        return rc;
      }
    }
    /* If a NULL pointer was passed as the collate function, fall through
    ** to the blob case and use memcmp().  */
  }
 
  /* Both values must be blobs.  Compare using memcmp().  */
  rc = memcmp(pMem1->z, pMem2->z, (pMem1->n>pMem2->n)?pMem2->n:pMem1->n);
  if (rc==0) rc = pMem1->n - pMem2->n;
  return rc;
}


/*
** The following functions:
**
** sqlite3VdbeSerialType()
** sqlite3VdbeSerialTypeLen()
** sqlite3VdbeSerialLen()
** sqlite3VdbeSerialPut()
** sqlite3VdbeSerialGet()
**
** encapsulate the code that serializes values for storage in SQLite
** data and index records. Each serialized value consists of a
** 'serial-type' and a blob of data. The serial type is an 8-byte unsigned
** integer, stored as a varint.
**
** In an SQLite index record, the serial type is stored directly before
** the blob of data that it corresponds to. In a table record, all serial
** types are stored at the start of the record, and the blobs of data at
** the end. Hence these functions allow the caller to handle the
** serial-type and data blob seperately.
**
** The following table describes the various storage classes for data:
**
**   serial type        bytes of data      type
**   --------------     ---------------    ---------------
**      0                     0            NULL
**      1                     1            signed integer
**      2                     2            signed integer
**      3                     3            signed integer
**      4                     4            signed integer
**      5                     6            signed integer
**      6                     8            signed integer
**      7                     8            IEEE float
**      8                     0            Integer constant 0
**      9                     0            Integer constant 1
**     10,11                               reserved for expansion
**    N>=12 and even       (N-12)/2        BLOB
**    N>=13 and odd        (N-13)/2        text
**
** The 8 and 9 types were added in 3.3.0, file format 4.  Prior versions
** of SQLite will not understand those serial types.
*/

/*
** Return the serial-type for the value stored in pMem.
*/
static u32 sqlite3VdbeSerialType(Mem *pMem, int file_format){
  int flags = pMem->flags;
  int n;

  if (flags&MEM_Null) return 0;
  if (flags&MEM_Int){
    /* Figure out whether to use 1, 2, 4, 6 or 8 bytes. */
#   define MAX_6BYTE ((((i64)0x00008000)<<32)-1)
    i64 i = pMem->u.i;
    u64 u;
    if (file_format>=4 && (i&1)==i) return 8+(u32)i;
    if (i<0){
      if (i<(-MAX_6BYTE)) return 6;
      /* Previous test prevents:  u = -(-9223372036854775808) */
      u = -i;
    } else  u = i;
    if (u<=127) return 1;
    if (u<=32767) return 2;
    if (u<=8388607) return 3;
    if (u<=2147483647) return 4;
    if (u<=MAX_6BYTE) return 5;
    return 6;
  }
  if (flags&MEM_Real) return 7;
  n = pMem->n;
  if (flags & MEM_Zero) n += pMem->u.nZero;
  assert(n>=0);
  return (n*2) + 12 + ((flags&MEM_Str)==0 ? 0 : 1);
}

/*
** Return the length of the data corresponding to the supplied serial-type.
*/
static u32 sqlite3VdbeSerialTypeLen(u32 serial_type){
  static const u8 aSize[] = { 0, 1, 2, 3, 4, 6, 8, 8, 0, 0, 0, 0 };
  if (serial_type>=12) return (serial_type-12)/2;
  else return aSize[serial_type];
}

/*
** Write the serialized data blob for the value stored in pMem into 
** buf. It is assumed that the caller has allocated sufficient space.
** Return the number of bytes written.
**
** nBuf is the amount of space left in buf[].  nBuf must always be
** large enough to hold the entire field.  Except, if the field is
** a blob with a zero-filled tail, then buf[] might be just the right
** size to hold everything except for the zero-filled tail.  If buf[]
** is only big enough to hold the non-zero prefix, then only write that
** prefix into buf[].  But if buf[] is large enough to hold both the
** prefix and the tail then write the prefix and set the tail to all
** zeros.
**
** Return the number of bytes actually written into buf[].  The number
** of bytes in the zero-filled tail is included in the return value only
** if those bytes were zeroed in buf[].
*/ 
static u32 sqlite3VdbeSerialPut(u8 *buf, int nBuf, Mem *pMem, int file_format){
  u32 serial_type = sqlite3VdbeSerialType(pMem, file_format);
  u32 len;

  /* Integer and Real */
  if (serial_type<=7 && serial_type>0){
    u64 v;
    u32 i;
    if (serial_type==7){
      assert(sizeof(v)==sizeof(pMem->r));
      memcpy(&v, &pMem->r, sizeof(v));
    }else  v = pMem->u.i;
    len = i = sqlite3VdbeSerialTypeLen(serial_type);
    assert(len<=(u32)nBuf);
    while (i--){
      buf[i] = (u8)(v&0xFF);
      v >>= 8;
    }
    return len;
  }

  /* String or blob */
  if (serial_type>=12){
    assert(pMem->n + ((pMem->flags & MEM_Zero)?pMem->u.nZero:0)
             == (int)sqlite3VdbeSerialTypeLen(serial_type));
    assert(pMem->n<=nBuf);
    len = pMem->n;
    memcpy(buf, pMem->z, len);
    if (pMem->flags & MEM_Zero){
      len += pMem->u.nZero;
      assert(nBuf>=0);
      if (len > (u32)nBuf) len = (u32)nBuf;
      memset(&buf[pMem->n], 0, len-pMem->n);
    }
    return len;
  }

  /* NULL or constants 0 or 1 */
  return 0;
}

/*
** Deserialize the data blob pointed to by buf as serial type serial_type
** and store the result in pMem.  Return the number of bytes read.
*/ 
static u32 sqlite3VdbeSerialGet(
  const unsigned char *buf,     /* Buffer to deserialize from */
  u32 serial_type,              /* Serial type to deserialize */
  Mem *pMem                     /* Memory cell to write value into */
){
  switch(serial_type){
    case 10:   /* Reserved for future use */
    case 11:   /* Reserved for future use */
    case 0: {  /* NULL */
      pMem->flags = MEM_Null;
      break;
    }
    case 1: { /* 1-byte signed integer */
      pMem->u.i = (signed char)buf[0];
      pMem->flags = MEM_Int;
      return 1;
    }
    case 2: { /* 2-byte signed integer */
      pMem->u.i = (((signed char)buf[0])<<8) | buf[1];
      pMem->flags = MEM_Int;
      return 2;
    }
    case 3: { /* 3-byte signed integer */
      pMem->u.i = (((signed char)buf[0])<<16) | (buf[1]<<8) | buf[2];
      pMem->flags = MEM_Int;
      return 3;
    }
    case 4: { /* 4-byte signed integer */
      pMem->u.i = (buf[0]<<24) | (buf[1]<<16) | (buf[2]<<8) | buf[3];
      pMem->flags = MEM_Int;
      return 4;
    }
    case 5: { /* 6-byte signed integer */
      u64 x = (((signed char)buf[0])<<8) | buf[1];
      u32 y = (buf[2]<<24) | (buf[3]<<16) | (buf[4]<<8) | buf[5];
      x = (x<<32) | y;
      pMem->u.i = *(i64*)&x;
      pMem->flags = MEM_Int;
      return 6;
    }
    case 6:   /* 8-byte signed integer */
    case 7: { /* IEEE floating point */
      u64 x;
      u32 y;
#if !defined(NDEBUG) && !defined(SQLITE_OMIT_FLOATING_POINT)
      /* Verify that integers and floating point values use the same
      ** byte order.  Or, that if SQLITE_MIXED_ENDIAN_64BIT_FLOAT is
      ** defined that 64-bit floating point values really are mixed
      ** endian.
      */
      static const u64 t1 = ((u64)0x3ff00000)<<32;
      static const double r1 = 1.0;
      u64 t2 = t1;
      assert(sizeof(r1)==sizeof(t2) && memcmp(&r1, &t2, sizeof(r1))==0);
#endif

      x = (buf[0]<<24) | (buf[1]<<16) | (buf[2]<<8) | buf[3];
      y = (buf[4]<<24) | (buf[5]<<16) | (buf[6]<<8) | buf[7];
      x = (x<<32) | y;
      if (serial_type==6){
        pMem->u.i = *(i64*)&x;
        pMem->flags = MEM_Int;
      }else {
        assert(sizeof(x)==8 && sizeof(pMem->r)==8);
        memcpy(&pMem->r, &x, sizeof(x));
        pMem->flags = sqlite3IsNaN(pMem->r) ? MEM_Null : MEM_Real;
      }
      return 8;
    }
    case 8:    /* Integer 0 */
    case 9: {  /* Integer 1 */
      pMem->u.i = serial_type-8;
      pMem->flags = MEM_Int;
      return 0;
    }
    default: {
      u32 len = (serial_type-12)/2;
      pMem->z = (char *)buf;
      pMem->n = len;
      pMem->xDel = 0;
      if (serial_type&0x01) pMem->flags = MEM_Str | MEM_Ephem;
      else pMem->flags = MEM_Blob | MEM_Ephem;
      return len;
    }
  }
  return 0;
}

/*
** Given the nKey-byte encoding of a record in pKey[], parse the
** record into a UnpackedRecord structure.  Return a pointer to
** that structure.
**
** The calling function might provide szSpace bytes of memory
** space at pSpace.  This space can be used to hold the returned
** VDbeParsedRecord structure if it is large enough.  If it is
** not big enough, space is obtained from sqlite3_malloc().
**
** The returned structure should be closed by a call to
** sqlite3VdbeDeleteUnpackedRecord().
*/ 
static UnpackedRecord *sqlite3VdbeRecordUnpack(
  RcKeyInfo *pKeyInfo,   /* Information about the record format */
  int nKey,              /* Size of the binary record */
  const void *pKey,      /* The binary record */
  char *pSpace,          /* Unaligned space available to hold the object */
  int szSpace            /* Size of pSpace[] in bytes */
){
  const unsigned char *aKey = (const unsigned char *)pKey;
  UnpackedRecord *p;  /* The unpacked record that we will return */
  int nByte;          /* Memory space needed to hold p, in bytes */
  int d;
  u32 idx;
  u16 u;              /* Unsigned loop counter */
  u32 szHdr;
  Mem *pMem;
  int nOff;           /* Increase pSpace by this much to 8-byte align it */
  
  /*
  ** We want to shift the pointer pSpace up such that it is 8-byte aligned.
  ** Thus, we need to calculate a value, nOff, between 0 and 7, to shift 
  ** it by.  If pSpace is already 8-byte aligned, nOff should be zero.
  */
  nOff = (8 - (SQLITE_PTR_TO_INT(pSpace) & 7)) & 7;
  pSpace += nOff;
  szSpace -= nOff;
  nByte = ROUND8(sizeof(UnpackedRecord)) + sizeof(Mem)*(pKeyInfo->nField+1);
  if (nByte>szSpace){
    p = (UnpackedRecord*) malloc(nByte);
    if (p==0) return 0;
    p->flags = UNPACKED_NEED_FREE | UNPACKED_NEED_DESTROY;
  } else {
    p = (UnpackedRecord*)pSpace;
    p->flags = UNPACKED_NEED_DESTROY;
  }
  p->pKeyInfo = pKeyInfo;
  p->nField = pKeyInfo->nField + 1;
  p->aMem = pMem = (Mem*)&((char*)p)[ROUND8(sizeof(UnpackedRecord))];
  assert(EIGHT_BYTE_ALIGNMENT(pMem));
  idx = getVarint32(aKey, szHdr);
  d = szHdr;
  u = 0;
  while (idx<szHdr && u<p->nField && d<=nKey){
    u32 serial_type;

    idx += getVarint32(&aKey[idx], serial_type);
    pMem->enc = pKeyInfo->enc;
    pMem->db = pKeyInfo->db;
    pMem->flags = 0;
    pMem->zMalloc = 0;
    d += sqlite3VdbeSerialGet(&aKey[d], serial_type, pMem);
    pMem++;
    u++;
  }
  assert(u<=pKeyInfo->nField + 1);
  p->nField = u;
  return p;
}


UnpackedRecord *myVdbeRecordUnpack(RcKeyInfo *pKeyInfo, int nKey,
                                   const void *pKey, char *pSpace, int szSpace){
  return sqlite3VdbeRecordUnpack(pKeyInfo, nKey, pKey, pSpace, szSpace);
}

/*
** This routine destroys a UnpackedRecord object.
*/
static void sqlite3VdbeDeleteUnpackedRecord(UnpackedRecord *p){
  int i;
  Mem *pMem;

  assert(p!=0);
  assert(p->flags & UNPACKED_NEED_DESTROY);
  for(i=0, pMem=p->aMem; i<p->nField; i++, pMem++){
    /* The unpacked record is always constructed by the
    ** sqlite3VdbeUnpackRecord() function above, which makes all
    ** strings and blobs static.  And none of the elements are
    ** ever transformed, so there is never anything to delete.
    */
    if (NEVER(pMem->zMalloc)) sqlite3VdbeMemRelease(pMem);
  }
  if (p->flags & UNPACKED_NEED_FREE) free(p);
}

void myVdbeDeleteUnpackedRecord(UnpackedRecord *p){
  sqlite3VdbeDeleteUnpackedRecord(p);
}

/*
** This function compares the two table rows or index records
** specified by {nKey1, pKey1} and pPKey2.  It returns a negative, zero
** or positive integer if key1 is less than, equal to or 
** greater than key2.  The {nKey1, pKey1} key must be a blob
** created by th OP_MakeRecord opcode of the VDBE.  The pPKey2
** key must be a parsed key such as obtained from
** sqlite3VdbeParseRecord.
**
** Key1 and Key2 do not have to contain the same number of fields.
** The key with fewer fields is usually compares less than the 
** longer key.  However if the UNPACKED_INCRKEY flags in pPKey2 is set
** and the common prefixes are equal, then key1 is less than key2.
** Or if the UNPACKED_MATCH_PREFIX flag is set and the prefixes are
** equal, then the keys are considered to be equal and
** the parts beyond the common prefix are ignored.
**
** If the UNPACKED_IGNORE_ROWID flag is set, then the last byte of
** the header of pKey1 is ignored.  It is assumed that pKey1 is
** an index key, and thus ends with a rowid value.  The last byte
** of the header will therefore be the serial type of the rowid:
** one of 1, 2, 3, 4, 5, 6, 8, or 9 - the integer serial types.
** The serial type of the final rowid will always be a single byte.
** By ignoring this last byte of the header, we force the comparison
** to ignore the rowid at the end of key1.
*/
static int sqlite3VdbeRecordCompare(
  int nKey1, const void *pKey1, /* Left key */
  UnpackedRecord *pPKey2        /* Right key */
){
  int d1;            /* Offset into aKey[] of next data element */
  u32 idx1;          /* Offset into aKey[] of next header element */
  u32 szHdr1;        /* Number of bytes in header */
  int i = 0;
  int nField;
  int rc = 0;
  const unsigned char *aKey1 = (const unsigned char *)pKey1;
  RcKeyInfo *pKeyInfo;
  Mem mem1;

  pKeyInfo = pPKey2->pKeyInfo;
  mem1.enc = pKeyInfo->enc;
  // mem1.db = pKeyInfo->db;
  /* mem1.flags = 0;  // Will be initialized by sqlite3VdbeSerialGet() */
  DEBUG_ONLY(mem1.zMalloc = 0;) /* Only needed by assert() statements */

  /* Compilers may complain that mem1.u.i is potentially uninitialized.
  ** We could initialize it, as shown here, to silence those complaints.
  ** But in fact, mem1.u.i will never actually be used initialized, and doing 
  ** the unnecessary initialization has a measurable negative performance
  ** impact, since this routine is a very high runner.  And so, we choose
  ** to ignore the compiler warnings and leave this variable uninitialized.
  */
  /*  mem1.u.i = 0;  // not needed, here to silence compiler warning */
  
  idx1 = getVarint32(aKey1, szHdr1);
  d1 = szHdr1;
  if (pPKey2->flags & UNPACKED_IGNORE_ROWID) szHdr1--;
  nField = pKeyInfo->nField;
  while (idx1<szHdr1 && i<pPKey2->nField){
    u32 serial_type1;

    // Read the serial types for the next element in each key
    idx1 += getVarint32(aKey1+idx1, serial_type1);
    if (d1>=nKey1 && sqlite3VdbeSerialTypeLen(serial_type1)>0) break;

    // Extract the values to be compared.
    d1 += sqlite3VdbeSerialGet(&aKey1[d1], serial_type1, &mem1);

    // Do the comparison
    rc = sqlite3MemCompare(&mem1, &pPKey2->aMem[i],
                           i<nField ? pKeyInfo->aColl[i] : 0);
    if (rc!=0){
      assert(mem1.zMalloc==0);  /* See comment below */

      /* Invert the result if we are using DESC sort order. */
      if (pKeyInfo->aSortOrder && i<nField && pKeyInfo->aSortOrder[i]) rc = -rc;
    
      /* If the PREFIX_SEARCH flag is set and all fields except the final
      ** rowid field were equal, then clear the PREFIX_SEARCH flag and set 
      ** pPKey2->rowid to the value of the rowid field in (pKey1, nKey1).
      ** This is used by the OP_IsUnique opcode.
      */
      if ((pPKey2->flags & UNPACKED_PREFIX_SEARCH) && i==(pPKey2->nField-1)){
        assert(idx1==szHdr1 && rc);
        assert(mem1.flags & MEM_Int);
        pPKey2->flags &= ~UNPACKED_PREFIX_SEARCH;
        pPKey2->rowid = mem1.u.i;
      }
    
      return rc;
    }
    i++;
  }

  /* No memory allocation is ever used on mem1.  Prove this using
  ** the following assert().  If the assert() fails, it indicates a
  ** memory leak and a need to call sqlite3VdbeMemRelease(&mem1).
  */
  assert(mem1.zMalloc==0);

  /* rc==0 here means that one of the keys ran out of fields and
  ** all the fields up to that point were equal. If the UNPACKED_INCRKEY
  ** flag is set, then break the tie by treating key2 as larger.
  ** If the UPACKED_PREFIX_MATCH flag is set, then keys with common prefixes
  ** are considered to be equal.  Otherwise, the longer key is the 
  ** larger.  As it happens, the pPKey2 will always be the longer
  ** if there is a difference.
  */
  assert(rc==0);
  if (pPKey2->flags & UNPACKED_INCRKEY) rc = -1;
  else if (pPKey2->flags & UNPACKED_PREFIX_MATCH) ; /* Leave rc==0 */
  else if (idx1<szHdr1) rc = 1;
  return rc;
}

RcKeyInfo *CloneKeyInfo(RcKeyInfo *prki){
  RcKeyInfo *newkey;
  if (!prki) return 0;
  int nfield = prki->nField;
  if (nfield <= 0) nfield = 1;
  // reserve space for RcKeyInfo + aSortOrder (if non-null) and CollSeq
  newkey = new(nfield, prki->aSortOrder ? nfield : 0) RcKeyInfo();
  assert(newkey);
  // copy fields
  newkey->db = prki->db;
  newkey->enc = prki->enc;
  newkey->nField = prki->nField;
  // copy Collating sequence
  memcpy(&newkey->aColl[0], &prki->aColl[0], nfield * sizeof(CollSeq*));
  // copy sort order if present
  if (prki->aSortOrder){
    newkey->aSortOrder = (u8*) ((char*) newkey + sizeof(RcKeyInfo) + (nfield-1) * sizeof(CollSeq*));
    memcpy(newkey->aSortOrder, prki->aSortOrder, nfield * sizeof(u8));
  } else newkey->aSortOrder = 0; // not present
  return newkey;
}


int myVdbeRecordCompare(int nKey1, const void *pKey1, UnpackedRecord *pPKey2){
  return sqlite3VdbeRecordCompare(nKey1, pKey1, pPKey2);
}


int VdbeRecordSerialSize(UnpackedRecord *pIdxKey, int file_format){
  int nHdr;
  Mem *aMem = pIdxKey->aMem;
  int i;
  u32 serial_type;
  int nVarint, nData, nZero;
  int len;
  
  // calculate length of header
  nHdr = 0;
  nData = 0;
  nZero = 0;
  for (i=0; i < pIdxKey->nField; ++i){
    serial_type = sqlite3VdbeSerialType(&aMem[i], file_format);
    len = sqlite3VdbeSerialTypeLen(serial_type);
    nData += len;
    nHdr += sqlite3VarintLen(serial_type);
    if (aMem[i].flags & MEM_Zero){
      nZero += aMem[i].u.nZero;
    } else if (len) nZero = 0;
  }
  nHdr += nVarint = sqlite3VarintLen(nHdr);
  if (nVarint < sqlite3VarintLen(nHdr)) ++nHdr;
  return nHdr + nData - nZero;
}

// Returns a pKey from a pIdxKey.
// The value returned is a dynamically allocated buffer that should be freed by the called with free().
// Also returns length of pkey in length parameter.
// 
char *myVdbeRecordPack(UnpackedRecord *pIdxKey, int file_format, int &length){
  char *pKey;
  int nHdr;
  Mem *aMem;
  int i;
  u32 serial_type;
  int nVarint, nData, nZero, nByte;
  int len;
  char *out;

  if (!pIdxKey) return 0;
  aMem = pIdxKey->aMem;
  // calculate lengths of header, data and zero
  nHdr = 0;
  nData = 0;
  nZero = 0;
  for (i=0; i < pIdxKey->nField; ++i){
    serial_type = sqlite3VdbeSerialType(&aMem[i], file_format);
    len = sqlite3VdbeSerialTypeLen(serial_type);
    nData += len;
    nHdr += sqlite3VarintLen(serial_type);
    if (aMem[i].flags & MEM_Zero){
      nZero += aMem[i].u.nZero;
    } else if (len) nZero = 0;
  }
  nHdr += nVarint = sqlite3VarintLen(nHdr);
  if (nVarint < sqlite3VarintLen(nHdr)) ++nHdr;
  nByte = nHdr + nData - nZero;
  pKey = (char*) malloc(nByte); assert(pKey);

  out = pKey;
  // write header size
  out += putVarint32((unsigned char*)out, nHdr);
  // write types in header
  for (i=0; i < pIdxKey->nField; ++i){
    serial_type = sqlite3VdbeSerialType(&aMem[i], file_format);
    out += putVarint32((unsigned char*)out, serial_type);
  }
  // write data
  for (i=0; i < pIdxKey->nField; ++i){
    out += sqlite3VdbeSerialPut((u8*) out, nByte - (out-pKey), &aMem[i], file_format);
  }
  length = nByte;
  return pKey;
}

// test whether myVdbeRecordPack is working. Returns true if ok, false
// otherwise
int testRecordPack(UnpackedRecord *pIdxKey, int file_format){
  int res, nKey;
  char *pKey;
  if (!pIdxKey) return 1;
  pKey = myVdbeRecordPack(pIdxKey, file_format, nKey); assert(pKey);
  res = myVdbeRecordCompare(nKey, pKey, pIdxKey);
  free(pKey);
  return res==0;
}


void *RcKeyInfo::operator new(std::size_t sz, int ncoll, int nsort){
  sz += ncoll * sizeof(CollSeq*) + nsort;
  return malloc(sz);
}
  
static void RcKeyInfo::operator delete(void *ptr, std::size_t sz){
  free(ptr);
}

void RcKeyInfo::printShort(){
  if (this){
    printf("%d%d%c", enc, nField, aSortOrder ? 'S' : '0');
    for (int i=0; i < nField; ++i){ printf("%d", aColl[i]->type); }
  } else printf("nil");
}
  

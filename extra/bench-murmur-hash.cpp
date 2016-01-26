//
// bench-murmur-hash.cpp
//
// 64-bit hash for 64-bit platforms


// The following code was released into the public domain by its author
//   see (https://sites.google.com/site/murmurhash/)
// 64-bit hash for 32-bit platforms
//-----------------------------------------------------------------------------
// MurmurHash2, 64-bit versions, by Austin Appleby

// The same caveats as 32-bit MurmurHash2 apply here - beware of alignment 
// and endian-ness issues if used across multiple platforms.

#include <stdint.h>

static const unsigned int _hash_seed = 0xBADCAB1E;
uint64_t MurmurHash64A(const void *key, int len){
  const uint64_t m = 0xc6a4a7935bd1e995;
  const unsigned int seed = _hash_seed;
  const int r = 47;

  uint64_t h = seed ^ (len * m);

  const uint64_t *data = (const uint64_t*) key;
  const uint64_t *end = data + (len/8);

  while(data != end)
  {
    uint64_t k = *data++;

    k *= m;
    k ^= k >> r;
    k *= m;

    h ^= k;
    h *= m;
  }

  const unsigned char *data2 = (const unsigned char*) data;

  switch(len & 7)
  {
  case 7: h ^= uint64_t(data2[6]) << 48;
  case 6: h ^= uint64_t(data2[5]) << 40;
  case 5: h ^= uint64_t(data2[4]) << 32;
  case 4: h ^= uint64_t(data2[3]) << 24;
  case 3: h ^= uint64_t(data2[2]) << 16;
  case 2: h ^= uint64_t(data2[1]) << 8;
  case 1: h ^= uint64_t(data2[0]);
          h *= m;
  };

  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}

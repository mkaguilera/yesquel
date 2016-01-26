//
// ipmisc.cpp
//
// Miscellaneous TCP/IP stuff
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
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>
#include <math.h>
#include <ifaddrs.h>
#include <map>
#include <list>

#include "tmalloc.h"
#include "os.h"
#include "options.h"
#include "ipmisc.h"

IPPort UDPDest::getIPPort(void){
  IPPort ipport;
  ipport.set(*(u32*)&destaddr.sin_addr, (u32)destaddr.sin_port);
  return ipport;
}

void IPPort::getUDPDest(UDPDest *udpdest){
  udpdest->destaddr.sin_family = AF_INET;
  udpdest->destaddr.sin_port = port;
  udpdest->sockaddr_len=16;
  memcpy((void*)&udpdest->destaddr.sin_addr, (void*)&ip, 4);
}

// return IP address of a given name or 0 if name does not exist
u32 IPMisc::resolveName(const char *name, u32 preferip, u32 prefermask){
  struct hostent *he;
  u32 thisip;

  he = gethostbyname(name);
  if (!he) return 0;
  assert(he->h_addrtype == AF_INET);

  if (!prefermask) preferip = 0; // anything goes

  int i;
  thisip=0;
  i=0;
  while (he->h_addr_list[i] != 0){
    thisip = * (u32*) he->h_addr_list[i];
    if (ntohl(thisip) != 0x7f000001) // not 127.0.0.1 please
      if ((thisip & prefermask) == preferip)
        return thisip;
    ++i;
  }
  // did not find preferred ip, return first one
  return *(u32*) he->h_addr_list[0];
}

// return my own ip address as a u32
u32 IPMisc::getMyIP(u32 preferip, u32 prefermask){
  int res;
  u32 thisip;
  u32 firstip;

  struct ifaddrs *head, *ptr;

  firstip = 0;
  res = getifaddrs(&head);
  if (res){ printf("getMyIP: getifaddrs failed errno %d\n", errno); exit(1);}

  if (!prefermask) preferip = 0; // anything goes
  
  for (ptr = head; ptr; ptr = ptr->ifa_next){
    if (!ptr->ifa_addr) continue;
    if ((ptr->ifa_addr->sa_family != AF_INET)) continue; // ipv4 please
    thisip = *(u32*)&((sockaddr_in*)ptr->ifa_addr)->sin_addr.s_addr;
    if (!firstip) firstip = thisip;
    if (ntohl(thisip) == 0x7f000001) continue; // no 127.0.0.1 please
    if ((thisip & prefermask) == preferip) return thisip;
  }
  // did not find preferred ip, return first one
   return firstip;
}


// Return printable string for given ip.
// Returned value is overwritten on each call.
char *IPMisc::ipToStr(u32 ip){
  static char retval[16];
  u8 a1,a2,a3,a4;
  ip = htonl(ip);
  a1 = ip>>24;
  a2 = ip>>16;
  a3 = ip>>8;
  a4 = ip;
  sprintf(retval, "%d.%d.%d.%d", a1, a2, a3, a4);
  return retval;
}
  

#pragma once

#include <sys/socket.h>
#include <errno.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <cstring>
#include <string>

namespace xyz {
namespace {

std::string GetIP(std::string host) {
  struct hostent* phe = gethostbyname(host.c_str());
  for (int i = 0; phe->h_addr_list[i] != 0; i++) {
    struct in_addr addr;
    memcpy(&addr, phe->h_addr_list[i], sizeof(struct in_addr));
    std::string ret = inet_ntoa(addr);
    return ret;
  }
}

}
}  // namespace xyz


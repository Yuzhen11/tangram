/**
 * From ps-lite
 */
#pragma once

#include <unistd.h>
#ifdef _MSC_VER
#include <tchar.h>
#include <winsock2.h>
#include <windows.h>
#include <iphlpapi.h>
#undef interface
#else
#include <net/if.h>
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#endif
#include <string>

namespace xyz {
namespace third_party {

/**
 * \brief return the IP address for given interface eth0, eth1, ...
 */
void GetIP(const std::string& interface, std::string* ip);


/**
 * \brief return the IP address and Interface the first interface which is not
 * loopback
 *
 * only support IPv4
 */
void GetAvailableInterfaceAndIP(std::string* interface, std::string* ip);

/**
 * \brief return an available port on local machine
 *
 * only support IPv4
 * \return 0 on failure
 */
int GetAvailablePort();

}  // namespace third_party
}  // namespace xyz
#include "ifaddrs.h"

#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef _WIN32
#include <arpa/inet.h>
#include <net/if.h>
#include <ifaddrs.h>
#include <endian.h>
#include <sys/types.h>
#include <sys/socket.h>
#else
#include <winsock2.h>
#include <ws2ipdef.h>
#include <ws2tcpip.h>
#endif

#define DISCOVERY_MULTICAST_GROUP "ff12:b6a4:6b1f:969:caee:acc2:5c93:73e1"

uint32_t * join_multicast(int fd, size_t * count)
{
	size_t capacity = 16;
	*count = 0;
	uint32_t * interfaces = malloc(sizeof(uint32_t) * capacity);

#ifdef _WIN32
	interfaces[0] = 0;
	*count = 1;
#else
	struct ifaddrs * addrs;
	if (getifaddrs(&addrs) < 0)
		return 0;

	for (struct ifaddrs * ifa = addrs; ifa; ifa = ifa->ifa_next) {
		if( ifa->ifa_addr && ifa->ifa_addr->sa_family == AF_INET6 &&
				! (ifa->ifa_flags & IFF_LOOPBACK) &&
				(ifa->ifa_flags & IFF_MULTICAST) &&
				! IN6_IS_ADDR_LINKLOCAL( & ((struct sockaddr_in6 *) ifa->ifa_addr)->sin6_addr ) ){
			int idx = if_nametoindex(ifa->ifa_name);

			bool seen = false;
			for (size_t i = 0; i < *count; i++) {
				if (interfaces[i] == idx) {
					seen = true;
					break;
				}
			}
			if (seen)
				continue;

			if (*count + 1 >= capacity) {
				capacity *= 2;
				uint32_t * nret = realloc(interfaces, sizeof(uint32_t) * capacity);
				if (nret) {
					interfaces = nret;
				} else {
					free(interfaces);
					*count = 0;
					return NULL;
				}
			}

			interfaces[*count] = idx;
			(*count)++;
		}
	}

	freeifaddrs(addrs);
#endif

	for (size_t i = 0; i < *count; i++) {
		struct ipv6_mreq group;
		group.ipv6mr_interface = interfaces[i];
		inet_pton(AF_INET6, DISCOVERY_MULTICAST_GROUP, &group.ipv6mr_multiaddr);
		int ret = setsockopt(fd, IPPROTO_IPV6, IPV6_ADD_MEMBERSHIP,
				(const void *) &group, sizeof(group));
		if (ret < 0)
			fprintf(stderr, "IPV6_ADD_MEMBERSHIP failed: %s\n", strerror(errno));
	}

	return interfaces;
}

#ifndef _WIN32

uint32_t * broadcast_addresses(void)
{
	struct ifaddrs * addrs;
	if (getifaddrs(&addrs) < 0)
		return 0;

	size_t capacity = 16, count = 0;
	uint32_t * ret = malloc(sizeof(uint32_t) * capacity);

	for (struct ifaddrs * ifa = addrs; ifa; ifa = ifa->ifa_next) {
		if (ifa->ifa_addr && ifa->ifa_addr->sa_family == AF_INET &&
				ifa->ifa_flags & IFF_BROADCAST) {
			if (count + 2 >= capacity) {
				capacity *= 2;
				uint32_t * nret = realloc(ret, sizeof(uint32_t) * capacity);
				if (nret) {
					ret = nret;
				} else {
					free(ret);
					return 0;
				}
			}

			ret[count] = ((struct sockaddr_in*)ifa->ifa_broadaddr)->sin_addr.s_addr;
			count++;
		}
	}

	freeifaddrs(addrs);
	ret[count] = 0;
	return ret;
}

#else // _WIN32

#include <winsock2.h>
#include <ws2tcpip.h>

#pragma comment(lib, "ws2_32.lib")

uint32_t * broadcast_addresses(void)
{
	uint32_t * ret = NULL;
	SOCKET wsock = INVALID_SOCKET;

	struct WSAData wsaData;
	if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0)
		return NULL;

	wsock = WSASocket(AF_INET, SOCK_DGRAM, IPPROTO_UDP, NULL, 0, 0);
	if (wsock == INVALID_SOCKET)
		goto cleanup;

	INTERFACE_INFO InterfaceList[32];
	unsigned long nBytesReturned;

	if (WSAIoctl(wsock, SIO_GET_INTERFACE_LIST, 0, 0,
				InterfaceList, sizeof(InterfaceList),
				&nBytesReturned, 0, 0) == SOCKET_ERROR)
		goto cleanup;

	int numInterfaces = nBytesReturned / sizeof(INTERFACE_INFO);

	size_t capacity = 16, count = 0;
	ret = malloc(sizeof(uint32_t) * capacity);

	for (int i = 0; i < numInterfaces && count < capacity - 1; i++)
		if (InterfaceList[i].iiFlags & IFF_BROADCAST)
			ret[count++] = InterfaceList[i].iiBroadcastAddress.AddressIn.sin_addr.s_addr;

	ret[count] = 0;
cleanup:
	if (wsock != INVALID_SOCKET)
		closesocket(wsock);
	WSACleanup();

	return ret;
}

#endif

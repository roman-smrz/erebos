#include "ifaddrs.h"

#ifndef _WIN32

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <net/if.h>
#include <stdlib.h>
#include <sys/types.h>
#include <endian.h>

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

#include "ifaddrs.h"

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

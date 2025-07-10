#include <stddef.h>
#include <stdint.h>

#ifndef _WIN32
#include <sys/socket.h>
#else
#include <winsock2.h>
#endif

struct InetAddress
{
	int family;
	uint8_t addr[16];
} __attribute__((packed));

uint32_t * erebos_join_multicast(int fd, size_t * count);
struct InetAddress * erebos_local_addresses( size_t * count );
uint32_t * erebos_broadcast_addresses(void);

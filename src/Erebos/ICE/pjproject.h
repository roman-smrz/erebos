#pragma once

#include <pjnath.h>
#include <HsFFI.h>

struct erebos_ice_cfg * erebos_ice_cfg_create( const char * stun_server, uint16_t stun_port,
		const char * turn_server, uint16_t turn_port );
void erebos_ice_cfg_free( struct erebos_ice_cfg * cfg );
void erebos_ice_cfg_stop_thread( struct erebos_ice_cfg * cfg );

pj_ice_strans * erebos_ice_create( const struct erebos_ice_cfg *, pj_ice_sess_role role,
		HsStablePtr sptr, HsStablePtr cb );
void erebos_ice_destroy(pj_ice_strans * strans);

ssize_t erebos_ice_encode_session(pj_ice_strans *, char * ufrag, char * pass,
		char * def, char * candidates[], size_t maxlen, size_t maxcand);
void erebos_ice_connect(pj_ice_strans * strans, HsStablePtr cb,
		const char * ufrag, const char * pass,
		const char * defcand, const char * candidates[], size_t ncand);
void erebos_ice_send(pj_ice_strans *, const char * data, size_t len);

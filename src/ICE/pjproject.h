#pragma once

#include <pjnath.h>
#include <HsFFI.h>

pj_ice_strans * ice_create(pj_ice_sess_role role, HsStablePtr sptr, HsStablePtr cb);
void ice_destroy(pj_ice_strans * strans);

ssize_t ice_encode_session(pj_ice_strans *, char * ufrag, char * pass,
		char * def, char * candidates[], size_t maxlen, size_t maxcand);
void ice_connect(pj_ice_strans * strans, HsStablePtr cb,
		const char * ufrag, const char * pass,
		const char * defcand, const char * candidates[], size_t ncand);
void ice_send(pj_ice_strans *, const char * data, size_t len);

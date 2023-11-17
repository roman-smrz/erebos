#include "pjproject.h"
#include "Erebos/ICE_stub.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <pthread.h>
#include <pjlib.h>
#include <pjlib-util.h>

static struct
{
	pj_caching_pool cp;
	pj_pool_t * pool;
	pj_ice_strans_cfg cfg;
	pj_sockaddr def_addr;
} ice;

struct user_data
{
	pj_ice_sess_role role;
	HsStablePtr sptr;
	HsStablePtr cb_init;
	HsStablePtr cb_connect;
};

static void ice_perror(const char * msg, pj_status_t status)
{
	char err[PJ_ERR_MSG_SIZE];
	pj_strerror(status, err, sizeof(err));
	fprintf(stderr, "ICE: %s: %s\n", msg, err);
}

static int ice_worker_thread(void * unused)
{
	PJ_UNUSED_ARG(unused);

	while (true) {
		pj_time_val max_timeout = { 0, 0 };
		pj_time_val timeout = { 0, 0 };

		max_timeout.msec = 500;

		pj_timer_heap_poll(ice.cfg.stun_cfg.timer_heap, &timeout);

		pj_assert(timeout.sec >= 0 && timeout.msec >= 0);
		if (timeout.msec >= 1000)
			timeout.msec = 999;

		if (PJ_TIME_VAL_GT(timeout, max_timeout))
			timeout = max_timeout;

		int c = pj_ioqueue_poll(ice.cfg.stun_cfg.ioqueue, &timeout);
		if (c < 0)
			pj_thread_sleep(PJ_TIME_VAL_MSEC(timeout));
	}

	return 0;
}

static void cb_on_rx_data(pj_ice_strans * strans, unsigned comp_id,
		void * pkt, pj_size_t size,
		const pj_sockaddr_t * src_addr, unsigned src_addr_len)
{
	struct user_data * udata = pj_ice_strans_get_user_data(strans);
	ice_rx_data(udata->sptr, pkt, size);
}

static void cb_on_ice_complete(pj_ice_strans * strans,
		pj_ice_strans_op op, pj_status_t status)
{
	if (status != PJ_SUCCESS) {
		ice_perror("cb_on_ice_complete", status);
		ice_destroy(strans);
		return;
	}

	struct user_data * udata = pj_ice_strans_get_user_data(strans);
	if (op == PJ_ICE_STRANS_OP_INIT) {
		pj_status_t istatus = pj_ice_strans_init_ice(strans, udata->role, NULL, NULL);
		if (istatus != PJ_SUCCESS)
			ice_perror("error creating session", istatus);

		if (udata->cb_init) {
			ice_call_cb(udata->cb_init);
			hs_free_stable_ptr(udata->cb_init);
			udata->cb_init = NULL;
		}
	}

	if (op == PJ_ICE_STRANS_OP_NEGOTIATION) {
		if (udata->cb_connect) {
			ice_call_cb(udata->cb_connect);
			hs_free_stable_ptr(udata->cb_connect);
			udata->cb_connect = NULL;
		}
	}
}

static void ice_init(void)
{
	static bool done = false;
	static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
	pthread_mutex_lock(&mutex);

	if (done) {
		pthread_mutex_unlock(&mutex);
		goto exit;
	}

	pj_log_set_level(1);

	if (pj_init() != PJ_SUCCESS) {
		fprintf(stderr, "pj_init failed\n");
		goto exit;
	}
	if (pjlib_util_init() != PJ_SUCCESS) {
		fprintf(stderr, "pjlib_util_init failed\n");
		goto exit;
	}
	if (pjnath_init() != PJ_SUCCESS) {
		fprintf(stderr, "pjnath_init failed\n");
		goto exit;
	}

	pj_caching_pool_init(&ice.cp, NULL, 0);

	pj_ice_strans_cfg_default(&ice.cfg);
	ice.cfg.stun_cfg.pf = &ice.cp.factory;

	ice.pool = pj_pool_create(&ice.cp.factory, "ice", 512, 512, NULL);

	if (pj_timer_heap_create(ice.pool, 100,
				&ice.cfg.stun_cfg.timer_heap) != PJ_SUCCESS) {
		fprintf(stderr, "pj_timer_heap_create failed\n");
		goto exit;
	}

	if (pj_ioqueue_create(ice.pool, 16, &ice.cfg.stun_cfg.ioqueue) != PJ_SUCCESS) {
		fprintf(stderr, "pj_ioqueue_create failed\n");
		goto exit;
	}

	pj_thread_t * thread;
	if (pj_thread_create(ice.pool, "ice", &ice_worker_thread,
				NULL, 0, 0, &thread) != PJ_SUCCESS) {
		fprintf(stderr, "pj_thread_create failed\n");
		goto exit;
	}

	ice.cfg.af = pj_AF_INET();
	ice.cfg.opt.aggressive = PJ_TRUE;

	ice.cfg.stun.server.ptr = "discovery1.erebosprotocol.net";
	ice.cfg.stun.server.slen = strlen(ice.cfg.stun.server.ptr);
	ice.cfg.stun.port = 29670;

	ice.cfg.turn.server = ice.cfg.stun.server;
	ice.cfg.turn.port = ice.cfg.stun.port;
	ice.cfg.turn.auth_cred.type = PJ_STUN_AUTH_CRED_STATIC;
	ice.cfg.turn.auth_cred.data.static_cred.data_type = PJ_STUN_PASSWD_PLAIN;
	ice.cfg.turn.conn_type = PJ_TURN_TP_UDP;

exit:
	done = true;
	pthread_mutex_unlock(&mutex);
}

pj_ice_strans * ice_create(pj_ice_sess_role role, HsStablePtr sptr, HsStablePtr cb)
{
	ice_init();

	pj_ice_strans * res;

	struct user_data * udata = malloc(sizeof(struct user_data));
	udata->role = role;
	udata->sptr = sptr;
	udata->cb_init = cb;

	pj_ice_strans_cb icecb = {
		.on_rx_data = cb_on_rx_data,
		.on_ice_complete = cb_on_ice_complete,
	};

	pj_status_t status = pj_ice_strans_create(NULL, &ice.cfg, 1,
			udata, &icecb, &res);

	if (status != PJ_SUCCESS)
		ice_perror("error creating ice", status);

	return res;
}

void ice_destroy(pj_ice_strans * strans)
{
	struct user_data * udata = pj_ice_strans_get_user_data(strans);
	if (udata->sptr)
		hs_free_stable_ptr(udata->sptr);
	if (udata->cb_init)
		hs_free_stable_ptr(udata->cb_init);
	if (udata->cb_connect)
		hs_free_stable_ptr(udata->cb_connect);
	free(udata);

	pj_ice_strans_stop_ice(strans);
	pj_ice_strans_destroy(strans);
}

ssize_t ice_encode_session(pj_ice_strans * strans, char * ufrag, char * pass,
		char * def, char * candidates[], size_t maxlen, size_t maxcand)
{
	int n;
	pj_str_t local_ufrag, local_pwd;
	pj_status_t status;

	pj_ice_strans_get_ufrag_pwd(strans, &local_ufrag, &local_pwd, NULL, NULL);

	n = snprintf(ufrag, maxlen, "%.*s", (int) local_ufrag.slen, local_ufrag.ptr);
	if (n < 0 || n == maxlen)
		return -PJ_ETOOSMALL;

	n = snprintf(pass, maxlen, "%.*s", (int) local_pwd.slen, local_pwd.ptr);
	if (n < 0 || n == maxlen)
		return -PJ_ETOOSMALL;

	pj_ice_sess_cand cand[PJ_ICE_ST_MAX_CAND];
	char ipaddr[PJ_INET6_ADDRSTRLEN];

	status = pj_ice_strans_get_def_cand(strans, 1, &cand[0]);
	if (status != PJ_SUCCESS)
		return -status;

	n = snprintf(def, maxlen, "%s %d",
			pj_sockaddr_print(&cand[0].addr, ipaddr, sizeof(ipaddr), 0),
			(int) pj_sockaddr_get_port(&cand[0].addr));
	if (n < 0 || n == maxlen)
		return -PJ_ETOOSMALL;

	unsigned cand_cnt = PJ_ARRAY_SIZE(cand);
	status = pj_ice_strans_enum_cands(strans, 1, &cand_cnt, cand);
	if (status != PJ_SUCCESS)
		return -status;

	for (unsigned i = 0; i < cand_cnt && i < maxcand; i++) {
		char ipaddr[PJ_INET6_ADDRSTRLEN];
		n = snprintf(candidates[i], maxlen,
				"%.*s %u %s %u %s",
				(int) cand[i].foundation.slen, cand[i].foundation.ptr,
				cand[i].prio,
				pj_sockaddr_print(&cand[i].addr, ipaddr, sizeof(ipaddr), 0),
				(unsigned) pj_sockaddr_get_port(&cand[i].addr),
				pj_ice_get_cand_type_name(cand[i].type));

		if (n < 0 || n == maxlen)
			return -PJ_ETOOSMALL;
	}

	return cand_cnt;
}

void ice_connect(pj_ice_strans * strans, HsStablePtr cb,
		const char * ufrag, const char * pass,
		const char * defcand, const char * tcandidates[], size_t ncand)
{
	unsigned def_port = 0;
	char     def_addr[80];
	pj_bool_t done = PJ_FALSE;
	char line[256];
	pj_ice_sess_cand candidates[PJ_ICE_ST_MAX_CAND];

	struct user_data * udata = pj_ice_strans_get_user_data(strans);
	udata->cb_connect = cb;

	def_addr[0] = '\0';

	if (ncand == 0) {
		fprintf(stderr, "ICE: no candidates\n");
		return;
	}

	int cnt = sscanf(defcand, "%s %u", def_addr, &def_port);
	if (cnt != 2) {
		fprintf(stderr, "ICE: error parsing default candidate\n");
		return;
	}

	int okcand = 0;
	for (int i = 0; i < ncand; i++) {
		char foundation[32], ipaddr[80], type[32];
		int prio, port;

		int cnt = sscanf(tcandidates[i], "%s %d %s %d %s",
				foundation, &prio,
				ipaddr, &port,
				type);
		if (cnt != 5)
			continue;

		pj_ice_sess_cand * cand = &candidates[okcand];
		pj_bzero(cand, sizeof(*cand));

		if (strcmp(type, "host") == 0)
			cand->type = PJ_ICE_CAND_TYPE_HOST;
		else if (strcmp(type, "srflx") == 0)
			cand->type = PJ_ICE_CAND_TYPE_SRFLX;
		else if (strcmp(type, "relay") == 0)
			cand->type = PJ_ICE_CAND_TYPE_RELAYED;
		else
			continue;

		cand->comp_id = 1;
		pj_strdup2(ice.pool, &cand->foundation, foundation);
		cand->prio = prio;

		int af = strchr(ipaddr, ':') ? pj_AF_INET6() : pj_AF_INET();
		pj_str_t tmpaddr = pj_str(ipaddr);
		pj_sockaddr_init(af, &cand->addr, NULL, 0);
		pj_status_t status = pj_sockaddr_set_str_addr(af, &cand->addr, &tmpaddr);
		if (status != PJ_SUCCESS) {
			fprintf(stderr, "ICE: invalid IP address \"%s\"\n", ipaddr);
			continue;
		}

		pj_sockaddr_set_port(&cand->addr, (pj_uint16_t)port);
		okcand++;
	}

	pj_str_t tmp_addr;
	pj_status_t status;

	int af = strchr(def_addr, ':') ? pj_AF_INET6() : pj_AF_INET();

	pj_sockaddr_init(af, &ice.def_addr, NULL, 0);
	tmp_addr = pj_str(def_addr);
	status = pj_sockaddr_set_str_addr(af, &ice.def_addr, &tmp_addr);
	if (status != PJ_SUCCESS) {
		fprintf(stderr, "ICE: invalid default IP address \"%s\"\n", def_addr);
		return;
	}
	pj_sockaddr_set_port(&ice.def_addr, (pj_uint16_t) def_port);

	pj_str_t rufrag, rpwd;
	status = pj_ice_strans_start_ice(strans,
			pj_cstr(&rufrag, ufrag), pj_cstr(&rpwd, pass),
			okcand, candidates);
	if (status != PJ_SUCCESS) {
		ice_perror("error starting ICE", status);
		return;
	}
}

void ice_send(pj_ice_strans * strans, const char * data, size_t len)
{
	if (!pj_ice_strans_sess_is_complete(strans)) {
		fprintf(stderr, "ICE: negotiation has not been started or is in progress\n");
		return;
	}

	pj_status_t status = pj_ice_strans_sendto(strans, 1, data, len,
			&ice.def_addr, pj_sockaddr_get_len(&ice.def_addr));
	if (status != PJ_SUCCESS && status != PJ_EPENDING)
		ice_perror("error sending data", status);
}

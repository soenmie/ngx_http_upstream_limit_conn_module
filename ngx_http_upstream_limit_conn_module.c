#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

typedef struct {
    ngx_str_t*          name;
    ngx_int_t           conns;
} ngx_http_upstream_peer_conn_ctx_t;

typedef struct {
    ngx_int_t                           max_conns;
    ngx_uint_t                           tries;
    ngx_http_upstream_peer_conn_ctx_t*  peer_conns;
    ngx_http_upstream_init_pt           base_init_upstream;
    ngx_http_upstream_init_peer_pt      base_init;
    ngx_event_get_peer_pt               base_get;
    ngx_event_free_peer_pt              base_free;
    void*                               data;
} ngx_http_upstream_limit_conn_conf_t;

static char *ngx_http_upstream_limit_conn(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);

static ngx_int_t ngx_http_upstream_init_limit_conn(ngx_conf_t* cf,
        ngx_http_upstream_srv_conf_t* us);

static ngx_int_t
ngx_http_upstream_init_limit_conn_peer(ngx_http_request_t* r,
                                       ngx_http_upstream_srv_conf_t* us);

static ngx_int_t
ngx_http_upstream_get_limit_conn_peer(ngx_peer_connection_t* pc, void* data);

static void
ngx_http_upstream_free_limit_conn_peer(ngx_peer_connection_t* pc, void* data, ngx_uint_t state);

static void* ngx_http_upstream_limit_conn_create_svr_conf(ngx_conf_t* cf);

static ngx_command_t ngx_http_upstream_limit_conn_commands[] = {

    {   ngx_string("limit"), /* directive */
        NGX_HTTP_UPS_CONF | NGX_CONF_NOARGS | NGX_CONF_TAKE123, /* upstream context
                      and takes no more than 4 arguments */
        ngx_http_upstream_limit_conn, /* configuration setup function */
        0, /* No offset. Only one context is supported. */
        0, /* No offset when storing the module configuration on struct. */
        NULL
    },

    ngx_null_command /* command termination */
};


/* The module context. */
static ngx_http_module_t ngx_http_upstream_limit_conn_module_ctx = {
    NULL, /* preconfiguration */
    NULL, /* postconfiguration */

    NULL, /* create main configuration */
    NULL, /* init main configuration */

    ngx_http_upstream_limit_conn_create_svr_conf, /* create server configuration */
    NULL, /* merge server configuration */

    NULL, /* create location configuration */
    NULL /* merge location configuration */
};

/* Module definition. */
ngx_module_t ngx_http_upstream_limit_conn_module = {
    NGX_MODULE_V1,
    &ngx_http_upstream_limit_conn_module_ctx, /* module context */
    ngx_http_upstream_limit_conn_commands, /* module directives */
    NGX_HTTP_MODULE, /* module type */
    NULL, /* init master */
    NULL, /* init module */
    NULL, /* init process */
    NULL, /* init thread */
    NULL, /* exit thread */
    NULL, /* exit process */
    NULL, /* exit master */
    NGX_MODULE_V1_PADDING
};

static void* ngx_http_upstream_limit_conn_create_svr_conf(ngx_conf_t* cf) {
    ngx_http_upstream_limit_conn_conf_t* conf;
    conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_upstream_limit_conn_conf_t));
    if (conf == NULL) {
        return NULL;
    }
    conf->max_conns = NGX_CONF_UNSET;
    conf->tries = NGX_CONF_UNSET_UINT;
    conf->peer_conns = NGX_CONF_UNSET_PTR;
    conf->base_init_upstream = NGX_CONF_UNSET_PTR;
    conf->base_init = NGX_CONF_UNSET_PTR;
    conf->base_get = NGX_CONF_UNSET_PTR;

    return conf;
}

static ngx_int_t
ngx_http_upstream_init_limit_conn(ngx_conf_t* cf,
                                  ngx_http_upstream_srv_conf_t* us) {
    ngx_http_upstream_limit_conn_conf_t*      conf;

    ngx_http_upstream_rr_peers_t*             peers;
    ngx_uint_t                                i, total;
    conf = ngx_http_conf_upstream_srv_conf(us, ngx_http_upstream_limit_conn_module);
    if (conf->base_init_upstream(cf, us) != NGX_OK) {
        return NGX_ERROR;
    }
    conf->base_init = us->peer.init;
    us->peer.init = ngx_http_upstream_init_limit_conn_peer;
    peers = us->peer.data;
    total = peers->number + (peers->next ? peers->next->number : 0);
    if (conf->tries == 0) {
        conf->tries = total;
    }
    conf->peer_conns = ngx_pcalloc(cf->pool, total * sizeof(ngx_http_upstream_peer_conn_ctx_t));
    for (i = 0; i < total; ++i) {
        ngx_http_upstream_rr_peer_t* peer;
        time_t                       now;
        peer = (i < peers->number) ? &peers->peer[i]: &peers->next->peer[i - peers->number];
        now = ngx_time();
        conf->peer_conns[i].name = &peer->name;
        conf->peer_conns[i].conns = 0;
    }
    return NGX_OK;
}

static ngx_int_t
ngx_http_upstream_init_limit_conn_peer(ngx_http_request_t* r,
                                       ngx_http_upstream_srv_conf_t* us) {
    ngx_http_upstream_limit_conn_conf_t*      conf;
    conf = ngx_http_conf_upstream_srv_conf(us, ngx_http_upstream_limit_conn_module);
    if (conf->base_init(r, us) != NGX_OK) {
        return NGX_ERROR;
    }
    conf->base_get = r->upstream->peer.get;
    conf->base_free = r->upstream->peer.free;
    conf->data = r->upstream->peer.data;

    r->upstream->peer.get = ngx_http_upstream_get_limit_conn_peer;
    r->upstream->peer.free = ngx_http_upstream_free_limit_conn_peer;
    r->upstream->peer.data = conf;
    return NGX_OK;
}

static ngx_int_t
ngx_http_upstream_get_limit_conn_peer(ngx_peer_connection_t* pc, void* data) {
    ngx_http_upstream_limit_conn_conf_t*     conf;
    ngx_http_upstream_rr_peer_data_t*        rrp;
    ngx_http_upstream_rr_peers_t*            peers;
    uintptr_t                                m;
    ngx_uint_t                                i, j, n, total;

    conf = data;
    rrp = conf->data;
    peers = rrp->peers;
    total = peers->number + (peers->next ? peers->next->number : 0);
    for (i = 0; i < conf->tries; ++i) {
        ngx_int_t   rc;
        ngx_str_t*  name;
        rc = conf->base_get(pc, rrp);
        if (rc != NGX_OK) {
            return rc;
        }
        name = pc->name;
        for (j = 0; j < total; ++j) {
            if (name == conf->peer_conns[j].name) {
                break;
            }
        }
        if (conf->peer_conns[j].conns >= conf->max_conns) {
            if (j > peers->number) {
                j -= peers->number;
            }
            n = j / (8 * sizeof(uintptr_t));
            m = (uintptr_t) 1 << j % (8 * sizeof(uintptr_t));
            if (rrp->tried[n] & m) {
                continue;
            }
        } else {
            ++conf->peer_conns[j].conns;
            break;
        }
    }
    if (i == conf->tries) {
        return NGX_BUSY;
    }
    return NGX_OK;
}

static void
ngx_http_upstream_free_limit_conn_peer(ngx_peer_connection_t* pc, void* data, ngx_uint_t state) {
    ngx_http_upstream_limit_conn_conf_t*      conf;
    ngx_http_upstream_rr_peer_data_t*         rrp;
    ngx_http_upstream_rr_peers_t*             peers;
    ngx_str_t*                                name;
    ngx_int_t                                 i, total;

    conf = data;
    rrp = conf->data;
    peers = rrp->peers;
    total = peers->number + (peers->next ? peers->next->number : 0);
    name = pc->name;
    for (i = 0; i < total; ++i) {
        if (name == conf->peer_conns[i].name) {
            --conf->peer_conns[i].conns;
            break;
        }
    }
    conf->base_free(pc, conf->data, state);
}
/**
 * Configuration setup function that installs the content handler.
 *
 * @param cf
 *   Module configuration structure pointer.
 * @param cmd
 *   Module directives structure pointer.
 * @param conf
 *   Module configuration structure pointer.
 * @return string
 *   Status of the configuration setup.
 */
static char *ngx_http_upstream_limit_conn(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{

    ngx_http_upstream_srv_conf_t*        uscf = NULL; /* pointer to uptream configuration */
    ngx_http_upstream_limit_conn_conf_t* ulccf = NULL;
    ngx_str_t*                           value = NULL;
    ngx_uint_t                           i;

    uscf = ngx_http_conf_get_module_srv_conf(cf, ngx_http_upstream_module);
    ulccf = ngx_http_conf_get_module_srv_conf(cf, ngx_http_upstream_limit_conn_module);

    if (uscf->peer.init_upstream) {
        ulccf->base_init_upstream = uscf->peer.init_upstream;
    } else {
        uscf->flags = NGX_HTTP_UPSTREAM_CREATE
                      | NGX_HTTP_UPSTREAM_WEIGHT
                      | NGX_HTTP_UPSTREAM_MAX_FAILS
                      | NGX_HTTP_UPSTREAM_FAIL_TIMEOUT
                      | NGX_HTTP_UPSTREAM_DOWN;
        ulccf->base_init_upstream = ngx_http_upstream_init_round_robin;
    }

    uscf->peer.init_upstream = ngx_http_upstream_init_limit_conn;

    value = cf->args->elts;

    for (i = 1; i < cf->args->nelts; i++) {
        if (ngx_strncmp(value[i].data, "max_conns=", 10) == 0) {
            ulccf->max_conns = ngx_atoi(value[i].data + 10, value[i].len - 10);
            if (ulccf->max_conns == NGX_ERROR) {
                return NGX_CONF_ERROR;
            }
            continue;
        }
        if (ngx_strncmp(value[i].data, "tries=", 6) == 0) {
            ulccf->tries = ngx_atoi(value[i].data + 6, value[i].len - 6);
            if (ulccf->tries == (ngx_uint_t)NGX_ERROR) {
                return NGX_CONF_ERROR;
            }
        }
    }
    if (ulccf->max_conns == NGX_CONF_UNSET) {
        ulccf->max_conns = 1000000;
    }
    if (ulccf->tries == NGX_CONF_UNSET_UINT) {
        ulccf->tries = 0;
    }
    return NGX_CONF_OK;
} /* ngx_http_upstream_limit_conn */

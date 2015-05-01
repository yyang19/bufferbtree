#ifndef _NODE_HEADER_
#define _NODE_HEADER_

node_t * node_create( bft_t *t, node_t *p, int type );
void node_free( bft_t *t, node_t *n );
STATUS node_accept( bft_t *t, node_t *dst_node, bft_req_t *start, int count );
STATUS node_add_key( bft_t *t, node_t *node, int i, int new_key );
#endif

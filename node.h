#ifndef _NODE_HEADER_
#define _NODE_HEADER_

node_t * node_create( bft_t *t, node_t *p, int type );
void node_free( bft_t *t, node_t *n );

#endif

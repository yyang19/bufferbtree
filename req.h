#ifndef _REQ_HEADER_
#define _REQ_HEADER_

bft_req_t * req_create( int key, void *val, bt_op_t op );
void  req_free( bft_t *tree, bft_req_t *r );
void req_list_free( bft_req_t *head );
int  request_comp( const void *a, const void *b );

bft_req_t * request_get( key_compare_func comp, bft_req_t *start, int key, int *exact );
int request_collect( bft_t *t, bft_req_t *req );
#endif

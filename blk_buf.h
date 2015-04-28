#ifndef _BLK_BUF_HEADER_
#define _BLK_BUF_HEADER_

blk_buffer_t * bb_create(void);
void bb_free( bft_t *t, blk_buffer_t *bb );
void bb_reset( blk_buffer_t *bb );
int bb_insert( bft_t *t, node_t *n, blk_buffer_t *bb );
void bb_emptying( bft_t *t, node_t *n );

#endif

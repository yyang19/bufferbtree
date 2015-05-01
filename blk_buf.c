#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include "bftree.h"
#include "core.h"
#include "req.h"
#include "quicksort_ll.h"

blk_buffer_t *
bb_create(void)
{
    blk_buffer_t *bb;

    bb= (blk_buffer_t *)malloc(sizeof(struct req_list));
    bb->req_first = NULL;
    bb->req_count = 0;

    return bb;
}

void 
bb_update( node_t *n, blk_buffer_t *bb, int c_idx )
{

}

void 
bb_reset( blk_buffer_t *bb )
{
    bb->req_first = NULL;
    bb->req_count = 0;
}

void
bb_free( bft_t *tree, blk_buffer_t *bb )
{
    bft_req_t *curr, *prev;

    curr = bb->req_first;

    while(curr){
        prev = curr;
        curr = curr->next;
        req_free( tree, prev );
    };

    free(bb);
}

void
bb_emptying( bft_t *t, node_t *n )
{
    int i;
    bft_req_t *sorted = NULL;
    assert( n->bb_size == t->m );

    for( i = 0; i < t->m; i++ ){
        t->opts->read_node_buffer( n,i ); //read from disk
        quicksort_ll( &n->buffers[i]->req_first, &request_comp );
        sorted = mergelists( sorted, n->buffers[i]->req_first, &request_comp );
    }

    if( !sorted )
        return;

    //dump to child nodes
    if( n->type == LEAF_NODE )
        flush( t, n, sorted );
    else
        cascade( t, n, sorted ); 

    for( i = 0; i < n->bb_size; i++ )
        bb_reset( n->buffers[i] );
    
    n->bb_size = 0;
}


int 
bb_insert( bft_t *t, node_t *n, blk_buffer_t *bb )
{
    int i;
    int ret = 0;
    bft_req_t *src;
    bft_req_t **dest;

    i = n->bb_size;
       
    assert( n->bb_size < t->m );

    src = bb->req_first;
    dest = &n->buffers[i]->req_first;

    while(src){
        *dest = src;
        src = src->next;
        dest = &( (*dest)->next );
    };
    
    // one I/O to write one block buffer
    t->opts->write_node_buffer( n,i );

    n->buffers[i]->req_count = bb->req_count;

    ++ n->bb_size;

    if( n->bb_size == t->m )
        bb_emptying( t, n );

    return ret;
}




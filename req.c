#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <sys/time.h>

#include "bftree.h"
#include "blk_buf.h"
#include "util.h"

bft_req_t *
req_create( int key, void *val, bt_op_t op )
{
    bft_req_t *r;

    r =(bft_req_t *)malloc( sizeof(struct request) );
    r->key = key;
    r->val = val;
    r->next = NULL;
    r->type = op;
    gettimeofday( &r->tm, NULL );

    return r;
}

void 
req_free( bft_t *tree, bft_req_t *r )
{
    free(r);
    r = NULL;
}

void
req_list_free( bft_req_t *head )
{
    bft_req_t *prev, *curr;
    
    curr = head;

    while( curr ){
        prev = curr;
        curr = curr->next;
        free( prev );
    };

    return;
}

void 
req_override( bft_t *tree, bft_req_t *old, bft_req_t *new)
{
    void *temp;
    temp = old->val;
    old->val = new->val;
    new->val = temp;
    
    if ( old->type == BT_PUT)
        tree->put_req_count--;
    else if( old->type == BT_DEL )
        tree->del_req_count--;

    old->type = new->type;

    req_free(tree, new);
}

int 
request_comp( const void *a, const void *b ){
    
    bft_req_t *ra, *rb;

    ra = (bft_req_t *)a;
    rb = (bft_req_t *)b;

    return ra->key == rb->key ? timevaldiff(&ra->tm,&rb->tm)<0 : ra->key>rb->key;
}

bft_req_t *
request_get( key_compare_func comp, bft_req_t *start, int key, int *exact )
{
    bft_req_t *curr_req, *prev_req;
    int compared;

    prev_req = NULL;
    curr_req = start;
    *exact = 0;

    while( curr_req ){
        compared = comp( (void *)&key, (void *)&curr_req->key );

        if( compared < 1 )
            break;

        if( compared == 0 ){
            *exact = 1;
            return curr_req;
        }
        
        prev_req = curr_req;
        curr_req = curr_req->next;
    }

    return prev_req;
}

/*
 * request_collect
 * Description: user requests are buffered (linked) in top_buffer (internal memory) that is of size B.
 *              The elements is written to the root's buffer once it is full
 * */
int
request_collect( bft_t *t, bft_req_t *req )
{
    int ret = 0;

    req->next = t->top_buffer->req_first; 
    t->top_buffer->req_first = req; 
    
    ++t->top_buffer->req_count;

    if( t->top_buffer->req_count == t->c ){ //a block of requests have been collected
        
        ret = bb_insert( t, t->root, t->top_buffer );

        bb_reset( t->top_buffer );
    }
    
    return ret;
}

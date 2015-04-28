#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <sys/time.h>

#include "bftree.h"
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

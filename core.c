/*  core.c
 *  Author: Yue Yang ( yueyang2010@gmail.com )
 *
 *
* Copyright (c) 2015, Yue Yang ( yueyang2010@gmail.com )
*  * All rights reserved.
*  *
*  - Redistribution and use in source and binary forms, with or without
*  modification, are permitted provided that the following conditions are met:
*  Redistributions of source code must retain the above copyright notice,
*  this list of conditions and the following disclaimer.
*
*  - Redistributions in binary form must reproduce the above copyright
*  notice, this list of conditions and the following disclaimer in the
*  documentation and/or other materials provided with the distribution.
*
*  - Neither the name of Redis nor the names of its contributors may be used
*  to endorse or promote products derived from this software without
*  specific prior written permission.
*  
*  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
*  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
*  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
*  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
*  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
*  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
*  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
*  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
*  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
*  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
*  POSSIBILITY OF SUCH DAMAGE.
*                          
*/

#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include "core.h"
#include "util.h"
#include "quicksort_ll.h"

/*------------- object creation and destroy ------------------------*/
void 
req_free( bft_t *tree, bft_req_t *r )
{
    free(r);
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

void
req_dump( bft_req_t *req ){

     printf( "Request dump\n" );
     printf( "Key = %d\t", req->key );
     printf( "Type = %d\t", req->type );
     printf( "Arrival time = <%ld.%06ld>\n", (long int)(req->tm.tv_sec), (long int)(req->tm.tv_usec) );
}

/*
 *  request_comp (a, b)
 *  return 1 : a>b
 *  return 0 : a<b; 
 *  note that a would not be equal to b in the comparison of bft_req_t objects.
 * */
int 
request_comp( const void *a, const void *b ){
    
    bft_req_t *ra, *rb;

    ra = (bft_req_t *)a;
    rb = (bft_req_t *)b;

    return ra->key == rb->key ? timevaldiff(&ra->tm,&rb->tm)<0 : ra->key>rb->key;
}
    
// block buffer
blk_buffer_t *
bb_create(void)
{
    blk_buffer_t *bb;

    bb= (blk_buffer_t *)malloc(sizeof(struct req_list));
    bb->req_first = NULL;
    bb->req_count = 0;

    return bb;
}

static void
bb_free( bft_t *tree, blk_buffer_t *bb )
{
    bft_req_t *curr, *next;

    curr = bb->req_first;

    while(curr){
        next = curr->next;
        req_free( tree, curr );
        curr = next;
    };

    free(bb);
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
bb_dump( blk_buffer_t *bb ){

    bft_req_t *curr = bb->req_first;

    while( curr ){
        req_dump( curr );
        curr = curr->next;
    };

    return;
}


// node
static node_t *
node_create( bft_t *t, node_t *p, int type )
{
    int i;
    node_t *n;
    n = (node_t *) malloc (sizeof(node_t));

    if( n ){
        n->containers = (blk_buffer_t **) malloc ( sizeof(blk_buffer_t *) * t->m );
        if( !n->containers )
            goto fail_containers;

        n->keys = (int *) malloc ( sizeof(int) * t->m );
        if( !n->keys )
            goto fail_keys;

        n->child = (struct node **) malloc ( sizeof(struct node *) * (t->m+1) );
        if( !n->child )
            goto fail_child;

        n->id = ++t->nNode;
        n->type = type;
        n->nElem = 0;
        n->parent = p;
        n->bb_count = t->m;
        n->bb_size = 0;
        n->key_count = 0;
        
        for( i=0; i<t->m; i++ )
            n->containers[i] = bb_create();

        memset( n->keys, 0xff, sizeof(int) * t->m ); 
        memset( n->child, 0x0, sizeof(int) * (t->m + 1) ); 

        goto out;
    }

fail_child:
    free( n->keys );    
fail_keys:
    free( n->containers );
fail_containers:
    free(n);
    n = NULL;
out:
    return n;
}

static void
node_free_single( bft_t *t, node_t *n )
{
    int i;

    for( i=0; i<n->bb_count; i++ )
        bb_free( t, n->containers[i] );

    free( n->containers );
    free( n->keys );
    free( n->child );
    free( n );
}

void
node_free( bft_t *t, node_t *n )
{
    int i;
    node_t *c;

    for( i=0; i<n->bb_size; i++ ){
        c = n->child[i];
        if( c )
            node_free_single( t, c );
    }

    node_free_single( t, n );
}

/*-------------------------- operations ------------------------*/
static void
block_buffer_emptying( bft_t *t, node_t *n )
{
    int i;
    bft_req_t *sorted = NULL;

    assert( n->bb_size == t->m );

    for( i = 0; i < t->m; i++ ){
        t->opts->read( n,i ); //read from disk
        quicksort_ll( &n->containers[i]->req_first, &request_comp );
        sorted = mergelists( sorted, n->containers[i]->req_first, &request_comp );
        quicksort_ll_dump( sorted );
    }

    if( !sorted )
        return;

    //dump to child nodes
}

static int 
block_buffer_insert( bft_t *t, node_t *n, blk_buffer_t *bb )
{
    int i;
    int ret = 0;
    bft_req_t *src;
    bft_req_t **dest;

    i = n->bb_size;
       
    assert( n->bb_size < t->m );

    src = bb->req_first;
    dest = &n->containers[i]->req_first;

    while(src){
        *dest = src;
        src = src->next;
        dest = &( (*dest)->next );
    };
    
    // one I/O to write one block buffer
    t->opts->write( n,i );

    n->containers[i]->req_count = bb->req_count;

    ++ n->bb_size;

    if( n->bb_size == t->m )
        block_buffer_emptying( t, n );

    return ret;
}


// request
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

int
request_collect( bft_t *t, bft_req_t *req )
{
    int ret = 0;

    req->next = t->top_buffer->req_first; 
    t->top_buffer->req_first = req; 
    
    ++t->top_buffer->req_count;

    if( t->top_buffer->req_count == t->c ){ //a block of requests have been collected
        if( !t->root )
            t->root = node_create( t, NULL, LEAF_NODE );
        
        ret = block_buffer_insert( t, t->root, t->top_buffer );

        bb_reset( t->top_buffer );
    }
    return ret;
}

void
block_buffer_destroy( bft_t *t, blk_buffer_t *bb )
{
    bb_free( t, bb );
}

#if 0
// blk_buffer
static uint32_t
bb_find( key_compare_func comp, node_t *n, int key, uint32_t c_start )
{
    int left, right, middle, result, compared;
    blk_buffer_t **containers;

    left = c_start;
    right = n->bb_size;
    compared = 0;
    containers = n->containers;
    
    while( left <= right ){
        middel = (left+right)/2;
        compared = comp( key, containers[middle]->req_first->key );
        if( compared<0 )
            right = middle - 1;
        else if( compared>0 )
            left = middle + 1;
        else{
            right = middle;
            break;
        }
    }   

    if( compared > 0 )
        result = left - 1;
    else
        result = right;

    return result == 0 ? result : right - 1;
}

static bft_req_t *
bb_get( bft_t *t, node_t *n, uint32_t c_idx, int key ){

    bft_req_t *curr;
    containter_t *c;
    int exact;
    int (*comp)(const void *, const void * );

    if( c_idx >= n->bb_size )
        return NULL;

    comp = t->opts->key_compare;
    blk_buffer = n->containers[c_idx];
    curr = request_get( comp, c->request_first, key, &exact );

    if( exact ){
        if( curr->type == BT_PUT )
            return curr;
        return NULL;
    }

    if( c->child ){
        c_idx = bb_find( comp, c->child, key, 0 );
        return bb_get( tree, c->child, c_idx, key );
    }

    return NULL;
}

static blk_buffer_t *
bb_shift_left( node_t *n, uint32_t c_idx )
{
    blk_buffer_t *removed;

    ASSERT( c_idx < n->bb_size );

    removed = n->containers[c_idx];
    memmove( &n->containers[c_idx], &n->containers[c_idx+1], (n->bb_size-c_idx-1)*sizeof(void *) );
    --n->bb_size;

    return removed;
}
#endif

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
#include "node.h"
#include "req.h"
#include "blk_buf.h"
#include "util.h"
#include "quicksort_ll.h"


    
/*-------------------------- operations ------------------------*/
static 
int list_len( bft_req_t *head )
{
    bft_req_t *curr = head;
    int len = 0;

    while( curr ){
        curr = curr->next;
        ++len;
    }

    return len;
}

static bft_req_t *
request_list_construct( bft_t *t, void *payload )
{
    int i;

    bft_req_t *list = (bft_req_t *)payload;
    bft_req_t *req;
    bft_req_t *head = NULL;

    for( i=0; i<t->c; i++ ){
        if( list[i].key != INVALID_KEY ){
            req = req_create( list[i].key, list[i].val, list[i].type );
            req->tm = list[i].tm;
            req->next = head;
            head = req;
        }
    }

    return head;
}

/*
 * enqueue 
 * description: insert a request in the head of the request list
 * */
STATUS
enqueue( bft_t *t, bft_req_t *req, blk_buffer_t *bb )
{
    if( !bb )
        return RET_NODE_BUFFER_NULL;

    if( bb->req_count == t->c )
        return RET_NODE_BUFFER_FULL;

    req->next = bb->req_first;
    bb->req_first = req;

    ++bb->req_count;

    return 0;
}

static STATUS
_sibiling_create( bft_t *tree, node_t *node, int i )
{
    int j;
    node_t *y, *z;

    y = node->child[i];
    
    z = node_create( tree, node, y->type );

    if( y->type == LEAF_NODE ){
        z->next = y->next;
        y->next = z;    
    }
    
    for( j=node->key_count+1; j>i; j-- )
        node->child[j] = node->child[j-1];

    node->child[i+1] = z;

    return 0;
}

STATUS
cascade( bft_t *t, node_t *n, bft_req_t *req ){

    int i;
    bft_req_t *start, *curr;
    int count;

    assert( n->type == INTERNAL_NODE );

    curr = req;
    start = curr;
    i = 0;
    count = 0;
    
    while( curr ){
        
        if( curr->key <= n->keys[i] ){
            ++count;
            curr = curr->next;
            continue;
        }

        if( count > 0 )
            node_accept( t, n->child[i], start, count );
        
        ++i;
        start = curr;
    };
    
    if( count > 0 )
        node_accept( t, n->child[i], start, count );

    return 0;
}

/*
 * _load_node_request 
 * Description : load all kv pairs from the child leaf of a given node, returning a sorted list of loaded requests.
 * */
static bft_req_t *
_load_node_request( bft_t *t, node_t *n ){
    
    int nChild;
    int i;
    void *payload;
    bft_req_t *sorted;
    bft_req_t *leaf_list;

    assert( n->type == LEAF_NODE );

    payload = malloc ( t->B );
    
    if( !payload )
        return NULL;

    nChild = n->key_count > 0 ? n->key_count+1 : 0 ;
    sorted = NULL;

    for( i = 0; i < nChild; i++ ){
        t->opts->read_node( n->child[i], payload, t->B );
        leaf_list = request_list_construct( t, payload );
        quicksort_ll( &leaf_list, &request_comp );
        sorted = mergelists( sorted, leaf_list, &request_comp );
    }

    free( payload );

    return sorted;
}
    
static STATUS
_flush_to_leaf( bft_t *t, node_t *n, bft_req_t *req )
{
    int i, count;
    bft_req_t *curr = req;
    void *payload = malloc ( t->B );

    assert( list_len( req ) <= t->b * t->c );
        
    memset( payload, 0xff, t->B );
    i=0;
    count = 0;

    while(curr){
        ((bft_req_t *)payload)[ (count++) % t->c ] = *curr;
        
        if( count % t->c == 0 || (!curr->next) ){
            if( !n->child[i] )
                n->child[i] = node_create( t, n, LEAF_BLOCK );

            t->opts->write_node( n->child[i], payload, t->B );
            if( curr->next ){
                n->keys[i] = curr->key;
                ++i;
            }
            memset( payload, 0xff, t->B );
        }    
        curr = curr->next;
    } 

    free( payload );

    n->key_count = i;

    // fill the rest of keys
    for( i=n->key_count+1;i<t->b;i++)
        n->keys[i] = INVALID_KEY;

    // fill the rest of child pointers
    for( i=n->key_count+1;i<=t->b;i++){

        if( n->child[i] ){
            node_free( t, n->child[i] );
            n->child[i] = NULL;
        }
    }

    n->bb_size = 0;
    
    t->opts->write_node( n, (void *)n, sizeof(node_t) );

    return 0;
}

void
flush( bft_t *t, node_t *n, bft_req_t *req )
{
    int i;
    node_t *s;
    bft_req_t *leaf_node_list;
    bft_req_t *sorted = req;
    bft_req_t *prev, *curr;
    int count;
    int total_elem;
    int new_key;

    leaf_node_list = _load_node_request( t, n );

    sorted = mergelists( sorted, leaf_node_list, &request_comp );

    total_elem = list_len(sorted);

    if( total_elem <= t->b * t->c ){
        // step 3 in Fig.3 of L.Arge[1]
        _flush_to_leaf( t, n, req );
    }
    else{
        // step 4 in Fig.3 of L.Arge[1]
        //
        if( n == t->root ){
            s = node_create( t, NULL, INTERNAL_NODE );
            t->root = s;
            s->child[0] = n;
            n->parent = s;
        }
    
        /*
         * Given the in-memory sorted list of requests,
         * the split performs as following:
         * 1) create a (right) sibling node
         * 2) partition the element in the list into 2 groups -- the first group consists of the first t->b/2 blocks of elements, while the second group consists of the rest of elements.
         * 3) insert the elements in each group, as unit of a block, into the splitted node and created node, respectively; key partitions are adjusted accordingly.
         * 4) update the key partition in parent nodes, which might propagate to the root. Note that if one-downward-pass algorithm does not need the backward propagation
         */

        // step 1 
        i = n->parent->key_count;
        
        if( i>0 ){
            while( i>=1 && n->keys[0]<n->parent->keys[i-1] )
                i--;
            i++;
        }

        _sibiling_create( t, n->parent, i );

        // step 2
        count = 0;
        curr = sorted;

        do{
            prev = curr;
            curr = curr->next;
            ++count;
        }while( count < total_elem/2 );

        assert( prev->next );

        new_key = prev->key;
        prev->next = NULL;
        
        // step 3
        _flush_to_leaf( t, n, sorted );
        _flush_to_leaf( t, n->next, curr );

        node_add_key( t, n->parent, i, new_key );
        // step 4  (not needed in one-downward-pass algorithm )

    }

    req_list_free( sorted );

    for( i=0; i<n->bb_count; i++ )
        bb_reset( n->buffers[i] );
}



#if 0
// blk_buffer
static uint32_t
bb_find( key_compare_func comp, node_t *n, int key, uint32_t c_start )
{
    int left, right, middle, result, compared;
    blk_buffer_t **buffers;

    left = c_start;
    right = n->bb_size;
    compared = 0;
    buffers = n->buffers;
    
    while( left <= right ){
        middel = (left+right)/2;
        compared = comp( key, buffers[middle]->req_first->key );
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
    blk_buffer = n->buffers[c_idx];
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

    removed = n->buffers[c_idx];
    memmove( &n->buffers[c_idx], &n->buffers[c_idx+1], (n->bb_size-c_idx-1)*sizeof(void *) );
    --n->bb_size;

    return removed;
}
/*
static int 
req_passdown( bft_t *t, blk_buffer_t *dst_bb, bft_req_t *req )
{
    int i;
    int ret = 0;

    i = dst_bb->req_count;
       
    assert( dst_bb->req_count <= t->c );

    req->next = dst_bb->req_first;
    dst_bb->req_first = req;

    // one I/O to write one block buffer
    t->opts->write_node_buffer( n,i );

    n->buffers[i]->req_count = bb->req_count;

    ++ n->bb_size;

    if( n->bb_size == t->m )
        bb_emptying( t, n );

    return ret;
}
*/


/*
 *  request_comp (a, b)
 *  return 1 : a>b
 *  return 0 : a<b; 
 *  note that a would not be equal to b in the comparison of bft_req_t objects.
 * */
#endif

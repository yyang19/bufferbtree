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

static void block_buffer_emptying( bft_t *t, node_t *n );

/*------------- object creation and destroy ------------------------*/
/*--------------------------req------------------------*/
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

static void
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

    n->containers[i]->req_count = bb->req_count;

    ++ n->bb_size;

    if( n->bb_size == t->m )
        block_buffer_emptying( t, n );

    return ret;
}
*/

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
node_t *
node_create( bft_t *t, node_t *p, int type )
{
    int i;
    node_t *n;
    n = (node_t *) malloc (sizeof(node_t));

    if( n ){
        
        n->disk_content = (void *) malloc ( t->B );
        if( !n->disk_content )
            goto fail_disk_content;

        if( type == LEAF_NODE || type == INTERNAL_NODE ){
        
            n->keys = (int *) malloc ( sizeof(int) * t->m );
            if( !n->keys )
                goto fail_keys;

            n->child = (struct node **) calloc ( t->m+1, sizeof(struct node *) );
            if( !n->child )
                goto fail_child;

            n->containers = (blk_buffer_t **) malloc ( sizeof(blk_buffer_t *) * t->m );
            if( !n->containers )
                goto fail_containers;
        }

        n->id = ++t->nNode;
        n->type = type;
        n->parent = p;
        
        if( type == LEAF_NODE || type == INTERNAL_NODE )
            n->bb_count = t->m;
        else if( type == LEAF_BLOCK )
            n->bb_count = 0;
        else
            assert(0);

        n->bb_size = 0;
        n->key_count = 0;
        
        if( type == LEAF_NODE || type == INTERNAL_NODE ){
            for( i=0; i<t->m; i++ )
                n->containers[i] = bb_create();
            
            memset( n->keys, 0xff, sizeof(int) * t->m ); 
        }

        goto out;
    }

fail_containers:
    free( n->child );
fail_child:
    free( n->keys );    
fail_keys:
    free( n->disk_content );
fail_disk_content:
    free(n);
    n = NULL;
out:
    return n;
}

static void
node_free_single( bft_t *t, node_t *n )
{
    int i;

    if( n->type == LEAF_NODE || n->type == INTERNAL_NODE ){
    
        for( i=0; i<n->bb_count; i++ )
            bb_free( t, n->containers[i] );

        free( n->containers );
        free( n->keys );
        free( n->child );
    }

    free( n->disk_content );
    free( n );
}

void
node_free( bft_t *t, node_t *n )
{
    int i;
    node_t *c;

    for( i=0; i<=n->key_count; i++ ){
        c = n->child[i];
        if( c )
            node_free_single( t, c );
    }

    node_free_single( t, n );
}

void
node_dump( node_t *n )
{
    int i;

    printf("Dump node (%p)\n", (void *)n);
    printf("ID : %d\n", n->id);
    printf("Type : %d\n", n->type);
    printf("Parent : %p\n", (void *)n->parent);
    printf("Buffer count : %d\n", n->bb_count);
    printf("Buffer size : %d\n", n->bb_size);
    
    for( i=0; i<n->bb_size; i++ )
        bb_dump( n->containers[i] );
    printf("\n");

    printf("Key count : %d\n", n->key_count);
    printf("Keys : ");
    for( i=0; i<n->key_count; i++ )
        printf("%d\t", n->keys[i]);
    printf("\n");
    
    printf("Children : ");
    for( i=0; i<=n->key_count; i++ )
        printf("%p\t", (void *)n->child[i]);
    printf("\n");

    printf("Read count : %d\n", n->rd_count);
    printf("Write count : %d\n", n->wr_count);
}

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

static blk_buffer_t *
request_list_construct( bft_t *t, void *payload )
{
    int i;

    bft_req_t *list = (bft_req_t *)payload;
    bft_req_t *req;
    blk_buffer_t *bb;
    
    bb = bb_create();

    for( i=0; i<t->c; i++ ){
        if( list[i].next ){
            req = req_create( list[i].key, list[i].val, list[i].type );
            req->tm = list[i].tm;
            req->next = bb->req_first;
            bb->req_first = req;
            ++bb->req_count;
        }
    }

    return bb;
}

/*
 * req_insert_bb 
 * description: insert a request in the head of the request list
 * */
static STATUS
req_insert_bb( bft_t *t, bft_req_t *req, blk_buffer_t *bb )
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

static int
fill_node_blk_buffer( bft_t *t, bft_req_t *start, int count, node_t *dst_node )
{
    int idx;
    int ret;
    blk_buffer_t *curr_container;
    bft_req_t *curr = start;

    if( !dst_node )
        return -1;

    idx = dst_node->bb_size;
    
    while( count>0 ){

        t->opts->read_node_buffer( dst_node, idx ); //read node buffer
        curr_container = dst_node->containers[idx];
       
        ret = req_insert_bb( t, curr, curr_container );
    
        if( ret == RET_NODE_BUFFER_NULL )
            return -1;

        if( ret == RET_NODE_BUFFER_FULL ){
            if( idx == t->m-1 ){
                block_buffer_emptying( t, dst_node );
                idx = 0;
            }
            ++idx;
            continue;
        }

        curr = curr->next;    
        --count;
    };

    return 0;
}

static STATUS
_split_child( bft_t *tree, node_t *node, int i )
{
    int j;
    int t = tree->a;
    node_t *y, *z;

    y = node->child[i];
    
    z = node_create( tree, node, y->type );

    z->key_count = t-1;

    //move the largest t-1 keys and tchild of y into z
    for( j=0; j<z->key_count; j++ )
        z->keys[j] = y->keys[t+j];
    

    if( y->type != LEAF_BLOCK ){
        for( j=0; j<=z->key_count; j++ )
            z->child[j] = z->child[t+j];
    }

    if( y->type == LEAF_NODE ){
        y->key_count = t;
        z->next = y->next;
        y->next = z;    
    }
    else
        y->key_count = t-1;

    //shift child pointers in node dest the right dest create a room for z
    
    for( j=node->key_count+1; j>i; j-- )
        node->child[j] = node->child[j-1];

    node->child[i+1] = z;

    for( j=node->key_count; j>i; j-- )
        node->keys[j] = node->keys[j-1];

    node->keys[i] = y->keys[t-1];
    node->key_count++;

    tree->opts->write_node( node, (void *)node, sizeof(node_t) );
    tree->opts->write_node( y, (void *)y, sizeof(node_t) );
    tree->opts->write_node( z, (void *)z, sizeof(node_t) );

    //NODE_WRITE(y);
    //NODE_WRITE(z);
    //NODE_WRITE(node);

    return 0;
}

static STATUS
node_push_to_child( bft_t *t, node_t *n, bft_req_t *req ){

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
            fill_node_blk_buffer( t, start, count, n->child[i] );
        
        ++i;
        start = curr;
    };
    
    if( count > 0 )
        fill_node_blk_buffer( t, start, count, n->child[i] );

    return 0;
}

static void
node_push_to_leaf( bft_t *t, node_t *n, bft_req_t *req )
{
    int i;
    int nChild;
    node_t *child;
    node_t *s;
    bft_req_t *sorted = req;
    bft_req_t *prev, *curr;
    blk_buffer_t *bb;
    int count;
    void *payload = malloc ( t->B );

    assert( n->type == LEAF_NODE );

    nChild = n->key_count;

    for( i = 0; i < nChild; i++ ){
        child = n->child[i];
        t->opts->read_node( child, payload, t->B );
        bb = request_list_construct( t, payload );
        quicksort_ll( &bb->req_first, &request_comp );
        sorted = mergelists( sorted, bb->req_first, &request_comp );
    }

    if( list_len(sorted) <= t->b * t->c ){
        // step 3 in Fig.3 of L.Arge[1]
        curr = sorted;
        count = 0;
        i = 0;
        
        memset( payload, 0xff, t->B );

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
        
        t->opts->write_node( n, (void *)n, sizeof(node_t) );

    }
    else{
        // step 4 in Fig.3 of L.Arge[1]
        //
        if( n == t->root ){
            s = node_create( t, NULL, INTERNAL_NODE );
            t->root = s;
            s->child[0] = n;
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
        _split_child( t, n, t->a );

        // step 2
        count = 0;
        curr = sorted;

        do{
            prev = curr;
            curr = curr->next;
            ++count;
        }while( count < (t->b/2) * t->c );

        assert( prev->next );

        prev->next = NULL;
        
        // step 3
        node_push_to_leaf( t, n, sorted );
        node_push_to_leaf( t, n->next, curr );

        // step 4  (not needed in one-downward-pass algorithm )

    }

    free( payload );

    req_list_free( sorted );
}

static void
block_buffer_emptying( bft_t *t, node_t *n )
{
    int i;
    bft_req_t *sorted = NULL;
    assert( n->bb_size == t->m );

    for( i = 0; i < t->m; i++ ){
        t->opts->read_node_buffer( n,i ); //read from disk
        quicksort_ll( &n->containers[i]->req_first, &request_comp );
        sorted = mergelists( sorted, n->containers[i]->req_first, &request_comp );
    }

    if( !sorted )
        return;

    //dump to child nodes
    if( n->type == LEAF_NODE ){
        node_push_to_leaf( t, n, sorted );
    }
    else{
        node_push_to_child( t, n, sorted ); 
    }

    for( i = 0; i < n->bb_size; i++ )
        bb_reset( n->containers[i] );
    
    n->bb_size = 0;
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
    t->opts->write_node_buffer( n,i );

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

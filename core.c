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

#include "bplustree.h"
#include "queue.h"

static bpt_t *bptree;

static void _descend( bpt_t *tree, node_t *node, int key );
static void _dump2( node_t *node, FILE *write_log );


/*------------- object creation and destroy ------------------------*/
// request
bt_req_t *
req_create( void *key, void *val, enum bt_op_t op )
{
    bt_req_t *r;

    r =(bt_req_t *)malloc( sizeof(struct request) );
    r->key = key;
    r->val = val;
    r->next = NULL;
    r->type = op;

    return r;
}

void 
req_free(struct bftree *tree, bt_req_t *r, int nofree )
{
    free(r);
}

void 
req_override(struct bftree *tree, bt_req_t *old, bt_req_t *new)
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

    req_free(tree, new, 0);
}

// container
static container_t *
container_create(void)
{
    containter_t *c;

    c = (container_t *)malloc(sizeof(struct container));
    c->req_first = NULL;
    c->req_count = 0;
    c->child = NULL;

    return c;
}

static void
container_free( bftree_t *tree, container_t *c )
{
    bt_req_t *curr, *next;

    curr = c->req_first;

    while(curr){
        next = curr->next;
        req_free( tree, curr, 0 );
        curr = next;
    };

    free(c);
}

void 
container_update( node_t *n, containter_t *c, int c_idx )
{


}

// node
static node_t *
node_create( bft_t *t, node_t *p, int type )
{
    node_t *n;
    n = (node_t *) malloc (sizeof(node_t));

    if( n ){
        n->containers = (container_t *) malloc ( sizeof(container_t *) * t->a );
        if( n->containers ){
            p->id = ++t->nNode;
            p->type = type;
            p->nElem = 0;
            n->parent = p;
            n->container_count = t->a;
            n->container_size = 0;
        }
        else{
            free(n);
            n=NULL;
        }
    }

    return n;
}

static void
node_free_single( bft_t *t, node_t *n )
{
    int i;

    for( i=0; i<n->container_size; i++ )
        container_free( t, n->containers[i] );

    free( n->containers );
    free( n );
}

void
node_free( bft_t *t, node_t *r )
{
    int i;
    container_t *c;

    for( i=0; i<n->container_size; i++ ){
        c = node->containers[i];
        if( c->child )
            node_free_single( t, c->child );
    }

    node_free_single( t, r );
}

/*-------------------------- operations ------------------------*/

// request
static bft_req_t *
request_get( key_compare_func comp, bt_req_t *start, void *key, int *exact )
{
    bt_req_t *curr_req, *prev_req;
    int compared;

    prev_req = NULL;
    curr_req = start;
    *exact = 0;

    while( curr_req ){
        compared = comp( key, curr_req->key );

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

// container
static uint32_t
container_find( key_compare_func comp, node_t *n, void *key, uint32_t c_start )
{
    int left, right, middle, result, compared;
    container_t **containers;

    left = c_start;
    right = n->container_size;
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
container_get( bft_t *t, node_t *n, uint32_t c_idx, void *key ){

    bft_req_t *curr;
    containter_t *c;
    int exact;
    int (*comp)(const void *, const void * );

    if( c_idx >= n->container_size )
        return NULL;

    comp = t->opts->key_compare;
    container = n->containers[c_idx];
    curr = request_get( comp, c->request_first, key, &exact );

    if( exact ){
        if( curr->type == BT_PUT )
            return curr;
        return NULL;
    }

    if( c->child ){
        c_idx = container_find( comp, c->child, key, 0 );
        return container_get( tree, c->child, c_idx, key );
    }

    return NULL;
}

static container_t *
container_shift_left( node_t *n, uint32_t c_idx )
{
    container_t *removed;

    ASSERT( c_idx < n->container_size );

    removed = n->containers[c_idx];
    memmove( &n->containers[c_idx], &n->containers[c_idx+1], (n->container_size-c_idx-1)*sizeof(void *) );
    --n->container_size;

    return removed;
}

// routines used by traverse
static void
_dfs( node_t *subroot, FILE *log )
{
    int i;
    nonleaf_t *nln;

    if( subroot->type == LEAF_NODE ){
        _dump2(subroot,log);
        return;
    }
    
    nln = (nonleaf_t *)subroot;

    for( i=0; i<=subroot->n; i++ ){
        _dfs( nln->children[i],log );
    }

    _dump2(subroot,log);

    return;
}

static void
_bfs( node_t *subroot, FILE *log )
{
    int i;
    nonleaf_t *nln;
    fifo_t *q;
    node_t *node;

    q = _fifo_init( 1000 );

    _fifo_push( q, subroot );

    while( ! _fifo_empty(q) ){
    
        //dequque a node 
        node = _fifo_peek(q);
        _dump2(node,log);

        if( node->type == NON_LEAF_NODE ){
            //enqueue the children
            nln = (nonleaf_t *)node;
            for( i=0; i<=node->n; i++ )
                _fifo_push( q, nln->children[i] );
        }

        _fifo_pop(q);
    }

    _fifo_destroy(q);

    return;
}


#ifdef DEBUG
void _d( bpt_t *t )
{
    int i,j;
    int ichildren;
    node_t *node;
    leaf_t *leaf;
    nonleaf_t *nln;

    if( !t )
        return;
    
    node = t->root;

    printf( "Level root: ");
    for( i = 0; i<node->n; i++ ){
        printf(" <%d> ", node->key[i]);
    }
    printf( "\n");

    if( node->n > 0 ){
        nln = (nonleaf_t *)node;
        ichildren = node->n;
        printf( "Level 2: ");
        for( i = 0; i<=ichildren; i++ ){
            node = nln->children[i];
            for( j = 0; j<node->n; j++ ){
                printf(" <%d> ", node->key[j]);
            }
            printf( "|\t");
        }
        printf( "\n");
    }
    
    printf( "Leaf level: ");
    node = t->root;

    while( node->type == NON_LEAF_NODE )
        node = ((nonleaf_t *)node)->children[0];
        
    assert( node->type == LEAF_NODE );

    leaf = (leaf_t *) node;

    while( leaf ){
        for(i=0; i<leaf->node.n; i++){
            printf(" <%d,%d> ", leaf->node.key[i], leaf->data[i]);                
        }
        printf( "|\t");
        leaf = leaf->next;
    }
    printf("\n");
}

#endif

/*  bplustree.c
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

static bft_t *bftree;

/*--------------------------req------------------------*/
bft_req_t *
req_create( void *key, void *val, enum bt_op_t op )
{
    bft_req_t *r;

    r =(bft_req_t *)malloc( sizeof(struct request) );
    r->key = key;
    r->val = val;
    r->next = NULL;
    r->type = op;

    return r;
}

void 
req_free(struct bftree *tree, bft_req_t *r, int nofree )
{
    free(r);
}

void 
req_override(struct bftree *tree, bft_req_t *old, bft_req_t *new)
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

/*--------------------------container------------------------*/
container_t *container_create()
{
    containter_t *c;

    c = (container_t *)malloc(sizeof(struct container))bh

}


static int
_key_binary_search(int *arr, int len, int key)
{
    int low = -1;
    int high = len;
    int mid;

    while (low + 1 < high) {
        mid = low + (high - low) / 2;
        if (key > arr[mid]) 
            low = mid;
        else
            high = mid;
    }

    if (high >= len || arr[high] != key) 
        return -high - 1;
    else 
        return high;
}

static int
_nodeInit( node_t *new, bft_t *tree, int type )
{
    int nKeys = tree->b_factor*2-1;

    new->type = type;
    new->n = 0;
    new->id = ++tree->max_node_id;
    new->wr_count=0;
    
    new->key =  (int *) malloc ( sizeof(int)*nKeys );
    memset( new->key, 0xff, nKeys * sizeof(int) );

    return 0;    
}

static void
_destroy_node( node_t *node )
{
    _dump2( node, bftree->node_log );
    free( node->key );

    return;
}

static struct non_leaf *
_create_nonleaf( bft_t *tree )
{
    int nKeys = tree->b_factor*2-1;
    int nChildren = nKeys+1;

    nonleaf_t *new = (nonleaf_t *)malloc(sizeof(nonleaf_t));

    assert(new);

    _nodeInit( &new->node, tree, NON_LEAF_NODE );

    new->children = (node_t **)malloc(nChildren * sizeof(node_t *)); 
    memset(new->children, 0, nChildren * sizeof(node_t *));

    return new;
}

static void 
_destroy_nonleaf( nonleaf_t **nonleaf )
{
    free( (*nonleaf)->children );
    _destroy_node( &(*nonleaf)->node );
    free( *nonleaf );
    *nonleaf = NULL;

    return;
}

static leaf_t *
_create_leaf( bft_t *tree )
{
    int nKeys = tree->b_factor*2-1;

    leaf_t *new = (leaf_t*)malloc(sizeof(leaf_t));
    assert(new);

    _nodeInit( &new->node, tree, LEAF_NODE );

    new->data = (int *) malloc ( sizeof(int)*nKeys );
    assert( new->data );
    memset( new->data, 0xff, nKeys * sizeof(int) );

    new->next = NULL;
    new->node.type = LEAF_NODE;
    
    return new;
}

static void
_destroy_leaf( leaf_t **leaf )
{
    free( (*leaf)->data );
    _destroy_node( &(*leaf)->node );
    free( *leaf );
    *leaf = NULL;

    return;
}

void
_destroy( node_t *subroot )
{
    int i;

    nonleaf_t *nln;
    leaf_t *ln;

    if( subroot->type == LEAF_NODE ){
        ln = (leaf_t *)subroot;
        _destroy_leaf( &ln );
        return;
    }
    
    nln = (nonleaf_t *)subroot;

    for( i=0; i<=subroot->n; i++ )
        _destroy( nln->children[i] );

    _destroy_nonleaf( &nln );

    return;
}

//routines used by query
static int
_node_search( node_t *node, int key ){
    
    int i;

    nonleaf_t *nln;
    leaf_t *ln;
    node_t *child;

    if( node->type == LEAF_NODE ){
        ln = (leaf_t *)node;
        i = _key_binary_search(ln->node.key, ln->node.n, key );
        if (i >= 0)
            return ln->data[i];
        else 
            return DATA_NOT_EXIST;
    }

    assert( node->type == NON_LEAF_NODE );
    
    nln = (nonleaf_t *)node;
    i = _key_binary_search(nln->node.key, nln->node.n, key );

    if(i >= 0)
        child = nln->children[i];
    else{
        i = -i - 1;
        child = nln->children[i];
    }

    return _node_search( child, key );
}

inline void
_node_write( node_t *node )
{
    node->wr_count ++;
    fprintf( bftree->write_log, "%d,", node->id);
    if( node->type == LEAF_NODE )
        fprintf(bftree->write_log,"leaf," );
    else if( node->type == NON_LEAF_NODE )
        fprintf(bftree->write_log,"non-leaf," );

    fprintf(bftree->write_log,"\n");
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

// routines used by insertion
static void
_split_child( bft_t *tree, node_t *node, int i )
{
    int j;
    int t = tree->b_factor;
    node_t *y, *z;
    nonleaf_t *nln = (nonleaf_t *)node;
    nonleaf_t *y_nln, *z_nln;
    leaf_t *y_ln, *z_ln;

    y = nln->children[i];
    
    if( y->type == LEAF_NODE ){
        y_ln = (leaf_t *)y;
        z_ln = _create_leaf(tree);
        z = &z_ln->node;
    }
    else{
        y_nln = (nonleaf_t *)y;
        z_nln = _create_nonleaf(tree);
        z = &z_nln->node;
    }

    z->n = t-1;

    //move the largest t-1 keys and tchildren of y into z
    for( j=0; j<z->n; j++ )
        z->key[j] = y->key[t+j];

    if( y->type != LEAF_NODE ){
        for( j=0; j<=z->n; j++ )
            z_nln->children[j] = y_nln->children[t+j];
    }
    else{
        for( j=0; j<z->n; j++ )
            z_ln->data[j] = y_ln->data[t+j];
    }

    if( y->type == LEAF_NODE ){
        y->n = t;
        z_ln->next = y_ln->next;
        y_ln->next = z_ln;    
    }
    else
        y->n = t-1;

    //shift child pointers in node dest the right dest create a room for z
    
    for( j=node->n+1; j>i; j-- )
        nln->children[j] = nln->children[j-1];

    nln->children[i+1] = z;

    for( j=node->n; j>i; j-- )
        node->key[j] = node->key[j-1];

    node->key[i] = y->key[t-1];
    node->n++;

    _node_write(y);
    _node_write(z);
    _node_write(node);

}

//public API

int
bftGet( bft_t *tree, int key )
{
    if( !tree->root )
        return 0;

    return _node_search( tree->root, key );
}


void
bftPut( bft_t *tree, void *key, void *val )
{
    node_t *node;
    nonleaf_t *s;

    bft_req_t *new_req;

    new_req = req_create( key, val, BT_PUT );

    if (tree->root == NULL) { //empty tree
        leaf_t *leaf = _create_leaf(tree);
        tree->root = &leaf->node;
    }

    node = tree->root;

    if( node->n == tree->b_factor*2-1 ){
        s = _create_nonleaf(tree);
        tree->root = &s->node;
        s->children[0] = node;
        _split_child(tree,&s->node,0);
        _insert_nonfull(tree,&s->node,key,data);
    }
    else
        _insert_nonfull(tree,node,key,data);

    return;
}

void
bftRemove( bft_t *tree, int key ){
    
    if( !tree->root )
        printf("Empty tree! No deletion\n");
    else
        return _descend( tree, tree->root, key );
}


void 
bftTraverse( bft_t *tree, int search_type )
{
    if( !tree->root ){
        printf("Empty tree! \n");
        return;
    }

    if( search_type == TRAVERSE_DFS )
        _dfs( tree->root, tree->node_log );
    else if( search_type == TRAVERSE_BFS )
        _bfs( tree->root, tree->node_log );
    else
        assert(0);

    return;
}

/*
 *  M: the number of elements that can fit in the internal memory
 *  B: the number of elements per block, where B <= M/2 .
 *  Let m=M/B be the number of blocks that fit into internal memory
 *  
 *  The buffer B-tree is an (a,b)-tree.
 * */
bft_t *
bftCreate( int a, int b, int M, int B, bft_opts_t *opts )
{
    bft_t *t;

    if( a>=b || M/B < b ){
        printf( "Invalid tree parameters\n" );
        return NULL;
    }

    t = ( bft_t * ) malloc ( sizeof(bft_t) );
    
    if( t ){
        t->a = a;
        t->b = b;
        t->M = M;
        t->B = B;
        t->root = NULL;
        t->opts = opts;
        t->del_payload_count = t->put_payload_count = 0;
        t->node_log = fopen( "node.log", "w+" );
        t->write_log = fopen( "write.log", "w+" );
    }

    return t;
}

void
bftDestroy( bft_t *t ){

    if( t->root )
        _destroy(tree->root);

    fclose( tree->node_log );
    fclose( tree->write_log );
     
    free(t);
}

#ifdef DEBUG
static void
_dump( node_t *node ){

    int i;

    if( !node ){
        printf("NULL node");
        return;
    }
    
    printf("Node %d, ", node->id );

    if( node->type == LEAF_NODE )
        printf("leaf, " );
    else if( node->type == NON_LEAF_NODE )
        printf("non-leaf, " );
    else
        assert(0);

    printf("%d, ", node->n );
    
    for( i=0; i<node->n; i++){
        printf("<%d>", node->key[i] );
        //if( node->type == LEAF_NODE )
        //    printf("\t data[%d] = %d\n", i, ((leaf_t *)node)->data[i] );
        //else
        //    printf("\n");
    }

    printf(", ");
    printf("%d ", node->wr_count);
    
    printf("\n");

    return;
}

static void
_dump2( node_t *node, FILE *log ){

    int i;

    if( !node ){
        fprintf( log, "NULL node");
        return;
    }
    
    fprintf( log, "%d, ", node->id );

    if( node->type == LEAF_NODE )
        fprintf( log, "leaf, " );
    else if( node->type == NON_LEAF_NODE )
        fprintf( log, "non-leaf, " );
    else
        assert(0);

    fprintf( log, "%d, ", node->n );
    
    for( i=0; i<node->n; i++)
        fprintf( log, "<%d>", node->key[i] );

    fprintf( log, ", ");
    fprintf( log, "%d ", node->wr_count);
    
    fprintf( log, "\n");

    return;
}


void _d( bft_t *t )
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

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

//public API
void *
bftGet( bft_t *tree, void *key )
{
    uint32_t c_idx;
    bft_req_t *r;

    if( !tree || !key )
        return NULL;

    c_idx = container_find( tree->opt->key_compare, tree->root, key, 0 );
    r = container_get( tree, tree->root, c_idx, key );

    return r ? r->val : NULL;
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
        t->nNode = 0;
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

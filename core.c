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

    //to be added
    return NULL;
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
_nodeInit( node_t *new, bpt_t *tree, int type )
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
    _dump2( node, bptree->node_log );
    free( node->key );

    return;
}

static struct non_leaf *
_create_nonleaf( bpt_t *tree )
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
_create_leaf( bpt_t *tree )
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
    fprintf( bptree->write_log, "%d,", node->id);
    if( node->type == LEAF_NODE )
        fprintf(bptree->write_log,"leaf," );
    else if( node->type == NON_LEAF_NODE )
        fprintf(bptree->write_log,"non-leaf," );

    fprintf(bptree->write_log,"\n");
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
_split_child( bpt_t *tree, node_t *node, int i )
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

static void
_insert_nonfull( bpt_t *tree, node_t *node, int key, int data )
{
    int i = node->n;
    nonleaf_t *nln;
    leaf_t *ln;
    
    if( node->type == LEAF_NODE ){
        ln = (leaf_t *)node;
        while( i>=1 && key<node->key[i-1] ){
            node->key[i] = node->key[i-1];
            ln->data[i] = ln->data[i-1];
            i--;    
        }

        node->key[i] = key;
        ln->data[i] = data;

        node->n ++;
        _node_write(node);
    }
    else{
        assert( node->type == NON_LEAF_NODE );

        while( i>=1 && key<node->key[i-1] )
            i--;

        i++;
        //NODE_READ(node);
        
        nln = (nonleaf_t *)node;

        if( nln->children[i-1]->n == tree->b_factor*2-1 ){
            _split_child( tree, node, i-1 );
            if( key>node->key[i-1] )
                i=i+1;
        }

        _insert_nonfull( tree, nln->children[i-1], key, data );
    }
}

// routines used by deletion
static void 
_node_key_shift_left( node_t *node, int index, int ptr_shift) 
{
    int idx;
    nonleaf_t *nln;
    leaf_t *ln;
    
    if( node->type == LEAF_NODE ){
        ln = (leaf_t *)node;
        for( idx=index; idx<node->n-1; idx++ ){
            node->key[idx] = node->key[idx+1];
            ln->data[idx] = ln->data[idx+1];
        }

    }
    else{ 

        nln = (nonleaf_t *)node;

        for( idx=index; idx<node->n-1; idx++ ){
            node->key[idx] = node->key[idx+1];
            if( ptr_shift )
                nln->children[idx]=nln->children[idx+1];
        }

        if( ptr_shift )
            nln->children[node->n-1]=nln->children[node->n];
    }

    node->n--;
}

static void 
_node_key_shift_right( node_t *node, int index, int ptr_shift )
{

    int idx;
    nonleaf_t *nln;
    leaf_t *ln;
    
    assert( index<=node->n );
    
    if( node->type == LEAF_NODE ){
        ln = (leaf_t *)node;
        for( idx=node->n; idx>index; idx-- ){
            node->key[idx] = node->key[idx-1];
            ln->data[idx] = ln->data[idx-1];
        }
    }
    else{
        nln = (nonleaf_t *)node;

        for( idx=node->n; idx>index; idx-- ){
            node->key[idx] = node->key[idx-1];
            if( ptr_shift )
                nln->children[idx+1]=nln->children[idx]; 
        } 
 
        if( ptr_shift )
            nln->children[idx+1]=nln->children[idx]; 
    }
    
    node->key[index] = INVALID_KEY;
    
    node->n++;
}

static void 
_move_key( int key, node_t *src, node_t *dest, int src_ptr_shift, int dest_ptr_shift )
{
    int src_pos = 0;
    int dest_pos = 0;

    while( src_pos<src->n && key>src->key[src_pos] )
        src_pos++;
    
    while( dest_pos<dest->n && key>dest->key[dest_pos] )
        dest_pos++;
    
    _node_key_shift_right( dest, dest_pos, dest_ptr_shift );
    
    dest->key[ dest_pos ] = src->key[src_pos]; 

    if( dest->type == LEAF_NODE )
        ((leaf_t *)dest)->data[ dest_pos ] = ((leaf_t *)src)->data[src_pos]; 
    
    _node_key_shift_left( src, src_pos, src_ptr_shift );
    
    //DISK_WRITE(src);
    //DISK_WRITE(dest);
}

static void 
_merge_node( node_t *left, node_t *right )
{
    int k;
    nonleaf_t *l_nln, *r_nln;
    leaf_t *l_ln, *r_ln;
    
    assert( left->type == right->type );

    if( left->type == LEAF_NODE ){
        l_ln = (leaf_t *) left;
        r_ln = (leaf_t *) right;
    }
    else{
        l_nln = (nonleaf_t *) left;
        r_nln = (nonleaf_t *) right;
    }

    if( left->type == LEAF_NODE ){
        for( k=0; k<right->n; k++ ){
            left->key[left->n+k] = right->key[k];
            l_ln->data[left->n+k] = r_ln->data[k];
        }
    }
    else{ 
        for( k=0; k<right->n; k++ ){
            left->key[left->n+k] = right->key[k];
            l_nln->children[left->n+k] = r_nln->children[k];
        }
    
        l_nln->children[left->n+right->n] = r_nln->children[right->n];
    }
    
    left->n += right->n;

    if( left->type == LEAF_NODE )
        l_ln->next = r_ln->next;
    
    if( right->type==NON_LEAF_NODE )
        _destroy_nonleaf(&r_nln);
    else
        _destroy_leaf(&r_ln);
    
    //DISK_WRITE(left);
    //DISK_WRITE(right);
}

static void
_remove_from_leaf( node_t *node, int idx )
{
    int i = idx;
    leaf_t *ln = (leaf_t *)node;
    
    assert( node->type == LEAF_NODE);
    assert( idx<node->n );
    
    while( i<node->n-1 ){
        node->key[i] = node->key[i+1];
        ln->data[i] = ln->data[i+1];
        i++;
    }
    
    node->n--;
    
    return;
}

static node_t *
_pre_descend_child( bpt_t *tree, node_t *parent, int idx )
{
    node_t *child = NULL;
    node_t *lsibling = NULL;
    node_t *rsibling = NULL;
    nonleaf_t *nln_parent;
    nonleaf_t *nln_child, *nln_lsibling, *nln_rsibling;

    int parent_key, predecessor_key, successor_key;
    
    int t = tree->b_factor;

    nln_parent = (nonleaf_t *)parent;

    child = nln_parent->children[idx]; 
    
    if( child->n>t-1 ) // not a minimal node
        return child;

    if ( idx == parent->n ){   
        lsibling = nln_parent->children[idx-1];
        rsibling = NULL; 
    }else if ( idx == 0 ) {
        lsibling = NULL;
        rsibling = nln_parent->children[1];
    }else {
        lsibling = nln_parent->children[idx - 1]; 
        rsibling = nln_parent->children[idx + 1];
    }
    
    if( lsibling && lsibling->n > t-1 ){ 
        assert(idx>0);
        
        predecessor_key = lsibling->key[lsibling->n-1];

        if( child->type == LEAF_NODE ){
            parent->key[idx-1] = lsibling->key[lsibling->n-2];
            _move_key( predecessor_key, lsibling, child, 1, 1 );
        }
        else{
            parent_key = parent->key[idx-1];
            if( parent_key != child->key[0] )
                _move_key( parent_key, parent, child, 0, 1 );//move 1-level down
            //parent->key[idx-1] = lsibling->key[lsibling->n-1];

            nln_child = (nonleaf_t *)child;
            nln_lsibling = (nonleaf_t *)lsibling;
            nln_child->children[0] = nln_lsibling->children[lsibling->n];
            
            _move_key( predecessor_key, lsibling, parent, 0, 0 );
        }
    }
    else if( rsibling && rsibling->n > t-1 ){
        
        successor_key = rsibling->key[0];

        if( child->type == LEAF_NODE ){
            parent->key[idx] = successor_key;
            _move_key( successor_key, rsibling, child, 1, 1 );
        }
        else{
            parent_key = parent->key[idx];
            if( parent_key != child->key[child->n-1] )
                _move_key( parent_key, parent, child, 0, 1 );//move 1-level down
            //parent->key[idx] = successor_key;

            nln_child = (nonleaf_t *)child;
            nln_rsibling = (nonleaf_t *)rsibling;
            nln_child->children[child->n] = nln_rsibling->children[0];
            
            _move_key( successor_key, rsibling, parent, 1, 0 );
        }

    }
    else if( lsibling && (lsibling->n == t-1) ){
        assert(idx>0);
        
        if( child->type == NON_LEAF_NODE ){
            parent_key = parent->key[idx-1];
            if( parent_key != lsibling->key[lsibling->n-1] )
                _move_key( parent_key, parent, lsibling, 1, 0 );//move 1-level down
        }
        else
            _node_key_shift_left( parent, idx-1, 1 );

        _merge_node( lsibling, child );
        nln_parent->children[idx-1] = lsibling;
        
        child = lsibling;
    }
    else if( rsibling && (rsibling->n == t-1) ){
        
        if( child->type == NON_LEAF_NODE ){
            parent_key = parent->key[idx];
            if( parent_key != child->key[child->n-1] )
                _move_key( parent_key, parent, child, 1, 0 );//move 1-level down
        }
        else
            _node_key_shift_left( parent, idx, 1 );

        _merge_node( child, rsibling );
        nln_parent->children[idx] = child;
    }
    else
        assert(0);
    
    return child;
}


static void
_descend( bpt_t *tree, node_t *node, int key )
{
    int i;
    nonleaf_t *nln;
    leaf_t *ln;
    node_t *child;

    i = _key_binary_search(node->key, node->n, key);

    if( i>= 0 ){ //key found in node 
        if( node->type == LEAF_NODE ){
            _remove_from_leaf( node, i );
            if( node->n==0 && node==tree->root ){
                ln = (leaf_t *)node;
                _destroy_leaf(&ln);
                tree->root = NULL;
            }
            return;
        }
    }
        
    //key not found in the node
    if( node->type == LEAF_NODE ){
        printf(" The key %d does not exist in the tree\n", key );
        return;
    }
    
    assert( node->type == NON_LEAF_NODE );

    nln = (nonleaf_t *)node;
    
    if (i < 0) 
        i = -i - 1;

    child = _pre_descend_child( tree, node, i );
    
    _descend( tree, child, key );
    
    if( node->n==0 && node==tree->root ){
        _destroy_nonleaf(&nln);
        tree->root = child;
    }
    
    return;
}


// public APIs
void
bptDump(bpt_t *tree)
{
    int i;
    node_t *node;
    leaf_t *leaf;

    printf("*******Tree dump*******\n");

    if( !tree->root ){
        printf("Empty tree\n");
        return;
    }

    node = tree->root;

    while( node )
        if( node->type == NON_LEAF_NODE )
            node = ((nonleaf_t *)node)->children[0];
        else
            break;
        
    assert( node->type == LEAF_NODE );

    leaf = (leaf_t *) node;

    while( leaf ){
        for(i=0; i<leaf->node.n; i++)
            printf("Key=%d, data=%d\n", leaf->node.key[i], leaf->data[i] );  

        leaf = leaf->next;
    }

    return;
}

int
bptGet( bpt_t *tree, int key )
{
    if( !tree->root )
        return 0;

    return _node_search( tree->root, key );
}


void
bptPut( bpt_t *tree, int key, int data)
{
    node_t *node;
    nonleaf_t *s;

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
bptRemove( bpt_t *tree, int key ){
    
    if( !tree->root )
        printf("Empty tree! No deletion\n");
    else
        return _descend( tree, tree->root, key );
}


void 
bptTraverse( bpt_t *tree, int search_type )
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

bpt_t *
bptInit( int b )
{
    bpt_t *t;

    assert( b>2 );

    t = ( bpt_t * ) malloc ( sizeof(bpt_t) );
    
    if( t ){
        t->b_factor = b;
        t->root = NULL;
        t->max_node_id=0;
        t->node_log = fopen( "node.log", "w+" );
        t->write_log = fopen( "write.log", "w+" );
    }

    bptree = t;

    return t;
}

void
bptDestroy( bpt_t *tree ){

    if( tree->root )
        _destroy(tree->root);

    fclose( tree->node_log );
    fclose( tree->write_log );
     
    free(tree);
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

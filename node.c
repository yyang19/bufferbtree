#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include "core.h"
#include "blk_buf.h"
#include "bftree.h"
#include "quicksort_ll.h"

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
        n->rd_count = 0;
        n->wr_count = 0;
        
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


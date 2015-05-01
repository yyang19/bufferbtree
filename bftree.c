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

#include "bftree.h"
#include "blk_buf.h"
#include "core.h"
#include "node.h"
#include "req.h"

//public API
void * 
bftGet( bft_t *tree, int key )
{
 //   uint32_t c_idx;
    int exact;
    bft_req_t *r;

    if( !tree || key == INVALID_KEY )
        return NULL;

    r = request_get( tree->opts->key_compare, tree->top_buffer->req_first, key, &exact );

    if( exact ){
        if( r->type == BT_PUT )
            return r->val;
        else if( r->type == BT_DEL )
            return NULL;
    }
/*
    c_idx = container_find( tree->opt->key_compare, tree->root, key, 0 );
    r = container_get( tree, tree->root, c_idx, key );
*/
    return r ? r->val : NULL;
}

void
bftPut( bft_t *tree, int key, void *val )
{
    bft_req_t *new_req;

    new_req = req_create( key, val, BT_PUT );

    if( tree->opts->log )
        fprintf( tree->req_log, "[Put] Key=%d at <%ld.%06ld>", key, (long int)(new_req->tm.tv_sec), (long int)(new_req->tm.tv_usec) );

    request_collect( tree, new_req );

    return;
}

void
bftRemove( bft_t *tree, int key )
{
    bft_req_t *new_req;

    new_req = req_create( key, NULL, BT_DEL );

    if( tree->opts->log )
        fprintf( tree->req_log, "[Del] Key=%d at <%ld.%06ld>", key, (long int)(new_req->tm.tv_sec), (long int)(new_req->tm.tv_usec) );

    request_collect( tree, new_req );
}


void 
bftTraverse( bft_t *tree, int search_type )
{
    if( !tree->root ){
        printf("Empty tree! \n");
        return;
    }

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
bftCreate( int a, int b, int m, int B, bft_opts_t *opts )
{
    bft_t *t;

    if( a>=b || m<1 ){
        printf( "Invalid tree parameters\n" );
        return NULL;
    }

    t = ( bft_t * ) malloc ( sizeof(bft_t) );
    
    if( t ){

        t->a = a;
        t->b = b;
        t->B = B;
        t->m = m;
        t->c = B/sizeof(bft_req_t);
        
        t->top_buffer = (blk_buffer_t *)malloc(sizeof(blk_buffer_t));
        if( !t->top_buffer )
            goto fail_top_buffer;
        t->top_buffer->req_first = NULL;
        t->top_buffer->req_count = 0;

        t->nNode = 0;
        t->root = node_create( t, NULL, LEAF_NODE );
        if( !t->root )
            goto fail_root;

        t->opts = opts;
        t->del_req_count = t->put_req_count = 0;
        t->req_log = fopen( "request.log", "w+" );
        t->node_log = fopen( "node.log", "w+" );
        t->write_log = fopen( "write.log", "w+" );

        goto out;
    }

fail_root:
    free(t->top_buffer);
fail_top_buffer:
    free(t);
    t=NULL;
out:    
    return t;
}

void
bftDestroy( bft_t *t ){

    if( t->root )
        node_free(t, t->root);

    bb_free( t, t->top_buffer );

    fclose( t->req_log );
    fclose( t->node_log );
    fclose( t->write_log );
     
    free(t);
}


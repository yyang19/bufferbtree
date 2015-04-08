/*  tree.h
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
#ifndef _HEADER_TREE_
#define _HEADER_TREE_

#include <stdint.h>
#include <stdio.h>
#include <sys/time.h>

#define KEY_NOT_FOUND (-1)
#define DATA_NOT_EXIST (-1)
#define INVALID_KEY (-1)

enum {
    LEAF_NODE,
    NON_LEAF_NODE = 1,
};

enum {
    TRAVERSE_BFS,
    TRAVERSE_DFS = 1,
};

typedef struct req_list{
    bft_req_t *req_first;
    bft_req_t *req_last;
    int req_count;
}rlist_t;

typedef rlist_t blk_buffer_t;

typedef struct node {
    int id;
    int type;
    int nElem;

    struct node *parent;
    blk_buffer_t **containers;
    int container_count;
    int container_size;
    
    //child node
    struct node *child;

    int wr_count;
}node_t;

typedef struct non_leaf {
    node_t node;
    node_t **children;
}nonleaf_t;

typedef struct leaf {
    node_t node;
    struct leaf *next;
    int *data;
}leaf_t;

blk_buffer_t * container_create( void );

void node_free( bft_t *, node_t * );
#endif

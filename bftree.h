
/*  bufferbtree.h
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
#ifndef _HEADER_BUFFERBTREE_
#define _HEADER_BUFFERBTREE_

#include <stdint.h>
#include <stdio.h>

#define INVALID_KEY (-1)

typedef int (*key_compare_func)(const void *key1, const void *key2);

typedef enum {
    BT_GET,
    BT_PUT,
    BT_DEL
}bt_op_t;

typedef struct bft_opts{
    int log;
    key_compare_func key_compare;
}bft_opts_t;

typedef struct request{
    int key;    //4 bytes
    int type;   //4 bytes
    void *val;  //8 bytes
    struct timeval tm;  //16 bytes
    struct request *next; //8 bytes
}bft_req_t;

typedef struct req_list{
    bft_req_t *req_first;
    bft_req_t *req_last;
    int req_count;
}rlist_t;

typedef rlist_t blk_buffer_t;


struct tree {
    int a;
    int b;
    int B;
    int m; // # of blocks buffered with each node
    struct node *root;

    blk_buffer_t top_buffer;

    bft_opts_t *opts;

    // derived param
    int c; // # of request in a block
    //statistics
    int nNode;
    int del_req_count;
    int put_req_count;
    // logging
    FILE *req_log;
    FILE *node_log;
    FILE *write_log;
};

typedef struct tree bft_t;

bft_t * bftCreate( int, int, int, int, bft_opts_t * );
void bftDestroy( bft_t * );
void *bftGet( bft_t *, int );
void bftPut( bft_t *, int, void * );
void bftRemove( bft_t *, int );
void bftDump( bft_t * );
void bftTraverse( bft_t *, int );

#endif

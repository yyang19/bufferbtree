
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
#include "core.h"

typedef int (*key_compare_func)(const void *key1, const void *key2);


enum {
    BT_GET,
    BT_PUT,
    BT_DEL
}bt_op_t;

typedef struct request{
    uint32_t key;
    uint32_t val;
    struct request *next;
    bt_op_t type;
}bft_req_t;

struct bft_opts{
    key_compare_func key_compare;
}bft_opts_t;

struct tree {
    int a;
    int b;
    int M;
    int B;
    struct node *root;

    bft_opts_t *opts;

    //statistics
    int del_payload_count;
    int put_payload_count;
    // logging
    FILE *node_log;
    FILE *write_log;
};

typedef struct tree bft_t;

bft_t * bftCreate( int );
void bftDestroy( bft_t * );
int bftGet( bft_t *, int );
void bftPut( bft_t *, int, int );
void bftRemove( bft_t *, int );
void bftDump( bft_t * );
void bftTraverse( bft_t *, int );

#endif

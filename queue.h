/*  queue.h
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
#ifndef _HEADER_BPLUSTREE_FIFO_
#define _HEADER_BPLUSTREE_FIFO_

#include <stdint.h>
#include "bplustree.h"

typedef struct
{
    node_t **array;

    int firstIdx;
    int lastIdx;
    int cnt;
    int len;
    
} fifo_t;

fifo_t * _fifo_init(int);
void _fifo_destroy( fifo_t * );
void _fifo_push( fifo_t *, node_t * );
node_t * _fifo_peek( fifo_t * );
void _fifo_pop( fifo_t * );
int _fifo_empty( fifo_t * );

#endif //_HEADER_BPLUSTREE_FIFO_

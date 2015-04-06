/*  queue.c
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

#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include "queue.h"

fifo_t *
_fifo_init( int n )
{
    fifo_t *fifo;

    fifo = (fifo_t *)malloc( sizeof(fifo_t) );

    fifo->array= (node_t **)malloc(n*sizeof(node_t*));
    fifo->firstIdx = 0;
    fifo->lastIdx = 0;
    fifo->cnt = 0;
    fifo->len = n;

    return fifo;
}

void
_fifo_destroy( fifo_t *fifo )
{
    free( fifo->array );
    free( fifo );
}

void 
_fifo_push( fifo_t *fifo, node_t *node )
{
    fifo->array[fifo->lastIdx++] = node;

    if (fifo->lastIdx == fifo->len)
        fifo->lastIdx = 0;
    
    fifo->cnt++;

    /* Check if queue is full */
    if (fifo->cnt>fifo->len)
    {
        fifo->cnt = fifo->len;

        fifo->firstIdx++;
        if (fifo->firstIdx == fifo->len)
            fifo->firstIdx = 0;
    }
}

void
_fifo_pop( fifo_t *fifo )
{
    if( fifo->cnt>0 )
    {
        fifo->firstIdx++;
        if (fifo->firstIdx == fifo->len)
            fifo->firstIdx = 0;
        
        fifo->cnt--;
    }
}

node_t *
_fifo_peek( fifo_t *fifo )
{
    return fifo->cnt>0 ? fifo->array[fifo->firstIdx] : NULL;
}

int
_fifo_empty(fifo_t *fifo)
{
    return fifo->cnt==0;
}


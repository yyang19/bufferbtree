#ifndef _HEADER_QUICKSORT_LL_
#define _HEADER_QUICKSORT_LL_

#include "bftree.h"

void quicksort_ll( bft_req_t **headRef, int (*comp)( const void *a, const void *b) );
bft_req_t *mergelists( bft_req_t *list1, bft_req_t *list2, int (*comp)( const void *a, const void *b) );
void  quicksort_ll_dump( bft_req_t *head );

#endif

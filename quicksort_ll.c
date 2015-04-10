#include <stdlib.h>
#include <stdio.h>
#include "bftree.h"
#include "util.h"
#include "core.h"
#include "quicksort_ll.h"

#define QSORT_LL_SWAP( a, b ) \
        bft_req_t *ra; \
        bft_req_t *rb; \
        int key; \
        int type; \
        void *val; \
        struct timeval tm; \
        struct request *next; \
        do{ \
            ra          = (bft_req_t *)a;   \
            rb          = (bft_req_t *)b;   \
            key         = ra->key;          \
            type        = ra->type;         \
            val         = ra->val;          \
            tm          = ra->tm;           \
            next        = ra->next;         \
            ra->key     = rb->key;          \
            ra->type    = rb->type;         \
            ra->val     = rb->val;          \
            ra->tm      = rb->tm;           \
            ra->next    = rb->next;         \
            rb->key     = key;              \
            rb->type    = type;             \
            rb->val     = val;              \
            rb->tm      = tm;               \
            rb->next    = next;             \
        }while(0) 

bft_req_t *
mergelists( bft_req_t *list1, 
            bft_req_t *list2, 
            int (*comp)( const void *a, const void *b) ) 
{
    if (list1 == NULL) return list2;
    if (list2 == NULL) return list1;
    
    if ( comp( list1, list2 )==0 ) {
        list1->next = mergelists(list1->next, list2, comp);
        return list1;
    } else {
        list2->next = mergelists(list2->next, list1, comp);
        return list2;
    }
}

void 
quicksort_ll_dump( bft_req_t *head )
{
    bft_req_t *req = head;

    while(req)
    {
        req_dump(req);
        req = req->next;
    }
    printf("\n");
}

static bft_req_t *
getTail( bft_req_t *cur )
{
    while (cur != NULL && cur->next != NULL)
        cur = cur->next;
    return cur;
}

// Partitions the list taking the last element as the pivot
static bft_req_t *
partition( bft_req_t *head, 
           bft_req_t *endd, 
           bft_req_t **newHead, 
           bft_req_t **newEnd,
           int (*comp)( const void *a, const void *b) )
{
    bft_req_t *pivot = endd;
    bft_req_t dummy;
    dummy.next=head;
    bft_req_t *prev = &dummy;
    bft_req_t *curr = head;

    int temp;
    while(curr!=pivot)
    {
        if( comp(curr,pivot)==0 ) //curr<pivot
        {
            prev=prev->next;
            QSORT_LL_SWAP( prev, curr );
        }

        curr=curr->next;
    }

    temp=prev->next->key;
    prev->next->key=pivot->key;
    pivot->key=temp;
    *newHead=head;
    *newEnd=endd;
    return (prev->next);

}

//here the sorting happens exclusive of the end node
bft_req_t *quickSortRecur(bft_req_t *head, bft_req_t *endd, int (*comp)( const void *a, const void *b) )
{
    // base condition
    if (!head || head == endd)
        return head;

    bft_req_t *newHead = NULL, *newEnd = NULL;

    // Partition the list, newHead and newEnd will be updated
    // by the partition function
    bft_req_t *pivot = partition(head, endd, &newHead, &newEnd, comp);

    // If pivot is the smallest element - no need to recur for
    // the left part.
    if (newHead != pivot)
    {
        // Set the node before the pivot node as NULL
        bft_req_t *tmp = newHead;
        while (tmp->next != pivot)
            tmp = tmp->next;
        tmp->next = NULL;

        // Recur for the list before pivot
        newHead = quickSortRecur(newHead, tmp, comp);

        // Change next of last node of the left half to pivot
        tmp = getTail(newHead);
        tmp->next =  pivot;
    }

    // Recur for the list after the pivot element
    pivot->next = quickSortRecur(pivot->next, newEnd, comp);

    return newHead;
}

// The main function for quick sort. This is a wrapper over recursive
// function quickSortRecur()
void 
quicksort_ll( bft_req_t **headRef, int (*comp)( const void *a, const void *b) )
{
    (*headRef) = quickSortRecur(*headRef, getTail(*headRef), comp);
    return;
}


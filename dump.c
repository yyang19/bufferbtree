#include "bftree.h"

void
req_dump( bft_req_t *req ){

     printf( "Key = %d\t", req->key );
     printf( "Type = %d\t", req->type );
     printf( "Arrival time = <%ld.%06ld>\n", (long int)(req->tm.tv_sec), (long int)(req->tm.tv_usec) );
}

void
bb_dump( blk_buffer_t *bb ){

    bft_req_t *curr = bb->req_first;

    while( curr ){
        req_dump( curr );
        curr = curr->next;
    };

    return;
}

void
list_dump( bft_req_t *head )
{
    bft_req_t *curr = head;

    while( curr ){
        req_dump( curr );
        curr = curr->next;
    };

    return;
}

void
node_dump( node_t *n )
{
    int i;

    printf("Dump node (%p)\n", (void *)n);
    printf("ID : %d\n", n->id);
    printf("Type : %d\n", n->type);
    printf("Parent : %p\n", (void *)n->parent);
    printf("Buffer count : %d\n", n->bb_count);
    printf("Buffer size : %d\n", n->bb_size);
    
    for( i=0; i<n->bb_size; i++ )
        bb_dump( n->containers[i] );
    printf("\n");

    printf("Key count : %d\n", n->key_count);
    printf("Keys : ");
    for( i=0; i<n->key_count; i++ )
        printf("%d\t", n->keys[i]);
    printf("\n");
    
    printf("Children : ");
    for( i=0; i<=n->key_count; i++ )
        printf("%p\t", (void *)n->child[i]);
    printf("\n");

    printf("Read count : %d\n", n->rd_count);
    printf("Write count : %d\n", n->wr_count);
}


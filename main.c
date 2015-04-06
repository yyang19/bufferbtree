#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <assert.h>

#include "bplustree.h"

#define MAX (1<<10)
#define TC_0_TRIAL (10000000)

FILE *write_log;

static void
_help( void ){
    printf( "B+ Tree options:\n" );
    printf("\t args: [1]branching factor r \n");
}

int 
random_in_range (unsigned int min, unsigned int max){
    
    int base_random = rand(); /* in [0, RAND_MAX] */
    
    if (RAND_MAX == base_random) return random_in_range(min, max);
    /* now guaranteed to be in [0, RAND_MAX) */
    int range       = max - min,
    remainder   = RAND_MAX % range,
    bucket      = RAND_MAX / range;
    /* There are range buckets, plus one smaller interval
    * *      *  within remainder of RAND_MAX */
    
   if (base_random < RAND_MAX - remainder) 
       return min + base_random/bucket;
    else 
       return random_in_range (min, max);
}

    static void 
shuffle_array( int *a, int ub )
{
    int i,j;
    int temp;

    for( i=ub-1; i>0; i-- ){
        j = random_in_range(0, i+1);
        temp = a[i];
        a[i]=a[j];
        a[j]=temp;
    }

}

static int *
create_array( int n, int max ){
    
    int in, im;
    int *a;

    im = 0;

    a = (int *)malloc( n*sizeof(int) );

    for (in = 0; in < max && im < n; ++in) {
        int rn = max - in;
        int rm = n - im;
        if ( random_in_range( 1,max ) % rn < rm)    
            /* Take it */
            a[im++] = in + 1; /* +1 since your range begins from 1 */
    }

    assert(im == n);

    shuffle_array( a, n );

    return a;
}

static void
reset_array( int *a, int len )
{
    int i;

    for( i=0; i<len; i++ )
        a[i]=-1;
}

static void
destroy_array( int *a ){
    free(a);
}

int main( int argc, char *argv[] ){
    
    int result = 0;
    int b,n;
    bpt_t *t; 
    int *keys;
    
    if( argc!=3 ){
        _help();
        result = -1;
    }

    if( result == 0 ){
        b = atoi( argv[1] );
        n = atoi( argv[2] );
        t = bptInit(b);
    }

    if( !t )
        return -1;    

#if 0  
     bptPut(t, 90, 0);
     bptDump(t);
     bptPut(t, 88, 0);
     bptDump(t);
     bptPut(t, 74, 0);
     bptDump(t);
     bptPut(t, 72, 0);
     bptDump(t);
     bptPut(t, 68, 0);
     bptDump(t);
     bptPut(t, 63, 0);
     bptDump(t);
     bptPut(t, 53, 0);
     bptDump(t);
     bptPut(t, 44, 0);
     bptDump(t);
     bptPut(t, 39, 0);
     bptDump(t);
     bptPut(t, 24, 0);
     bptDump(t);
     bptPut(t, 15, 0);
     bptDump(t);
     bptPut(t, 10, 0);
     bptDump(t);
     bptPut(t, 1, 0);
     bptDump(t);
     //bptDump(t);
     printf("key:24 data:%d\n", bptGet(t, 24));
     printf("key:72 data:%d\n", bptGet(t, 72));
     printf("key:01 data:%d\n", bptGet(t, 1));
     printf("key:39 data:%d\n", bptGet(t, 39));
     printf("key:53 data:%d\n", bptGet(t, 53));
     printf("key:63 data:%d\n", bptGet(t, 63));
     printf("key:90 data:%d\n", bptGet(t, 90));
     printf("key:88 data:%d\n", bptGet(t, 88));
     printf("key:15 data:%d\n", bptGet(t, 15));
     printf("key:10 data:%d\n", bptGet(t, 10));
     printf("key:44 data:%d\n", bptGet(t, 44));
     printf("key:68 data:%d\n", bptGet(t, 68));
     printf("key:74 data:%d\n", bptGet(t, 74));
     printf("key:44 data:%d\n", bptGet(t, 44));
     printf("key:45 data:%d\n", bptGet(t, 45));
     printf("key:46 data:%d\n", bptGet(t, 46));
     printf("key:47 data:%d\n", bptGet(t, 47));
     printf("key:321 data:%d\n", bptGet(t, 321));
     printf("key:5528 data:%d\n", bptGet(t, 5528));
     printf("key:848 data:%d\n", bptGet(t, 848));
     printf("key:748 data:%d\n", bptGet(t, 748));
     printf("key:1528 data:%d\n", bptGet(t, 1528));
     printf("key:2528 data:%d\n", bptGet(t, 2528));
     bptRemove(t, 90);
     bptDump(t);
     bptRemove(t, 88);
     bptDump(t);
     bptRemove(t, 74);
     bptDump(t);
     bptRemove(t, 72);
     bptDump(t);
     bptRemove(t, 68);
     bptDump(t);
     bptRemove(t, 63);
     bptDump(t);
     bptRemove(t, 53);
     bptDump(t);
     bptRemove(t, 44);
     bptDump(t);
     bptRemove(t, 39);
     bptDump(t);
     bptRemove(t, 24);
     bptDump(t);
     bptRemove(t, 15);
     bptDump(t);
     bptRemove(t, 10);
     bptDump(t);
     bptRemove(t, 1);
     bptDump(t);
#endif
     
     int i;
#if 0
     /* Ordered insertion and deletion */
     for (i = 1; i <= n; i++) {
         bptPut(t, i, i);
     }
     
     bptTraverse(t);
     
     for (i = 1; i <= n; i++) {
         bptRemove(t, i);
     }
     bptDump(t);
     
     /* Ordered insertion and reversed deletion */
     for (i = 1; i <= n; i++) {
         bptPut(t, i, i);
     }
     bptDump(t);
     while (--i > 0) {
         bptRemove(t, i);
     }
     bptDump(t);
     
     /* Reversed insertion and ordered deletion */
     for (i = n; i > 0; i--) {
         bptPut(t, i, i);
     }
     bptDump(t);
     for (i = 1; i <= n; i++) {
         bptRemove(t, i);
     }
     bptDump(t);

     /* Reversed insertion and reversed deletion */
     for (i = n; i > 0; i--) {
         bptPut(t, i, i);
     }
     bptDump(t);
     
     for (i = n; i > 0; i--) {
         bptRemove(t, i);
     }
     bptDump(t);
    
#endif
#if 1     
     keys = create_array( n, n );
     
     for (i = 0; i < n; i++) {
         //bptPut(t, i, i);
         bptPut(t, n-i, n-i);
         //bptPut(t, keys[i], keys[i]);
     }
     //bptDump(t);
     
     for (i = 0; i < n; i++) {
         //bptRemove(t, i);
         //bptRemove(t, n-i);
         //bptRemove(t, keys[i]);
     }

     bptTraverse(t,TRAVERSE_BFS);
#endif
     //bptRemove(t, keys[0]);

     bptDestroy( t );
     reset_array( keys, n);
     destroy_array( keys );

     return result;
}


#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <assert.h>

#include "bftree.h"

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

int 
comp_int( const void *a, const void *b )
{
    return (*((int *)a)) > (*((int *)b)) ; 
}

void
node_write( node_t *n, int b_idx ){
    
    ++n->wr_count;
} 

int main( int argc, char *argv[] ){
    
    int result = 0;
    int n;
    bft_t *t; 
    int *keys;
    int val;
    int m = 8;
    int B = 320;

    bft_opts_t opts;
    opts.log = 1;
    opts.key_compare = &comp_int;
    opts.write = &node_write;
    
    if( argc!=3 ){
        _help();
        result = -1;
    }

    if( result == 0 ){
        m = atoi( argv[1] );
        n = atoi( argv[2] );
        t = bftCreate(m/4,m,m,B,&opts);
    }

    if( !t )
        return -1;    

#if 0  
     bftPut(t, 90, 0);
     bftDump(t);
     bftPut(t, 88, 0);
     bftDump(t);
     bftPut(t, 74, 0);
     bftDump(t);
     bftPut(t, 72, 0);
     bftDump(t);
     bftPut(t, 68, 0);
     bftDump(t);
     bftPut(t, 63, 0);
     bftDump(t);
     bftPut(t, 53, 0);
     bftDump(t);
     bftPut(t, 44, 0);
     bftDump(t);
     bftPut(t, 39, 0);
     bftDump(t);
     bftPut(t, 24, 0);
     bftDump(t);
     bftPut(t, 15, 0);
     bftDump(t);
     bftPut(t, 10, 0);
     bftDump(t);
     bftPut(t, 1, 0);
     bftDump(t);
     //bftDump(t);
     printf("key:24 data:%d\n", bftGet(t, 24));
     printf("key:72 data:%d\n", bftGet(t, 72));
     printf("key:01 data:%d\n", bftGet(t, 1));
     printf("key:39 data:%d\n", bftGet(t, 39));
     printf("key:53 data:%d\n", bftGet(t, 53));
     printf("key:63 data:%d\n", bftGet(t, 63));
     printf("key:90 data:%d\n", bftGet(t, 90));
     printf("key:88 data:%d\n", bftGet(t, 88));
     printf("key:15 data:%d\n", bftGet(t, 15));
     printf("key:10 data:%d\n", bftGet(t, 10));
     printf("key:44 data:%d\n", bftGet(t, 44));
     printf("key:68 data:%d\n", bftGet(t, 68));
     printf("key:74 data:%d\n", bftGet(t, 74));
     printf("key:44 data:%d\n", bftGet(t, 44));
     printf("key:45 data:%d\n", bftGet(t, 45));
     printf("key:46 data:%d\n", bftGet(t, 46));
     printf("key:47 data:%d\n", bftGet(t, 47));
     printf("key:321 data:%d\n", bftGet(t, 321));
     printf("key:5528 data:%d\n", bftGet(t, 5528));
     printf("key:848 data:%d\n", bftGet(t, 848));
     printf("key:748 data:%d\n", bftGet(t, 748));
     printf("key:1528 data:%d\n", bftGet(t, 1528));
     printf("key:2528 data:%d\n", bftGet(t, 2528));
     bftRemove(t, 90);
     bftDump(t);
     bftRemove(t, 88);
     bftDump(t);
     bftRemove(t, 74);
     bftDump(t);
     bftRemove(t, 72);
     bftDump(t);
     bftRemove(t, 68);
     bftDump(t);
     bftRemove(t, 63);
     bftDump(t);
     bftRemove(t, 53);
     bftDump(t);
     bftRemove(t, 44);
     bftDump(t);
     bftRemove(t, 39);
     bftDump(t);
     bftRemove(t, 24);
     bftDump(t);
     bftRemove(t, 15);
     bftDump(t);
     bftRemove(t, 10);
     bftDump(t);
     bftRemove(t, 1);
     bftDump(t);
#endif
     
     int i;
#if 0
     /* Ordered insertion and deletion */
     for (i = 1; i <= n; i++) {
         bftPut(t, i, i);
     }
     
     bftTraverse(t);
     
     for (i = 1; i <= n; i++) {
         bftRemove(t, i);
     }
     bftDump(t);
     
     /* Ordered insertion and reversed deletion */
     for (i = 1; i <= n; i++) {
         bftPut(t, i, i);
     }
     bftDump(t);
     while (--i > 0) {
         bftRemove(t, i);
     }
     bftDump(t);
     
     /* Reversed insertion and ordered deletion */
     for (i = n; i > 0; i--) {
         bftPut(t, i, i);
     }
     bftDump(t);
     for (i = 1; i <= n; i++) {
         bftRemove(t, i);
     }
     bftDump(t);

     /* Reversed insertion and reversed deletion */
     for (i = n; i > 0; i--) {
         bftPut(t, i, i);
     }
     bftDump(t);
     
     for (i = n; i > 0; i--) {
         bftRemove(t, i);
     }
     bftDump(t);
    
#endif
#if 1     
     keys = create_array( n, n );
     
     for (i = 0; i < n; i++) {
         val = i;
         bftPut(t, i, (void *)&val );
         //bftPut(t, keys[i], keys[i]);
     }
     //bftDump(t);
     
     //bftTraverse(t,TRAVERSE_BFS);
#endif
     //bftRemove(t, keys[0]);

     bftDestroy( t );
     reset_array( keys, n);
     destroy_array( keys );

     return result;
}


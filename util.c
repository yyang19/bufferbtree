#include <sys/time.h>

/* struct timeval {
 *     long tv_sec;         seconds 
 *     long tv_usec;   microseconds 
 * }
 */

long 
timevaldiff(struct timeval *ta, struct timeval *tb)
{
      long msec;
      msec=(tb->tv_sec-ta->tv_sec)*1000;
      msec+=(tb->tv_usec-ta->tv_usec)/1000;
      return msec;
}


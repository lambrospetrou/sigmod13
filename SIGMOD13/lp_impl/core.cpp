/*
 * core.cpp version 1.0
 * Copyright (c) 2013 KAUST - InfoCloud Group (All Rights Reserved)
 * Author: Amin Allam
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "../include/core.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <pthread.h>
#include <semaphore.h>

#include <vector>
#include <list>
#include <algorithm>

///////////////////////////////////////////////////////////////////////////////////////////////

int gettime()
{
	struct timeval t2; gettimeofday(&t2,NULL);
	return t2.tv_sec*1000+t2.tv_usec/1000;
}

void err_mem(char* msg){
   perror(msg);
   exit(1);
}

/********************************************************************************************
 *  THREADPOOL STRUCTURE
 *************************************/

// ORACLE THREADPOOL EXAMPLE
// http://docs.oracle.com/cd/E19963-01/html/821-1601/ggfbr.html#ggecw

/*
 * Declarations for the clients of a thread pool.
 */

/*
 * The thr_pool_t type is opaque to the client.
 * It is created by thr_pool_create() and must be passed
 * unmodified to the remainder of the interfaces.
 */
typedef    struct thr_pool    thr_pool_t;

/*
 * Create a thread pool.
 *    min_threads:    the minimum number of threads kept in the pool,
 *            always available to perform work requests.
 *    max_threads:    the maximum number of threads that can be
 *            in the pool, performing work requests.
 *    linger:        the number of seconds excess idle worker threads
 *            (greater than min_threads) linger before exiting.
 *    attr:        attributes of all worker threads (can be NULL);
 *            can be destroyed after calling thr_pool_create().
 * On error, thr_pool_create() returns NULL with errno set to the error code.
 */
extern    thr_pool_t    *thr_pool_create(unsigned int min_threads, unsigned int max_threads,
                unsigned int linger, pthread_attr_t *attr);

/*
 * Enqueue a work request to the thread pool job queue.
 * If there are idle worker threads, awaken one to perform the job.
 * Else if the maximum number of workers has not been reached,
 * create a new worker thread to perform the job.
 * Else just return after adding the job to the queue;
 * an existing worker thread will perform the job when
 * it finishes the job it is currently performing.
 *
 * The job is performed as if a new detached thread were created for it:
 *    pthread_create(NULL, attr, void *(*func)(void *), void *arg);
 *
 * On error, thr_pool_queue() returns -1 with errno set to the error code.
 */
extern    int    thr_pool_queue(thr_pool_t *pool,
            void *(*func)(void *), void *arg);

/*
 * Wait for all queued jobs to complete.
 */
extern    void    thr_pool_wait(thr_pool_t *pool);

/*
 * Cancel all queued jobs and destroy the pool.
 */
extern    void    thr_pool_destroy(thr_pool_t *pool);


/*
 * Thread pool implementation.
 * See <thr_pool.h> for interface declarations.
 */

#if !defined(_REENTRANT)
#define    _REENTRANT
#endif

#include <stdlib.h>
#include <signal.h>
#include <errno.h>

/*
 * FIFO queued job
 */
typedef struct job job_t;
struct job {
    job_t    *job_next;        /* linked list of jobs */
    void    *(*job_func)(void *);    /* function to call */
    void    *job_arg;        /* its argument */
};

/*
 * List of active worker threads, linked through their stacks.
 */
typedef struct active active_t;
struct active {
    active_t    *active_next;    /* linked list of threads */
    pthread_t    active_tid;    /* active thread id */
};

/*
 * The thread pool, opaque to the clients.
 */
struct thr_pool {
    thr_pool_t    *pool_forw;    /* circular linked list */
    thr_pool_t    *pool_back;    /* of all thread pools */
    pthread_mutex_t    pool_mutex;    /* protects the pool data */
    pthread_cond_t    pool_busycv;    /* synchronization in pool_queue */
    pthread_cond_t    pool_workcv;    /* synchronization with workers */
    pthread_cond_t    pool_waitcv;    /* synchronization in pool_wait() */
    active_t    *pool_active;    /* list of threads performing work */
    job_t        *pool_head;    /* head of FIFO job queue */
    job_t        *pool_tail;    /* tail of FIFO job queue */
    pthread_attr_t    pool_attr;    /* attributes of the workers */
    int        pool_flags;    /* see below */
    unsigned int        pool_linger;    /* seconds before idle workers exit */
    int        pool_minimum;    /* minimum number of worker threads */
    int        pool_maximum;    /* maximum number of worker threads */
    int        pool_nthreads;    /* current number of worker threads */
    int        pool_idle;    /* number of idle workers */
};

/* pool_flags */
#define    POOL_WAIT    0x01        /* waiting in thr_pool_wait() */
#define    POOL_DESTROY    0x02        /* pool is being destroyed */

/* the list of all created and not yet destroyed thread pools */
static thr_pool_t *thr_pools = NULL;

/* protects thr_pools */
static pthread_mutex_t thr_pool_lock = PTHREAD_MUTEX_INITIALIZER;

/* set of all signals */
static sigset_t fillset;

static void *worker_thread(void *);

static int
create_worker(thr_pool_t *pool)
{
    sigset_t oset;
    int error;
    pthread_t pid;

    (void) pthread_sigmask(SIG_SETMASK, &fillset, &oset);
    error = pthread_create( &pid, &pool->pool_attr, worker_thread, pool);
    (void) pthread_sigmask(SIG_SETMASK, &oset, NULL);

    return (error);
}

/*
 * Worker thread is terminating.  Possible reasons:
 * - excess idle thread is terminating because there is no work.
 * - thread was cancelled (pool is being destroyed).
 * - the job function called pthread_exit().
 * In the last case, create another worker thread
 * if necessary to keep the pool populated.
 */
static void
worker_cleanup(thr_pool_t *pool)
{
    --pool->pool_nthreads;
    if (pool->pool_flags & POOL_DESTROY) {
        if (pool->pool_nthreads == 0)
            (void) pthread_cond_broadcast(&pool->pool_busycv);
    } else if (pool->pool_head != NULL &&
        pool->pool_nthreads < pool->pool_maximum &&
        create_worker(pool) == 0) {
        pool->pool_nthreads++;
    }
    (void) pthread_mutex_unlock(&pool->pool_mutex);
}

static void
notify_waiters(thr_pool_t *pool)
{
    if (pool->pool_head == NULL && pool->pool_active == NULL) {
        pool->pool_flags &= ~POOL_WAIT;
        (void) pthread_cond_broadcast(&pool->pool_waitcv);
    }
}

/*
 * Called by a worker thread on return from a job.
 */
static void
job_cleanup(thr_pool_t *pool)
{
    pthread_t my_tid = pthread_self();
    active_t *activep;
    active_t **activepp;

    (void) pthread_mutex_lock(&pool->pool_mutex);
    for (activepp = &pool->pool_active;
        (activep = *activepp) != NULL;
        activepp = &activep->active_next) {
        if (activep->active_tid == my_tid) {
            *activepp = activep->active_next;
            break;
        }
    }
    if (pool->pool_flags & POOL_WAIT)
        notify_waiters(pool);
}

pthread_mutex_t lp_pthreads_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_t lp_pthreads[16];
int lp_position=0;

struct LP_ThreadArgs{
	void* args;
	char tid;
};

static void *
worker_thread(void *arg)
{
    thr_pool_t *pool = (thr_pool_t *)arg;
    int timedout;
    job_t *job;
    void *(*func)(void *);
    active_t active;

    char tid;
    pthread_mutex_lock( &lp_pthreads_mutex );
    lp_pthreads[lp_position ] = pthread_self() ;
    tid = lp_position++;
    pthread_mutex_unlock( &lp_pthreads_mutex );

    fprintf( stderr, "new thread created pthread_t[%p] tid[%d]\n", lp_pthreads[tid], tid );

    struct timeval tp;
    struct timespec ts;

    /*
     * This is the worker's main loop.  It will only be left
     * if a timeout occurs or if the pool is being destroyed.
     */
    (void) pthread_mutex_lock(&pool->pool_mutex);
    pthread_cleanup_push( reinterpret_cast<void (*)(void*)>(worker_cleanup), pool);
    active.active_tid = pthread_self();
    for (;;) {
        /*
         * We don't know what this thread was doing during
         * its last job, so we reset its signal mask and
         * cancellation state back to the initial values.
         */
        (void) pthread_sigmask(SIG_SETMASK, &fillset, NULL);
        (void) pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);
        (void) pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);

        timedout = 0;
        pool->pool_idle++;
        if (pool->pool_flags & POOL_WAIT)
            notify_waiters(pool);
        while (pool->pool_head == NULL &&
            !(pool->pool_flags & POOL_DESTROY)) {
            if (pool->pool_nthreads <= pool->pool_minimum) {
                (void) pthread_cond_wait(&pool->pool_workcv,
                    &pool->pool_mutex);
            } else {
                gettimeofday(&tp, NULL);
                ts.tv_sec = tp.tv_sec;
                ts.tv_nsec = tp.tv_usec * 1000;
                ts.tv_sec += pool->pool_linger;
                if (pool->pool_linger == 0 ||
                    pthread_cond_timedwait(&pool->pool_workcv,
                    &pool->pool_mutex, &ts) == ETIMEDOUT) {
                    timedout = 1;
                    break;
                }
            }
        }
        pool->pool_idle--;
        if (pool->pool_flags & POOL_DESTROY)
            break;
        if ((job = pool->pool_head) != NULL) {
            timedout = 0;
            func = job->job_func;
            arg = job->job_arg;
            pool->pool_head = job->job_next;
            if (job == pool->pool_tail)
                pool->pool_tail = NULL;
            active.active_next = pool->pool_active;
            pool->pool_active = &active;
            (void) pthread_mutex_unlock(&pool->pool_mutex);
            pthread_cleanup_push(reinterpret_cast<void (*)(void*)>(job_cleanup), pool);
            free(job);
            /*
             * Call the specified job function.
             */
            (void) func(arg);
            /*
             * If the job function calls pthread_exit(), the thread
             * calls job_cleanup(pool) and worker_cleanup(pool);
             * the integrity of the pool is thereby maintained.
             */
            pthread_cleanup_pop(1);    /* job_cleanup(pool) */
        }
        if (timedout && pool->pool_nthreads > pool->pool_minimum) {
            /*
             * We timed out and there is no work to be done
             * and the number of workers exceeds the minimum.
             * Exit now to reduce the size of the pool.
             */
            break;
        }
    }
    pthread_cleanup_pop(1);    /* worker_cleanup(pool) */
    return (NULL);
}

static void
clone_attributes(pthread_attr_t *new_attr, pthread_attr_t *old_attr)
{
    struct sched_param param;
    void *addr;
    size_t size;
    int value;

    (void) pthread_attr_init(new_attr);

    if (old_attr != NULL) {
        (void) pthread_attr_getstack(old_attr, &addr, &size);
        /* don't allow a non-NULL thread stack address */
        (void) pthread_attr_setstack(new_attr, NULL, size);

        (void) pthread_attr_getscope(old_attr, &value);
        (void) pthread_attr_setscope(new_attr, value);

        (void) pthread_attr_getinheritsched(old_attr, &value);
        (void) pthread_attr_setinheritsched(new_attr, value);

        (void) pthread_attr_getschedpolicy(old_attr, &value);
        (void) pthread_attr_setschedpolicy(new_attr, value);

        (void) pthread_attr_getschedparam(old_attr, &param);
        (void) pthread_attr_setschedparam(new_attr, &param);

        (void) pthread_attr_getguardsize(old_attr, &size);
        (void) pthread_attr_setguardsize(new_attr, size);
    }

    /* make all pool threads be detached threads */
    (void) pthread_attr_setdetachstate(new_attr, PTHREAD_CREATE_DETACHED);
}

thr_pool_t *
thr_pool_create(unsigned int min_threads, unsigned int max_threads, unsigned int linger,
    pthread_attr_t *attr)
{
    thr_pool_t    *pool;

    (void) sigfillset(&fillset);

    if (min_threads > max_threads || max_threads < 1) {
        errno = EINVAL;
        return (NULL);
    }

    if ((pool = (thr_pool_t*)malloc(sizeof (*pool))) == NULL) {
        errno = ENOMEM;
        return (NULL);
    }
    (void) pthread_mutex_init(&pool->pool_mutex, NULL);
    (void) pthread_cond_init(&pool->pool_busycv, NULL);
    (void) pthread_cond_init(&pool->pool_workcv, NULL);
    (void) pthread_cond_init(&pool->pool_waitcv, NULL);
    pool->pool_active = NULL;
    pool->pool_head = NULL;
    pool->pool_tail = NULL;
    pool->pool_flags = 0;
    pool->pool_linger = linger;
    pool->pool_minimum = min_threads;
    pool->pool_maximum = max_threads;
    pool->pool_nthreads = 0;
    pool->pool_idle = 0;

    /*
     * We cannot just copy the attribute pointer.
     * We need to initialize a new pthread_attr_t structure using
     * the values from the caller-supplied attribute structure.
     * If the attribute pointer is NULL, we need to initialize
     * the new pthread_attr_t structure with default values.
     */
    clone_attributes(&pool->pool_attr, attr);

    /* insert into the global list of all thread pools */
    (void) pthread_mutex_lock(&thr_pool_lock);
    if (thr_pools == NULL) {
        pool->pool_forw = pool;
        pool->pool_back = pool;
        thr_pools = pool;
    } else {
        thr_pools->pool_back->pool_forw = pool;
        pool->pool_forw = thr_pools;
        pool->pool_back = thr_pools->pool_back;
        thr_pools->pool_back = pool;
    }
    (void) pthread_mutex_unlock(&thr_pool_lock);

//    // INSTANTIATE THE MINIMUM THREAD WORKERS - Edited by Lambros Petrou
//    for( int i=0; i<pool->pool_minimum; i++ ){
//    	if( create_worker(pool) ){
//    		perror( "Could not instantiate worker immediately" );
//    	}
//    	pool->pool_nthreads++;
//    }

    return (pool);
}

int
thr_pool_queue(thr_pool_t *pool, void *(*func)(void *), void *arg)
{
    job_t *job;

    if ((job = (job_t*)malloc(sizeof (*job))) == NULL) {
        errno = ENOMEM;
        return (-1);
    }
    job->job_next = NULL;
    job->job_func = func;
    job->job_arg = arg;

    (void) pthread_mutex_lock(&pool->pool_mutex);

    if (pool->pool_head == NULL)
        pool->pool_head = job;
    else
        pool->pool_tail->job_next = job;
    pool->pool_tail = job;

    if (pool->pool_idle > 0)
        (void) pthread_cond_signal(&pool->pool_workcv);
    else if (pool->pool_nthreads < pool->pool_maximum &&
        create_worker(pool) == 0)
        pool->pool_nthreads++;

    (void) pthread_mutex_unlock(&pool->pool_mutex);
    return (0);
}

void
thr_pool_wait(thr_pool_t *pool)
{
    (void) pthread_mutex_lock(&pool->pool_mutex);
    pthread_cleanup_push(reinterpret_cast<void (*)(void*)>(pthread_mutex_unlock), &pool->pool_mutex);
    while (pool->pool_head != NULL || pool->pool_active != NULL) {
        pool->pool_flags |= POOL_WAIT;
        (void) pthread_cond_wait(&pool->pool_waitcv, &pool->pool_mutex);
    }
    pthread_cleanup_pop(1);    /* pthread_mutex_unlock(&pool->pool_mutex); */
}

void
thr_pool_destroy(thr_pool_t *pool)
{
    active_t *activep;
    job_t *job;

    (void) pthread_mutex_lock(&pool->pool_mutex);
    pthread_cleanup_push(reinterpret_cast<void (*)(void*)>(pthread_mutex_unlock), &pool->pool_mutex);

    /* mark the pool as being destroyed; wakeup idle workers */
    pool->pool_flags |= POOL_DESTROY;
    (void) pthread_cond_broadcast(&pool->pool_workcv);

    /* cancel all active workers */
    for (activep = pool->pool_active;
        activep != NULL;
        activep = activep->active_next)
        (void) pthread_cancel(activep->active_tid);

    /* wait for all active workers to finish */
    while (pool->pool_active != NULL) {
        pool->pool_flags |= POOL_WAIT;
        (void) pthread_cond_wait(&pool->pool_waitcv, &pool->pool_mutex);
    }

    /* the last worker to terminate will wake us up */
    while (pool->pool_nthreads != 0)
        (void) pthread_cond_wait(&pool->pool_busycv, &pool->pool_mutex);

    pthread_cleanup_pop(1);    /* pthread_mutex_unlock(&pool->pool_mutex); */

    /*
     * Unlink the pool from the global list of all pools.
     */
    (void) pthread_mutex_lock(&thr_pool_lock);
    if (thr_pools == pool)
        thr_pools = pool->pool_forw;
    if (thr_pools == pool)
        thr_pools = NULL;
    else {
        pool->pool_back->pool_forw = pool->pool_forw;
        pool->pool_forw->pool_back = pool->pool_back;
    }
    (void) pthread_mutex_unlock(&thr_pool_lock);

    /*
     * There should be no pending jobs, but just in case...
     */
    for (job = pool->pool_head; job != NULL; job = pool->pool_head) {
        pool->pool_head = job->job_next;
        free(job);
    }
    (void) pthread_attr_destroy(&pool->pool_attr);
    free(pool);
}

// dummy function to create all the threads
void* dummy_thread_do(void* a){
    return NULL;
}


/********************************************************************************************
 *  THREADPOOL STRUCTURE END
 ********************************************************************************************/

#define NO_LP_LOCKS_ENABLED
//#define LP_LOCKS_ENABLED

#define NUM_THREADS 16
#define TRIES_EACH_TYPE 4
#define TOTAL_SEARCHERS (2*TRIES_EACH_TYPE+1)

#define VALID_CHARS 26

/********************************************************************************************
 *  TRIE STRUCTURE
 *************************************/

typedef struct _QueryNode QueryNode;
struct _QueryNode{
	QueryID qid;
	char pos;
};

typedef std::vector<QueryNode> QueryArrayList;

struct LockingMech{
	char padding[128];
	pthread_mutex_t mutex;
};
typedef struct _ResultTrieSearch ResultTrieSearch;
struct _ResultTrieSearch{
	char padding[128];
	QueryArrayList *qids;
};
// both initialized in InitializeIndex()
ResultTrieSearch global_results[NUM_THREADS+1];
LockingMech global_results_locks[NUM_THREADS+1]; // one lock for each result set

typedef struct _TrieNode TrieNode;
struct _TrieNode{
   TrieNode* children[VALID_CHARS];
   QueryArrayList *qids;
};

// TRIE FUNCTIONS
TrieNode* TrieNode_Constructor(){
   TrieNode* n = (TrieNode*)malloc(sizeof(TrieNode));
   if( !n ) err_mem("error allocating TrieNode");
   n->qids = 0;
   memset( n->children, 0, VALID_CHARS*sizeof(TrieNode*) );
   return n;
}

void TrieNode_Destructor( TrieNode* node ){
    for( char i=0; i<VALID_CHARS; i++ ){
    	if( node->children[i] != 0 ){
            TrieNode_Destructor( node->children[i] );
    	}
    }
    if( node->qids )
        delete node->qids;
    free( node );
}

TrieNode* TrieInsert( TrieNode* node, const char* word, char word_sz, QueryID qid, char word_pos ){
   char ptr=0;
   char pos;
   while( ptr < word_sz ){
      pos = word[ptr] - 'a';
      if( node->children[pos] == 0 ){
         node->children[pos] = TrieNode_Constructor();
      }
      node = node->children[pos];
      ptr++;
   }
   if( !node->qids ){
       node->qids = new QueryArrayList();
   }
   QueryNode qn;
   qn.qid = qid;
   qn.pos = word_pos;
   node->qids->push_back(qn);
   return node;
}

////////////////////////////////////////////////
// above are the same regardless of query type
////////////////////////////////////////////////

void TrieExactSearchWord( LockingMech* lockmech, TrieNode* root, const char* word, char word_sz, char tid, ResultTrieSearch* local_results ){
	//fprintf( stderr, "[1] [%p] [%p] [%.*s] [%d] [%p]\n", lockmech, root, word_sz, word, 0, results );

   char p, i, found=1;
   for( p=0; p<word_sz; p++ ){
	   i = word[p] -'a';
	   if( root->children[i] != 0 ){
           root = root->children[i];
	   }else{
		   found=0;
		   break;
	   }
   }
   if( found && root->qids ){
       // WE HAVE A MATCH SO get the List of the query ids and add them to the result
        #ifdef LP_LOCKS_ENABLED
	        pthread_mutex_lock(&lockmech->mutex);
        #endif
		for (QueryArrayList::iterator it = root->qids->begin(), end = root->qids->end(); it != end; it++) {
			//global_results[tid]->qids->push_back(*it);
			local_results->qids->push_back(*it);
		}
		#ifdef LP_LOCKS_ENABLED
			pthread_mutex_unlock(&lockmech->mutex);
		#endif
    }
}

struct HammingNode{
	TrieNode* node;
	char letter;
	char depth;
	char tcost;
};
void TrieHammingSearchWord( LockingMech* lockmech, TrieNode* node, const char* word, int word_sz, char tid, ResultTrieSearch* local_results, char maxCost ){
	//fprintf( stderr, "[2] [%p] [%p] [%.*s] [%d] [%p]\n", lockmech, node, word_sz, word, maxCost, results );

	HammingNode current, n;
	std::vector<HammingNode> hamming_stack;
	char j;

	// add the initial nodes
	for( j=0; j<VALID_CHARS; j++ ){
	   if( node->children[j] != 0 ){
		   current.depth = 1;
		   current.node = node->children[j];
		   current.letter = 'a' + j;
		   current.tcost = 0;
		   hamming_stack.push_back(current);
	   }
    }

	while (!hamming_stack.empty()) {
		current = hamming_stack.back();
		hamming_stack.pop_back();

		if (current.letter != word[current.depth - 1]) {
			current.tcost++;
		}
		if (current.tcost <= maxCost) {
			if (word_sz == current.depth && current.node->qids != 0) {
				// ADD THE node->qids[] INTO THE RESULTS
				#ifdef LP_LOCKS_ENABLED
					pthread_mutex_lock(&lockmech->mutex);
				#endif
				for (QueryArrayList::iterator it = current.node->qids->begin(),end = current.node->qids->end(); it != end; it++) {
					//global_results[tid]->qids->push_back(*it);
					local_results->qids->push_back(*it);
				}
				#ifdef LP_LOCKS_ENABLED
					pthread_mutex_unlock(&lockmech->mutex);
				#endif
			} else if (word_sz > current.depth) {
				for (j = 0; j < VALID_CHARS; j++) {
					if (current.node->children[j] != 0) {
						n.depth = current.depth + 1;
						n.node = current.node->children[j];
						n.letter = 'a' + j;
						n.tcost = current.tcost;
						hamming_stack.push_back(n);
					}
				}
			}
		}
	}
}

struct EditNode{
	TrieNode* node;
	char previous[MAX_WORD_LENGTH+1];
	char letter;
};
void TrieEditSearchWord(LockingMech* lockmech, TrieNode* node, const char* word, int word_sz, char tid, ResultTrieSearch* local_results, char maxCost ){
	//fprintf( stderr, "[3] [%p] [%p] [%.*s] [%d] [%p]\n", lockmech, node, word_sz, word, maxCost, results );

    EditNode c, n;
    char current[MAX_WORD_LENGTH+1];
    std::vector<EditNode> edit_stack;
    char i, insertCost, deleteCost, replaceCost, j, k;

	for (i = 0; i < VALID_CHARS; ++i) {
		if (node->children[i] != 0) {
			c.letter = 'a' + i;
			c.node = node->children[i];
			for( j=0; j<=word_sz; j++ )
			    c.previous[j] = j;
			edit_stack.push_back(c);
		}
	}

	while( !edit_stack.empty() ){
		c = edit_stack.back();
		edit_stack.pop_back();

        current[0] = c.previous[0]+1;

        for( i=1; i<=word_sz; i++ ){
     	   if( word[i-1] == c.letter ){
     		   current[i] = c.previous[i-1];
     	   }else{
     		   insertCost = current[i-1] + 1;
     		   deleteCost = c.previous[i] + 1;
     		   replaceCost = c.previous[i-1] + 1;
     		   // find the minimum for this column
     		   insertCost = insertCost < replaceCost ? insertCost : replaceCost;
     		   current[i] = insertCost < deleteCost ? insertCost : deleteCost;
     	   }
        }
        if( current[word_sz] <= maxCost && c.node->qids!=0 ){
            // ADD THE node->qids[] INTO THE RESULTS
			#ifdef LP_LOCKS_ENABLED
				pthread_mutex_lock(&lockmech->mutex);
			#endif
			for (QueryArrayList::iterator it = c.node->qids->begin(), end =	c.node->qids->end(); it != end; it++) {
				//global_results[tid]->qids->push_back(*it);
				local_results->qids->push_back(*it);
			}
			#ifdef LP_LOCKS_ENABLED
				pthread_mutex_unlock(&lockmech->mutex);
			#endif
        }

        // if there are more changes available recurse
		for (i = 0; i <= word_sz; i++) {
			if (current[i] <= maxCost) {
				for (j = 0; j < VALID_CHARS; j++) {
					if (c.node->children[j] != 0) {
						n.letter = 'a' + j;
						n.node = c.node->children[j];
						for (k = 0; k <= word_sz; k++)
							n.previous[k] = current[k];
						edit_stack.push_back(n);
					}
				}
				break; // break because we only need one occurence of cost less than maxCost
			}// there is no possible match further
		}
	}
}



/********************************************************************************************
 *  TRIE STRUCTURE END
 ********************************************************************************************/

/********************************************************************************************
 * TRIE VISITED STRUCTURE
 *************************************/
struct TrieNodeVisited{
   TrieNodeVisited* children[VALID_CHARS];
   char exists;
};
TrieNodeVisited* TrieNodeVisited_Constructor(){
   TrieNodeVisited* n = (TrieNodeVisited*)malloc(sizeof(TrieNodeVisited));
   if( !n ) err_mem("error allocating TrieNode");
   memset( n->children, 0, VALID_CHARS*sizeof(TrieNodeVisited*) );
   n->exists = 0;
   return n;
}
void TrieNodeVisited_Destructor( TrieNodeVisited* node ){
    for( char i=0; i<VALID_CHARS; i++ ){
    	if( node->children[i] != 0 ){
            TrieNodeVisited_Destructor( node->children[i] );
    	}
    }
    free( node );
}
// Returns 0 if new word added or 1 if existed
char TrieVisitedIS( TrieNodeVisited* node, const char* word, char word_sz ){
   char ptr, pos, existed = 1;
   for( ptr=0; ptr<word_sz; ptr++ ){
      pos = word[ptr] - 'a';
      if( node->children[pos] == 0 ){
          node->children[pos] = TrieNodeVisited_Constructor();
      }
      node = node->children[pos];
   }
   if( !node->exists ){
	   existed = 0;
       node->exists = 1;
   }
   return existed;
}

/********************************************************************************************
 *  TRIE VISITED STRUCTURE END
 ********************************************************************************************/




/********************************************************************************************
 *   STRUCTURE
 *************************************/

typedef struct _QuerySetNode QuerySetNode;
struct _QuerySetNode{
	//QueryID qid;
	MatchType type;
	//char cost;
	void **words;
	char words_num;
};
typedef std::vector<QuerySetNode*> QuerySet;

struct DocResultsNode{
	DocID docid;
	QueryID *qids;
	unsigned int sz;
};
typedef std::vector<DocResultsNode> DocResults;


/********************************************************************************************
 *   STRUCTURE END
 ********************************************************************************************/
/********************************************************************************************
 *  CACHE STRUCTURES
 *************************************/
unsigned int tcl_hash( const void *v, char sz ){
	const char* string = (char*)v;
	unsigned int result = 0;
	int c;
	while ( sz-- > 0 ) {
		c = *string++;
		result += (result<<3) + c;
	}
	return result;
}
struct CacheEntry{
	pthread_mutex_t mutex;
	char word[MAX_WORD_LENGTH+1];
	QueryID *qids;
};
typedef std::vector<CacheEntry> Cache;
#define CACHE_SIZE 100000
#define VISITED_SIZE 2*CACHE_SIZE

/********************************************************************************************
 *  CACHE STRUCTURES END
 ********************************************************************************************/


/********************************************************************************************
 *  GLOBALS
 *************************************/

Cache *cache_exact;

TrieNode *trie_exact;
TrieNode **trie_hamming;
TrieNode **trie_edit;

QuerySet *querySet; // std::vector<QuerySetNode*>
DocResults *docResults; // std::list<DocResultsNode>

// THREADPOOL
thr_pool_t* threadpool;

// STRUCTURES FOR PTHREADS

#define WORDS_PROCESSED_BY_THREAD 150
struct TrieSearchData{
	const char* words[WORDS_PROCESSED_BY_THREAD];
	char words_sz[WORDS_PROCESSED_BY_THREAD];
	short words_num;
	char padding[128];
};



unsigned int MAX_TRIE_SEARCH_DATA = 2048;
unsigned int current_trie_search_data = 0;
std::vector<TrieSearchData> trie_search_data_pool;

void* TrieSearchWord( void* args ){
	LP_ThreadArgs* lpargs = (LP_ThreadArgs*)args;
	TrieSearchData *tsd = (TrieSearchData *)lpargs->args;

	//fprintf( stderr, "words_num[%d] words[%p] words_sz[%p]\n", tsd->words_num, tsd->words, tsd->words_sz );

	const char* w;
	char wsz;
	char tid = lpargs->tid;

	for( int i=0, j=tsd->words_num; i<j; i++ ){
		wsz = tsd->words_sz[i];
		w = tsd->words[i];
		TrieExactSearchWord( &global_results_locks[tid], trie_exact, w, wsz, 0, &global_results[tid] );
		TrieHammingSearchWord( &global_results_locks[tid], trie_hamming[0], w, wsz, 0, &global_results[tid], 0 );
	    TrieHammingSearchWord( &global_results_locks[tid], trie_hamming[1], w, wsz, 0, &global_results[tid], 1 );
		TrieHammingSearchWord( &global_results_locks[tid], trie_hamming[2], w, wsz, 0, &global_results[tid], 2 );
		TrieHammingSearchWord( &global_results_locks[tid], trie_hamming[3], w, wsz, 0, &global_results[tid], 3 );
		TrieEditSearchWord( &global_results_locks[tid], trie_edit[0], w, wsz, 0, &global_results[tid], 0);
		TrieEditSearchWord( &global_results_locks[tid], trie_edit[1], w, wsz, 0, &global_results[tid], 1 );
		TrieEditSearchWord( &global_results_locks[tid], trie_edit[2], w, wsz, 0, &global_results[tid], 2 );
		TrieEditSearchWord( &global_results_locks[tid], trie_edit[3], w, wsz, 0, &global_results[tid], 3 );
	}

	//free( tsd ); - no free because we have 2048 global stacked search data
	return 0;
}


/********************************************************************************************
 *  GLOBALS END
 ********************************************************************************************/






/********************************************************************************************
 * TRIE LP_MERGESORT STRUCTURE
 *************************************/

struct BinaryTreeNode{
	BinaryTreeNode* left;
	BinaryTreeNode* right;
	QueryID qid;
	char visited;
};


#define LPMERGESORT_CHILDREN 10
struct TrieNodeLPMergesort{
	TrieNodeLPMergesort* children[LPMERGESORT_CHILDREN];
    unsigned int qid;
    char pos[MAX_QUERY_WORDS];
    char entries;
    char valid;
};

struct LPMergesortResult{
	QueryID *ids;
	unsigned int num_res;
};



TrieNodeLPMergesort *lp_mergesort_tries[NUM_THREADS+1];

BinaryTreeNode* BinaryTree_Constructor(){
	BinaryTreeNode* t = (BinaryTreeNode*)malloc(sizeof(BinaryTreeNode));
	t->left = 0;
	t->right = 0;
	t->qid = 0;
	t->visited = 0;
	return t;
}

void BinaryTree_Destructor( BinaryTreeNode* node ){
	if( node->left )
		BinaryTree_Destructor(node->left);
	if( node->right )
		BinaryTree_Destructor(node->right);
	free( node );
}

void BinaryTreeInsert( BinaryTreeNode* node, QueryID qid ){
	//fprintf( stderr, "[%d] ", qid );
	while( 1 ){
		if( node->qid == qid ){
			break;
		}else if( node->qid < qid ){
			if( node->right ){
			    node = node->right;
			    continue;
			}else{
				node->right = BinaryTree_Constructor();
				node = node->right;
				break;
			}
		}else if( node->qid > qid ){
			if( node->left ){
				node=node->left;
				continue;
			}else{
				node->left = BinaryTree_Constructor();
				node = node->left;
				break;
			}
		}
	}
	node->qid = qid;
	//fprintf( stderr, "[%d] ", node->qid );
}

LPMergesortResult* BinaryTreeTrieFlat( BinaryTreeNode* tree )
{
	LPMergesortResult* res = (LPMergesortResult*) malloc(sizeof(LPMergesortResult));

	std::vector<QueryID> ids;
	std::vector<BinaryTreeNode*> stack;

	// add the children for this trie
	stack.push_back(tree);

	BinaryTreeNode* c;
	while (!stack.empty()) {
		c = stack.back();
		c->visited = 1;
		if (c->left && !c->left->visited){
			stack.push_back(c->left);
			continue;
		}
		stack.pop_back();
		ids.push_back(c->qid); // PLEASE NOTICE THAT THE FIRST ENTRY IS qid=0 AND SHOULD NOT BE CONSIDERED IN THE FINAL RESULTS
		if (c->right && !c->right->visited){
			stack.push_back(c->right);
		}
	}

	unsigned int front, sz;
	res->ids = (QueryID*) malloc(sizeof(QueryID) * ids.size()-1);
	for (front = 1, sz = ids.size(); front < sz; front++){
	    //fprintf( stderr, "%d ", ids[front] );
		res->ids[front-1] = ids[front];
	}
	res->num_res = ids.size()-1;
	return res;
}

// TRIE FUNCTIONS
TrieNodeLPMergesort* TrieNodeLPMergesort_Constructor(){
	TrieNodeLPMergesort* n = (TrieNodeLPMergesort*)malloc(sizeof(TrieNodeLPMergesort));
    if( !n ) err_mem("error allocating TrieNode");
    memset( n->children, 0, LPMERGESORT_CHILDREN*sizeof(TrieNodeLPMergesort*) );
    n->qid = 0;
    n->pos[0] = n->pos[1] = n->pos[2] = n->pos[3] = n->pos[4] = -1;
    n->entries = 0;
    n->valid = 0;
    return n;
}
void TrieNodeLPMergesort_Destructor( TrieNodeLPMergesort* node ){
    for( char i=0; i<LPMERGESORT_CHILDREN; i++ ){
    	if( node->children[i] != 0 ){
    		TrieNodeLPMergesort_Destructor( node->children[i] );
    	}
    }
    free( node );
}

// Returns 0 if new word added or 1 if existed
void TrieLPMergesortInsert( TrieNodeLPMergesort* node, unsigned int qid, char pos ){
   char i;
   unsigned int nqid = qid;
   for( ; nqid>0;  ){
	   i = nqid % 10;
	   nqid /= 10;
      if( node->children[i] == 0 ){
          node->children[i] = TrieNodeLPMergesort_Constructor();
      }
      node = node->children[i];
   }
   // check if we have a different pos
   if( node->pos[pos] == -1 ){
       node->entries++;
       node->pos[pos] = 1;
       node->valid = 1;
       node->qid = qid;
   }
}

void TrieLPMergesortFill( TrieNodeLPMergesort* root, QueryArrayList* qids ){
	for( unsigned int i=0, sz=qids->size(); i<sz; i++ ){
		TrieLPMergesortInsert(root, qids->at(i).qid, qids->at(i).pos);
	}
}

void TrieLPMergesortCheckQueries( TrieNodeLPMergesort* root ){
	std::vector<TrieNodeLPMergesort*> stack;
	char i;
	// add the children for this trie
	for( i=0; i<LPMERGESORT_CHILDREN; i++ )
		if( root->children[i] )
			stack.push_back(root->children[i]);
	TrieNodeLPMergesort* c;
	while( !stack.empty() ){
		c = stack.back();
		stack.pop_back();
		// check the current node if it's a valid
		if( c->valid ){
			// we must check that the query satisfies constraints
			if( c->entries == querySet->at( c->qid )->words_num ){
				// this is a valid qid and must be included
                // may be inserted in another structure for final results

			}else{
				c->valid = 0;
			}
		}
		// add the children for this node
		for( i=0; i<LPMERGESORT_CHILDREN; i++ )
			if( c->children[i] )
				stack.push_back(c->children[i]);
	}
}

// merges Other's nodes into root
void TrieLPMergesortMerge( TrieNodeLPMergesort* root, TrieNodeLPMergesort* other ){
	std::vector<TrieNodeLPMergesort*> stack;
	char i;
	// add the children for this trie
	for( i=0; i<LPMERGESORT_CHILDREN; i++ )
		if( other->children[i] )
			stack.push_back(other->children[i]);
	TrieNodeLPMergesort* c;
	while( !stack.empty() ){
		c = stack.back();
		stack.pop_back();
		// check the current node if it's a valid
		if( c->valid ){
			for( char i=0; i<c->entries; i++ )
			    TrieLPMergesortInsert( root, c->qid, c->pos[i] );
		}
		// add the children for this node
		for( i=0; i<LPMERGESORT_CHILDREN; i++ )
			if( c->children[i] )
				stack.push_back(c->children[i]);
	}
}

// returns the valid nodes inside an array
LPMergesortResult* TrieLPMergesortFlat( TrieNodeLPMergesort* root ){

	BinaryTreeNode* binroot = BinaryTree_Constructor();

	std::vector<TrieNodeLPMergesort*> queue;
	char i;
	unsigned int front=0, sz;
	// add the children for this trie
	for( i=0; i<LPMERGESORT_CHILDREN; i++ )
		if( root->children[i] )
			queue.push_back(root->children[i]);
	TrieNodeLPMergesort* c;
	while( front < queue.size() ){
		c = queue[front++];
		// check the current node if it's a valid
		if( c->valid ){
			//fprintf( stderr, "qid[%u] ", c->qid );
			BinaryTreeInsert( binroot, c->qid );
		}
		// add the children for this node
		for( i=0; i<LPMERGESORT_CHILDREN; i++ )
			if( c->children[i] )
				queue.push_back(c->children[i]);
	}

	LPMergesortResult* res = BinaryTreeTrieFlat( binroot );
	BinaryTree_Destructor( binroot );
	return res;
}

sem_t semaphores[NUM_THREADS+1];
void* TrieLPMergesort( void* args ){
	LP_ThreadArgs* lpargs = (LP_ThreadArgs*)args;
	char tid = lpargs->tid;

	pthread_mutex_lock( &lp_pthreads_mutex );
	for (unsigned int i = 0; i < NUM_THREADS; i++) {
		if (lp_pthreads[i] == pthread_self()) {
			tid = i;
			break;
		}
	}
	pthread_mutex_unlock( &lp_pthreads_mutex );

	TrieLPMergesortFill( lp_mergesort_tries[tid] , global_results[tid].qids );
	TrieLPMergesortCheckQueries( lp_mergesort_tries[tid] );

	fprintf( stderr, "phase[%d] tid[%d]\n", 0, tid );

	sem_post( &semaphores[tid] );

	int total_threads = NUM_THREADS+1;
	int working_threads = total_threads>>1;
    int phase = 0;

	while( tid >= total_threads-working_threads ){
    	phase++;
    	fprintf( stderr, "phase[%d] tid[%d]\n", phase, tid );

    	sem_wait( &semaphores[ tid-working_threads ] );
    	TrieLPMergesortMerge( lp_mergesort_tries[tid], lp_mergesort_tries[tid-working_threads] );
    	TrieLPMergesortCheckQueries( lp_mergesort_tries[tid] );
    	sem_post( &semaphores[ tid ] );
    	working_threads >>= 1;
    }


	return 0;
}

/********************************************************************************************
 *  TRIE LP_MERGESORT STRUCTURE END
 ********************************************************************************************/







///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode InitializeIndex(){

	for( char i=0; i<NUM_THREADS+1; i++ )
	    sem_init( &semaphores[i], 0, 0 );

	/*
	cache_exact = new Cache();
	cache_exact->resize(CACHE_SIZE);
	for( int i=0; i<CACHE_SIZE; i++ ){
		cache_exact->at(i).qids = 0;
	    cache_exact->at(i).word[0] = '\0';
		pthread_mutex_init(&cache_exact->at(i).mutex, NULL);
	}
	*/

	trie_exact = TrieNode_Constructor();
    trie_hamming = (TrieNode**)malloc(sizeof(TrieNode*)*4);
    trie_edit = (TrieNode**)malloc(sizeof(TrieNode*)*4);

    trie_hamming[0] = TrieNode_Constructor();
    trie_hamming[1] = TrieNode_Constructor();
    trie_hamming[2] = TrieNode_Constructor();
    trie_hamming[3] = TrieNode_Constructor();
    trie_edit[0] = TrieNode_Constructor();
    trie_edit[1] = TrieNode_Constructor();
    trie_edit[2] = TrieNode_Constructor();
    trie_edit[3] = TrieNode_Constructor();

    //global_results_locks = (LockingMech*)malloc(sizeof(LockingMech)*NUM_THREADS);
    //global_results = (ResultTrieSearch*)malloc( sizeof(ResultTrieSearch)*NUM_THREADS );
    for( char i=0; i<NUM_THREADS+1; i++ ){
    	global_results[i].qids = new QueryArrayList();
    	global_results[i].qids->resize(128);
    	global_results[i].qids->clear();
    }

    trie_search_data_pool.resize(MAX_TRIE_SEARCH_DATA);

    querySet = new QuerySet();
    // add dummy query to start from index 1 because query ids start from 1 instead of 0
    querySet->push_back((QuerySetNode*)malloc(sizeof(QuerySetNode)));
    docResults = new DocResults();

    // Initialize the threadpool with 12 threads
    //threadpool = thpool_init( NUM_THREADS );
    threadpool = thr_pool_create( NUM_THREADS, NUM_THREADS, 2, NULL );

     //INSTANTIATE THE MINIMUM THREAD WORKERS - Edited by Lambros Petrou
        for( int i=0; i<NUM_THREADS; i++ ){
//        	pthread_mutex_lock(&threadpool->pool_mutex);
//        	if( create_worker( threadpool ) ){
//        		perror( "Could not instantiate worker immediately" );
//        	}
//        	threadpool->pool_nthreads++;
//        	pthread_mutex_unlock(&threadpool->pool_mutex);
        	thr_pool_queue( threadpool, reinterpret_cast<void* (*)(void*)>(dummy_thread_do), NULL );
        }

	return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode DestroyIndex(){

	for( char i=0; i<NUM_THREADS+1; i++ )
		sem_destroy( &semaphores[i] );

/*
	for( int i=0; i<CACHE_SIZE; i++ )
		pthread_mutex_destroy(&cache_exact->at(i).mutex);
	delete cache_exact;
*/
    TrieNode_Destructor( trie_exact  );
    TrieNode_Destructor( trie_hamming[0] );
    TrieNode_Destructor( trie_hamming[1] );
    TrieNode_Destructor( trie_hamming[2] );
    TrieNode_Destructor( trie_hamming[3] );
    TrieNode_Destructor( trie_edit[0] );
    TrieNode_Destructor( trie_edit[1] );
    TrieNode_Destructor( trie_edit[2] );
    TrieNode_Destructor( trie_edit[3] );

    free( trie_hamming );
    free( trie_edit );

    //free( global_results_locks );
    for( char i=0; i<NUM_THREADS+1; i++ ){
      	delete global_results[i].qids;
    }
    //free( global_results );

    for( unsigned int i=0, sz=querySet->size(); i<sz; i-- ){
    	free( querySet->at(i) );
    }
    delete querySet;
    delete docResults;

    // destroy the thread pool
    thr_pool_destroy(threadpool);

	return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode StartQuery(QueryID query_id, const char* query_str, MatchType match_type, unsigned int match_dist)
{
	QuerySetNode* qnode = (QuerySetNode*)malloc(sizeof(QuerySetNode));
	qnode->type = match_type;
	qnode->words = (void**)malloc(sizeof(TrieNode*)*MAX_QUERY_WORDS);
    qnode->words_num = 0;

    TrieNode**t=0, *n;
	const char *start, *end;
	for( start=query_str; *start; start = end ){
		while( *start == ' ' ) start++;
		end = start;
		while( *end >= 'a' && *end <= 'z' ) end++;
		switch( match_type ){
		case MT_EXACT_MATCH:
		   n = TrieInsert( trie_exact , start, end-start, query_id, qnode->words_num );
		   break;
		case MT_HAMMING_DIST:
		   t = trie_hamming;
		   break;
		case MT_EDIT_DIST:
		   t = trie_edit;
		   break;
		}// end of match_type
		if( match_type != MT_EXACT_MATCH ){
			switch (match_dist) {
			case 0:
				n = TrieInsert(t[0], start, end - start, query_id, qnode->words_num);
				break;
			case 1:
				n = TrieInsert(t[1], start, end - start, query_id, qnode->words_num);
				break;
			case 2:
				n = TrieInsert(t[2], start, end - start, query_id, qnode->words_num);
				break;
			case 3:
				n = TrieInsert(t[3], start, end - start, query_id, qnode->words_num);
				break;
			}// end of match_dist
		}
        qnode->words[qnode->words_num] = n;
        qnode->words_num++;

	}// end for each word

	querySet->push_back(qnode); // add the new query in the query set

	return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode EndQuery(QueryID query_id)
{
	// Remove this query from the active query set
	QuerySetNode* n = querySet->at(query_id);
	for( char i=n->words_num-1; i>=0; i-- ){
		for( QueryArrayList::iterator it=((TrieNode*)n->words[i])->qids->begin(), end=((TrieNode*)n->words[i])->qids->end(); it != end; it++  ){
		    if( it->qid == query_id && it->pos == i ){
		    	((TrieNode*)n->words[i])->qids->erase(it);
		    	break;
		    }
		}
	}
	return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////

bool compareQueryNodes( const QueryNode &a, const QueryNode &b){
	if( a.qid < b.qid  )
		return true;
	if( a.qid > b.qid )
		return false;
	return a.pos <= b.pos;
}

// TODO -
// TODO - Check for the same words in the same document
// TODO - Check the cache for words in previous documents too and get the results without running the algorithms again

ErrorCode MatchDocument(DocID doc_id, const char* doc_str){
	char k,szz;
	// results are new for each document
	// TODO - With this we might not need the global_results
	for( k=0; k<NUM_THREADS+1; k++ ){
		lp_mergesort_tries[k] = TrieNodeLPMergesort_Constructor();
	}



	TrieNodeVisited *visited = TrieNodeVisited_Constructor();


	///////////////////////////////////////////////
    //int s = gettime();

    short batch_words=0;
	TrieSearchData *tsd = NULL;
	const char *start, *end;
	char sz;
	char tid=NUM_THREADS;
    for( start=doc_str; *start; start = end ){
	    // FOR EACH WORD DO THE MATCHING

    	while( *start == ' ' ) start++;
    	end = start;
    	while( *end >= 'a' && *end <= 'z' ) end++;
    	sz = end-start;


    	// skip word if found before in the document
    	if( TrieVisitedIS( visited, start, sz ) ){
    		//fprintf( stderr, "doc[%d] word[%.*s] existed\n", doc_id, sz, start );
    	   	continue;
    	}


        batch_words++;
    	if( batch_words == 1 ){
    		current_trie_search_data = (current_trie_search_data + 1) % MAX_TRIE_SEARCH_DATA;
    		tsd = &trie_search_data_pool[current_trie_search_data];
    	}
    	tsd->words[batch_words-1] = start;
    	tsd->words_sz[batch_words-1] = sz;

    	if( batch_words == WORDS_PROCESSED_BY_THREAD ){
    		tsd->words_num = batch_words;
    		batch_words = 0;
    		thr_pool_queue( threadpool, reinterpret_cast<void* (*)(void*)>(TrieSearchWord), tsd );
    	}else if( *end == '\0' ){
    		// very few words or the last ones so just handle them yourself
    		// the same thing the THREAD_JOB does
    		const char* w;
    		char wsz;
    		for( int i=0, j=batch_words; i<j; i++ ){
    			wsz = tsd->words_sz[i];
    			w = tsd->words[i];
    			TrieExactSearchWord( &global_results_locks[tid], trie_exact, w, wsz, 0, &global_results[tid] );
    			TrieHammingSearchWord( &global_results_locks[tid], trie_hamming[0], w, wsz, 0, &global_results[tid], 0 );
    		    TrieHammingSearchWord( &global_results_locks[tid], trie_hamming[1], w, wsz, 0, &global_results[tid], 1 );
    			TrieHammingSearchWord( &global_results_locks[tid], trie_hamming[2], w, wsz, 0, &global_results[tid], 2 );
    			TrieHammingSearchWord( &global_results_locks[tid], trie_hamming[3], w, wsz, 0, &global_results[tid], 3 );
    			TrieEditSearchWord( &global_results_locks[tid], trie_edit[0], w, wsz, 0, &global_results[tid], 0);
    			TrieEditSearchWord( &global_results_locks[tid], trie_edit[1], w, wsz, 0, &global_results[tid], 1 );
    			TrieEditSearchWord( &global_results_locks[tid], trie_edit[2], w, wsz, 0, &global_results[tid], 2 );
    			TrieEditSearchWord( &global_results_locks[tid], trie_edit[3], w, wsz, 0, &global_results[tid], 3 );
    		}
    	}

    	// move on to the next word
    }

    // check if there are words unhandled at the moment because of premature exit of the loop
    // at the point where we check for previous existence
    if( batch_words > 0 ){
        // very few words or the last ones so just handle them yourself
        // the same thing the THREAD_JOB does
        const char* w;
        char wsz;
        for( int i=0, j=batch_words; i<j; i++ ){
        	wsz = tsd->words_sz[i];
        	w = tsd->words[i];
        	TrieExactSearchWord( &global_results_locks[tid], trie_exact, w, wsz, 0, &global_results[tid] );
        	TrieHammingSearchWord( &global_results_locks[tid], trie_hamming[0], w, wsz, 0, &global_results[tid], 0 );
            TrieHammingSearchWord( &global_results_locks[tid], trie_hamming[1], w, wsz, 0, &global_results[tid], 1 );
        	TrieHammingSearchWord( &global_results_locks[tid], trie_hamming[2], w, wsz, 0, &global_results[tid], 2 );
        	TrieHammingSearchWord( &global_results_locks[tid], trie_hamming[3], w, wsz, 0, &global_results[tid], 3 );
        	TrieEditSearchWord( &global_results_locks[tid], trie_edit[0], w, wsz, 0, &global_results[tid], 0);
        	TrieEditSearchWord( &global_results_locks[tid], trie_edit[1], w, wsz, 0, &global_results[tid], 1 );
        	TrieEditSearchWord( &global_results_locks[tid], trie_edit[2], w, wsz, 0, &global_results[tid], 2 );
        	TrieEditSearchWord( &global_results_locks[tid], trie_edit[3], w, wsz, 0, &global_results[tid], 3 );
        }
    }


    // destroy visited table
    TrieNodeVisited_Destructor( visited );

    // //////////////////////////
    // FIND A WAY TO SYNCHRONIZE THREADPOOL
    /////////////////////////////
    thr_pool_wait( threadpool );

    //fprintf( stderr, "doc[%u] miliseconds: %d\n", doc_id, gettime()-s );

    //    for( k=0; k<NUM_THREADS+1; k++ ){
    //    	TrieLPMergesortFill( lp_mergesort_tries[tid] , global_results[k].qids );
    //    }
    //    TrieLPMergesortCheckQueries( lp_mergesort_tries[tid] );

    LP_ThreadArgs *args;
    for( batch_words=0; batch_words<NUM_THREADS; batch_words++ ){
        args = (LP_ThreadArgs*)malloc( sizeof(LP_ThreadArgs) );
        args->tid= batch_words;
        args->args = NULL;
    	thr_pool_queue( threadpool, reinterpret_cast<void* (*)(void*)>(TrieLPMergesort), args);
    }

//    args = (LP_ThreadArgs*)malloc( sizeof(LP_ThreadArgs) );
//    args->args = NULL;
//    args->tid = tid;
//    TrieLPMergesort( &args );

    thr_pool_wait( threadpool );
    LPMergesortResult *res = TrieLPMergesortFlat( lp_mergesort_tries[tid] );





/*
    // TODO - merge the results - CAN BE PARALLED
    for( k=0, szz=NUM_THREADS; k<szz; k++ ){
    	//fprintf( stderr, "total results[%d]: %d\n", k, results[k]->qids->size() );
    	for( QueryArrayList::iterator it=global_results[k].qids->begin(), end=global_results[k].qids->end(); it != end; it++ ){
    		global_results[NUM_THREADS].qids->push_back(*it);
    	}
    }

    ResultTrieSearch *results_all;
    results_all = &global_results[NUM_THREADS];


    //int s = gettime();
    // TODO - Parallel sorting
    std::stable_sort( results_all->qids->begin(), results_all->qids->end(), compareQueryNodes );
    //parallelMergesort( results->qids, results->qids->size(), 4 );
    //fprintf( stderr, "doc[%u] results: %lu miliseconds: %d\n", doc_id, results_all->qids->size(), gettime()-s );

    std::vector<QueryID> ids;
    char counter=0;
    QueryNode qn_p, qn_c;
    // IF WE HAVE RESULTS FOR THIS DOCUMENT
    if( ! results_all->qids->empty() ){
        qn_p.qid = results_all->qids->begin()->qid;
        qn_p.pos = results_all->qids->begin()->pos;
        counter=1;

		for( QueryArrayList::iterator it=++results_all->qids->begin(), end=results_all->qids->end(); it != end; it++ ){
			qn_c = *it;
			if( qn_p.qid == qn_c.qid ){
				if( qn_p.pos == qn_c.pos ){
					continue;
				}else{
					counter++;
					qn_p.pos = qn_c.pos;
				}
			}else{
				// we have finished checking a query
				if( counter == querySet->at(qn_p.qid)->words_num ){
					//fprintf( stderr, "\ncounter: %d %u[%d]\n", counter, qn_p.qid, querySet->at(qn_p.qid)->words_num );
					ids.push_back(qn_p.qid);
				}
				counter = 1;
				qn_p.pos = qn_c.pos;
				qn_p.qid = qn_c.qid;
			}
		}

    	// handle the last result because the for loop exited without inserting it
    	if( counter == querySet->at(qn_p.qid)->words_num ){
    	    ids.push_back(qn_p.qid);
    	}
    }
*/

	DocResultsNode doc;
	doc.docid=doc_id;
	doc.sz= res->num_res; //ids.size();
	doc.qids = res->ids;
	//doc.qids=0;
	//if(doc.sz) doc.qids=(QueryID*)malloc(doc.sz*sizeof(unsigned int));
	//for(int i=0, szz=doc.sz;i<szz;i++) doc.qids[i]=ids[i];
	// Add this result to the set of undelivered results
	docResults->push_back(doc);

	for( k=0; k<NUM_THREADS+1; k++ ){
		TrieNodeLPMergesort_Destructor( lp_mergesort_tries[k] );
	}

	for( k=0,szz=NUM_THREADS+1; k<szz; k++ ){
		global_results[k].qids->clear();
	}

	return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode GetNextAvailRes(DocID* p_doc_id, unsigned int* p_num_res, QueryID** p_query_ids)
{
    // get the docResult from the back of the list if any and return it
	*p_doc_id=0; *p_num_res=0; *p_query_ids=0;
	if(docResults->empty()) return EC_NO_AVAIL_RES;
	*p_doc_id=docResults->back().docid; *p_num_res=docResults->back().sz; *p_query_ids=docResults->back().qids;
	docResults->pop_back();
	return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////

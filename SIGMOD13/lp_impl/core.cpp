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

static void *
worker_thread(void *arg)
{
    thr_pool_t *pool = (thr_pool_t *)arg;
    int timedout;
    job_t *job;
    void *(*func)(void *);
    active_t active;

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




/********************************************************************************************
 *  THREADPOOL STRUCTURE END
 ********************************************************************************************/

#define LP_LOCKS_ENABLED

/********************************************************************************************
 *  TRIE STRUCTURE
 *************************************/

#define NUM_THREADS 9
#define TRIES_EACH_TYPE 4
#define TOTAL_SEARCHERS (2*TRIES_EACH_TYPE+1)

#define VALID_CHARS 26



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
ResultTrieSearch global_results[NUM_THREADS];
LockingMech global_results_locks[NUM_THREADS]; // one lock for each result set

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
 *  PARALLELISM STRUCTURES
 *************************************/

//for parallel implementation
void parallelMergesort(QueryArrayList* lyst, unsigned int size, unsigned int tlevel);
void *parallelMergesortHelper(void *threadarg);

/********************************************************************************************
 *  PARALLELISM STRUCTURES END
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
 *  GLOBALS
 *************************************/

TrieNode *trie_exact;
TrieNode **trie_hamming;
TrieNode **trie_edit;

QuerySet *querySet; // std::vector<QuerySetNode*>
DocResults *docResults; // std::list<DocResultsNode>


// THREADPOOL
thr_pool_t* threadpool;

// STRUCTURES FOR PTHREADS
struct TrieSearchData{
	char type;
	LockingMech* lock;
	TrieNode *node;
	const char* word;
	int word_sz;
	ResultTrieSearch* results;
	char tid;
	char maxCost;
};

void* TrieSearchWord( void* tsd_ ){
	TrieSearchData *tsd = (TrieSearchData *)tsd_;
	//fprintf( stderr, "[%d] [%p] [%p] [%.*s] [%d] [%p]\n", tsd->type, tsd->lock, tsd->node, tsd->word_sz, tsd->word, tsd->maxCost, tsd->results );
	//fprintf(stderr, "tid[%d] type[%d] word: %.*s\n", tsd->tid, tsd->type, tsd->word_sz, tsd->word );
	switch( tsd->type ){
	case 0:
		TrieExactSearchWord( tsd->lock, tsd->node, tsd->word, tsd->word_sz, tsd->tid, tsd->results );
		break;
	case 1:
		TrieHammingSearchWord( tsd->lock, tsd->node, tsd->word, tsd->word_sz, tsd->tid, tsd->results, tsd->maxCost );
		break;
	case 2:
		TrieEditSearchWord( tsd->lock, tsd->node, tsd->word, tsd->word_sz, tsd->tid, tsd->results, tsd->maxCost);
		break;
    }
	free( tsd_ );
	return 0;
}


/********************************************************************************************
 *  GLOBALS END
 ********************************************************************************************/




///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode InitializeIndex(){

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
    for( char i=0; i<NUM_THREADS; i++ ){
    	global_results[i].qids = new QueryArrayList();
    }

    querySet = new QuerySet();
    // add dummy query to start from index 1 because query ids start from 1 instead of 0
    querySet->push_back((QuerySetNode*)malloc(sizeof(QuerySetNode)));
    docResults = new DocResults();

    // Initialize the threadpool with 12 threads
    //threadpool = thpool_init( NUM_THREADS );
    threadpool = thr_pool_create( NUM_THREADS, 18, 2, NULL );
	return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode DestroyIndex(){
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
    for( char i=0; i<NUM_THREADS; i++ ){
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

// TODO
ErrorCode MatchDocument(DocID doc_id, const char* doc_str){
	char k,szz;
	// results are new for each document
	for( k=0,szz=NUM_THREADS; k<szz; k++ ){
		global_results[k].qids->clear();
	}


	//threadpool = thpool_init( NUM_THREADS );

	//fprintf( stderr, "\n\n%p\n\n", thpool_jobqueue_peek(threadpool) );

	///////////////////////////////////////////////
    //int s = gettime();
	unsigned int total_words=0;
	TrieSearchData *tsd;
	const char *start, *end;
	char sz;
	char tid=-1;
    for( start=doc_str; *start; start = end ){
	    // FOR EACH WORD DO THE MATCHING

    	while( *start == ' ' ) start++;
    	end = start;
    	while( *end >= 'a' && *end <= 'z' ) end++;
    	sz = end-start;
    	total_words++;

    	//fprintf( stderr, "doc[%d] word: %.*s\n", doc_id, sz, start );

    	// exact matching
    	tsd = (TrieSearchData *)malloc(sizeof(TrieSearchData));
    	tsd->type = 0;
    	tid = (tid+1) % NUM_THREADS;
    	tsd->tid = tid;
    	tsd->lock = &global_results_locks[tid];
    	tsd->results = &global_results[tid];
    	tsd->node = trie_exact;
    	tsd->word = start;
    	tsd->word_sz = sz;
    	//thpool_add_work( threadpool, reinterpret_cast<void* (*)(void*)>(TrieSearchWord), tsd );
    	thr_pool_queue( threadpool, reinterpret_cast<void* (*)(void*)>(TrieSearchWord), tsd );

    	// hamming matching
    	for( k=0; k<TRIES_EACH_TYPE; k++ ){
    		tsd = (TrieSearchData *)malloc(sizeof(TrieSearchData));
    		tsd->type = 1;
        	tid = (tid+1) % NUM_THREADS;
        	tsd->tid = tid;
        	tsd->lock = &global_results_locks[tid];
        	tsd->results = &global_results[tid];
    		tsd->node = trie_hamming[k];
    		tsd->maxCost = k;
    		tsd->word = start;
    		tsd->word_sz = sz;
    		//thpool_add_work( threadpool, reinterpret_cast<void* (*)(void*)>(TrieSearchWord), tsd );
    		thr_pool_queue( threadpool, reinterpret_cast<void* (*)(void*)>(TrieSearchWord), tsd );
    	}
    	// edit matching

		for ( k = 0; k < TRIES_EACH_TYPE; k++) {
			tsd = (TrieSearchData *)malloc(sizeof(TrieSearchData));
			tsd->type = 2;
	    	tid = (tid+1) % NUM_THREADS;
	    	tsd->tid = tid;
	    	tsd->lock = &global_results_locks[tid];
			tsd->results = &global_results[tid];
			tsd->node = trie_edit[k];
			tsd->maxCost = k;
    		tsd->word = start;
    		tsd->word_sz = sz;
			//thpool_add_work( threadpool, reinterpret_cast<void* (*)(void*)>(TrieSearchWord), tsd );
    		thr_pool_queue( threadpool, reinterpret_cast<void* (*)(void*)>(TrieSearchWord), tsd );
		}

/*
    	TrieExactSearchWord( &global_results_locks[0], trie_exact, start, sz, 0, &global_results[0] );
    	TrieHammingSearchWord( &global_results_locks[1], trie_hamming[0], start, sz, 0, &global_results[1], 0 );
    	TrieHammingSearchWord( &global_results_locks[2], trie_hamming[1], start, sz, 0, &global_results[2], 1 );
    	TrieHammingSearchWord( &global_results_locks[3], trie_hamming[2], start, sz, 0, &global_results[3], 2 );
    	TrieHammingSearchWord( &global_results_locks[4], trie_hamming[3], start, sz, 0, &global_results[4], 3 );
    	TrieEditSearchWord( &global_results_locks[5], trie_edit[0], start, sz, 0, &global_results[5], 0);
    	TrieEditSearchWord( &global_results_locks[6], trie_edit[1], start, sz, 0, &global_results[6], 1 );
    	TrieEditSearchWord( &global_results_locks[7], trie_edit[2], start, sz, 0, &global_results[7], 2 );
    	TrieEditSearchWord( &global_results_locks[8], trie_edit[3], start, sz, 0, &global_results[8], 3 );
*/
    }
    //fprintf( stderr, "doc[%u] total_words: %d\n", doc_id, total_words );

    // //////////////////////////
    // FIND A WAY TO SYNCHRONIZE THREADPOOL
    /////////////////////////////
    thr_pool_wait( threadpool );


    // TODO - merge the results - CAN BE PARALLED
    for( k=1, szz=NUM_THREADS; k<szz; k++ ){
    	//fprintf( stderr, "total results[%d]: %d\n", k, results[k]->qids->size() );
    	for( QueryArrayList::iterator it=global_results[k].qids->begin(), end=global_results[k].qids->end(); it != end; it++ ){
    		global_results[0].qids->push_back(*it);
    	}
    }

    ResultTrieSearch *results_all;
    results_all = &global_results[0];


    //int s = gettime();
    // TODO - Parallel sorting
    std::stable_sort( results_all->qids->begin(), results_all->qids->end(), compareQueryNodes );
    //parallelMergesort( results->qids, results->qids->size(), 4 );
    //fprintf( stderr, "doc[%u] results: %lu miliseconds: %d\n", doc_id, results->qids->size(), gettime()-s );

/*
    fprintf( stderr, "\ndoc[%u] results->qids: %p [size: %lu]\n", doc_id ,results->qids, results->qids->size() );
    for( QueryArrayList::iterator it=results->qids->begin(), end=results->qids->end(); it != end; it++ )
    	fprintf( stderr, "%u[%d][%d] ", it->qid, querySet->at(it->qid)->words_num, it->pos );
*/

    //int s = gettime();

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
    //fprintf( stderr, "doc[%u] results: %lu miliseconds: %d\n", doc_id, results->qids->size(), gettime()-s );

	DocResultsNode doc;
	doc.docid=doc_id;
	doc.sz=ids.size();
	doc.qids=0;
	if(doc.sz) doc.qids=(QueryID*)malloc(doc.sz*sizeof(unsigned int));
	for(int i=0, szz=doc.sz;i<szz;i++) doc.qids[i]=ids[i];
	// Add this result to the set of undelivered results
	docResults->push_back(doc);

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























struct thread_data{
	QueryArrayList* lyst;
	QueryArrayList* back;
	unsigned int low;
	unsigned int high;
	unsigned int level;
};














//merge the elements from the sorted sublysts in [low, mid] and (mid, high]
void mergeP(QueryArrayList* lyst, QueryArrayList* back, unsigned int low, unsigned int mid, unsigned int high)
{
	unsigned int ai = low, bi = mid + 1, i = low;
	while (ai <= mid && bi <= high)
	{
		if ( compareQueryNodes(lyst->at(ai), lyst->at(bi)) )
		{
			back->at(i) = lyst->at(ai);
			ai++;
		}else
		{
			back->at(i) = lyst->at(bi);
			bi++;
		}
		i++;
	}
	if (ai > mid)
	{
		for( unsigned int sz=high-bi+1; sz>0 ; i++, bi++ ,sz-- ){
			back->at(i) = lyst->at(bi);
		}
		//memcpy(&back[i], &lyst[bi], (high-bi+1)*sizeof(double));
	}else
	{
		for( unsigned int sz=mid-ai+1; sz>0; i++, ai++, sz-- ){
					back->at(i) = lyst->at(ai);
				}
		//memcpy(&back[i], &lyst[ai], (mid-ai+1)*sizeof(double));
	}
	for( unsigned int sz=high-low+1; sz>0; low++,sz-- ){
				lyst->at(low) = back->at(low);
			}
	//memcpy(&lyst[low], &back[low], (high-low+1)*sizeof(double));
}

//the actual C mergesort method, with back list to avoid
//NlogN memory (2N instead).
void mergesortHelper(QueryArrayList* lyst, QueryArrayList* back, unsigned int low, unsigned int high)
{
	if (low == high) return;
	unsigned int mid = low + (high-low)/2;
	mergesortHelper(lyst, back, low, mid);
	mergesortHelper(lyst, back, mid+1, high);
	mergeP(lyst, back, low, mid, high);
}

/*
parallel mergesort top level:
instantiate parallelMergesortHelper thread, and that's
basically it.
*/
void parallelMergesort(QueryArrayList* lyst, unsigned int size, unsigned int tlevel)
{
	int rc;
	void *status;

	QueryArrayList* back = new QueryArrayList();
	back->resize(lyst->size());

	//Want joinable threads (usually default).
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

	//pthread function can take only one argument, so struct.
	struct thread_data td;
	td.lyst = lyst;
	td.back = back;
	td.low = 0;
	td.high = size - 1;
	td.level = tlevel;

	//The top-level thread.
	pthread_t theThread;
	rc = pthread_create(&theThread, &attr, parallelMergesortHelper,
						(void *) &td);
	if (rc)
	{
    	printf("ERROR; return code from pthread_create() is %d\n", rc);
    	exit(-1);
    }

	//Now join the thread (wait, as joining blocks) and quit.
	pthread_attr_destroy(&attr);
	rc = pthread_join(theThread, &status);
	if (rc)
	{
		printf("ERROR; return code from pthread_join() is %d\n", rc);
		exit(-1);
	}
	//printf("Main: completed join with top thread having a status of %ld\n",
	//		(long)status);

	delete back;

}

/*
parallelMergesortHelper
-if the level is still > 0, then make
parallelMergesortHelper threads to solve the left and
right-hand sides, then merge results after joining
 and quit.
*/
void *parallelMergesortHelper(void *threadarg)
{
	unsigned int mid;
	int  t, rc;
	void *status;

	struct thread_data *my_data;
	my_data = (struct thread_data *) threadarg;

	//fyi:
	//printf("Thread responsible for [%d, %d], level %d.\n",
	//		my_data->low, my_data->high, my_data->level);

	if (my_data->level <= 0 || my_data->low == my_data->high)
	{
		//We have plenty of threads, finish with sequential.
		mergesortHelper(my_data->lyst, my_data->back,
						my_data->low, my_data->high);
		pthread_exit(NULL);
	}

	//Want joinable threads (usually default).
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);


	//At this point, we will create threads for the
	//left and right sides.  Must create their data args.
	struct thread_data thread_data_array[2];
	mid = (my_data->low + my_data->high)/2;

	for (t = 0; t < 2; t ++)
	{
		thread_data_array[t].lyst = my_data->lyst;
		thread_data_array[t].back = my_data->back;
		thread_data_array[t].level = my_data->level - 1;
	}
	thread_data_array[0].low = my_data->low;
	thread_data_array[0].high = mid;
	thread_data_array[1].low = mid+1;
	thread_data_array[1].high = my_data->high;

	//Now, instantiate the threads.
	pthread_t threads[2];
	for (t = 0; t < 2; t ++)
	{
		rc = pthread_create(&threads[t], &attr, parallelMergesortHelper,
							(void *) &thread_data_array[t]);
		if (rc)
		{
    		printf("ERROR; return code from pthread_create() is %d\n", rc);
    		exit(-1);
    	}
	}

	pthread_attr_destroy(&attr);
	//Now, join the left and right threads and merge.
	for (t = 0; t < 2; t ++)
	{
		rc = pthread_join(threads[t], &status);
		if (rc)
		{
			printf("ERROR; return code from pthread_join() is %d\n", rc);
			exit(-1);
		}
	}

	//Call the sequential merge now that the left and right
	//sides are sorted.
	mergeP(my_data->lyst, my_data->back, my_data->low, mid, my_data->high);

	pthread_exit(NULL);
}


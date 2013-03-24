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

/* Individual job */
typedef struct thpool_job_t{
	void*  (*function)(void* arg);                     /**< function pointer         */
	void*                     arg;                     /**< function's argument      */
	struct thpool_job_t*     next;                     /**< pointer to next job      */
	struct thpool_job_t*     prev;                     /**< pointer to previous job  */
}thpool_job_t;


/* Job queue as doubly linked list */
typedef struct thpool_jobqueue{
	thpool_job_t *head;                                /**< pointer to head of queue */
	thpool_job_t *tail;                                /**< pointer to tail of queue */
	int           jobsN;                               /**< amount of jobs in queue  */
	sem_t        *queueSem;                            /**< semaphore(this is probably just holding the same as jobsN) */
}thpool_jobqueue;


/* The threadpool */
typedef struct thpool_t{
	pthread_t*       threads;                          /**< pointer to threads' ID   */
	int              threadsN;                         /**< amount of threads        */
	thpool_jobqueue* jobqueue;                         /**< pointer to the job queue */
}thpool_t;


/* Container for all things that each thread is going to need */
typedef struct thread_data_p{
	pthread_mutex_t *mutex_p;
	thpool_t        *tp_p;
}thread_data_p;



/* =========================== FUNCTIONS ================================================ */


/* ----------------------- Threadpool specific --------------------------- */

/**
 * @brief  Initialize threadpool
 *
 * Allocates memory for the threadpool, jobqueue, semaphore and fixes
 * pointers in jobqueue.
 *
 * @param  number of threads to be used
 * @return threadpool struct on success,
 *         NULL on error
 */
thpool_t* thpool_init(int threadsN);


/**
 * @brief What each thread is doing
 *
 * In principle this is an endless loop. The only time this loop gets interuppted is once
 * thpool_destroy() is invoked.
 *
 * @param threadpool to use
 * @return nothing
 */
void* thpool_thread_do(void* tp_p);


/**
 * @brief Add work to the job queue
 *
 * Takes an action and its argument and adds it to the threadpool's job queue.
 * If you want to add to work a function with more than one arguments then
 * a way to implement this is by passing a pointer to a structure.
 *
 * ATTENTION: You have to cast both the function and argument to not get warnings.
 *
 * @param  threadpool to where the work will be added to
 * @param  function to add as work
 * @param  argument to the above function
 * @return int
 */
int thpool_add_work(thpool_t* tp_p, void *(*function_p)(void*), void* arg_p);


/**
 * @brief Destroy the threadpool
 *
 * This will 'kill' the threadpool and free up memory. If threads are active when this
 * is called, they will finish what they are doing and then they will get destroyied.
 *
 * @param threadpool a pointer to the threadpool structure you want to destroy
 */
void thpool_destroy(thpool_t* tp_p);



/* ------------------------- Queue specific ------------------------------ */


/**
 * @brief Initialize queue
 * @param  pointer to threadpool
 * @return 0 on success,
 *        -1 on memory allocation error
 */
int thpool_jobqueue_init(thpool_t* tp_p);


/**
 * @brief Add job to queue
 *
 * A new job will be added to the queue. The new job MUST be allocated
 * before passed to this function or else other functions like thpool_jobqueue_empty()
 * will be broken.
 *
 * @param pointer to threadpool
 * @param pointer to the new job(MUST BE ALLOCATED)
 * @return nothing
 */
void thpool_jobqueue_add(thpool_t* tp_p, thpool_job_t* newjob_p);


/**
 * @brief Remove last job from queue.
 *
 * This does not free allocated memory so be sure to have peeked() \n
 * before invoking this as else there will result lost memory pointers.
 *
 * @param  pointer to threadpool
 * @return 0 on success,
 *         -1 if queue is empty
 */
int thpool_jobqueue_removelast(thpool_t* tp_p);


/**
 * @brief Get last job in queue (tail)
 *
 * Gets the last job that is inside the queue. This will work even if the queue
 * is empty.
 *
 * @param  pointer to threadpool structure
 * @return job a pointer to the last job in queue,
 *         a pointer to NULL if the queue is empty
 */
thpool_job_t* thpool_jobqueue_peek(thpool_t* tp_p);


/**
 * @brief Remove and deallocate all jobs in queue
 *
 * This function will deallocate all jobs in the queue and set the
 * jobqueue to its initialization values, thus tail and head pointing
 * to NULL and amount of jobs equal to 0.
 *
 * @param pointer to threadpool structure
 * */
void thpool_jobqueue_empty(thpool_t* tp_p);



static int thpool_keepalive=1;

/* Create mutex variable */
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER; /* used to serialize queue access */




/* Initialise thread pool */
thpool_t* thpool_init(int threadsN){
	thpool_t* tp_p;

	if (!threadsN || threadsN<1) threadsN=1;

	/* Make new thread pool */
	tp_p=(thpool_t*)malloc(sizeof(thpool_t));                              /* MALLOC thread pool */
	if (tp_p==NULL){
		fprintf(stderr, "thpool_init(): Could not allocate memory for thread pool\n");
		return NULL;
	}
	tp_p->threads=(pthread_t*)malloc(threadsN*sizeof(pthread_t));          /* MALLOC thread IDs */
	if (tp_p->threads==NULL){
		fprintf(stderr, "thpool_init(): Could not allocate memory for thread IDs\n");
		return NULL;
	}
	tp_p->threadsN=threadsN;

	/* Initialise the job queue */
	if (thpool_jobqueue_init(tp_p)==-1){
		fprintf(stderr, "thpool_init(): Could not allocate memory for job queue\n");
		return NULL;
	}

	/* Initialise semaphore*/
	tp_p->jobqueue->queueSem=(sem_t*)malloc(sizeof(sem_t));                 /* MALLOC job queue semaphore */
	sem_init(tp_p->jobqueue->queueSem, 0, 0); /* no shared, initial value */

	/* Make threads in pool */
	int t;
	for (t=0; t<threadsN; t++){
		//printf("Created thread %d in pool \n", t);
		pthread_create(&(tp_p->threads[t]), NULL, &thpool_thread_do, (void *)tp_p); /* MALLOCS INSIDE PTHREAD HERE */
	}

	return tp_p;
}


/* What each individual thread is doing
 * */
/* There are two scenarios here. One is everything works as it should and second if
 * the thpool is to be killed. In that manner we try to BYPASS sem_wait and end each thread. */
void* thpool_thread_do(void* tp_p_){
	thpool_t* tp_p = (thpool_t*)tp_p_;

	while(thpool_keepalive){

		if (sem_wait(tp_p->jobqueue->queueSem)) {/* WAITING until there is work in the queue */
			perror("thpool_thread_do(): Waiting for semaphore");
			exit(1);
		}

		if (thpool_keepalive){

			/* Read job from queue and execute it */
			void*(*func_buff)(void* arg);
			void*  arg_buff;
			thpool_job_t* job_p;

			pthread_mutex_lock(&mutex);                  /* LOCK */

			job_p = thpool_jobqueue_peek(tp_p);
			func_buff=job_p->function;
			arg_buff =job_p->arg;
			thpool_jobqueue_removelast(tp_p);

			pthread_mutex_unlock(&mutex);                /* UNLOCK */

			func_buff(arg_buff);               			 /* run function */
			free(job_p);                                                       /* DEALLOC job */
		}
		else
		{
			return 0; /* EXIT thread*/
		}
	}
	return 0;
}


/* Add work to the thread pool */
int thpool_add_work(thpool_t* tp_p, void *(*function_p)(void*), void* arg_p){
	thpool_job_t* newJob;

	newJob=(thpool_job_t*)malloc(sizeof(thpool_job_t));                        /* MALLOC job */
	if (newJob==NULL){
		fprintf(stderr, "thpool_add_work(): Could not allocate memory for new job\n");
		exit(1);
	}

	/* add function and argument */
	newJob->function=function_p;
	newJob->arg=arg_p;

	/* add job to queue */
	pthread_mutex_lock(&mutex);                  /* LOCK */
	thpool_jobqueue_add(tp_p, newJob);
	pthread_mutex_unlock(&mutex);                /* UNLOCK */

	return 0;
}


/* Destroy the threadpool */
void thpool_destroy(thpool_t* tp_p){
	int t;

	/* End each thread's infinite loop */
	thpool_keepalive=0;

	/* Awake idle threads waiting at semaphore */
	for (t=0; t<(tp_p->threadsN); t++){
		if (sem_post(tp_p->jobqueue->queueSem)){
			fprintf(stderr, "thpool_destroy(): Could not bypass sem_wait()\n");
		}
	}

	/* Kill semaphore */
	if (sem_destroy(tp_p->jobqueue->queueSem)!=0){
		fprintf(stderr, "thpool_destroy(): Could not destroy semaphore\n");
	}

	/* Wait for threads to finish */
	for (t=0; t<(tp_p->threadsN); t++){
		pthread_join(tp_p->threads[t], NULL);
	}

	thpool_jobqueue_empty(tp_p);

	/* Dealloc */
	free(tp_p->threads);                                                   /* DEALLOC threads             */
	free(tp_p->jobqueue->queueSem);                                        /* DEALLOC job queue semaphore */
	free(tp_p->jobqueue);                                                  /* DEALLOC job queue           */
	free(tp_p);                                                            /* DEALLOC thread pool         */
}



/* =================== JOB QUEUE OPERATIONS ===================== */



/* Initialise queue */
int thpool_jobqueue_init(thpool_t* tp_p){
	tp_p->jobqueue=(thpool_jobqueue*)malloc(sizeof(thpool_jobqueue));      /* MALLOC job queue */
	if (tp_p->jobqueue==NULL) return -1;
	tp_p->jobqueue->tail=NULL;
	tp_p->jobqueue->head=NULL;
	tp_p->jobqueue->jobsN=0;
	return 0;
}


/* Add job to queue */
void thpool_jobqueue_add(thpool_t* tp_p, thpool_job_t* newjob_p){ /* remember that job prev and next point to NULL */

	newjob_p->next=NULL;
	newjob_p->prev=NULL;

	thpool_job_t *oldFirstJob;
	oldFirstJob = tp_p->jobqueue->head;

	/* fix jobs' pointers */
	switch(tp_p->jobqueue->jobsN){

		case 0:     /* if there are no jobs in queue */
					tp_p->jobqueue->tail=newjob_p;
					tp_p->jobqueue->head=newjob_p;
					break;

		default: 	/* if there are already jobs in queue */
					oldFirstJob->prev=newjob_p;
					newjob_p->next=oldFirstJob;
					tp_p->jobqueue->head=newjob_p;

	}

	(tp_p->jobqueue->jobsN)++;     /* increment amount of jobs in queue */
	sem_post(tp_p->jobqueue->queueSem);

	int sval;
	sem_getvalue(tp_p->jobqueue->queueSem, &sval);
}


/* Remove job from queue */
int thpool_jobqueue_removelast(thpool_t* tp_p){
	thpool_job_t *oldLastJob;
	oldLastJob = tp_p->jobqueue->tail;

	/* fix jobs' pointers */
	switch(tp_p->jobqueue->jobsN){

		case 0:     /* if there are no jobs in queue */
					return -1;
					break;

		case 1:     /* if there is only one job in queue */
					tp_p->jobqueue->tail=NULL;
					tp_p->jobqueue->head=NULL;
					break;

		default: 	/* if there are more than one jobs in queue */
					oldLastJob->prev->next=NULL;               /* the almost last item */
					tp_p->jobqueue->tail=oldLastJob->prev;

	}

	(tp_p->jobqueue->jobsN)--;

	int sval;
	sem_getvalue(tp_p->jobqueue->queueSem, &sval);
	return 0;
}


/* Get first element from queue */
thpool_job_t* thpool_jobqueue_peek(thpool_t* tp_p){
	return tp_p->jobqueue->tail;
}

/* Remove and deallocate all jobs in queue */
void thpool_jobqueue_empty(thpool_t* tp_p){

	thpool_job_t* curjob;
	curjob=tp_p->jobqueue->tail;

	while(tp_p->jobqueue->jobsN){
		tp_p->jobqueue->tail=curjob->prev;
		free(curjob);
		curjob=tp_p->jobqueue->tail;
		tp_p->jobqueue->jobsN--;
	}

	/* Fix head and tail */
	tp_p->jobqueue->tail=NULL;
	tp_p->jobqueue->head=NULL;
}




/********************************************************************************************
 *  THREADPOOL STRUCTURE END
 ********************************************************************************************/

/********************************************************************************************
 *  TRIE STRUCTURE
 *************************************/

struct LockingMech{
	char padding[64];
	pthread_mutex_t mutex;
};

typedef struct _QueryNode QueryNode;
struct _QueryNode{
	QueryID qid;
	char pos;
};

typedef std::vector<QueryNode> QueryArrayList;

typedef struct _ResultTrieSearch ResultTrieSearch;
struct _ResultTrieSearch{
	QueryArrayList *qids;
};

#define VALID_CHARS 26
typedef struct _TrieNode TrieNode;
struct _TrieNode{
   TrieNode* children[VALID_CHARS];
   QueryArrayList *qids;
};

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

ResultTrieSearch* TrieExactSearchWord( LockingMech* lockmech, TrieNode* root, const char* word, char word_sz, ResultTrieSearch *results ){
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
	   fprintf(stderr, "exact match for %.*s\n", word_sz, word);
       // WE HAVE A MATCH SO get the List of the query ids and add them to the result
	   pthread_mutex_lock(&lockmech->mutex);
		for (QueryArrayList::iterator it = root->qids->begin(), end = root->qids->end(); it != end; it++) {
			results->qids->push_back(*it);
		}
	   pthread_mutex_unlock(&lockmech->mutex);
   }
   return results;
}

struct HammingNode{
	TrieNode* node;
	char letter;
	char depth;
	char tcost;
};
void TrieHammingSearchWord( LockingMech* lockmech, TrieNode* node, const char* word, int word_sz, ResultTrieSearch* results, char maxCost ){
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
				pthread_mutex_lock(&lockmech->mutex);
				for (QueryArrayList::iterator it = current.node->qids->begin(),end = current.node->qids->end(); it != end; it++) {
					results->qids->push_back(*it);
				}
				pthread_mutex_unlock(&lockmech->mutex);
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

void TrieEditSearchWord(LockingMech* lockmech, TrieNode* node, const char* word, int word_sz, ResultTrieSearch* results, char maxCost ){
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
        	pthread_mutex_lock(&lockmech->mutex);
			for (QueryArrayList::iterator it = c.node->qids->begin(), end =	c.node->qids->end(); it != end; it++) {
				results->qids->push_back(*it);
			}
        	pthread_mutex_unlock(&lockmech->mutex);
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
 *  QUERY SET STRUCTURE
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

/********************************************************************************************
 *  QUERY SET STRUCTURE END
 ********************************************************************************************/

/********************************************************************************************
 *  DOCUMENT RESULT STRUCTURE
 *************************************/
struct DocResultsNode{
	DocID docid;
	QueryID *qids;
	unsigned int sz;
};
typedef std::vector<DocResultsNode> DocResults;


/********************************************************************************************
 *  DOCUMENT RESULT STRUCTURE END
 ********************************************************************************************/

#define NUM_THREADS 9
#define NUM_TRIES_EACH_TYPE 4
#define TOTAL_SEARCHERS (2*NUM_TRIES_EACH_TYPE+1)

/********************************************************************************************
 *  GLOBALS
 *************************************/
TrieNode *trie_exact;
TrieNode **trie_hamming;
TrieNode **trie_edit;

/////////////////////////////////////////////////////////////////////////////////
// TODO - BE CAREFULL WITH THE GLOBAL STACK IN PARALLEL CODE
LockingMech exact_locks[1];
LockingMech hamming_locks[NUM_TRIES_EACH_TYPE];
LockingMech edit_locks[NUM_TRIES_EACH_TYPE];
/////////////////////////////////////////////////////////////////////////////////

QuerySet *querySet; // std::vector<QuerySetNode*>
DocResults *docResults; // std::list<DocResultsNode>


// THREADPOOL
thpool_t* threadpool;

// STRUCTURES FOR PTHREADS
struct TrieSearchData{
	char type;
	LockingMech* lock;
	TrieNode *node;
	const char* word;
	int word_sz;
	ResultTrieSearch* results;
	char maxCost;
};

void* TrieSearchWord( void* tsd_ ){
	TrieSearchData *tsd = (TrieSearchData *)tsd_;
	switch( tsd->type ){
	case 0:
		TrieExactSearchWord( tsd->lock, tsd->node, tsd->word, tsd->word_sz, tsd->results );
		break;
	case 1:
		TrieHammingSearchWord( tsd->lock, tsd->node, tsd->word, tsd->word_sz, tsd->results, tsd->maxCost );
		break;
	case 2:
		TrieEditSearchWord( tsd->lock, tsd->node, tsd->word, tsd->word_sz, tsd->results, tsd->maxCost);
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

	//int s= gettime();

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

    pthread_mutex_init( &exact_locks[0].mutex, NULL );
    for( char i=0; i<NUM_TRIES_EACH_TYPE; i++ ){
    	pthread_mutex_init( &hamming_locks[i].mutex, NULL );
    }
    for( char i=0; i<NUM_TRIES_EACH_TYPE; i++ ){
    	pthread_mutex_init( &edit_locks[i].mutex, NULL);
    }

    querySet = new QuerySet();
    // add dummy query to start from index 1 because query ids start from 1 instead of 0
    querySet->push_back((QuerySetNode*)malloc(sizeof(QuerySetNode)));
    docResults = new DocResults();

    // Initialize the threadpool with 12 threads
    //threadpool = thpool_init( NUM_THREADS );

    //fprintf( stderr, "%d\n", gettime()-s );

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

    for( unsigned int i=0, sz=querySet->size(); i<sz; i-- ){
    	free( querySet->at(i) );
    }
    delete querySet;
    delete docResults;

    pthread_mutex_destroy(&exact_locks[0].mutex);
    for( short int i=0; i<NUM_TRIES_EACH_TYPE; i++ ){
      	pthread_mutex_destroy(&hamming_locks[i].mutex);
    }
    for( short int i=0; i<NUM_TRIES_EACH_TYPE; i++ ){
        pthread_mutex_destroy(&edit_locks[i].mutex);
    }
    // destroy the thread pool
    //thpool_destroy( threadpool );

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
// PARALLELISM MUST BE DONE PERFECTLY HERE - IN MATCHING DOCUMENTS
ErrorCode MatchDocument(DocID doc_id, const char* doc_str){
	char k,szz;
	// results are new for each document
	ResultTrieSearch *results[TOTAL_SEARCHERS], *results_all;
	for( k=0,szz=TOTAL_SEARCHERS; k<szz; k++ ){
		results[k] = (ResultTrieSearch*)malloc(sizeof(ResultTrieSearch));
		if( !results[k] ) err_mem( "could not allocate ResultsTrieSearch for document" );
	    results[k]->qids = new QueryArrayList();
	}


	threadpool = thpool_init( NUM_THREADS );

	// check if a query fully
    // parallel quicksort in multi-threaded INSTEAD of the position of each word inside the query node

	///////////////////////////////////////////////
    //int s = gettime();
	TrieSearchData *tsd;
	const char *start, *end;
	char sz;
    for( start=doc_str; *start; start = end ){
	    // FOR EACH WORD DO THE MATCHING

    	while( *start == ' ' ) start++;
    	end = start;
    	while( *end >= 'a' && *end <= 'z' ) end++;

		//results->qids->push_back(*it);
    	sz = end-start;


    	// exact matching
    	tsd = (TrieSearchData *)malloc(sizeof(TrieSearchData));
    	tsd->type = 0;
    	tsd->lock = &exact_locks[0];
    	tsd->node = trie_exact;
    	tsd->results = results[0];
    	tsd->word = start;
    	tsd->word_sz = sz;
    	thpool_add_work( threadpool, reinterpret_cast<void* (*)(void*)>(TrieSearchWord), tsd );

    	// hamming matching
    	for( k=0; k<NUM_TRIES_EACH_TYPE; k++ ){
    		tsd = (TrieSearchData *)malloc(sizeof(TrieSearchData));
    		tsd->type = 1;
    		tsd->lock = &hamming_locks[k];
    		tsd->node = trie_hamming[k];
    		tsd->maxCost = k;
    		tsd->results = results[k+1];
    		tsd->word = start;
    		tsd->word_sz = sz;
    		thpool_add_work( threadpool, reinterpret_cast<void* (*)(void*)>(TrieSearchWord), tsd );
    	}
    	// edit matching

		for ( k = 0; k < NUM_TRIES_EACH_TYPE; k++) {
			tsd = (TrieSearchData *)malloc(sizeof(TrieSearchData));
			tsd->type = 2;
			tsd->lock = &edit_locks[k];
			tsd->node = trie_edit[k];
			tsd->maxCost = k;
			tsd->results = results[k+5];
    		tsd->word = start;
    		tsd->word_sz = sz;
			thpool_add_work( threadpool, reinterpret_cast<void* (*)(void*)>(TrieSearchWord), tsd );
		}

    	/*
    	TrieExactSearchWord( &exact_locks[0], trie_exact, results[0], start, sz );
    	TrieHammingSearchWord( &hamming_locks[0], trie_hamming[0], start, sz, results[1], 0 );
    	TrieHammingSearchWord( &hamming_locks[1], trie_hamming[1], start, sz, results[2], 1 );
    	TrieHammingSearchWord( &hamming_locks[2], trie_hamming[2], start, sz, results[3], 2 );
    	TrieHammingSearchWord( &hamming_locks[3], trie_hamming[3], start, sz, results[4], 3 );
    	TrieEditSearchWord( &edit_locks[0], trie_edit[0], start, sz, results[5], 0);
    	TrieEditSearchWord( &edit_locks[1], trie_edit[1], start, sz, results[6], 1);
    	TrieEditSearchWord( &edit_locks[2], trie_edit[2], start, sz, results[7], 2);
    	TrieEditSearchWord( &edit_locks[3], trie_edit[3], start, sz, results[8], 3);
    	*/
    }
    //fprintf( stderr, "doc[%u] results: %lu miliseconds: %d\n", doc_id, results->qids->size(), gettime()-s );

    // //////////////////////////
    // FIND A WAY TO SYNCHRONIZE THREADPOOL
    /////////////////////////////
    thpool_destroy( threadpool );

    // TODO - merge the results - CAN BE PARALLED
    for( k=1, szz=TOTAL_SEARCHERS; k<szz; k++ ){
    	fprintf( stderr, "total results[%d]: %d\n", k, results[k]->qids->size() );
    	for( QueryArrayList::iterator it=results[k]->qids->begin(), end=results[k]->qids->end(); it != end; it++ ){
    		results[0]->qids->push_back(*it);
    	}
    }

    results_all = results[0];


    //int s = gettime();
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

    for( k=0,szz=TOTAL_SEARCHERS; k<szz; k++ ){
        delete results[k]->qids;
    }

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


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

typedef struct _lp_tpjob lp_tpjob;
struct _lp_tpjob{
    void *args;
    void *(*func)(int, void *);
    lp_tpjob *next;
};

typedef struct{
   int workers_ids;
   int nthreads;
   int pending_jobs;
   lp_tpjob *jobs_head;
   lp_tpjob *jobs_tail;

   pthread_cond_t cond_jobs;
   pthread_mutex_t mutex_pool;

   pthread_t *worker_threads;

   int synced_threads;
   pthread_cond_t pool_sync;

}lp_threadpool;

void lp_threadpool_addjob( lp_threadpool* pool, void *(*func)(int, void *), void* args ){
     lp_tpjob *njob = (lp_tpjob*)malloc( sizeof(lp_tpjob) );
     if( !njob ){
         perror( "Could not create a lp_tpjob...\n" );
         return;
     }
     njob->args = args;
     njob->func = func;
     njob->next = 0;

     // ENTER POOL CRITICAL SECTION
     pthread_mutex_lock( &pool->mutex_pool );
     //////////////////////////////////////

     // empty job queue
     if( pool->pending_jobs == 0 ){
         pool->jobs_head = njob;
         pool->jobs_tail = njob;
     }else{
         // add new job to the tail of the queue
         pool->jobs_tail->next = njob;
         pool->jobs_tail = njob;
     }

     pool->pending_jobs++;

     //fprintf( stderr, "job added [%d]\n", pool->pending_jobs );

     // EXIT POOL CRITICAL SECTION
     pthread_mutex_unlock( &pool->mutex_pool );
     //////////////////////////////////////

     // signal any worker_thread that new job is available
     pthread_cond_signal( &pool->cond_jobs );
}

lp_tpjob* lp_threadpool_fetchjob( lp_threadpool* pool ){
    lp_tpjob* job;
    // lock pool
    pthread_mutex_lock( &pool->mutex_pool );

       while( pool->pending_jobs == 0 )
           pthread_cond_wait( &pool->cond_jobs, &pool->mutex_pool );

       // available job pending
        --pool->pending_jobs;
        job = pool->jobs_head;
        pool->jobs_head = pool->jobs_head->next;
        // if no more jobs available
        if( pool->jobs_head == 0 ){
            pool->jobs_tail = 0;
        }

    //fprintf( stderr, "job removed - remained[%d]\n", pool->pending_jobs );

    // pool unlock
    pthread_mutex_unlock( &pool->mutex_pool );

    return job;
}
// returns an id from 1 to number of threads (eg. threads=12, ids = 1,2,3,4,5,6,7,8,9,10,11,12)
int lp_threadpool_uniquetid( lp_threadpool* pool ){
   // lock pool
   int _tid;
   pthread_mutex_lock( &pool->mutex_pool );
   _tid = pool->workers_ids;
   pool->workers_ids = ( ( pool->workers_ids + 1 ) % (pool->nthreads+1) )  ;
   pthread_mutex_unlock( &pool->mutex_pool );
   return _tid;
}

void* lp_tpworker_thread( void* _pool ){

   lp_threadpool* pool = ((lp_threadpool*)_pool);
   int _tid=lp_threadpool_uniquetid( pool );

   //fprintf( stderr, "thread[%d] entered worker_thread infite\n", _tid );

   for(;;){

       // fetch next job - blocking method
       lp_tpjob* njob = lp_threadpool_fetchjob( pool );

       // execute the function passing in the thread_id - the TID starts from 1 - POOL_THREADS
       njob->func( _tid , njob->args );

       // release memory the job holds
       free( njob );
   }
   return 0;
}

lp_threadpool* lp_threadpool_init( int threads ){
    lp_threadpool* pool = (lp_threadpool*)malloc( sizeof(lp_threadpool) );
    pool->workers_ids = 1; // threads start from 1 to NTHREADS
    pool->nthreads = threads;
    pool->pending_jobs = 0;
    pool->jobs_head=0;
    pool->jobs_tail=0;

    pthread_cond_init( &pool->cond_jobs, NULL );
    pthread_mutex_init( &pool->mutex_pool, NULL );

    // lock pool in order to prevent worker threads to start before initializing the whole pool
    pthread_mutex_lock( &pool->mutex_pool );

    pthread_t *worker_threads = (pthread_t*)malloc(sizeof(pthread_t)*threads);
    for( int i=0; i<threads; i++ ){
        pthread_create( &worker_threads[i], NULL, reinterpret_cast<void* (*)(void*)>(lp_tpworker_thread), pool );
        //fprintf( stderr, "[%p] thread[%d] started\n", worker_threads[i] );
    }

    pool->worker_threads = worker_threads;

    pthread_cond_init( &pool->pool_sync, NULL );
    pool->synced_threads = 0;

    // unlock pool for workers
    pthread_mutex_unlock( &pool->mutex_pool );

    return pool;
}

void lp_threadpool_destroy(lp_threadpool* pool){

    pthread_cond_destroy( &pool->cond_jobs );
    pthread_cond_destroy( &pool->pool_sync );
	free(pool->worker_threads);
	for (lp_tpjob* j = pool->jobs_head, *t = 0; j; j = t) {
		t = j->next;
		free(j);
	}
	free(pool);
}


void synchronize_threads(int tid, void * arg){
    lp_threadpool* pool = (lp_threadpool*)arg;

	fprintf( stderr, "thread[%d] entered synchronization\n", tid );

	pthread_mutex_lock( &pool->mutex_pool );

	++pool->synced_threads;
	if( pool->synced_threads == pool->nthreads ){
		pool->synced_threads = 0;
		pthread_cond_broadcast( &pool->pool_sync );
	}else{
		pthread_cond_wait( &pool->pool_sync, &pool->mutex_pool );
	}

	pthread_mutex_unlock( &pool->mutex_pool );

	fprintf( stderr, ":: thread[%d] exited synchronization\n", tid );
}
void lp_threadpool_synchronize_all(lp_threadpool* pool){
	for( int i=1; i<=pool->nthreads; i++ ){
	  	lp_threadpool_addjob( pool, reinterpret_cast<void* (*)(int,void*)>(synchronize_threads), (void*)pool);
    }
}

void synchronize_threads_master(int tid, void * arg){
    lp_threadpool* pool = (lp_threadpool*)arg;

	//fprintf( stderr, "thread[%d] entered synchronization\n", tid );

	pthread_mutex_lock( &pool->mutex_pool );

	++pool->synced_threads;
	if( pool->synced_threads == (pool->nthreads + 1) ){
		pool->synced_threads = 0;
		pthread_cond_broadcast( &pool->pool_sync );
	}else{
		pthread_cond_wait( &pool->pool_sync, &pool->mutex_pool );
	}

	pthread_mutex_unlock( &pool->mutex_pool );

	//fprintf( stderr, ":: thread[%d] exited synchronization\n", tid );
}
void lp_threadpool_synchronize_master(lp_threadpool* pool){
	for( int i=1; i<=pool->nthreads; i++ ){
	  	lp_threadpool_addjob( pool, reinterpret_cast<void* (*)(int,void*)>(synchronize_threads_master), (void*)pool);
    }
	synchronize_threads_master(0, (void*)pool);
}

/********************************************************************************************
 *  THREADPOOL STRUCTURE END
 ********************************************************************************************/

#define NO_LP_LOCKS_ENABLED
//#define LP_LOCKS_ENABLED

#define NUM_THREADS 24
#define TOTAL_WORKERS NUM_THREADS+1
#define LP_NUM_THREADS 32

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

bool compareQueryNodes( const QueryNode &a, const QueryNode &b){
	if( a.qid < b.qid  )
		return true;
	if( a.qid > b.qid )
		return false;
	return a.pos <= b.pos;
}

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
ResultTrieSearch global_results[LP_NUM_THREADS+1];
LockingMech global_results_locks[LP_NUM_THREADS+1]; // one lock for each result set

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

void TrieExactSearchWord( TrieNode* root, const char* word, char word_sz, ResultTrieSearch* local_results ){
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
		for (QueryArrayList::iterator it = root->qids->begin(), end = root->qids->end(); it != end; it++) {
			//global_results[tid]->qids->push_back(*it);
			local_results->qids->push_back(*it);
		}
    }
}

struct HammingNode{
	TrieNode* node;
	char letter;
	char depth;
	char tcost;
};
void TrieHammingSearchWord( TrieNode* node, const char* word, int word_sz, ResultTrieSearch* local_results, char maxCost ){
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
				for (QueryArrayList::iterator it = current.node->qids->begin(),end = current.node->qids->end(); it != end; it++) {
					//global_results[tid]->qids->push_back(*it);
					local_results->qids->push_back(*it);
				}
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
void TrieEditSearchWord( TrieNode* node, const char* word, int word_sz, ResultTrieSearch* local_results, char maxCost ){
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
			for (QueryArrayList::iterator it = c.node->qids->begin(), end =	c.node->qids->end(); it != end; it++) {
				//global_results[tid]->qids->push_back(*it);
				local_results->qids->push_back(*it);
			}
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
lp_threadpool* threadpool;

// STRUCTURES FOR PTHREADS
#define WORDS_PROCESSED_BY_THREAD 50
struct TrieSearchData{
	const char* words[WORDS_PROCESSED_BY_THREAD];
	char words_sz[WORDS_PROCESSED_BY_THREAD];
	short words_num;
	char padding[128];
};

unsigned int MAX_TRIE_SEARCH_DATA = MAX_DOC_LENGTH / 2;
unsigned int current_trie_search_data = 0;
std::vector<TrieSearchData> trie_search_data_pool;

pthread_mutex_t mutex_tid = PTHREAD_MUTEX_INITIALIZER;
void* TrieSearchWord( int tid, void* args ){
	TrieSearchData *tsd = (TrieSearchData *)args;

	//fprintf( stderr, "words_num[%d] words[%p] words_sz[%p]\n", tsd->words_num, tsd->words, tsd->words_sz );

	const char* w;
	char wsz;

	//fprintf( stderr, "search[%d]\n", tid );

	for( int i=0, j=tsd->words_num; i<j; i++ ){
		wsz = tsd->words_sz[i];
		w = tsd->words[i];
		TrieExactSearchWord(  trie_exact, w, wsz, &global_results[tid] );
		TrieHammingSearchWord(  trie_hamming[0], w, wsz, &global_results[tid], 0 );
	    TrieHammingSearchWord(  trie_hamming[1], w, wsz, &global_results[tid], 1 );
		TrieHammingSearchWord(  trie_hamming[2], w, wsz, &global_results[tid], 2 );
		TrieHammingSearchWord(  trie_hamming[3], w, wsz, &global_results[tid], 3 );
		TrieEditSearchWord(  trie_edit[0], w, wsz, &global_results[tid], 0 );
		TrieEditSearchWord(  trie_edit[1], w, wsz, &global_results[tid], 1 );
		TrieEditSearchWord(  trie_edit[2], w, wsz, &global_results[tid], 2 );
		TrieEditSearchWord(  trie_edit[3], w, wsz, &global_results[tid], 3 );
	}

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

TrieNodeLPMergesort *lp_mergesort_tries[LP_NUM_THREADS];

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
	if( qids == (QueryArrayList *) 0xffffffffffffffff )
		return;
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

// not working as is
void* TrieLPMergesort( int tid, void* args ){

	lp_threadpool* pool = (lp_threadpool*)args;

    fprintf( stderr, "thread[%d] entered sort\n", tid );

	TrieLPMergesortFill( lp_mergesort_tries[tid] , global_results[tid].qids );
	TrieLPMergesortCheckQueries( lp_mergesort_tries[tid] );

	int working_threads = LP_NUM_THREADS>>1;
    int phase = 0;

	while( tid < working_threads ){
		//fprintf( stderr, "tid[%d] working threads[%d]\n", tid, working_threads );

    	phase++;

    	// wait for the other active thread to finish
    	if( tid + working_threads < NUM_THREADS ){
			//sem_wait( &(pool->sem_sync[ tid + working_threads ]) );
    	}
    	TrieLPMergesortMerge( lp_mergesort_tries[tid], lp_mergesort_tries[tid+working_threads] );

    	working_threads >>= 1;
    }

	TrieLPMergesortCheckQueries( lp_mergesort_tries[tid] );

	// thread with id == 0 does not need to se_post since no one is waiting for him
	//sem_post( &pool->sem_sync[tid] );

	return 0;
}

/********************************************************************************************
 *  TRIE LP_MERGESORT STRUCTURE END
 ********************************************************************************************/



/*
 *  PARALLEL ALGORITHMS
 */
struct mergesort_p{
	pthread_cond_t finished;
	pthread_cond_t* conds;
	pthread_mutex_t* mutexes;
	int *ready;
	lp_threadpool* pool;
};
void lp_mergesort_parallel( int tid, void* args ){
	    mergesort_p* msp = (mergesort_p*)args;

	    fprintf( stderr, "thread[%d] entered sort\n", tid );

		int working_threads = LP_NUM_THREADS>>1;
	    int phase = 0;

	    //PHASE 0 - each thread sorts its part of the array
	    std::stable_sort( global_results[tid].qids->begin(), global_results[tid].qids->end(), compareQueryNodes );

		while( tid < working_threads ){
			//fprintf( stderr, "tid[%d] working threads[%d]\n", tid, working_threads );

	    	phase++;

	    	// wait for the other active thread to finish
	    	if( tid + working_threads < NUM_THREADS ){
				pthread_mutex_lock( &msp->mutexes[tid+working_threads] );
				if( msp->ready[tid + working_threads] == 0 )
					pthread_cond_wait( &msp->conds[tid+working_threads], &msp->mutexes[tid] );
				pthread_mutex_unlock( &msp->mutexes[tid+working_threads] );
	    	}

	    	// merge the two lists
            std::vector<QueryNode> temp = *(global_results[tid].qids);
            std::vector<QueryNode> *other = global_results[tid+working_threads].qids;
            std::vector<QueryNode> *qids = global_results[tid].qids;
            int i,j,isz,jsz,k=0;
            isz=temp.size();
            jsz=qids->size();
            qids->clear();
            for( i=0,j=0; i<isz && i<jsz;  ){
            	if( compareQueryNodes( temp.at(i), other->at(j) ) ){
                    qids->push_back(temp[i++]);
            	}else{
            		qids->push_back(other->at(i++));
            	}
            }
            for( ; i<isz; i++ )
            	qids->push_back(temp[i++]);
            for( ; j<jsz; j++ )
            	qids->push_back(other->at(i++));


	    	working_threads >>= 1;
	    }

		pthread_mutex_lock( &msp->mutexes[tid] );
		msp->ready[tid] = 1;
		pthread_cond_signal(&msp->conds[tid]);
		if( tid > 0 )
		    pthread_cond_wait( &msp->finished, &msp->mutexes[tid] );
		pthread_mutex_unlock( &msp->mutexes[tid] );

		if( tid == 0 ){
			pthread_cond_broadcast(&msp->finished);
		}

		return;
}




///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode InitializeIndex(){

//	for( char i=0; i<LP_NUM_THREADS; i++ )
//	    sem_init( &semaphores[i], 0, 0 );

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
    for( char i=0; i<LP_NUM_THREADS; i++ ){
    	global_results[i].qids = new QueryArrayList();
    	global_results[i].qids->resize(128);
    	global_results[i].qids->clear();
    }

    trie_search_data_pool.resize(MAX_TRIE_SEARCH_DATA);

    querySet = new QuerySet();
    // add dummy query to start from index 1 because query ids start from 1 instead of 0
    querySet->push_back((QuerySetNode*)malloc(sizeof(QuerySetNode)));
    docResults = new DocResults();

    // Initialize the threadpool
    threadpool = lp_threadpool_init( NUM_THREADS );


	return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode DestroyIndex(){

//	for( char i=0; i<LP_NUM_THREADS; i++ )
//		sem_destroy( &semaphores[i] );

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
    for( char i=0; i<LP_NUM_THREADS; i++ ){
      	delete global_results[i].qids;
    }
    //free( global_results );

    for( unsigned int i=0, sz=querySet->size(); i<sz; i-- ){
    	free( querySet->at(i) );
    }
    delete querySet;
    delete docResults;

    // destroy the thread pool
    //thr_pool_destroy(threadpool);

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

// TODO -
// TODO - Check for the same words in the same document
// TODO - Check the cache for words in previous documents too and get the results without running the algorithms again

std::vector<QueryID> ids; // used for the final doc results
ErrorCode MatchDocument(DocID doc_id, const char* doc_str){
	char k,szz;
	ids.clear();

	// results are new for each document
	// TODO - With this we might not need the global_results
	//for( k=0; k<LP_NUM_THREADS; k++ ){
		//lp_mergesort_tries[k] = TrieNodeLPMergesort_Constructor();
	//}


	TrieNodeVisited *visited = TrieNodeVisited_Constructor();




	///////////////////////////////////////////////
    //int s = gettime();

    short batch_words=0;
	TrieSearchData *tsd = NULL;
	const char *start, *end;
	char sz;
	int tid=0;
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
    		lp_threadpool_addjob( threadpool, reinterpret_cast<void* (*)(int, void*)>(TrieSearchWord), tsd );
    	}else if( *end == '\0' ){
    		// very few words or the last ones so just handle them yourself
    		// the same thing the THREAD_JOB does
    		const char* w;
    		char wsz;
    		for( int i=0, j=batch_words; i<j; i++ ){
    			wsz = tsd->words_sz[i];
    			w = tsd->words[i];
    			TrieExactSearchWord( trie_exact, w, wsz, &global_results[tid] );
    			TrieHammingSearchWord( trie_hamming[0], w, wsz, &global_results[tid], 0 );
    		    TrieHammingSearchWord(  trie_hamming[1], w, wsz, &global_results[tid], 1 );
    			TrieHammingSearchWord(  trie_hamming[2], w, wsz, &global_results[tid], 2 );
    			TrieHammingSearchWord(  trie_hamming[3], w, wsz, &global_results[tid], 3 );
    			TrieEditSearchWord(  trie_edit[0], w, wsz, &global_results[tid], 0);
    			TrieEditSearchWord(  trie_edit[1], w, wsz, &global_results[tid], 1 );
    			TrieEditSearchWord(  trie_edit[2], w, wsz, &global_results[tid], 2 );
    			TrieEditSearchWord(  trie_edit[3], w, wsz, &global_results[tid], 3 );
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
        	TrieExactSearchWord( trie_exact, w, wsz, &global_results[tid] );
        	TrieHammingSearchWord( trie_hamming[0], w, wsz, &global_results[tid], 0 );
            TrieHammingSearchWord(  trie_hamming[1], w, wsz, &global_results[tid], 1 );
        	TrieHammingSearchWord(  trie_hamming[2], w, wsz, &global_results[tid], 2 );
        	TrieHammingSearchWord(  trie_hamming[3], w, wsz, &global_results[tid], 3 );
        	TrieEditSearchWord(  trie_edit[0], w, wsz, &global_results[tid], 0);
        	TrieEditSearchWord(  trie_edit[1], w, wsz, &global_results[tid], 1 );
        	TrieEditSearchWord(  trie_edit[2], w, wsz, &global_results[tid], 2 );
        	TrieEditSearchWord(  trie_edit[3], w, wsz, &global_results[tid], 3 );
        }
    }


        /////////////////////////////
        // FIND A WAY TO SYNCHRONIZE THREADPOOL
        /////////////////////////////
        lp_threadpool_synchronize_master( threadpool );
        // destroy visited table
        TrieNodeVisited_Destructor( visited );

        //fprintf( stderr, "doc[%u] miliseconds: %d\n", doc_id, gettime()-s );


        //int s = gettime();



        mergesort_p msp;
        pthread_cond_t sorting_conds[LP_NUM_THREADS];;
        pthread_mutex_t sorting_mutexes[LP_NUM_THREADS];
        int sorting_ready[LP_NUM_THREADS];
        for( k=0; k<LP_NUM_THREADS; k++ ){
        	pthread_cond_init( &sorting_conds[k], NULL );
            sorting_mutexes[k] = PTHREAD_MUTEX_INITIALIZER;
            sorting_ready[k] = 0;
        }
        pthread_cond_init( &msp.finished, NULL );
        msp.conds = sorting_conds;
        msp.mutexes = sorting_mutexes;
        msp.ready = sorting_ready;


            for( batch_words=0; batch_words<NUM_THREADS; batch_words++ ){
            	lp_threadpool_addjob( threadpool, reinterpret_cast<void* (*)(int,void*)>(lp_mergesort_parallel), (void*)&msp);
            }
            TrieLPMergesort( 0, &msp );

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

        // TODO - Parallel sorting
        std::stable_sort( results_all->qids->begin(), results_all->qids->end(), compareQueryNodes );
        //parallelMergesort( results->qids, results->qids->size(), 4 );
*/
        //fprintf( stderr, "doc[%u] results: %lu miliseconds: %d\n", doc_id, results_all->qids->size(), gettime()-s );

        ResultTrieSearch *results_all;
	    results_all = &global_results[0];


        // FIND THE UNIQUE IDS AND CHECK IF THE QueryIDS are valid as per their words

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



     DocResultsNode doc;
     	doc.docid=doc_id;
     	doc.sz=ids.size();
     	doc.qids=0;
     	if(doc.sz) doc.qids=(QueryID*)malloc(doc.sz*sizeof(unsigned int));
     	for(int i=0, szz=doc.sz;i<szz;i++) doc.qids[i]=ids[i];
     	// Add this result to the set of undelivered results
     	docResults->push_back(doc);




	for( k=0; k<LP_NUM_THREADS; k++ ){
		//TrieNodeLPMergesort_Destructor( lp_mergesort_tries[k] );
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

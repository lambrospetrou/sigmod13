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

//////////////////////////////////////////

#define NO_LP_LOCKS_ENABLED
//#define LP_LOCKS_ENABLED

#define NUM_THREADS 24
#define TOTAL_WORKERS NUM_THREADS+1
#define LP_NUM_THREADS 32

#define VALID_CHARS 26

#define DOC_RESULTS_SIZE 2000000

/********************************************************************************************
 * TRIE VISITED STRUCTURE
 *************************************/
struct TrieNodeVisited{
   TrieNodeVisited* children[VALID_CHARS];
   char exists;
};



#define SPARSE_ARRAY_NODE_DATA 13107 // 2^16 / 5 in order to fit in cache block 64K
struct SparseArrayNode{
	SparseArrayNode* next;
	char data[SPARSE_ARRAY_NODE_DATA][MAX_QUERY_WORDS];
	unsigned int low;
	unsigned int high;
};

struct Document{
	char *doc; // might be faster if not fixed size
	DocID doc_id;
	int total_jobs;
	int finished_jobs;
	//char *qids; // initialized to num of queries * 5 or 8
	TrieNodeVisited *visited; // only the matching job
	pthread_mutex_t mutex_finished_jobs;
	SparseArrayNode* query_ids;
	pthread_mutex_t mutex_query_ids;
};


SparseArrayNode* SparseArray_Constructor();
void SparseArray_Destructor(SparseArrayNode* n);
void SparseArraySet( SparseArrayNode* sa, unsigned int index, char pos );
unsigned int* SparseArrayCompress(SparseArrayNode* array, unsigned int * total);








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
   pthread_cond_t sleep;
   pthread_barrier_t pool_barrier;



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

       while( pool->pending_jobs == 0 ){
    	   pool->synced_threads++;
    	   //fprintf( stderr, "fecth_job: synced_threads[%d]\n", pool->synced_threads );
    	   if( pool->synced_threads == pool->nthreads ){
    		   // signal anyone waiting for complete synchronization
    		   pthread_cond_broadcast(&pool->sleep);
    	   }
           pthread_cond_wait( &pool->cond_jobs, &pool->mutex_pool );
           pool->synced_threads--;
       }



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
    pthread_cond_init( &pool->sleep, NULL );
    pool->synced_threads = 0;

    pthread_mutex_init( &pool->mutex_pool, NULL );

    // lock pool in order to prevent worker threads to start before initializing the whole pool
    pthread_mutex_lock( &pool->mutex_pool );

    pthread_t *worker_threads = (pthread_t*)malloc(sizeof(pthread_t)*threads);
    for( int i=0; i<threads; i++ ){
        pthread_create( &worker_threads[i], NULL, reinterpret_cast<void* (*)(void*)>(lp_tpworker_thread), pool );
        //fprintf( stderr, "[%p] thread[%d] started\n", worker_threads[i] );
    }

    pool->worker_threads = worker_threads;

    pthread_barrier_init( &pool->pool_barrier, NULL, 25 );

    // unlock pool for workers
    pthread_mutex_unlock( &pool->mutex_pool );

    return pool;
}

void lp_threadpool_destroy(lp_threadpool* pool){
	pthread_cond_destroy( &pool->sleep );
    pthread_cond_destroy( &pool->cond_jobs );
	free(pool->worker_threads);
	for (lp_tpjob* j = pool->jobs_head, *t = 0; j; j = t) {
		t = j->next;
		free(j);
	}
	free(pool);
}


//void synchronize_threads_master(int tid, void * arg){
//    lp_threadpool* pool = (lp_threadpool*)arg;
//
//	//fprintf( stderr, "thread[%d] entered synchronization\n", tid );
//
//	pthread_mutex_lock( &pool->mutex_pool );
//
//	++pool->synced_threads;
//	if( pool->synced_threads == (pool->nthreads + 1) ){
//		pool->synced_threads = 0;
//		pthread_cond_broadcast( &pool->pool_sync );
//	}else{
//		pthread_cond_wait( &pool->pool_sync, &pool->mutex_pool );
//	}
//
//	pthread_mutex_unlock( &pool->mutex_pool );
//
//	//fprintf( stderr, ":: thread[%d] exited synchronization\n", tid );
//}
void synchronize_threads_master(int tid, void * arg){
    lp_threadpool* pool = (lp_threadpool*)arg;

	//fprintf( stderr, "thread[%d] entered synchronization\n", tid );

	pthread_barrier_wait( &pool->pool_barrier );
	//fprintf( stderr, ":: thread[%d] exited synchronization\n", tid );
}
void lp_threadpool_synchronize_master(lp_threadpool* pool){
	for( int i=1; i<=pool->nthreads; i++ ){
	  	lp_threadpool_addjob( pool, reinterpret_cast<void* (*)(int,void*)>(synchronize_threads_master), (void*)pool);
    }
	synchronize_threads_master(0, (void*)pool);
}

void synchronize_complete(lp_threadpool* pool){

	pthread_mutex_lock( &pool->mutex_pool );

	while( pool->synced_threads < pool->nthreads ){
        pthread_cond_wait( &pool->sleep, &pool->mutex_pool );
        //fprintf( stderr, "sunchronize_complete: synced_threads[%d]\n", pool->synced_threads );
	}

    pthread_mutex_unlock( &pool->mutex_pool );
}

/********************************************************************************************
 *  THREADPOOL STRUCTURE END
 ********************************************************************************************/







/********************************************************************************************
 *  TRIE STRUCTURE
 *************************************/

typedef struct _QueryNode QueryNode;
struct _QueryNode{
	QueryID qid;
	char pos;
};

typedef std::vector<QueryNode> QueryArrayList;

typedef struct _ResultTrieSearch ResultTrieSearch;
struct _ResultTrieSearch{
	char padding[128];
	QueryArrayList *qids;
};
// both initialized in InitializeIndex()
//ResultTrieSearch global_results[LP_NUM_THREADS+1];

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

void TrieExactSearchWord( char tid, TrieNode* root, const char* word, char word_sz, /*char* doc_results*/Document*doc ){
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
			pthread_mutex_lock( &doc->mutex_query_ids );
							SparseArraySet( doc->query_ids, it->qid, it->pos );
							pthread_mutex_unlock( &doc->mutex_query_ids );

			//doc_results[(it->qid)*5 + it->pos] = 1;
			//local_results->qids->push_back(*it);
		}
    }
}

struct HammingNode{
	TrieNode* node;
	char letter;
	char depth;
	char tcost;
};
void TrieHammingSearchWord( char tid, TrieNode* node, const char* word, int word_sz, /*char* doc_results*/Document*doc, char maxCost ){
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
					pthread_mutex_lock( &doc->mutex_query_ids );
									SparseArraySet( doc->query_ids, it->qid, it->pos );
									pthread_mutex_unlock( &doc->mutex_query_ids );
					//doc_results[(it->qid)*5 + it->pos] = 1;
					//local_results->qids->push_back(*it);
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
void TrieEditSearchWord( char tid, TrieNode* node, const char* word, int word_sz, /*char* doc_results*/Document*doc, char maxCost ){
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
				pthread_mutex_lock( &doc->mutex_query_ids );
				SparseArraySet( doc->query_ids, it->qid, it->pos );
				pthread_mutex_unlock( &doc->mutex_query_ids );
				//doc_results[(it->qid)*5 + it->pos] = 1;
				//local_results->qids->push_back(*it);
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

void TrieVisitedClear(TrieNodeVisited* node){
	for( char i=0; i<VALID_CHARS; i++ ){
	    	if( node->children[i] != 0 ){
	    		TrieVisitedClear( node->children[i] );
	    	}
	    }
	    node->exists = 0;
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





// STRUCTURES FOR PTHREADS
#define WORDS_PROCESSED_BY_THREAD 400
struct TrieSearchData{
	const char* words[WORDS_PROCESSED_BY_THREAD];
	char words_sz[WORDS_PROCESSED_BY_THREAD];
	short words_num;
	char padding[128];
	DocID doc_id;
};


Document* DocumentConstructor(){
    Document* doc = (Document*)malloc(sizeof(Document));
    doc->doc=(char*)malloc( MAX_DOC_LENGTH );
    doc->doc_id=0;
    doc->finished_jobs = 0;
    doc->total_jobs = MAX_DOC_LENGTH;
    //doc->qids = (char*)malloc( DOC_RESULTS_SIZE );
    //memset(doc->qids, 0, DOC_RESULTS_SIZE);
    doc->visited = TrieNodeVisited_Constructor();
    pthread_mutex_init( &doc->mutex_finished_jobs, NULL );
    pthread_mutex_init( &doc->mutex_query_ids, NULL );
    doc->query_ids = SparseArray_Constructor();
    return doc;
}

void DocumentDestructor( Document *doc ){
	free( doc->doc );
	//free( doc->qids );
	TrieNodeVisited_Destructor( doc->visited );
	SparseArray_Destructor( doc->query_ids );
	pthread_mutex_destroy( &doc->mutex_finished_jobs );
	pthread_mutex_destroy( &doc->mutex_query_ids );
	free( doc );
}

struct DocumentHandlerNode{
	DocID doc_id;
	//char *str;
};

/********************************************************************************************
 *  GLOBALS
 *************************************/

int lastMethodCalled = -1; // 1=StartQuery, 2=EndQuery, 3=MatchDocument, 4=InitializeIndex, 5=DestroyIndex
std::vector<Document*> documents;

pthread_mutex_t mutex_doc_results = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond_doc_results = PTHREAD_COND_INITIALIZER;

TrieNode *trie_exact;
TrieNode **trie_hamming;
TrieNode **trie_edit;

QuerySet *querySet; // std::vector<QuerySetNode*>
DocResults *docResults; // std::list<DocResultsNode>

// THREADPOOL
lp_threadpool* threadpool;


/********************************************************************************************
 *  SPARSE ARRAY STRUCTURE
 *************************************/

SparseArrayNode* SparseArray_Constructor(){
	SparseArrayNode* sa = (SparseArrayNode*)malloc(sizeof(SparseArrayNode));
	sa->low = 0;
	sa->high = sa->low + SPARSE_ARRAY_NODE_DATA-1;
	memset( sa->data, 0, SPARSE_ARRAY_NODE_DATA*MAX_QUERY_WORDS );
	sa->next = 0;
	return sa;
}
void SparseArray_Destructor(SparseArrayNode* n){
	for( SparseArrayNode* prev; n; n=prev ){
		prev = n->next;
		free( n );
	}
}

void SparseArraySet( SparseArrayNode* sa, unsigned int index, char pos ){
	SparseArrayNode* prev=sa;
	for( ; sa && sa->high < index ; sa=sa->next ){
		prev = sa;
	}
    // the case where we finished the array without results
	// OR
	// we must create a node before this one because this is a block for bigger ids
	if( sa == 0 || sa->low > index ){
		sa = SparseArray_Constructor();
		sa->next = prev->next;
        prev->next = sa;
        sa->low = ( (index / SPARSE_ARRAY_NODE_DATA)*SPARSE_ARRAY_NODE_DATA );
        sa->high = sa->low + SPARSE_ARRAY_NODE_DATA-1;
	}

	// sa holds the block where we need to insert the value
	sa->data[ index - sa->low ][pos] = 1;
	//fprintf( stderr, "qid[%u] pos[%d] sa_index[%u] sa_low[%u] sa_high[%u]\n", index, pos, index - sa->low, sa->low, sa->high );
}

unsigned int* SparseArrayCompress(SparseArrayNode* array, unsigned int * total){
	unsigned int nids = querySet->size();
	unsigned int *final_ids = (unsigned int*)malloc( sizeof(unsigned int)*nids );
	unsigned int total_found=0, row;
	char pos=0;
	for( ; array ; array = array->next ){
		for( int i=0; i<SPARSE_ARRAY_NODE_DATA; i++ ){
			row = array->low + i;
			pos = array->data[ i ][0] + array->data[ i ][1] + array->data[ i ][2] + array->data[ i ][3] + array->data[ i ][4];
			if( row < nids && row > 0 && pos == querySet->at(row)->words_num ){
				//fprintf( stderr, "row[%u]\n",  row );
                final_ids[ total_found++ ] = row;
			}
		}
	}
	*total = total_found;
	return final_ids;
}


/********************************************************************************************
 *  END SPARSE ARRAY STRUCTURE
 *************************************/



// it is assumed that this document finished processing
void* FinishingJob( int tid, void* args ){
    //DocID *doc_id = (DocID*)args;
	DocumentHandlerNode* doch = ((DocumentHandlerNode*)args);

    //fprintf( stderr, "Finishing[%u]\n", doch->doc_id );

    Document* doc = documents[doch->doc_id];
    //char *doc_results = doc->qids;

//    unsigned int total_results;
//            unsigned int nids = querySet->size();
//            unsigned int *final_ids = (unsigned int*)malloc( sizeof(unsigned int)*nids );
//            memset( final_ids, 0, sizeof(unsigned int)*nids );
//            for( unsigned int i=0; i<DOC_RESULTS_SIZE; i++ ){
//            	//fprintf( stderr, "\n%d ", i );
//            	if( doc_results[i] == 1 ){
//            		doc_results[i] = 0;
//                    ++final_ids[i/5];
//            	}
//            }
//            for( unsigned int i=1; i<nids; i++ ){
//            	if( final_ids[ i ] == querySet->at(i)->words_num ){
//            		final_ids[total_results++] = i;
//            	}
//            }

            //DocumentDestructor(doc);

            unsigned int total_results;
            unsigned int *final_ids = SparseArrayCompress(doc->query_ids, &total_results);

            DocResultsNode docr;
            docr.docid=doch->doc_id;
            docr.sz=total_results;
            docr.qids=final_ids;

            pthread_mutex_lock( &mutex_doc_results );
            docResults->push_back(docr);
            //fprintf( stderr, "finished doc[%d] doc[%d]\n", docr.docid, docResults->back().docid );
            pthread_mutex_unlock( &mutex_doc_results );
            pthread_cond_signal( &cond_doc_results );


            //free( doc_id );

	return 0;
}

void* TrieSearchWord( int tid, void* args ){
	TrieSearchData *tsd = (TrieSearchData *)args;
	//fprintf( stderr, "words_num[%d] words[%p] words_sz[%p]\n", tsd->words_num, tsd->words, tsd->words_sz );
	const char* w;
	char wsz;
	Document *doc = documents[tsd->doc_id];

	//char *dres = doc->qids;
	for( int i=0, j=tsd->words_num; i<j; i++ ){
		wsz = tsd->words_sz[i];
		w = tsd->words[i];
		TrieExactSearchWord(  tid, trie_exact, w, wsz, doc );
		TrieHammingSearchWord( tid, trie_hamming[0], w, wsz, doc, 0 );
		TrieHammingSearchWord( tid, trie_hamming[1], w, wsz, doc, 1 );
		TrieHammingSearchWord( tid, trie_hamming[2], w, wsz, doc, 2 );
		TrieHammingSearchWord( tid, trie_hamming[3], w, wsz, doc, 3 );
		TrieEditSearchWord( tid, trie_edit[0], w, wsz, doc, 0 );
		TrieEditSearchWord( tid, trie_edit[1], w, wsz, doc, 1 );
		TrieEditSearchWord( tid, trie_edit[2], w, wsz, doc, 2 );
		TrieEditSearchWord( tid, trie_edit[3], w, wsz, doc, 3 );
	}

	// check if all jobs are finished
	pthread_mutex_lock( &doc->mutex_finished_jobs );
	    doc->finished_jobs = doc->finished_jobs + 1;
		if( doc->total_jobs == doc->finished_jobs ){
			doc->total_jobs = -1;
			//fprintf( stderr, "[%d]", *(&tsd->doc_id) );
			lp_threadpool_addjob(threadpool,reinterpret_cast<void* (*)(int, void*)>(FinishingJob), &documents[tsd->doc_id]->doc_id );
		}
	pthread_mutex_unlock( &doc->mutex_finished_jobs );

	free( tsd );

	return 0;
}



void* DocumentHandler( int tid, void* args ){
	DocumentHandlerNode* doch = ((DocumentHandlerNode*)args);
    Document *document = documents[doch->doc_id];


//    fprintf( stderr, "handling doc[%d] [%d]\n", doch->doc_id, documents[doch->doc_id]->doc_id );

    unsigned int total_jobs = 0;
	unsigned int batch_words=0;
	    //int total_words=0;
		TrieSearchData *tsd = NULL;
		const char *start, *end;
		char sz;
	    for( start=document->doc; *start; start = end ){
		    // FOR EACH WORD DO THE MATCHING

	    	while( *start == ' ' ) start++;
	    	end = start;
	    	while( *end >= 'a' && *end <= 'z' ) end++;
	    	sz = end-start;


	    	// skip word if found before in the document
	    	if( TrieVisitedIS( document->visited, start, sz ) ){
	    		//fprintf( stderr, "doc[%d] word[%.*s] existed\n", doc_id, sz, start );
	    	   	continue;
	    	}

	    	//total_words++;

	        batch_words++;
	    	if( batch_words == 1 ){
	    		//document->current_trie_search_data = (document->current_trie_search_data + 1) % MAX_TRIE_SEARCH_DATA;
	    		//tsd = &document->trie_search_data_pool[document->current_trie_search_data];
	    		tsd = (TrieSearchData*)malloc(sizeof(TrieSearchData));
	    		tsd->doc_id = document->doc_id;
	    		//fprintf( stderr, "handling doc[%d] [%d] [%d]\n", doch->doc_id, documents[doch->doc_id]->doc_id, tsd->doc_id );
	    		++total_jobs;
	    	}

	    	tsd->words[batch_words-1] = start;
	    	tsd->words_sz[batch_words-1] = sz;

	    	if( batch_words == WORDS_PROCESSED_BY_THREAD ){
	    		tsd->words_num = batch_words;
	    		batch_words = 0;
	    		lp_threadpool_addjob( threadpool, reinterpret_cast<void* (*)(int, void*)>(TrieSearchWord), tsd );
	    		//fprintf( stderr, "handling doc[%d] [%d] [%d]\n", doch->doc_id, documents[doch->doc_id]->doc_id, tsd->doc_id );
	    		//fprintf(stderr, "doc_id[%d] total_jobs_submitted[%d]\n", document->doc_id, total_doc_jobs_submitted );
	    	}

	    	// move on to the next word
	    }

	    // check if there are words unhandled at the moment because of premature exit of the loop
	    // at the point where we check for previous existence

	if (batch_words > 0) {
		tsd->words_num = batch_words;
		batch_words = 0;
		lp_threadpool_addjob(threadpool,reinterpret_cast<void* (*)(int, void*)>(TrieSearchWord), tsd );
		//fprintf( stderr, "handling doc[%d] [%d] [%d]\n", doch->doc_id, documents[doch->doc_id]->doc_id, tsd->doc_id );
		//fprintf(stderr, "doc_id[%d] total_jobs_submitted[%d]\n", document->doc_id, total_doc_jobs_submitted );
	}

	pthread_mutex_lock( &document->mutex_finished_jobs );
	document->total_jobs = total_jobs;
	if( document->total_jobs == document->finished_jobs ){
				document->total_jobs = -1;
				//fprintf( stderr, "[%d]", doch->doc_id );
				lp_threadpool_addjob(threadpool,reinterpret_cast<void* (*)(int, void*)>(FinishingJob), doch );
			}
	pthread_mutex_unlock( &document->mutex_finished_jobs );


	return 0;
}






/********************************************************************************************
 *  GLOBALS END
 ********************************************************************************************/





///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode InitializeIndex(){

	lastMethodCalled = 4;

	//fprintf( stderr, "\n\ninitialized index called\n\n" );

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

    documents.push_back((Document*)malloc(sizeof(Document))); // add dummy doc

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

	//fprintf( stderr, "\n\ndestroy index called\n\n" );

	synchronize_complete(threadpool);

	if( lastMethodCalled == 3 ){

			lastMethodCalled = 5;
		}

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
	// if the last call was match document then wait for the jobs to finish
	if( lastMethodCalled == 3 ){
		synchronize_complete(threadpool);
		lastMethodCalled = 1;
	}

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

	// if the last call was match document then wait for the jobs to finish
		if( lastMethodCalled == 3 ){
			synchronize_complete(threadpool);
			lastMethodCalled = 2;
		}

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

//////////////////////////////////////////////////////////////////////////////////////////////

// TODO -
// TODO - Check for the same words in the same document
// TODO - Check the cache for words in previous documents too and get the results without running the algorithms again
ErrorCode MatchDocument(DocID doc_id, const char* doc_str){

	lastMethodCalled = 3;

	Document* doc = DocumentConstructor();
	doc->doc_id = doc_id;
	documents.push_back( doc );
	strcpy( doc->doc, doc_str );

	DocumentHandlerNode* dn = (DocumentHandlerNode*)malloc(sizeof(DocumentHandlerNode));
	dn->doc_id = doc->doc_id;
	lp_threadpool_addjob(threadpool,reinterpret_cast<void* (*)(int, void*)>(DocumentHandler), dn );

	//fprintf( stderr, "DocumentHandler[%d] added\n", dn->doc_id );

	///////////////////////////////////////////////
    //int s = gettime();

        //lp_threadpool_synchronize_master( threadpool );
        //TrieNodeVisited_Destructor( visited );

        //fprintf( stderr, "doc[%u] miliseconds: %d\n", doc_id, gettime()-s );

        //int s = gettime();

//
//            for( batch_words=0; batch_words<NUM_THREADS; batch_words++ ){
//            	lp_threadpool_addjob( threadpool, reinterpret_cast<void* (*)(int,void*)>(lp_mergesort_parallel), (void*)bars);
//            }
//            lp_mergesort_parallel( 0, bars );

        //fprintf( stderr, "doc[%u] results: %lu miliseconds: %d\n", doc_id, results_all->qids->size(), gettime()-s );

        // FIND THE UNIQUE IDS AND CHECK IF THE QueryIDS are valid as per their words

/*
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




	return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode GetNextAvailRes(DocID* p_doc_id, unsigned int* p_num_res, QueryID** p_query_ids)
{

	DocResultsNode dr;
	pthread_mutex_lock( &mutex_doc_results );
	while( docResults->size() < 1 ){
		pthread_cond_wait( &cond_doc_results, &mutex_doc_results );
	}
	dr = docResults->back();
	docResults->pop_back();
	pthread_mutex_unlock( &mutex_doc_results );

	//fprintf( stderr, "getNextResult tid[%d]\n", dr.docid );

    // get the docResult from the back of the list if any and return it
	*p_doc_id=0; *p_num_res=0; *p_query_ids=0;
	//if(docResults->empty()) return EC_NO_AVAIL_RES;
	*p_doc_id=dr.docid; *p_num_res=dr.sz; *p_query_ids=dr.qids;

	//fprintf( stderr, "getNextResult tid[%d]\n", dr.docid );

	return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////

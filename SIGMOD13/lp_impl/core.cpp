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
#define NUM_THREADS 24
#define TOTAL_WORKERS NUM_THREADS+1

#define WORDS_PROCESSED_BY_THREAD 100
#define SPARSE_ARRAY_NODE_DATA 4096 //13107 // 2^16 / 5 in order to fit in cache block 64K

#define VALID_CHARS 26

/***********************************************************
 * STRUCTURES
 ***********************************************************/

struct QuerySetNode{
	MatchType type;
	unsigned int match_dist;
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

struct QueryNode{
	QueryID qid;
	char pos;
};

typedef std::vector<QueryNode> QueryArrayList;

struct ResultTrieSearch{
	char padding[128];
	QueryArrayList *qids;
};

struct TrieNode{
   TrieNode* children[VALID_CHARS];
   QueryArrayList *qids[4];
};

struct TrieSearchData{
	const char* words[WORDS_PROCESSED_BY_THREAD];
	char words_sz[WORDS_PROCESSED_BY_THREAD];
	short words_num;
	char padding[128];
	DocID doc_id;
};


struct TrieNodeVisited{
   TrieNodeVisited* children[VALID_CHARS];
   char exists;
};

struct SparseArrayNode{
	SparseArrayNode* next;
	SparseArrayNode* prev;
	char data[SPARSE_ARRAY_NODE_DATA][MAX_QUERY_WORDS];
	unsigned int low;
	unsigned int high;
};
struct SparseArray{
	SparseArrayNode* head;
	SparseArrayNode* tail;
	unsigned int num_nodes;
	unsigned int mid_high;
};

struct Document{
	char *doc; // might be faster if not fixed size
	//char doc[MAX_DOC_LENGTH];
	DocID doc_id;
	int total_jobs;
	int finished_jobs;
	TrieNodeVisited *visited; // only the matching job
	pthread_mutex_t mutex_finished_jobs;
	SparseArray* query_ids;
	pthread_mutex_t mutex_query_ids;
};

struct DocumentHandlerNode{
	DocID doc_id;
};

struct lp_tpjob{
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

   char headsTime;

}lp_threadpool;

struct HammingNode{
	TrieNode* node;
	char letter;
	char depth;
	char tcost;
};

struct EditNode{
	TrieNode* node;
	char previous[MAX_WORD_LENGTH+1];
	char letter;
};

/***********************************************************************
 * GLOBALS
 ***********************************************************************/

int lastMethodCalled = -1; // 1=StartQuery, 2=EndQuery, 3=MatchDocument, 4=InitializeIndex, 5=DestroyIndex

lp_threadpool* threadpool;

std::vector<Document*> documents;
QuerySet *querySet; // std::vector<QuerySetNode*>

TrieNode *trie_exact;
TrieNode **trie_hamming;
TrieNode **trie_edit;

pthread_mutex_t mutex_cache = PTHREAD_MUTEX_INITIALIZER;

DocResults *docResults; // std::list<DocResultsNode>
pthread_mutex_t mutex_doc_results = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond_doc_results = PTHREAD_COND_INITIALIZER;

/**********************************************************************
 * FUNCTION PROTOTYPES
 *********************************************************************/
void lp_threadpool_destroy(lp_threadpool* pool);
lp_threadpool* lp_threadpool_init( int threads );
void lp_threadpool_addjob( lp_threadpool* pool, void *(*func)(int, void *), void* args );
lp_tpjob* lp_threadpool_fetchjob( lp_threadpool* pool );
int lp_threadpool_uniquetid( lp_threadpool* pool );
void* lp_tpworker_thread( void* _pool );
void synchronize_threads_master(int tid, void * arg);
void lp_threadpool_synchronize_master(lp_threadpool* pool);
void synchronize_complete(lp_threadpool* pool);

SparseArrayNode* SparseArrayNode_Constructor();
SparseArray* SparseArray_Constructor();
void SparseArray_Destructor(SparseArray* n);
void SparseArraySet( SparseArray* sa, unsigned int index, char pos );
unsigned int* SparseArrayCompress(SparseArray* array, unsigned int * total);

TrieNode* TrieNode_Constructor();
void TrieNode_Destructor( TrieNode* node );
TrieNode* TrieInsert( TrieNode* node, const char* word, char word_sz, QueryID qid, char word_pos, unsigned int match_distance );
void TrieRemove( TrieNode* node, QueryID qid, char pos, char match_distance );
void TrieExactSearchWord( TrieNode* root, const char* word, char word_sz, Document*doc );
void TrieHammingSearchWord( TrieNode* node, const char* word, int word_sz, Document*doc );
void TrieEditSearchWord( TrieNode* node, const char* word, int word_sz, Document*doc );

//void CacheInsertResults( TrieNode* cache, const char* word, char word_sz, QueryArrayList* qids );
//QueryArrayList* TrieFind( TrieNode* node, const char* word, char word_sz );


TrieNodeVisited* TrieNodeVisited_Constructor();
void TrieNodeVisited_Destructor( TrieNodeVisited* node );
char TrieVisitedIS( TrieNodeVisited* node, const char* word, char word_sz );
void TrieVisitedClear(TrieNodeVisited* node);

Document* DocumentConstructor();
void DocumentDestructor( Document *doc );
void DocumentDeallocate(Document *doc);

// worker_threads functions
void* FinishingJob( int tid, void* args );
void* TrieSearchWord( int tid, void* args );
void* DocumentHandler( int tid, void* args );

///////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////
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


    free( documents[0] );
    for( unsigned int i=1,sz=documents.size(); i<sz; i++ )
    	DocumentDestructor( documents[i] );

    // destroy the thread pool

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
	qnode->match_dist = match_dist;
	qnode->words = (void**)malloc(sizeof(TrieNode*)*MAX_QUERY_WORDS);
    qnode->words_num = 0;

    TrieNode**t=0, *n = 0;
	const char *start, *end;
	for( start=query_str; *start; start = end ){
		while( *start == ' ' ) start++;
		end = start;
		while( *end >= 'a' && *end <= 'z' ) end++;
		switch( match_type ){
		case MT_EXACT_MATCH:
		   n = TrieInsert( trie_exact , start, end-start, query_id, qnode->words_num, 0 );
		   break;
		case MT_HAMMING_DIST:
			n = TrieInsert( trie_hamming[0] , start, end-start, query_id, qnode->words_num, match_dist );
		   break;
		case MT_EDIT_DIST:
			n = TrieInsert( trie_edit[0] , start, end-start, query_id, qnode->words_num, match_dist );
		   break;
		}// end of match_type

//		if( match_type != MT_EXACT_MATCH ){
//			switch (match_dist) {
//			case 0:
//				n = TrieInsert(t[0], start, end - start, query_id, qnode->words_num);
//				break;
//			case 1:
//				n = TrieInsert(t[1], start, end - start, query_id, qnode->words_num);
//				break;
//			case 2:
//				n = TrieInsert(t[2], start, end - start, query_id, qnode->words_num);
//				break;
//			case 3:
//				n = TrieInsert(t[3], start, end - start, query_id, qnode->words_num);
//				break;
//			}// end of match_dist
//		}

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
		TrieRemove( (TrieNode*)n->words[i], query_id, i, n->match_dist );
		// void TrieRemove( TrieNode* node, QueryID qid, char pos, char match_distance )
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


































/*
 * FUNCTION IMPLEMENTATIONS
 */


// THREADPOOL STRUCTURE

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
    	 if( pool->headsTime == 1 ){
             pool->jobs_tail->next = njob;
             pool->jobs_tail = njob;
    	 }else{
    		 njob->next = pool->jobs_head;
    		 pool->jobs_head = njob;
    	 }
    	 pool->headsTime *= -1;
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
int lp_threadpool_uniquetid( lp_threadpool* pool ){
	// returns an id from 1 to number of threads (eg. threads=12, ids = 1,2,3,4,5,6,7,8,9,10,11,12)
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

    pool->headsTime = 0;

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

// TRIE FUNCTIONS

TrieNode* TrieNode_Constructor(){
   TrieNode* n = (TrieNode*)malloc(sizeof(TrieNode));
   if( !n ) err_mem("error allocating TrieNode");
   n->qids[0] = n->qids[1] = n->qids[2] = n->qids[3] = 0;
   memset( n->children, 0, VALID_CHARS*sizeof(TrieNode*) );
   return n;
}
void TrieNode_Destructor( TrieNode* node ){
    for( char i=0; i<VALID_CHARS; i++ ){
    	if( node->children[i] != 0 ){
            TrieNode_Destructor( node->children[i] );
    	}
    }
    if( node->qids[0] ) delete node->qids[0];
    if( node->qids[1] ) delete node->qids[1];
    if( node->qids[2] ) delete node->qids[2];
    if( node->qids[3] ) delete node->qids[3];
    free( node );
}
TrieNode* TrieInsert( TrieNode* node, const char* word, char word_sz, QueryID qid, char word_pos, unsigned int match_distance ){
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
   if( !node->qids[match_distance] ){
       node->qids[match_distance] = new QueryArrayList();
   }
   QueryNode qn;
   qn.qid = qid;
   qn.pos = word_pos;
   node->qids[match_distance]->push_back(qn);
   return node;
}
void TrieRemove( TrieNode* node, QueryID qid, char pos, char match_distance ){
    QueryArrayList* qids = node->qids[match_distance];
    for( QueryArrayList::iterator it=qids->begin(), end=qids->end(); it != end; it++  ){
        if( it->qid == qid && it->pos == pos ){
    	   	qids->erase(it);
    		break;
   	    }
    }
}
void TrieExactSearchWord( TrieNode* root, const char* word, char word_sz, Document*doc ){
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
   if( found && root->qids[0] ){
       // WE HAVE A MATCH SO get the List of the query ids and add them to the result
	   pthread_mutex_lock( &doc->mutex_query_ids );
	   for (QueryArrayList::iterator it = root->qids[0]->begin(), end = root->qids[0]->end(); it != end; it++) {
		   SparseArraySet( doc->query_ids, it->qid, it->pos );
		}
	   pthread_mutex_unlock( &doc->mutex_query_ids );
    }
}
void TrieHammingSearchWord( TrieNode* node, const char* word, char word_sz, Document*doc ){
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
		if (current.tcost <= 3) { // 3= maximum match_distance being checked
			if (word_sz == current.depth ) {
				// ADD THE node->qids[] INTO THE RESULTS
				QueryArrayList* qids;
				pthread_mutex_lock( &doc->mutex_query_ids );
				for( char dist=current.tcost; dist<=3; dist++ ){
					qids = current.node->qids[dist];
					if( qids != 0 ){
						for (QueryArrayList::iterator it = qids->begin(),end = qids->end(); it != end; it++) {
							SparseArraySet( doc->query_ids, it->qid, it->pos );
						}
					}
				}
				pthread_mutex_unlock( &doc->mutex_query_ids );
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
void TrieEditSearchWord( TrieNode* node, const char* word, int word_sz, Document*doc ){
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
        if( current[word_sz] <= 3 ){ // 3 = maximum distance cost
            // ADD THE node->qids[] INTO THE RESULTS
        	QueryArrayList* qids;
        	pthread_mutex_lock( &doc->mutex_query_ids );
        		for( char dist=current[word_sz]; dist<=3; dist++ ){
        			qids = c.node->qids[dist];
        			if( qids != 0 ){
        	      		for (QueryArrayList::iterator it = qids->begin(),end = qids->end(); it != end; it++) {
        					SparseArraySet( doc->query_ids, it->qid, it->pos );
        				}
        			}
        		}
        	pthread_mutex_unlock( &doc->mutex_query_ids );
        }

        // if there are more changes available recurse
		for (i = 0; i <= word_sz; i++) {
			if (current[i] <= 3) { // maximum match_distance cost =3
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

// TRIE VISITED STRUCTURE END

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
char TrieVisitedIS( TrieNodeVisited* node, const char* word, char word_sz ){
	// Returns 0 if new word added or 1 if existed
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

//  DOCUMENT STRUCTURE

Document* DocumentConstructor(){
    Document* doc = (Document*)malloc(sizeof(Document));
    doc->doc=(char*)malloc( MAX_DOC_LENGTH );
    doc->doc_id=0;
    doc->finished_jobs = 0;
    doc->total_jobs = MAX_DOC_LENGTH;
    doc->visited = TrieNodeVisited_Constructor();
    pthread_mutex_init( &doc->mutex_finished_jobs, NULL );
    pthread_mutex_init( &doc->mutex_query_ids, NULL );
    doc->query_ids = SparseArray_Constructor();
    return doc;
}
void DocumentDestructor( Document *doc ){
	pthread_mutex_destroy( &doc->mutex_finished_jobs );
	pthread_mutex_destroy( &doc->mutex_query_ids );
	free( doc );
}
void DocumentDeallocate(Document *doc){
	free( doc->doc );
    TrieNodeVisited_Destructor( doc->visited );
	SparseArray_Destructor( doc->query_ids );
}

// SPARSE ARRAY STRUCTURE

SparseArrayNode* SparseArrayNode_Constructor(){
	SparseArrayNode* n = (SparseArrayNode*)malloc(sizeof(SparseArrayNode));
		n->low = 0;
		n->high = n->low + SPARSE_ARRAY_NODE_DATA-1;
		memset( n->data, 0, SPARSE_ARRAY_NODE_DATA*MAX_QUERY_WORDS );
		n->next = n->prev = 0;
		return n;
}
SparseArray* SparseArray_Constructor(){
	SparseArray *array = (SparseArray*)malloc(sizeof(SparseArray));
	array->tail = array->head = SparseArrayNode_Constructor();
	array->num_nodes = 1;
	array->mid_high = array->head->high;
	return array;
}
void SparseArray_Destructor(SparseArray* n){
	for( SparseArrayNode* prev; n->head; n->head=prev ){
		prev = n->head->next;
		free( n->head );
	}
	free( n );
}
void SparseArraySet( SparseArray* sa, unsigned int index, char pos ){
	SparseArrayNode* prev, *cnode;

	if( index < sa->mid_high ){
	    // START FROM THE HEAD AND SEARCH FORWARD

		for( cnode=sa->head; cnode && cnode->high < index ; cnode=cnode->next ){
			prev = cnode;
		}
		// the case where we finished the array without results
		// OR
		// we must create a node before this one because this is a block for bigger ids
		if( cnode == 0 || cnode->low > index ){
			cnode = SparseArrayNode_Constructor();
			cnode->next = prev->next;
			cnode->prev = prev;
			prev->next = cnode;
			if( cnode->next != 0 ){
				cnode->next->prev = cnode;
			}
			cnode->low = ( (index / SPARSE_ARRAY_NODE_DATA)*SPARSE_ARRAY_NODE_DATA );
			cnode->high = cnode->low + SPARSE_ARRAY_NODE_DATA-1;
			sa->num_nodes++;
			if( sa->num_nodes % 2 == 1 ){
				// we must increase the mid_high
				unsigned int i=0;
				for( prev=sa->head; i < (sa->num_nodes/2) ; i++ ){
					prev = prev->next;
				}
				sa->mid_high = prev->high;
			}
		}
	}else{
		// START FROM THE TAIL AND SEARCH BACKWARD
		for( cnode=sa->tail; cnode->low > index ; cnode=cnode->prev ){
			// move back
		}
		// the case where we stopped at a node with HIGH less than index and we must create a new node cause LOW is also less than index
		// OR
		// the case where we stopped at the right node
		if( cnode->high < index ){
			prev = cnode;
			cnode = SparseArrayNode_Constructor();
			if( prev == sa->tail ){
				sa->tail = cnode;
			}
			cnode->next = prev->next;
			cnode->prev = prev;
			prev->next = cnode;
			if (cnode->next != 0) {
				cnode->next->prev = cnode;
			}
			cnode->low = ((index / SPARSE_ARRAY_NODE_DATA)* SPARSE_ARRAY_NODE_DATA);
			cnode->high = cnode->low + SPARSE_ARRAY_NODE_DATA - 1;
			sa->num_nodes++;
			if (sa->num_nodes % 2 == 1) {
				// we must increase the mid_high
				unsigned int i = 0;
				for (prev = sa->head; i < (sa->num_nodes / 2); i++) {
					prev = prev->next;
				}
				sa->mid_high = prev->high;
			}

		}
	}


	// cnode holds the block where we need to insert the value
	cnode->data[ index - cnode->low ][pos] = 1;
	//fprintf( stderr, "qid[%u] pos[%d] sa_index[%u] sa_low[%u] sa_high[%u]\n", index, pos, index - sa->low, sa->low, sa->high );
}
unsigned int* SparseArrayCompress(SparseArray* array, unsigned int * total){
	unsigned int nids = querySet->size();
	unsigned int *final_ids = (unsigned int*)malloc( sizeof(unsigned int)*nids );
	unsigned int total_found=0, row;
	char pos=0;
	SparseArrayNode*cnode = array->head;
	for( ; cnode ; cnode = cnode->next ){
		for( int i=0; i<SPARSE_ARRAY_NODE_DATA; i++ ){
			row = cnode->low + i;
			pos = cnode->data[ i ][0] + cnode->data[ i ][1] + cnode->data[ i ][2] + cnode->data[ i ][3] + cnode->data[ i ][4];
			if( row < nids && row > 0 && pos == querySet->at(row)->words_num ){
				//fprintf( stderr, "row[%u]\n",  row );
                final_ids[ total_found++ ] = row;
			}
		}
	}
	*total = total_found;
	return final_ids;
}

// worker_thread functions

void* FinishingJob( int tid, void* args ){
	// it is assumed that this document finished processing
	DocumentHandlerNode* doch = ((DocumentHandlerNode*)args);

    //fprintf( stderr, "Finishing[%u]\n", doch->doc_id );

    Document* doc = documents[doch->doc_id];


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


            DocumentDeallocate(doc);

	return 0;
}
bool compareQueryNodes( const QueryNode &a, const QueryNode &b){
	if( a.qid < b.qid  )
		return true;
	if( a.qid > b.qid )
		return false;
	return a.pos <= b.pos;
}
void* TrieSearchWord( int tid, void* args ){
	TrieSearchData *tsd = (TrieSearchData *)args;
	//fprintf( stderr, "words_num[%d] words[%p] words_sz[%p]\n", tsd->words_num, tsd->words, tsd->words_sz );
	const char* w;
	char wsz;
	Document *doc = documents[tsd->doc_id];
	for( int i=0, j=tsd->words_num; i<j; i++ ){
		wsz = tsd->words_sz[i];
		w = tsd->words[i];

        	TrieExactSearchWord( trie_exact, w, wsz, doc );
			TrieHammingSearchWord( trie_hamming[0], w, wsz, doc );
			//TrieHammingSearchWord( trie_hamming[1], w, wsz, doc );
			//TrieHammingSearchWord( trie_hamming[2], w, wsz, doc );
			//TrieHammingSearchWord( trie_hamming[3], w, wsz, doc );
			TrieEditSearchWord( trie_edit[0], w, wsz, doc );
			//TrieEditSearchWord( trie_edit[1], w, wsz, doc );
			//TrieEditSearchWord( trie_edit[2], w, wsz, doc );
			//TrieEditSearchWord( trie_edit[3], w, wsz, doc );

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


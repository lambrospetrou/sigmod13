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

#include <vector>
#include <unordered_map>
#include <string>

//////////////////////////////////////////
#define NUM_THREADS 400
#define TOTAL_WORKERS NUM_THREADS+1

#define WORDS_PROCESSED_BY_THREAD 150
#define SPARSE_ARRAY_NODE_DATA 9000

#define VALID_CHARS 26

#define MAX( a, b ) ( ((a) >= (b))?(a):(b) )
#define MIN( a, b ) ( ((a) <= (b))?(a):(b) )
#define MIN3(a, b, c) ((a) < (b) ? ((a) < (c) ? (a) : (c)) : ((b) < (c) ? (b) : (c)))

/***********************************************************
 * STRUCTURES
 ***********************************************************/


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

struct TrieNode{
   TrieNode* children[VALID_CHARS];
   QueryArrayList *qids;
   pthread_mutex_t mutex_node;
   char wsz;
   unsigned int income_pos;
};

struct TrieSet{
	TrieNode* tries[9]; // 0=exact 1-4=hamming 5-8=edit
};

struct QueryDB{
	TrieSet tries[MAX_WORD_LENGTH+1];
};

typedef std::vector<TrieNode*> QueryNodesList;
struct TrieNodeIndex{
   TrieNodeIndex* children[VALID_CHARS];
   QueryNodesList *query_nodes;
   pthread_mutex_t mutex_node;
   unsigned int income_index[MAX_WORD_LENGTH+1][9];
};
struct IndexDB{
	TrieNodeIndex *tries[MAX_WORD_LENGTH+1];
};

struct QuerySetNode{
	MatchType type;
	TrieNode**words;
	char words_num;
};
typedef std::vector<QuerySetNode*> QuerySet;

struct IncomeQuery{
	TrieNode* query_node;
	char word[MAX_WORD_LENGTH];
	char wsz;
	MatchType match_type;
	char match_dist;
};
struct IncomeQueryDB{
	std::vector<IncomeQuery> queries[MAX_WORD_LENGTH+1][9]; // 0= exact, 1-4= hamming 0,1,2,3, 4-8= edit 0,1,2,3
};

struct TrieSearchData{
	char* words[WORDS_PROCESSED_BY_THREAD];
	char words_sz[WORDS_PROCESSED_BY_THREAD];
	short words_num;
	DocID doc_id;
};

struct TrieNodeVisited{
   TrieNodeVisited* children[VALID_CHARS];
   char exists;
};

struct SparseArrayNode{
	SparseArrayNode* next;
	SparseArrayNode* prev;
	char data[SPARSE_ARRAY_NODE_DATA][MAX_QUERY_WORDS+1];
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

struct StartQueryNode{
	unsigned int query_id;
	char start[MAX_QUERY_LENGTH+1];
	MatchType match_type;
	char match_dist;
};


/***********************************************************************
 * GLOBALS
 ***********************************************************************/

int lastMethodCalled = -1; // 1=StartQuery, 2=EndQuery, 3=MatchDocument, 4=InitializeIndex, 5=DestroyIndex

lp_threadpool* threadpool;

std::vector<Document*> documents;
//pthread_mutex_t mutex_query_set = PTHREAD_MUTEX_INITIALIZER;
QuerySet *querySet; // std::vector<QuerySetNode*>

QueryDB db_query;
IndexDB db_index;
IncomeQueryDB db_income;
//pthread_mutex_t mutex_dbincome = PTHREAD_MUTEX_INITIALIZER;

DocResults *docResults;
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
TrieNode* TrieInsert( TrieNode* node, const char* word, char word_sz, QueryID qid, char word_pos );
TrieNode* TrieCreateEmptyIfNotExists( TrieNode* node, const char* word, char word_sz );
QueryArrayList* TrieFind( TrieNode* node, const char* word, char word_sz );
TrieNode* TrieFindID( TrieNode* root, const char* word, char word_sz, unsigned int query_id, char pos );
void TrieExactSearchWord( TrieNodeIndex* created_index_node, TrieNode* node, const char* word, char word_sz, Document*doc );
void TrieHammingSearchWord( TrieNodeIndex* created_index_node, TrieNode* node, const char* word, int word_sz, Document*doc, char maxCost );
void TrieEditSearchWord( TrieNodeIndex* created_index_node, TrieNode* node, const char* word, int word_sz, Document*doc, char maxCost );

 TrieNodeIndex* TrieNodeIndex_Constructor();
void TrieNodeIndex_Destructor( TrieNodeIndex* node );
TrieNodeIndex* TrieIndexCreateEmptyIfNotExists( TrieNodeIndex* node, const char* word, char word_sz );

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

	for( int i=0; i<MAX_WORD_LENGTH+1; i++ ){
		for( int j=0; j<9; j++ )
		    db_query.tries[i].tries[j] = TrieNode_Constructor();
	}

	for( int i=0; i<MAX_WORD_LENGTH+1; i++ ){
	    db_index.tries[i] = TrieNodeIndex_Constructor();
	}

    documents.push_back((Document*)malloc(sizeof(Document))); // add dummy doc

    querySet = new QuerySet();
    querySet->resize( 16000 );
    querySet->clear();
    // add dummy query to start from index 1 because query ids start from 1 instead of 0
    querySet->push_back((QuerySetNode*)malloc(sizeof(QuerySetNode)));
    docResults = new DocResults();
    docResults->resize(16000);
    docResults->clear();

    for( int i=0; i<MAX_WORD_LENGTH+1; i++ ){
		db_income.queries[i][0].resize(1000);
		db_income.queries[i][0].clear();
		db_income.queries[i][2].resize(1000);
		db_income.queries[i][2].clear();
		db_income.queries[i][3].resize(1000);
		db_income.queries[i][3].clear();
		db_income.queries[i][4].resize(1000);
		db_income.queries[i][4].clear();
		db_income.queries[i][6].resize(1000);
		db_income.queries[i][6].clear();
		db_income.queries[i][7].resize(1000);
		db_income.queries[i][7].clear();
		db_income.queries[i][8].resize(1000);
		db_income.queries[i][8].clear();
    }

    // Initialize the threadpool
    threadpool = lp_threadpool_init( NUM_THREADS );


	return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode DestroyIndex(){

	//fprintf( stderr, "\n\ndestroy index called\n\n" );

	synchronize_complete(threadpool);

	for( int i=0; i<MAX_WORD_LENGTH+1; i++ ){
		for( int j=0; j<9; j++ )
		    TrieNode_Destructor( db_query.tries[i].tries[j] );
	}

	for( int i=0; i<MAX_WORD_LENGTH+1; i++ ){
	    TrieNodeIndex_Destructor( db_index.tries[i] );
	}

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

	   int i,j;

		QuerySetNode* qnode = (QuerySetNode*)malloc(sizeof(QuerySetNode));
		qnode->type = match_type;
		qnode->words = (TrieNode**)malloc(sizeof(TrieNode*)*MAX_QUERY_WORDS);
	    qnode->words_num = 0;

	    int trie_index=0;
		switch( match_type ){
		case MT_EXACT_MATCH:
			trie_index = 0;
		   break;
		case MT_HAMMING_DIST:
			trie_index = 1;
		   break;
		case MT_EDIT_DIST:
			trie_index = 5;
		   break;
		}// end of match_type
		if( match_type != MT_EXACT_MATCH ){
			switch (match_dist) {
			case 0:
				trie_index += 0;
				break;
			case 1:
				trie_index += 1;
				break;
			case 2:
				trie_index += 2;
				break;
			case 3:
				trie_index += 3;
				break;
			}// end of match_dist
		}

		char qwords[MAX_QUERY_WORDS][MAX_WORD_LENGTH+1];
		qwords[0][0] = qwords[1][0] = qwords[2][0] = qwords[3][0] = qwords[4][0] = '\0';

	    int wsz;
	    char found;
		const char *start, *end;
		IncomeQuery income;
		for( start=query_str; *start; start = end ){
			while( *start == ' ' ) start++;
			end = start;
			while( *end >= 'a' && *end <= 'z' ) end++;
			wsz = end - start;
			// check if the word appeared before
	        found = 0;
	        for( i=0; i<MAX_QUERY_WORDS; i++ ){
	        	for( j=0; j<wsz; j++ ){
	        		if( qwords[i][j]!=start[j] ){
	        			break;
	        		}
	        	}
	        	if( j==wsz && qwords[i][j]=='\0' ){
	        		found = 1;
	        		break;
	        	}
	        }
	        if( found ) continue;
	        else{
	        	for( i=0; i<wsz; i++ )
	        		qwords[qnode->words_num][i] = start[i];
	        	qwords[qnode->words_num][wsz] = '\0';
	        }

	        ///////////////////////////////////////////////////
	        // START PROCESSING NEW WORD FOR QUERY
	        ///////////////////////////////////////////////////

	        int current_word_index = qnode->words_num;

	        // insert the query word inside the QueryDB
	        TrieNode *querydb_node = TrieInsert(db_query.tries[wsz].tries[trie_index], start, wsz, query_id, current_word_index);
	        qnode->words[current_word_index] = querydb_node;
	        unsigned int qids_sz = querydb_node->qids->size();

	        if( qids_sz <= 1 ){
	    		// JUST ADD THE NEW TRIENODE INSIDE THE INCOME_QUERIES

	    		// possible optimization if instead of holding IncomeQuery structures, to hold pointer to structures
	    		// to avoid copying the whole structure each time

	    		income.match_dist = match_dist;
	    		income.match_type = match_type;
	    		income.query_node = querydb_node;
	    		income.wsz = wsz;
	    		for( i=0; i<wsz; i++ ){
	    			income.word[i] = start[i];
	    		}

	    		// lock it when it will be parallel
	    		db_income.queries[wsz][trie_index].push_back( income );
	    		querydb_node->wsz = wsz;
	    		querydb_node->income_pos = db_income.queries[wsz][trie_index].size()-1; // we inform the querydb_node where the word is located inside the IncomeQueries
	    	}
	        ///////////////////////////////////////////////////
	        // end PROCESSING NEW WORD FOR QUERY
	        ///////////////////////////////////////////////////

			qnode->words_num++;

		}// end for each word

	    querySet->push_back(qnode); // add the new query in the query set - still unfinished though


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
	for( char i=0,sz=n->words_num; i<sz; i++ ){
		for( QueryArrayList::iterator it=n->words[i]->qids->begin(), end=n->words[i]->qids->end(); it != end; it++  ){
		    if( it->qid == query_id && it->pos == i ){
		    	n->words[i]->qids->erase(it);
		    	break;
		    }
		}
	}

	return EC_SUCCESS;
}

//////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode MatchDocument(DocID doc_id, const char* doc_str){
	if( lastMethodCalled != 3 ){
		synchronize_complete(threadpool);
		lastMethodCalled = 3;
	}

	Document* doc = DocumentConstructor();
	doc->doc_id = doc_id;
	documents.push_back( doc );
	strcpy( doc->doc, doc_str );

	DocumentHandlerNode* dn = (DocumentHandlerNode*)malloc(sizeof(DocumentHandlerNode));
	dn->doc_id = doc->doc_id;
	lp_threadpool_addjob(threadpool,reinterpret_cast<void* (*)(int, void*)>(DocumentHandler), dn );

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

    // get the docResult from the back of the list if any and return it
	*p_doc_id=0; *p_num_res=0; *p_query_ids=0;
	*p_doc_id=dr.docid; *p_num_res=dr.sz; *p_query_ids=dr.qids;

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
//    	 if( pool->headsTime == 1 ){
//             pool->jobs_tail->next = njob;
//             pool->jobs_tail = njob;
//    	 }else{
//    		 njob->next = pool->jobs_head;
//    		 pool->jobs_head = njob;
//    	 }
//    	 pool->headsTime *= -1;
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
   n->qids = 0;
   memset( n->children, 0, VALID_CHARS*sizeof(TrieNode*) );
   pthread_mutex_init( &n->mutex_node, NULL );
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
    pthread_mutex_destroy( &node->mutex_node );
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
       node->qids->resize( 128 );
       node->qids->clear();
   }
   QueryNode qn;
   qn.qid = qid;
   qn.pos = word_pos;
   node->qids->push_back(qn);
   return node;
}
TrieNode* TrieCreateEmptyIfNotExists( TrieNode* node, const char* word, char word_sz ){
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
   return node;
}
QueryArrayList* TrieFind( TrieNode* root, const char* word, char word_sz ){
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
		   return root->qids;
	    }
	   return 0;
}
TrieNode* TrieFindID( TrieNode* root, const char* word, char word_sz, unsigned int query_id, char pos ){
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
		   for( QueryArrayList::iterator f=root->qids->begin(), ff=root->qids->end(); f!=ff; f++ ){
			   if( f->qid == query_id && f->pos == pos )
			       return root;
		   }
	    }
	   return 0;
}
void TrieExactSearchWord( TrieNodeIndex* created_index_node,TrieNode* root, const char* word, char word_sz, Document*doc ){
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
	   pthread_mutex_lock( &doc->mutex_query_ids );
	   created_index_node->query_nodes->push_back(root);
	   for (QueryArrayList::iterator it = root->qids->begin(), end = root->qids->end(); it != end; it++) {
		   SparseArraySet( doc->query_ids, it->qid, it->pos );
		}
	   pthread_mutex_unlock( &doc->mutex_query_ids );
    }
}
void TrieHammingSearchWord( TrieNodeIndex* created_index_node, TrieNode* node, const char* word, int word_sz, Document*doc, char maxCost ){
	//fprintf( stderr, "[2] [%p] [%p] [%.*s] [%d] [%p]\n", lockmech, node, word_sz, word, maxCost, results );
    QueryArrayList localqids;
	HammingNode current, n;
	std::vector<HammingNode> hamming_stack;
	unsigned int stack_size=0;
	char j;

	// add the initial nodes
	for( j=0; j<VALID_CHARS; j++ ){
	   if( node->children[j] != 0 ){
		   current.depth = 1;
		   current.node = node->children[j];
		   current.letter = 'a' + j;
		   current.tcost = 0;
		   hamming_stack.push_back(current);
		   stack_size++;
	   }
    }

	//while ( !hamming_stack.empty()) {
	while( stack_size > 0 ){
		current = hamming_stack.back();
		hamming_stack.pop_back();
		stack_size--;

		if (current.letter != word[current.depth - 1]) {
			current.tcost++;
		}
		if (current.tcost <= maxCost) {
			if (word_sz == current.depth && current.node->qids != 0) {
				// ADD THE node->qids[] INTO THE RESULTS
				   created_index_node->query_nodes->push_back(current.node);
				   for (QueryArrayList::iterator it = current.node->qids->begin(), end = current.node->qids->end(); it != end; it++) {
					   localqids.push_back( *it );
					}
			} else if (word_sz > current.depth) {
				for (j = 0; j < VALID_CHARS; j++) {
					if (current.node->children[j] != 0) {
						n.depth = current.depth + 1;
						n.node = current.node->children[j];
						n.letter = 'a' + j;
						n.tcost = current.tcost;
						hamming_stack.push_back(n);
						stack_size++;
					}
				}
			}
		}
	}

	   pthread_mutex_lock( &doc->mutex_query_ids );
	   for (QueryArrayList::iterator it = localqids.begin(), end = localqids.end(); it != end; it++) {
		   SparseArraySet( doc->query_ids, it->qid, it->pos );
		}
	   pthread_mutex_unlock( &doc->mutex_query_ids );

}
void TrieEditSearchWord( TrieNodeIndex* created_index_node, TrieNode* node, const char* word, int word_sz, Document*doc, char maxCost ){
	//fprintf( stderr, "[3] [%p] [%p] [%.*s] [%d] [%p]\n", lockmech, node, word_sz, word, maxCost, results );
    QueryArrayList localqids;
    EditNode c, n;
    char current[MAX_WORD_LENGTH+1];
    std::vector<EditNode> edit_stack;
    unsigned int stack_size=0;
    char i, insertCost, deleteCost, replaceCost, j, k;

	for (i = 0; i < VALID_CHARS; ++i) {
		if (node->children[i] != 0) {
			c.letter = 'a' + i;
			c.node = node->children[i];
			for( j=0; j<=word_sz; j++ )
			    c.previous[j] = j;
			edit_stack.push_back(c);
			stack_size++;
		}
	}
    char cost;
	//while( !edit_stack.empty() ){
	while( stack_size > 0 ){
		c = edit_stack.back();
		edit_stack.pop_back();
        stack_size--;

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
			   created_index_node->query_nodes->push_back(c.node);
			   for (QueryArrayList::iterator it = c.node->qids->begin(), end = c.node->qids->end(); it != end; it++) {
				   localqids.push_back( *it );
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
						stack_size++;
					}
				}
				break; // break because we only need one occurence of cost less than maxCost
			}// there is no possible match further
		}
	}
	   pthread_mutex_lock( &doc->mutex_query_ids );
	   for (QueryArrayList::iterator it = localqids.begin(), end = localqids.end(); it != end; it++) {
		   SparseArraySet( doc->query_ids, it->qid, it->pos );
		}
	   pthread_mutex_unlock( &doc->mutex_query_ids );
}

 TrieNodeIndex* TrieNodeIndex_Constructor(){
	   TrieNodeIndex* n = (TrieNodeIndex*)malloc(sizeof(TrieNodeIndex));
	   if( !n ) err_mem("error allocating TrieNodeIndex");
	   n->query_nodes = 0;
	   memset( n->children, 0, VALID_CHARS*sizeof(TrieNodeIndex*) );
	   pthread_mutex_init( &n->mutex_node, NULL );
	   memset( n->income_index, 0, (MAX_WORD_LENGTH+1) * sizeof(unsigned int) * 9 );
	   return n;
}
void TrieNodeIndex_Destructor( TrieNodeIndex* node ){
    for( char i=0; i<VALID_CHARS; i++ ){
    	if( node->children[i] != 0 ){
            TrieNodeIndex_Destructor( node->children[i] );
    	}
    }
    if( node->query_nodes )
        delete node->query_nodes;
    pthread_mutex_destroy( &node->mutex_node );
    free( node );
}
TrieNodeIndex* TrieIndexCreateEmptyIfNotExists( TrieNodeIndex* node, const char* word, char word_sz ){
   char ptr=0;
   char pos;
   while( ptr < word_sz ){
      pos = word[ptr] - 'a';
      if( node->children[pos] == 0 ){
         node->children[pos] = TrieNodeIndex_Constructor();
      }
      node = node->children[pos];
      ptr++;
   }
   return node;
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
	std::vector<TrieNodeVisited*> stack;
	unsigned int stack_size=1;
	stack.push_back( node );
	TrieNodeVisited* cnode;
	while( stack_size > 0 ){
		stack_size--;
		cnode = stack.back();
		stack.pop_back();
	    for( char i=0; i<VALID_CHARS; i++ ){
	    	if( cnode->children[i] != 0 ){
	            stack.push_back(cnode->children[i]);
	            stack_size++;
	    	}
	    }
	    free( cnode );
	}
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
		memset( n->data, 0, SPARSE_ARRAY_NODE_DATA*(MAX_QUERY_WORDS+1) );
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
	SparseArrayNode* prev=sa->head, *cnode;

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
			//if( sa->num_nodes % 2 == 1 ){
			if( (sa->num_nodes & 1) == 1 ){
				// we must increase the mid_high
				unsigned int i=0,sz=(sa->num_nodes >> 2);
				for( prev=sa->head; i < sz ; i++ ){
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
			//if (sa->num_nodes % 2 == 1) {
			if ((sa->num_nodes & 1) == 1) {
				// we must increase the mid_high
				unsigned int i = 0, sz=(sa->num_nodes >> 2);
				for (prev = sa->head; i < sz; i++) {
					prev = prev->next;
				}
				sa->mid_high = prev->high;
			}

		}
	}


	// cnode holds the block where we need to insert the value
	cnode->data[ index - cnode->low ][MAX_QUERY_WORDS] = 1;
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
			if( !cnode->data[ i ][MAX_QUERY_WORDS] )
				continue;
			row = cnode->low + i;
			pos = cnode->data[ i ][0] + cnode->data[ i ][1] + cnode->data[ i ][2] + cnode->data[ i ][3] + cnode->data[ i ][4];
			if( pos == querySet->at(row)->words_num ){
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
	char* w;
	char wsz;
	Document *doc = documents[tsd->doc_id];

	TrieNodeIndex *created_index_node;
	TrieNodeIndex *locked_nodes[WORDS_PROCESSED_BY_THREAD];
	unsigned int locked_nodes_num=0;

	unsigned int i,j,k,ksz;
	char x,y;
	unsigned int income_indexes[MAX_WORD_LENGTH+1][9];
	for( unsigned int i=0; i<MAX_WORD_LENGTH+1; i++ ){
		income_indexes[i][0] = db_income.queries[i][0].size();
		//income_indexes[i][1] = db_income.queries[i][1].size();
		income_indexes[i][2] = db_income.queries[i][2].size();
		income_indexes[i][3] = db_income.queries[i][3].size();
		income_indexes[i][4] = db_income.queries[i][4].size();
		//income_indexes[i][5] = db_income.queries[i][5].size();
		income_indexes[i][6] = db_income.queries[i][6].size();
		income_indexes[i][7] = db_income.queries[i][7].size();
		income_indexes[i][8] = db_income.queries[i][8].size();
	}


	for( i=0, j=tsd->words_num; i<j; i++ ){
		wsz = tsd->words_sz[i];
		w = tsd->words[i];

		//////////////////////////////////////////////
		// START THE WORD PROCESSING
		//////////////////////////////////////////////

		char update_index_node = 0;

        pthread_mutex_lock( &db_index.tries[wsz]->mutex_node );
        created_index_node = TrieIndexCreateEmptyIfNotExists( db_index.tries[wsz], w, wsz );
        QueryNodesList *query_nodes;// = created_index_node->query_nodes;
        pthread_mutex_unlock( &db_index.tries[wsz]->mutex_node );


        if( pthread_mutex_trylock( &created_index_node->mutex_node ) != 0 ){
        	locked_nodes[locked_nodes_num++] = created_index_node;
        	continue;
        }


        //pthread_mutex_lock( &created_index_node->mutex_node );

        if( (query_nodes= created_index_node->query_nodes) == 0 ){

            // WE DID NOT FOUND THE WORD INSIDE THE INDEX SO WE MUST MAKE THE CALCULATIONS AND INSERT IT
        		//fprintf(stderr, "searching first time for word[ %.*s ]\n", wsz, w);

                created_index_node->query_nodes = new QueryNodesList();
                created_index_node->query_nodes->resize( 128 );
                created_index_node->query_nodes->clear();

				// Search the QueryDB and find the matches for the current word
				TrieExactSearchWord( created_index_node, db_query.tries[wsz].tries[0], w, wsz, doc );
				created_index_node->income_index[wsz][0] = income_indexes[wsz][0];
				TrieHammingSearchWord( created_index_node, db_query.tries[wsz].tries[2], w, wsz, doc, 1 );
				TrieHammingSearchWord( created_index_node, db_query.tries[wsz].tries[3], w, wsz, doc, 2 );
				TrieHammingSearchWord( created_index_node, db_query.tries[wsz].tries[4], w, wsz, doc, 3 );
				created_index_node->income_index[wsz][1] = income_indexes[wsz][1];
				created_index_node->income_index[wsz][2] = income_indexes[wsz][2];
				created_index_node->income_index[wsz][3] = income_indexes[wsz][3];
				for( int low_sz=wsz-3, high_sz=wsz+3; low_sz<=high_sz; low_sz++   ){
					if( low_sz < MIN_WORD_LENGTH || low_sz > MAX_WORD_LENGTH )
						continue;
					TrieEditSearchWord( created_index_node, db_query.tries[low_sz].tries[6], w, wsz, doc, 1 );
					TrieEditSearchWord( created_index_node, db_query.tries[low_sz].tries[7], w, wsz, doc, 2 );
					TrieEditSearchWord( created_index_node, db_query.tries[low_sz].tries[8], w, wsz, doc, 3 );
					// mark the indexes as fully checked
					created_index_node->income_index[low_sz][6] = income_indexes[low_sz][6];
					created_index_node->income_index[low_sz][7] = income_indexes[low_sz][7];
					created_index_node->income_index[low_sz][8] = income_indexes[low_sz][8];
				}

        }else{

        	std::unordered_map<std::pair<std::string,std::string>,int>::const_iterator it;

			// TODO - update index
			// WE MUST CHECK EVERY INCOME LIST - ONLY THE VALID ONES AS PER WSZ -
			// AND IF NECESSARY MAKE THE CALCULATIONS FOR INCOME QUERIES SINCE THE LAST TIME
			// THIS WORD WAS PROCESSED. exact, hamming and edit matching
        	char valid;
        	//char matrix[MAX_WORD_LENGTH+1][MAX_WORD_LENGTH+1];
        	char *previous, *current;
        	char _previous[MAX_WORD_LENGTH+1];
        	char _current[MAX_WORD_LENGTH+1];
        	for( int low_sz=wsz-3, high_sz=wsz+3; low_sz<=high_sz; low_sz++ ){
        		if( low_sz < MIN_WORD_LENGTH || low_sz > MAX_WORD_LENGTH )
        			continue;
        		for( int dist_index=6; dist_index<9; dist_index++ ){
					// check if we need to update the index word for queries with low_sz size words
						for( k=created_index_node->income_index[low_sz][dist_index], ksz=income_indexes[low_sz][dist_index]; k<ksz; k++ ){
							valid = 1;
							IncomeQuery *iq = &db_income.queries[low_sz][dist_index].at(k);

								// normal calculation with only 2*M size
	//                            char *t;
	//                            previous = _previous;
	//                            current = _current;
	//        					previous[0] = 0;
	//        					for( y=1; y<=iq->wsz; y++ )
	//        						previous[y] = y;
	//        					for (x = 1; x <= wsz; x++) {
	//								current[0] = x;
	//								for (y = 1; y <= iq->wsz; y++) {
	//									if (w[x - 1] == iq->word[y - 1]) {
	//										current[y] = previous[y - 1];
	//									} else {
	//										current[y] = MIN3(current[y-1] , previous[y], previous[y-1] ) + 1;
	//									}
	//								}
	//                                t = previous;
	//                                previous = current;
	//                                current = t;
	//							}
	//        					if( previous[iq->wsz] <= iq->match_dist ){
	//        						created_index_node->query_nodes->push_back(iq->query_node);
	//        					}


								// normal calculation with a matrix N*M
	//        					matrix[0][0] = 0;
	//        					for( y=1; y<=iq->wsz; y++ )
	//        						matrix[0][y] = y;
	//        					for (x = 1; x <= wsz; x++) {
	//								matrix[x][0] = x;
	//								for (y = 1; y <= iq->wsz; y++) {
	//									if (w[x - 1] == iq->word[y - 1]) {
	//										matrix[x][y] = matrix[x-1][y - 1];
	//									} else {
	//										matrix[x][y] = MIN3(matrix[x][y-1] , matrix[x-1][y], matrix[x-1][y-1] ) + 1;
	//									}
	//								}
	//							}
	//        					if( matrix[x-1][iq->wsz] <= iq->match_dist ){
	//        						created_index_node->query_nodes->push_back(iq->query_node);
	//        					}

								// OPTIMIZED EDIT DISTANCE FOR DIAGONAL CALCULATION ONLY
	//        					char left, right;
	//        					matrix[0][0] = 0;
	//        					for( y=1; y<=iq->wsz; y++ ){
	//        						matrix[0][y] = y;
	//        					}
	//        					for( x=1; x<=wsz; x++ ){
	//        						matrix[x][0] = x;
	//        					}
	//        					// for each letter of the document word
	//        					for( x=1; x<= wsz; x++ ){
	//        						left = MAX( 1, x-iq->match_dist );
	//        						right = MIN( iq->wsz, x+iq->match_dist + 1 );
	//        						if( right < iq->wsz )
	//        							matrix[x][right+1] = 50;
	//        						if( left > 1 ){
	//        							matrix[x][left-1] = 50;
	//        						}
	//        						for( y=left; y<=right; y++ ){
	//        							if( w[x-1] == iq->word[y-1] ){
	//        								matrix[x][y] = matrix[x-1][y-1];
	//        							}else{
	//        								matrix[x][y] = MIN3( matrix[x-1][y-1], matrix[x-1][y], matrix[x][y-1] ) + 1;
	//        							}
	//        						}
	//        					}
	//        			        if( matrix[ x-1 ][ y-1 ] <= iq->match_dist ){
	//        					   	created_index_node->query_nodes->push_back(iq->query_node);
	//        					}

								// OPTIMIZED DIAGONAL with 2 rows


								char left, right, *t;
								previous = _previous;
								current = _current;
								previous[0] = 0;
								for( y=1; y<=iq->wsz; y++ ){
									previous[y] = y;
									current[y] = 50;
								}
								// for each letter of the document word
								for( x=1; x<= wsz; x++ ){
									left = MAX( 1, x-iq->match_dist );
									right = MIN( iq->wsz, x+iq->match_dist + 1 );
									if( left > 1 )
										current[left-1] = 50;
									else
										current[0] = x;
									for( y=left; y<=right; y++ ){
										if( w[x-1] == iq->word[y-1] ){
											current[y] = previous[y-1];
										}else{
											current[y] = MIN3( previous[y-1], previous[y], current[y-1] ) + 1;
										}
									}
									t = previous;
									previous = current;
									current = t;
								}
								if( previous[ y-1 ] <= iq->match_dist ){
									created_index_node->query_nodes->push_back(iq->query_node);
								}

						}
					// set the state of this income_queries as completely checked
        	        created_index_node->income_index[low_sz][dist_index] = income_indexes[low_sz][dist_index];
        		}// end of this distance edit
        	}// end for each low_sz income

        	// check exact
        		for( k=created_index_node->income_index[wsz][0], ksz=income_indexes[wsz][0]; k<ksz; k++ ){
        		    //valid = 1;
        			IncomeQuery *iq = &db_income.queries[wsz][0].at(k);
        			if( iq->wsz != wsz ) continue; // next word
					for (x = 0; x < wsz; x++) {
						if ((iq->word[x] ^ w[x])) {
							break;
						}
					}
					if ( x == wsz ) {
						//fprintf( stderr, "inserted![%.*s] doc_word[%.*s]\n", iq.wsz, iq.word, wsz, w );
						created_index_node->query_nodes->push_back(iq->query_node);
					}
        		}
        		created_index_node->income_index[wsz][0] = income_indexes[wsz][0];

        	// check hamming
        	for( unsigned int dist_index=2; dist_index<5; dist_index++ ){
					for (k = created_index_node->income_index[wsz][dist_index], ksz =income_indexes[wsz][dist_index]; k < ksz; k++) {
						IncomeQuery *iq = &db_income.queries[wsz][dist_index].at(k);
						if( iq->wsz != wsz ) continue; // next word
						char cost=0;
						for( x=0; x<wsz;x++ ){
							if( (iq->word[x] ^ w[x]) ){
								cost++;
								if( cost > iq->match_dist ){
									break;
								}
							}
						}
						if( x == wsz ){
							created_index_node->query_nodes->push_back(iq->query_node);
						}
					}
					created_index_node->income_index[wsz][dist_index] = income_indexes[wsz][dist_index];
        	}

        	// FILL DOCUMENT with QUERY IDS matching this word
			pthread_mutex_lock(&doc->mutex_query_ids);
			for( QueryNodesList::iterator it=created_index_node->query_nodes->begin(), end=created_index_node->query_nodes->end(); it!=end; it++ ){
				for( QueryArrayList::iterator qit=(*it)->qids->begin(), qend=(*it)->qids->end(); qit!=qend; qit++ ){
					SparseArraySet( doc->query_ids, qit->qid, qit->pos );
				}
			}
			pthread_mutex_unlock(&doc->mutex_query_ids);

        }// end if update_index

        pthread_mutex_unlock( &created_index_node->mutex_node );


	    //////////////////////////////////////////////
		// WORD PROCESSING END
	    /////////////////////////////////////////////

	}

	// get the results for the nodes that got updated by other threads
	// if they were locked previously it means that a thread in this MatchDocuments() batch
	// updated them with the new queries
	for( i=0; i<locked_nodes_num; i++ ){
		created_index_node = locked_nodes[i];
		pthread_mutex_lock( &created_index_node->mutex_node );
		pthread_mutex_lock(&doc->mutex_query_ids);
		for( QueryNodesList::iterator it=created_index_node->query_nodes->begin(), end=created_index_node->query_nodes->end(); it!=end; it++ ){
			for( QueryArrayList::iterator qit=(*it)->qids->begin(), qend=(*it)->qids->end(); qit!=qend; qit++ ){
				SparseArraySet( doc->query_ids, qit->qid, qit->pos );
			}
		}
		pthread_mutex_unlock(&doc->mutex_query_ids);
		pthread_mutex_unlock( &created_index_node->mutex_node );
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
		char *start, *end;
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
	    		++total_jobs;
	    	}

	    	tsd->words[batch_words-1] = start;
	    	tsd->words_sz[batch_words-1] = sz;

	    	if( batch_words == WORDS_PROCESSED_BY_THREAD ){
	    		tsd->words_num = batch_words;
	    		batch_words = 0;
	    		lp_threadpool_addjob( threadpool, reinterpret_cast<void* (*)(int, void*)>(TrieSearchWord), tsd );
	    	}

	    	// move on to the next word
	    }

	    // check if there are words unhandled at the moment because of premature exit of the loop
	    // at the point where we check for previous existence

	if (batch_words > 0) {
		tsd->words_num = batch_words;
		batch_words = 0;
		lp_threadpool_addjob(threadpool,reinterpret_cast<void* (*)(int, void*)>(TrieSearchWord), tsd );
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

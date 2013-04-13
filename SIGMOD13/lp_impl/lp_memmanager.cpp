#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>

#include <vector>

#define NUM_THREADS 18
#define TOTAL_WORKERS NUM_THREADS+1

#define WORDS_PROCESSED_BY_THREAD 30
#define SPARSE_ARRAY_NODE_DATA 7000

#define VALID_CHARS 26
#define MAX_WORD_LENGTH 31
#define MAX_QUERY_WORDS 5


struct QueryNode{
	unsigned int qid;
	char pos;
};
typedef std::vector<QueryNode> QueryArrayList;

struct TrieNode{
	char wsz;
	unsigned int income_pos;
	QueryArrayList *qids;
	TrieNode* children[VALID_CHARS];
	pthread_mutex_t mutex_node;
};

typedef std::vector<TrieNode*> QueryNodesList;
struct TrieNodeIndex{
	TrieNodeIndex* children[VALID_CHARS];
	QueryNodesList *query_nodes;
	pthread_rwlock_t lock_node;
	unsigned int income_index[MAX_WORD_LENGTH+1][9];
};


struct QuerySetNode{
	int type;
	TrieNode**words;
	char words_num;
};


struct TrieSearchData{
	char* words[WORDS_PROCESSED_BY_THREAD];
	char words_sz[WORDS_PROCESSED_BY_THREAD];
	short words_num;
	unsigned int doc_id;
};

struct TrieNodeVisited{
	TrieNodeVisited* children[VALID_CHARS];
	char exists;
};

struct SparseArrayNode{
	SparseArrayNode* next;
	SparseArrayNode* prev;
	unsigned int low;
	unsigned int high;
	char data[SPARSE_ARRAY_NODE_DATA][MAX_QUERY_WORDS+1];
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
	unsigned int doc_id;
	int total_jobs;
	int finished_jobs;
	TrieNodeVisited *visited; // only the matching job
	pthread_mutex_t mutex_finished_jobs;
	SparseArray* query_ids;
	pthread_mutex_t mutex_query_ids;
};

struct DocumentHandlerNode{
	unsigned int doc_id;
};


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

struct lp_tpjob{
	char dummy;
	void *args;
	void *(*func)(int, void *);
	lp_tpjob *next;
};

int gettime()
{
	struct timeval t2; gettimeofday(&t2,NULL);
	return t2.tv_sec*1000+t2.tv_usec/1000;
}

#define ALLOCATIONS 99999999



//////////////////////////////////////////////////////////////////////////
// MEMMANAGER
//////////////////////////////////////////////////////////////////////////

struct raw_memnode{
	raw_memnode* next;
	char* data;
};

void* AllocateRawMemBlock( raw_memnode *raw_head, unsigned int size ){
	char *n = ( char* )malloc( size );
	raw_memnode *rn = (raw_memnode*)malloc( sizeof( raw_memnode ) );
	rn->data = n;
	rn->next = raw_head->next;
	raw_head->next = rn;
	return n;
}

void DeallocateRawMem( raw_memnode *raw_head ){
	raw_memnode *n, *t;
	for( n=raw_head->next; n; n=t ){
		t = n->next;
		free( n->data );
		free( n );
	}
	//free( raw_head );
}

#define TYPE_SIZE_TPJOB 24
#define TYPE_ID_TPJOB 1
#define POOL_SIZE_TPJOB 10000

#define TYPE_SIZE_TRIENODE 264
#define TYPE_ID_TRIENODE 2
#define POOL_SIZE_TRIENODE 50000

struct memblock_tpjob{
	char type_id;
	lp_tpjob tpjob;
	char isfree;
	int block_size;
	memblock_tpjob *next;
};

struct memblock_trienode{
	char type_id;
	TrieNode trienode;
	char isfree;
	int block_size;
	memblock_trienode *next;
};

struct st_mempool{

	// define the raw_memory container
	raw_memnode raw_head;

	// define the head nodes for the available blocks for each type
	memblock_tpjob *head_tpjob;
	memblock_trienode *head_trienode;
};

st_mempool* st_mempool_init(){
	st_mempool *stp = (st_mempool*)malloc( sizeof( st_mempool ) );
	stp->raw_head.next = 0;
	stp->head_tpjob = 0;
	stp->head_trienode = 0;
	return stp;
}

void st_mempool_destroy( st_mempool *mempool ){
	DeallocateRawMem( &mempool->raw_head );
}

void st_mempool_initblock( st_mempool *mempool, void* base, char type_id ){
	switch( type_id ){
	case TYPE_ID_TPJOB:
	{
		memblock_tpjob *nblock, *prev = mempool->head_tpjob;
		for( unsigned int i=0; i<POOL_SIZE_TPJOB; i++ ){
			//nblock = &(static_cast<memblock_tpjob*>(base)[i*(sizeof(memblock_tpjob))]);
			nblock = (memblock_tpjob*)((char*)base + i*sizeof(memblock_tpjob));
			nblock->block_size = TYPE_SIZE_TPJOB;
			nblock->isfree = 1;
			nblock->next = prev;
			nblock->type_id = TYPE_ID_TPJOB;
			prev = nblock;
		}
		mempool->head_tpjob = prev;
		break;
	}
	case TYPE_ID_TRIENODE:
	{
		memblock_trienode *nblock, *prev = mempool->head_trienode;
		for( unsigned int i=0; i<POOL_SIZE_TRIENODE; i++ ){
			//nblock = &(static_cast<memblock_tpjob*>(base)[i*(sizeof(memblock_tpjob))]);
			nblock = (memblock_trienode*)((char*)base + i*sizeof(memblock_trienode));
			nblock->block_size = TYPE_SIZE_TRIENODE;
			nblock->isfree = 1;
			nblock->next = prev;
			nblock->type_id = TYPE_ID_TRIENODE;
			prev = nblock;
		}
		mempool->head_trienode = prev;
		break;
	}
	}// end of switch type
}

void* st_mempool_alloc( st_mempool* mempool, unsigned int size ){
	switch( size ){
	case TYPE_SIZE_TPJOB:
	{
		if( mempool->head_tpjob == 0 ){
			void * nbase = AllocateRawMemBlock( &mempool->raw_head, sizeof(memblock_tpjob) * POOL_SIZE_TPJOB );
			st_mempool_initblock( mempool, nbase, TYPE_ID_TPJOB );
		}
		memblock_tpjob *base = mempool->head_tpjob;
		mempool->head_tpjob = base->next; // to get the next free block pointer
		base->isfree = 0; // not free anymore
		return &base->tpjob;
		break;
	}
	case TYPE_SIZE_TRIENODE:
	{
		if( mempool->head_trienode == 0 ){
			void * nbase = AllocateRawMemBlock( &mempool->raw_head, sizeof(memblock_trienode) * POOL_SIZE_TRIENODE );
			st_mempool_initblock( mempool, nbase, TYPE_ID_TRIENODE );
		}
		memblock_trienode *base = mempool->head_trienode;
		mempool->head_trienode = base->next; // to get the next free block pointer
		base->isfree = 0; // not free anymore
		return &base->trienode;
		break;
	}
	}// END OF SWITCH
	return 0;
}

void st_mempool_free( st_mempool *mempool, void *ptr ){
	char *base = (char*)ptr;
	--base; // move to the start of the object
	switch( *base ){
	case TYPE_ID_TPJOB:
	{
		memblock_tpjob *tpjob = (memblock_tpjob*)base;
		tpjob->isfree = 1;
		tpjob->next = mempool->head_tpjob;
		mempool->head_tpjob = tpjob;
		break;
	}
	case TYPE_ID_TRIENODE:
	{
		memblock_trienode *trienode = (memblock_trienode*)base;
		trienode->isfree = 1;
		trienode->next = mempool->head_trienode;
		mempool->head_trienode = trienode;
		break;
	}
	}// end of switch
}


//////////////////////////////////////////////////////////////////////////




int main(){
//	fprintf( stdout, "EditNode[%lu]\n", sizeof(EditNode) );
//	fprintf( stdout, "HammingNode[%lu]\n", sizeof(HammingNode) );
//	fprintf( stdout, "DocumentHandlerNode[%lu]\n", sizeof(DocumentHandlerNode) );
//	fprintf( stdout, "Document[%lu]\n", sizeof(Document) );
//	fprintf( stdout, "SparseArray[%lu]\n", sizeof(SparseArray) );
//	fprintf( stdout, "SpaseArrayNode[%lu]\n", sizeof(SparseArrayNode) );
//	fprintf( stdout, "TrieNodeVisited[%lu]\n", sizeof(TrieNodeVisited) );
	fprintf( stdout, "TrieSearchData[%lu]\n", sizeof(TrieSearchData) );
//	fprintf( stdout, "QuerySetNode[%lu]\n", sizeof(QuerySetNode) );
//	fprintf( stdout, "TrieNodeIndex[%lu]\n", sizeof(TrieNodeIndex) );
//	fprintf( stdout, "TrieNode[%lu]\n", sizeof(TrieNode) );
//	fprintf( stdout, "lp_tpjob[%lu]\n", sizeof(lp_tpjob) );
//	fprintf( stdout, "void*[%lu]\n", sizeof(void*) );



//	st_mempool *mempool = st_mempool_init();
//
//	std::vector<lp_tpjob *> tp_job;
//	tp_job.reserve(ALLOCATIONS);
//	start = gettime();
//	for( unsigned int i=0; i<ALLOCATIONS; i++ ){
//	    tp_job.push_back( (lp_tpjob*)st_mempool_alloc( mempool, sizeof(lp_tpjob) ) );
//	}
//	for( unsigned int i=0; i<ALLOCATIONS; i++ ){
//	    st_mempool_free( mempool, tp_job[i] );
//	}
//	fprintf( stdout, "my time[ %llu ]\n", gettime()-start );
//
//	std::vector<lp_tpjob *> tp_job_c;
//	tp_job.reserve(ALLOCATIONS);
//	start = gettime();
//	for( unsigned int i=0; i<ALLOCATIONS; i++ ){
//	    tp_job_c.push_back( (lp_tpjob*)malloc( sizeof(lp_tpjob) ) );
//	}
//	for( unsigned int i=0; i<ALLOCATIONS; i++ ){
//	    free( tp_job_c[i] );
//	}
//	fprintf( stdout, "c time[ %llu ]\n", gettime()-start );

//	unsigned long long start = gettime();
//	for( unsigned int i=0; i<ALLOCATIONS; i++  ){
//
//		free( (TrieNode*)malloc(sizeof(TrieNode)) );
//
//	}
//    fprintf( stdout, "time[ %llu ]\n", gettime()-start );

	return 0;
}









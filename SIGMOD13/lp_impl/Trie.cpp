#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <errno.h>
#include <stdio.h>

#include <vector>
std::vector<char*> results;

int matches=0;

void err_mem(char* msg){
   perror(msg);
   exit(1);
}



#define VEC_TYPE unsigned int

typedef struct _Vector Vector;
struct _Vector{
	VEC_TYPE *data;
	unsigned long long size;
	unsigned long long maxSize;
};

Vector* Vector_Constructor(){
	Vector* v = (Vector*)malloc(sizeof(Vector));
	if(v == NULL){ err_mem("error constructing vector"); }
	v->size = 0;
	v->maxSize = 128;
	v->data = (VEC_TYPE*)malloc(sizeof(VEC_TYPE)*v->maxSize);
	if(v->data == NULL){ err_mem("error allocating vector data"); }
	return v;
}

VEC_TYPE* Vector_get(Vector* v, unsigned long long i){
	if( i<0 || i>=v->size ){return NULL;}
	return &v->data[i];
}

VEC_TYPE* Vector_push_back(Vector* v, VEC_TYPE val ){
	if( v->size == v->maxSize ){
		// reallocate
		v->maxSize *= 2;
		VEC_TYPE* td = (VEC_TYPE*)malloc(v->maxSize * sizeof(VEC_TYPE));
		if( !td ) err_mem("error expanding vector size");
		memcpy( td, v->data, v->size * sizeof(VEC_TYPE) );
		free( v->data );
		v->data = td;
	}
	v->data[v->size] = val;
	++v->size;
	return &v->data[v->size-1];
}

void Vector_Destructor(Vector *v){
	free(v->data);
	free(v);
}

unsigned long long Vector_size(Vector* v){
	return v->size;
}











typedef struct _ResultTrieSearch ResultTrieSearch;
struct _ResultTrieSearch{
	unsigned int size;
	Vector qids;
};

#define VALID_CHARS 26
typedef struct _TrieNode TrieNode;
struct _TrieNode{
   char *word;
   TrieNode** children;
};

TrieNode* TrieNode_Constructor(){
   TrieNode* n = (TrieNode*)malloc(sizeof(TrieNode));
   if( !n ) err_mem("error allocating TrieNode");
   memset( n, 0, sizeof(TrieNode) );
   n->children = (TrieNode**)malloc(VALID_CHARS*sizeof(TrieNode*));
   if( !n->children ) err_mem("error allocating children()");
   memset( n->children, 0, VALID_CHARS*sizeof(TrieNode*) );
   return n;
}

void TrieInsert( TrieNode* node, char* word ){
   char* ptr=word;
   int pos;
   while( *ptr ){
      pos = *ptr - 'a';
      if( node->children[pos] == 0 ){
         node->children[pos] = TrieNode_Constructor();
      }
      node = node->children[pos];
      ptr++;
   }
   node->word = strdup(word);
}


void TrieSearchWord_Recursive(TrieNode* node, char letter, char* word, int word_sz, char*previousRow, /* results set, */ int maxCost ){
   char* currentRow = (char*)malloc(word_sz+1);
   if( !currentRow ){
      err_mem( "error allocating current row" );
   }

   //memcpy( currentRow, previousRow, word_sz+1 );
   currentRow[0] = previousRow[0] + 1;

   char i, insertCost, deleteCost, replaceCost, j;
   for( i=1; i<=word_sz; i++ ){
      insertCost = currentRow[i-1] + 1;
      deleteCost = previousRow[i] + 1;

      if( word[i-1] != letter ){
         replaceCost = previousRow[i-1] + 1;
      }else{
         replaceCost = previousRow[i-1];
      }
      // find the minimum for this column
      insertCost = insertCost < replaceCost ? insertCost : replaceCost;
      currentRow[i] = insertCost < deleteCost ? insertCost : deleteCost;
   }

   if( currentRow[word_sz] <= maxCost && node->word!=0 ){
	  //fprintf(stdout, "added word: %s for: %s cost: %d", node->word, word, currentRow[word_sz]);
      // ADD THE node->word INTO THE RESULTS
      //
      matches++;
      results.push_back(node->word);
   }

   // if there are more changes available recurse
   for( i=0; i<=word_sz; i++ )
      if( currentRow[i] <= maxCost ){
	 for( j=0; j<VALID_CHARS; j++ ){
            if( node->children[j] != 0 ){
               TrieSearchWord_Recursive(node->children[j], 'a'+j, word, word_sz, currentRow, /* results set, */ maxCost);
	    }
	 }
         break;
      }
}


void TrieSearchWord( TrieNode* root, char* word, int maxCost ){
   // declare results
   //
   char*p;
   int sz;
   for( sz=0,p=word; *p; sz++, p++ );
   char *currentRow = (char*)malloc(sz+1);
   if( !currentRow ){
      err_mem( "error allocating current row" );
   }
   // create the current row // 0,1,2,3,4,,,sz
   char i;
   for( i=0; i<=sz; i++ ){
      currentRow[i]=i;
   }
   // for each children branch of the trie search the word
   for( i=0; i<VALID_CHARS; ++i ){
      if( root->children[i] != 0 ){
         TrieSearchWord_Recursive(
        		   root->children[i],
	               i +'a',
				   word,
				   sz,
				   currentRow,
				   /* results set, */
				   maxCost);
      }
   }
   // return results
}


unsigned long long getTime(){
   struct timeval tv;
   gettimeofday(&tv, NULL);
   return tv.tv_sec * 1000000 + tv.tv_usec;
}

int main(int argc, char** argv){
   
   unsigned long long start = getTime();

   // create the index
   TrieNode *root = TrieNode_Constructor();
   //FILE* dict = fopen("/usr/share/dict/words", "r");
   char word[128];
   while( fscanf( stdin , "%s", word) > 0 ){
      TrieInsert( root, word );
   }

   // for each argument keyword search the index
   for( argc-- ; argc>0; argc-- ){
	  TrieSearchWord( root, argv[argc], 3 );
      // should print results here
   }
   
   unsigned long long end = getTime();
   fprintf( stdout, "\nTotal time: %llu Matches: %d\n\n", end-start, matches );
   for( int i=0; i<results.size(); i++ )
      fprintf(stdout, "[%d] %s\n", i, results[i] );

   return 0;
}





























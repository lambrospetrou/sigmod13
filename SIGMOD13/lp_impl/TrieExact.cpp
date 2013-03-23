#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <errno.h>
#include <stdio.h>

#include <list>
#include <vector>

void err_mem(char* msg){
   perror(msg);
   exit(1);
}

typedef struct _ResultTrieSearch ResultTrieSearch;
struct _ResultTrieSearch{
	//unsigned int size;
	std::list<unsigned int> *qids;
};

#define VALID_CHARS 26
typedef struct _TrieNode TrieNode;
struct _TrieNode{
   char *word;
   TrieNode** children;
   std::list<unsigned int> *qids;
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

void TrieInsert( TrieNode* node, char* word, unsigned int qid ){
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
   if( !node->word )
       node->word = strdup(word);
   if( !node->qids ){
       node->qids = new std::list<unsigned int>();
   }
   node->qids->push_back(qid);
}

////////////////////////////////////////////////
// above are the same
////////////////////////////////////////////////

ResultTrieSearch* TrieExactSearchWord( TrieNode* root, char* word ){
   // declare results
   ResultTrieSearch *results = (ResultTrieSearch*)malloc(sizeof(ResultTrieSearch));
   //results.size=0;
   // initialize vector of ints
   char*p, i, found=1;
   for( p=word; *p; p++ ){
	   i = *p -'a';
	   if( root->children[i] ){
           root = root->children[i];
	   }else{
		   found=0;
		   break;
	   }
   }
   if( found && root->word ){
       // WE HAVE A MATCH SO get the List of the query ids and add them to the result
	   //results.size = root->qids->size();
	   results->qids = root->qids;
   }
   return results;
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
   unsigned int qid=1;
   char word[128];
   while( fscanf( stdin , "%s", word) > 0 ){
      TrieInsert( root, word, qid++ );
   }

   std::list<unsigned int> res;

   // for each argument keyword search the index
   for( argc-- ; argc>0; argc-- ){
	   ResultTrieSearch *rts = TrieExactSearchWord( root, argv[argc] );
       for( std::list<unsigned int>::iterator it=rts->qids->begin() ; it != rts->qids->end(); it++ ){
    	   res.push_back(*it);
       }
   }
   
   unsigned long long end = getTime();
   fprintf( stdout, "\nTotal time: %llu Matches: %lu\n\n", end-start, res.size() );
   /*
   for( int i=0; i<results.size(); i++ )
      fprintf(stdout, "[%d] %s\n", i, results[i] );
   */
   int i;
   std::list<unsigned int>::iterator it;
   for( i=0, it=res.begin(); it != res.end(); it++, i++ ){
	   fprintf(stdout, "[%d] %u\n", i, *it );
   }
   return 0;
}





























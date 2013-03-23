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
   //char *word;
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
   //if( !node->word )
     //  node->word = strdup(word);
   if( !node->qids ){
       node->qids = new std::list<unsigned int>();
   }
   node->qids->push_back(qid);
}

////////////////////////////////////////////////
// above are the same
////////////////////////////////////////////////

void TrieHammingSearchWord_Recursive(TrieNode* node, char letter, char* word, int word_sz, char*previousRow, ResultTrieSearch* results, char maxCost ){
   char* currentRow = (char*)malloc(word_sz+1);
   if( !currentRow ){
      err_mem( "error allocating current row" );
   }

   currentRow[0] = previousRow[0] + 1;

   char i, replaceCost, j;
   for( i=1; i<=word_sz; i++ ){
      if( word[i-1] != letter ){
         currentRow[i] = previousRow[i-1] + 1;
      }else{
         currentRow[i] = previousRow[i-1];
      }
   }

   if( currentRow[word_sz] <= maxCost && node->qids!=0 ){
       // ADD THE node->qids[] INTO THE RESULTS
	   for( std::list<unsigned int>::iterator it=node->qids->begin() ; it != node->qids->end(); it++ ){
	       results->qids->push_back(*it);
	   }
   }

   // if there are more changes available recurse
   for( i=0; i<=word_sz; i++ ){
      if( currentRow[i] <= maxCost ){
	      for( j=0; j<VALID_CHARS; j++ ){
             if( node->children[j] != 0 ){
                TrieHammingSearchWord_Recursive(node->children[j], 'a'+j, word, word_sz, currentRow, results, maxCost);
	         }
          }
	      break; // break because we only need one occurence of cost less than maxCost
       }
   }
}

void TrieEditSearchWord_Recursive(TrieNode* node, char letter, char* word, int word_sz, char*previousRow, ResultTrieSearch* results, char maxCost ){
   char* currentRow = (char*)malloc(word_sz+1);
   if( !currentRow ){
      err_mem( "error allocating current row" );
   }

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

   if( currentRow[word_sz] <= maxCost && node->qids!=0 ){
       // ADD THE node->qids[] INTO THE RESULTS
	   for( std::list<unsigned int>::iterator it=node->qids->begin() ; it != node->qids->end(); it++ ){
	       results->qids->push_back(*it);
	   }
   }

   // if there are more changes available recurse
   for( i=0; i<=word_sz; i++ ){
      if( currentRow[i] <= maxCost ){
	      for( j=0; j<VALID_CHARS; j++ ){
             if( node->children[j] != 0 ){
                TrieHammingSearchWord_Recursive(node->children[j], 'a'+j, word, word_sz, currentRow, results, maxCost);
	         }
          }
	      break; // break because we only need one occurence of cost less than maxCost
       }
   }
}


ResultTrieSearch* TrieEditHammingSearchWord( TrieNode* root, char* word, char maxCost, char hammingORedit ){
   // declare results
   char*p;
   int sz;
   for( sz=0,p=word; *p; sz++, p++ );
   ResultTrieSearch *results = (ResultTrieSearch*)malloc(sizeof(ResultTrieSearch));
   results->qids = new std::list<unsigned int>();
   char *currentRow = (char*)malloc(sz+1);
   if( !currentRow || !results ){
      err_mem( "error allocating TrieHamming" );
   }
   // create the current row // 0,1,2,3,4,,,sz
   char i;
   for( i=0; i<=sz; i++ ){
      currentRow[i]=i;
   }
   // for each children branch of the trie search the word
   for( i=0; i<VALID_CHARS; ++i ){
      if( root->children[i] != 0 ){
    	  if( hammingORedit == 1 )
    		  TrieHammingSearchWord_Recursive(
        		   root->children[i],
	               i +'a',
				   word,
				   sz,
				   currentRow,
				   results,
				   maxCost);
    	  else if( hammingORedit == 2 )
    		  TrieEditSearchWord_Recursive(
    		       root->children[i],
    		  	   i +'a',
    		  	   word,
    		  	   sz,
    		  	   currentRow,
    		  	   results,
    		  	   maxCost);
      }
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
	   unsigned int qid=1;
	   char word[128];
	   while( fscanf( stdin , "%s", word) > 0 ){
	      TrieInsert( root, word, qid++ );
	   }

	   std::list<unsigned int> res;

	   // for each argument keyword search the index
	   for( argc-- ; argc>0; argc-- ){
		   ResultTrieSearch* rts = TrieEditHammingSearchWord( root, argv[argc], 3, 2 );
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





























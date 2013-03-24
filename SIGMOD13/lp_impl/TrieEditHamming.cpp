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

typedef struct _QueryNode QueryNode;
typedef struct _QueryNode{
	unsigned int qid;
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
   TrieNode** children;
   QueryArrayList *qids;
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

TrieNode* TrieInsert( TrieNode* node, const char* word, char word_sz, unsigned int qid, char word_pos ){
   char ptr=0;
   int pos;
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
// above are the same
////////////////////////////////////////////////
/*
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
*/

void TrieHammingSearchWord_Recursive(TrieNode* node, char letter, const char* word, int word_sz, char*previousRow, ResultTrieSearch* results, char maxCost ){
   char* currentRow = (char*)malloc(word_sz+1);
   if( !currentRow ){
      err_mem( "error allocating current row" );
   }

   currentRow[0] = previousRow[0] + 1;

   char i,j;
   for( i=1; i<=word_sz; i++ ){
      if( word[i-1] != letter ){
         currentRow[i] = previousRow[i-1] + 1;
      }else{
         currentRow[i] = previousRow[i-1];
      }
   }

   if( currentRow[word_sz] <= maxCost && node->qids!=0 ){
       // ADD THE node->qids[] INTO THE RESULTS
	   for( QueryArrayList::iterator it=node->qids->begin() ; it != node->qids->end(); it++ ){
	       results->qids->push_back(*it);
	   }
   }



   // if there are more changes available recurse
   for( i=0; i<=word_sz; i++ ){
      if( currentRow[i] <= maxCost ){
	      for( j=0; j<VALID_CHARS; j++ ){
             if( node->children[j] != (TrieNode*)0 ){
                TrieHammingSearchWord_Recursive( (TrieNode*)node->children[j], 'a'+j, word, word_sz, currentRow, results, maxCost);
	         }
          }
	      break; // break because we only need one occurence of cost less than maxCost
       }
   }

   free(currentRow);
}

void TrieEditSearchWord_Recursive(TrieNode* node, char letter, const char* word, int word_sz, char*previousRow, ResultTrieSearch* results, char maxCost ){
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
	   for( QueryArrayList::iterator it=node->qids->begin() ; it != node->qids->end(); it++ ){
	       results->qids->push_back(*it);
	   }
   }

   // if there are more changes available recurse
   for( i=0; i<=word_sz; i++ ){
      if( currentRow[i] <= maxCost ){
	      for( j=0; j<VALID_CHARS; j++ ){
             if( node->children[j] != (TrieNode*)0 ){
                TrieHammingSearchWord_Recursive((TrieNode*)node->children[j], 'a'+j, word, word_sz, currentRow, results, maxCost);
	         }
          }
	      break; // break because we only need one occurence of cost less than maxCost
       }
   }
   free(currentRow);
}

ResultTrieSearch* TrieEditHammingSearchWord( TrieNode* root, ResultTrieSearch *results, const char* word, char sz, char maxCost, char hammingORedit ){
   // declare results
   char*p;
   //int sz;
   //for( sz=0,p=word; *p; sz++, p++ );

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
      if( root->children[i] != (TrieNode*)0 ){
    	  if( hammingORedit == 1 )
    		  TrieHammingSearchWord_Recursive(
    			   (TrieNode*)root->children[i],
	               i +'a',
				   word,
				   sz,
				   currentRow,
				   results,
				   maxCost);
    	  else if( hammingORedit == 2 )
    		  TrieEditSearchWord_Recursive(
    			   (TrieNode*)root->children[i],
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
	      TrieInsert( root, word, strlen(word), qid++, 1 );
	   }

	   ResultTrieSearch *results = (ResultTrieSearch*)malloc(sizeof(ResultTrieSearch));
	   	results->qids = new QueryArrayList();
	   // for each argument keyword search the index
	   for( argc-- ; argc>0; argc-- ){
		   ResultTrieSearch* rts = TrieEditHammingSearchWord( root, results, argv[argc], strlen(argv[argc]), 3, 1 );
	   }

	   unsigned long long end = getTime();
	   fprintf( stdout, "\nTotal time: %llu Matches: %lu\n\n", end-start, results->qids->size() );

	   return 0;
}





























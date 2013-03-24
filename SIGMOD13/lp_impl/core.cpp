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

#include <vector>
#include <list>
#include <algorithm>

///////////////////////////////////////////////////////////////////////////////////////////////

void err_mem(char* msg){
   perror(msg);
   exit(1);
}

/********************************************************************************************
 *  TRIE STRUCTURE
 *************************************/
/********************************************************************************************
 *  TRIE STRUCTURE END
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
   //n->children = (TrieNode**)malloc(VALID_CHARS*sizeof(TrieNode*));
   //if( !n->children ) err_mem("error allocating children()");
   //memset( n->children, 0, VALID_CHARS*sizeof(TrieNode*) );
   for( int i=0; i<26; i++ )
	   n->children[i] = 0;
   return n;
}

void TrieNode_Destructor( TrieNode* node ){
    for( char i=0; i<VALID_CHARS; i++ ){
    	if( node->children[i] != 0 ){
            TrieNode_Destructor( node->children[i] );
    	}
    }
    //free( node->children );
    if( node->qids )
        delete node->qids;
    free( node );
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
// above are the same regardless of query type
////////////////////////////////////////////////

ResultTrieSearch* TrieExactSearchWord( TrieNode* root, ResultTrieSearch *results, const char* word, char word_sz ){

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
	   for( QueryArrayList::iterator it=root->qids->begin(), end=root->qids->end() ; it != end; it++ ){
	   	       results->qids->push_back(*it);
	   	   }
   }
   return results;
}


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
	   for( QueryArrayList::iterator it=node->qids->begin(), end=node->qids->end() ; it != end; it++ ){
		   if( it->qid == 92 ){
			   fprintf( stderr, "\n\nmaxCost: %d currentRow[sz]: %d letter: %c word: %s\n\n",maxCost, currentRow[word_sz], letter, word  );
		   }
	       results->qids->push_back(*it);
	   }
   }



   // if there are more changes available recurse
   for( i=0; i<=word_sz; i++ ){
      if( currentRow[i] <= maxCost ){
	      for( j=0; j<VALID_CHARS; j++ ){
             if( node->children[j] != 0 ){
                TrieHammingSearchWord_Recursive( node->children[j], 'a'+j, word, word_sz, currentRow, results, maxCost);
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

	   if( word[i-1] == letter ){
		   currentRow[i] = previousRow[i-1];
	   }else{
		   insertCost = currentRow[i-1] + 1;
		   deleteCost = previousRow[i] + 1;
		   replaceCost = previousRow[i-1] + 1;
		   // find the minimum for this column
		   insertCost = insertCost < replaceCost ? insertCost : replaceCost;
		   currentRow[i] = insertCost < deleteCost ? insertCost : deleteCost;
	   }

	   /*

       if( word[i-1] != letter ){
          replaceCost = previousRow[i-1] + 1;
       }else{
          replaceCost = previousRow[i-1];
       }
       // find the minimum for this column
       insertCost = insertCost < replaceCost ? insertCost : replaceCost;
       currentRow[i] = insertCost < deleteCost ? insertCost : deleteCost;
       */
   }

   if( currentRow[word_sz] <= maxCost && node->qids!=0 ){
       // ADD THE node->qids[] INTO THE RESULTS
	   for( QueryArrayList::iterator it=node->qids->begin(), end=node->qids->end() ; it != end; it++ ){
	       results->qids->push_back(*it);
	   }
   }

   // if there are more changes available recurse
   for( i=0; i<=word_sz; i++ ){
      if( currentRow[i] <= maxCost ){
	      for( j=0; j<VALID_CHARS; j++ ){
             if( node->children[j] != 0 ){
                TrieEditSearchWord_Recursive(node->children[j], 'a'+j, word, word_sz, currentRow, results, maxCost);
	         }
          }
	      break; // break because we only need one occurence of cost less than maxCost
       }
   }
   free(currentRow);
}

ResultTrieSearch* TrieEditHammingSearchWord( TrieNode* root, ResultTrieSearch *results, const char* word, char sz, char maxCost, char hammingORedit ){
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
   free( currentRow );
   return results;
}


/********************************************************************************************
 *  TRIE STRUCTURE END
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

/********************************************************************************************
 *  GLOBALS
 *************************************/
TrieNode *trie_exact;
TrieNode **trie_hamming;
TrieNode **trie_edit;

QuerySet *querySet; // std::vector<QuerySetNode*>
DocResults *docResults; // std::list<DocResultsNode>

//VisitedWords visited;

/********************************************************************************************
 *  GLOBALS END
 ********************************************************************************************/




///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode InitializeIndex(){
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

    querySet = new QuerySet();
    // add dummy query to start from index 1
    querySet->push_back((QuerySetNode*)malloc(sizeof(QuerySetNode)));
    docResults = new DocResults();

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

	//fprintf( stderr, "q[%u][%d][%d]\n", querySet->size(), qnode->words_num, qnode->type );

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

	// WE DO NOT DEALLOCATE THE QUERY NODE to avoid delays

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
	// results are new for each document
	ResultTrieSearch *results = (ResultTrieSearch*)malloc(sizeof(ResultTrieSearch));
	results->qids = new QueryArrayList();
    if( !results->qids ) err_mem( "could not allocate ResultsTrieSearch for document" );

	// check if a query fully
    // parallel quicksort in multi-threaded INSTEAD of the position of each word inside the query node

	///////////////////////////////////////////////

	const char *start, *end;
	char sz;
    for( start=doc_str; *start; start = end ){
	    // FOR EACH WORD DO THE MATCHING

    	while( *start == ' ' ) start++;
    	end = start;
    	while( *end >= 'a' && *end <= 'z' ) end++;

    	sz = end-start;
    	TrieExactSearchWord( (TrieNode*)trie_exact, results, start, sz );
    	TrieEditHammingSearchWord( (TrieNode*)trie_hamming[0], results, start, sz, 0, 1 );
    	TrieEditHammingSearchWord( (TrieNode*)trie_hamming[1], results, start, sz, 1, 1 );
    	TrieEditHammingSearchWord( (TrieNode*)trie_hamming[2], results, start, sz, 2, 1 );
    	TrieEditHammingSearchWord( (TrieNode*)trie_hamming[3], results, start, sz, 3, 1 );
    	TrieEditHammingSearchWord( (TrieNode*)trie_edit[0], results, start, sz, 0, 2 );
    	TrieEditHammingSearchWord( (TrieNode*)trie_edit[1], results, start, sz, 1, 2 );
    	TrieEditHammingSearchWord( (TrieNode*)trie_edit[2], results, start, sz, 2, 2 );
    	TrieEditHammingSearchWord( (TrieNode*)trie_edit[3], results, start, sz, 3, 2 );
    }

    //results->qids->sort(compareQueryNodes);
    std::stable_sort( results->qids->begin(), results->qids->end(), compareQueryNodes );

    fprintf( stderr, "\ndoc[%u] results->qids: %p [size: %lu]\n", doc_id ,results->qids, results->qids->size() );
    for( QueryArrayList::iterator it=results->qids->begin(), end=results->qids->end(); it != end; it++ )
    	fprintf( stderr, "%u[%d][%d] ", it->qid, querySet->at(it->qid)->words_num, it->pos );

    if( doc_id == 19 )
    	exit(1);

    std::vector<QueryID> ids;
    char counter=0;
    QueryNode qn_p, qn_c;
    // IF WE HAVE RESULTS FOR THIS DOCUMENT
    if( ! results->qids->empty() ){
        qn_p.qid = results->qids->begin()->qid;
        qn_p.pos = results->qids->begin()->pos;
        counter=1;

		for( QueryArrayList::iterator it=results->qids->begin(), end=results->qids->end(); it != end; it++ ){
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

    delete results->qids;
    free(results);

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

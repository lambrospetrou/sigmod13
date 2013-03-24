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
   //if( !n ) err_mem("error allocating TrieNode");
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
    //free( node->children );
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

struct HammingNode{
	TrieNode* node;
	char letter;
	char depth;
	char tcost;
};
void TrieHammingSearchWord(std::vector<HammingNode> *hamming_stack, TrieNode* node, const char* word, int word_sz, ResultTrieSearch* results, char maxCost ){
	HammingNode current, n;
	char j;

	// add the initial nodes
	for( j=0; j<VALID_CHARS; j++ ){
	   if( node->children[j] != 0 ){
		   current.depth = 1;
		   current.node = node->children[j];
		   current.letter = 'a' + j;
		   current.tcost = 0;
		   hamming_stack->push_back(current);
	   }
    }

	while (!hamming_stack->empty()) {
		current = hamming_stack->back();
		hamming_stack->pop_back();

		if (current.letter != word[current.depth - 1]) {
			current.tcost++;
		}
		if (current.tcost <= maxCost) {
			if (word_sz == current.depth && current.node->qids != 0) {
				// ADD THE node->qids[] INTO THE RESULTS
				for (QueryArrayList::iterator it = current.node->qids->begin(),
						end = current.node->qids->end(); it != end; it++) {
					results->qids->push_back(*it);
				}
			} else if (word_sz > current.depth) {
				for (j = 0; j < VALID_CHARS; j++) {
					if (current.node->children[j] != 0) {
						n.depth = current.depth + 1;
						n.node = current.node->children[j];
						n.letter = 'a' + j;
						n.tcost = current.tcost;
						hamming_stack->push_back(n);
					}
				}
			}
		}
	}
}

struct EditNode{
	TrieNode* node;
	//char previous[MAX_WORD_LENGTH+1];
	char *previous;
	char letter;
};
char global_chars_edit[(MAX_WORD_LENGTH+1)*5000000]; // instead of allocating new char tables every iteration just point to the table here
void TrieEditSearchWord(std::vector<EditNode> *edit_stack, TrieNode* node, const char* word, int word_sz, ResultTrieSearch* results, char maxCost ){
    EditNode c, n;
    char current[MAX_WORD_LENGTH+1];

    char* next = global_chars_edit; // to be used for global_chars

    char i, insertCost, deleteCost, replaceCost, j, k;

	for (i = 0; i < VALID_CHARS; ++i) {
		if (node->children[i] != 0) {
			c.letter = 'a' + i;
			c.node = node->children[i];

			c.previous = next; // only for global_chars
			next += (MAX_WORD_LENGTH+1);

			for( j=0; j<=word_sz; j++ )
			    c.previous[j] = j;
			edit_stack->push_back(c);
		}
	}


	while( !edit_stack->empty() ){
		c = edit_stack->back();
		edit_stack->pop_back();

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
     	   for( QueryArrayList::iterator it=c.node->qids->begin(), end=c.node->qids->end() ; it != end; it++ ){
     	       results->qids->push_back(*it);
     	   }
        }

        // if there are more changes available recurse
		for (i = 0; i <= word_sz; i++) {
			if (current[i] <= maxCost) {
				for (j = 0; j < VALID_CHARS; j++) {
					if (c.node->children[j] != 0) {
						n.letter = 'a' + j;
						n.node = c.node->children[j];

						n.previous = next; // only for global_chars
						next += (MAX_WORD_LENGTH+1);


						for (k = 0; k <= word_sz; k++)
							n.previous[k] = current[k];
						edit_stack->push_back(n);
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

/////////////////////////////////////////////////////////////////////////////////
// TODO - BE CAREFULL WITH THE GLOBAL STACK IN PARALLEL CODE
std::vector<HammingNode> *hamming_stack=new std::vector<HammingNode>();
std::vector<EditNode> *edit_stack=new std::vector<EditNode>();
// moved above the function in order to be callable
// char global_chars_edit[(MAX_WORD_LENGTH+1)*5000000]; // instead of allocating new char tables every iteration just point to the table here
/////////////////////////////////////////////////////////////////////////////////

QuerySet *querySet; // std::vector<QuerySetNode*>
DocResults *docResults; // std::list<DocResultsNode>



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
    // add dummy query to start from index 1 because query ids start from 1 instead of 0
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

    delete edit_stack;
    delete hamming_stack;

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
    	TrieHammingSearchWord(hamming_stack, (TrieNode*)trie_hamming[0], start, sz, results, 0 );
    	TrieHammingSearchWord(hamming_stack, (TrieNode*)trie_hamming[1], start, sz, results, 1 );
    	TrieHammingSearchWord(hamming_stack, (TrieNode*)trie_hamming[2], start, sz, results, 2 );
    	TrieHammingSearchWord(hamming_stack, (TrieNode*)trie_hamming[3], start, sz, results, 3 );
    	TrieEditSearchWord(edit_stack, (TrieNode*)trie_edit[0], start, sz, results, 0);
    	TrieEditSearchWord(edit_stack, (TrieNode*)trie_edit[1], start, sz, results, 1);
    	TrieEditSearchWord(edit_stack, (TrieNode*)trie_edit[2], start, sz, results, 2);
    	TrieEditSearchWord(edit_stack, (TrieNode*)trie_edit[3], start, sz, results, 3);
    }

    //results->qids->sort(compareQueryNodes);
    std::stable_sort( results->qids->begin(), results->qids->end(), compareQueryNodes );

/*
    fprintf( stderr, "\ndoc[%u] results->qids: %p [size: %lu]\n", doc_id ,results->qids, results->qids->size() );
    for( QueryArrayList::iterator it=results->qids->begin(), end=results->qids->end(); it != end; it++ )
    	fprintf( stderr, "%u[%d][%d] ", it->qid, querySet->at(it->qid)->words_num, it->pos );
*/

    std::vector<QueryID> ids;
    char counter=0;
    QueryNode qn_p, qn_c;
    // IF WE HAVE RESULTS FOR THIS DOCUMENT
    if( ! results->qids->empty() ){
        qn_p.qid = results->qids->begin()->qid;
        qn_p.pos = results->qids->begin()->pos;
        counter=1;

		for( QueryArrayList::iterator it=++results->qids->begin(), end=results->qids->end(); it != end; it++ ){
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

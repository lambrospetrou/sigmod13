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
using namespace std;

///////////////////////////////////////////////////////////////////////////////////////////////

/********************************************************************************************
 *  TRIE STRUCTURE
 *************************************/
/********************************************************************************************
 *  TRIE STRUCTURE END
 ********************************************************************************************/





/********************************************************************************************
 *  TRIE STRUCTURE
 *************************************/

void err_mem(char* msg){
   perror(msg);
   exit(1);
}

typedef struct _TrieNode TrieNode;
struct _TrieNode{
   char *word;
   TrieNode** children;
};

TrieNode* TrieNode_Constructor(){
   TrieNode* n = (TrieNode*)malloc(sizeof(TrieNode));
   if( !n ) err_mem("error allocating TrieNode");
   memset( n, 0, sizeof(TrieNode) );
   n->children = (TrieNode**)malloc(26*sizeof(TrieNode*));
   if( !n->children ) err_mem("error allocating children()");
   memset( n->children, 0, 26*sizeof(TrieNode*) );
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
      // ADD THE node->word INTO THE RESULTS
      //
      matches++;
      results.push_back(node->word);
   }

   // if there are more changes available recurse
   for( i=0; i<=word_sz; i++ )
      if( currentRow[i] <= maxCost ){
	 for( j=0; j<26; j++ ){
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
   for( p=word; *p; p++ ){
      currentRow[*p-'a']=*p-'a';
   }
   // for each children branch of the trie search the word
   char i;
   for( i=0; i<26; ++i ){
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




/********************************************************************************************
 *  TRIE STRUCTURE END
 ********************************************************************************************/










// Computes edit distance between a null-terminated string "a" with length "na"
//  and a null-terminated string "b" with length "nb" 
int EditDistance(char* a, int na, char* b, int nb)
{
	int oo=0x7FFFFFFF;

	static int T[2][MAX_WORD_LENGTH+1];

	int ia, ib;

	int cur=0;
	ia=0;

	for(ib=0;ib<=nb;ib++)
		T[cur][ib]=ib;

	cur=1-cur;

	for(ia=1;ia<=na;ia++)
	{
		for(ib=0;ib<=nb;ib++)
			T[cur][ib]=oo;

		int ib_st=0;
		int ib_en=nb;

		if(ib_st==0)
		{
			ib=0;
			T[cur][ib]=ia;
			ib_st++;
		}

		for(ib=ib_st;ib<=ib_en;ib++)
		{
			int ret=oo;

			int d1=T[1-cur][ib]+1;
			int d2=T[cur][ib-1]+1;
			int d3=T[1-cur][ib-1]; if(a[ia-1]!=b[ib-1]) d3++;

			if(d1<ret) ret=d1;
			if(d2<ret) ret=d2;
			if(d3<ret) ret=d3;

			T[cur][ib]=ret;
		}

		cur=1-cur;
	}

	int ret=T[1-cur][nb];

	return ret;
}

///////////////////////////////////////////////////////////////////////////////////////////////

// Computes Hamming distance between a null-terminated string "a" with length "na"
//  and a null-terminated string "b" with length "nb" 
unsigned int HammingDistance(char* a, int na, char* b, int nb)
{
	int j, oo=0x7FFFFFFF;
	if(na!=nb) return oo;
	
	unsigned int num_mismatches=0;
	for(j=0;j<na;j++) if(a[j]!=b[j]) num_mismatches++;
	
	return num_mismatches;
}

///////////////////////////////////////////////////////////////////////////////////////////////

// Keeps all information related to an active query
struct Query
{
	QueryID query_id;
	char str[MAX_QUERY_LENGTH];
	MatchType match_type;
	unsigned int match_dist;
};

///////////////////////////////////////////////////////////////////////////////////////////////

// Keeps all query ID results associated with a document
struct Document
{
	DocID doc_id;
	unsigned int num_res;
	QueryID* query_ids;
};

///////////////////////////////////////////////////////////////////////////////////////////////

// Keeps all currently active queries
vector<Query> queries;

// Keeps all currently available results that has not been returned yet
vector<Document> docs;

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode InitializeIndex(){return EC_SUCCESS;}

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode DestroyIndex(){return EC_SUCCESS;}

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode StartQuery(QueryID query_id, const char* query_str, MatchType match_type, unsigned int match_dist)
{
	Query query;
	query.query_id=query_id;
	strcpy(query.str, query_str);
	query.match_type=match_type;
	query.match_dist=match_dist;
	// Add this query to the active query set
	queries.push_back(query);
	return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode EndQuery(QueryID query_id)
{
	// Remove this query from the active query set
	unsigned int i, n=queries.size();
	for(i=0;i<n;i++)
	{
		if(queries[i].query_id==query_id)
		{
			queries.erase(queries.begin()+i);
			break;
		}
	}
	return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode MatchDocument(DocID doc_id, const char* doc_str)
{
	char cur_doc_str[MAX_DOC_LENGTH];
	strcpy(cur_doc_str, doc_str);

	unsigned int i, n=queries.size();
	vector<unsigned int> query_ids;

	// Iterate on all active queries to compare them with this new document
	for(i=0;i<n;i++)
	{
		bool matching_query=true;
		Query* quer=&queries[i];

		int iq=0;
		while(quer->str[iq] && matching_query)
		{
			while(quer->str[iq]==' ') iq++;
			if(!quer->str[iq]) break;
			char* qword=&quer->str[iq];

			int lq=iq;
			while(quer->str[iq] && quer->str[iq]!=' ') iq++;
			char qt=quer->str[iq];
			quer->str[iq]=0;
			lq=iq-lq;

			bool matching_word=false;
			char *tqword;

			int id=0;
			while(cur_doc_str[id] && !matching_word)
			{
			    tqword = qword;

				while(cur_doc_str[id]==' ') id++;
				if(!cur_doc_str[id]) break;
				char* dword=&cur_doc_str[id];

				int ld=id;
				while(cur_doc_str[id] && cur_doc_str[id]!=' ') id++;
				char dt=cur_doc_str[id];
				cur_doc_str[id]=0;

				ld=id-ld;

				if(quer->match_type==MT_EXACT_MATCH)
				{
				   if( ld != lq ){
				      matching_word = false;
				   }else{
				      while( *tqword && *tqword==*dword ){
				         tqword++; dword++;
				      }
				      matching_word = (*tqword ^ *dword) ? false : true;
				   }

					//if(strcmp(qword, dword)==0) matching_word=true;
				}
				else if(quer->match_type==MT_HAMMING_DIST)
				{
					unsigned int num_mismatches=0;//HammingDistance(qword, lq, dword, ld);
					//if(num_mismatches<=quer->match_dist) matching_word=true;
					if( lq ^ ld ){
					   matching_word = false;
					}else{
					   while( *tqword )
					      if( *tqword++ ^ *dword++ ) num_mismatches++;
					   matching_word = (num_mismatches<=quer->match_dist)?true:false;
					}
				}
				else if(quer->match_type==MT_EDIT_DIST)
				{
					unsigned int edit_dist=EditDistance(qword, lq, dword, ld);
					if(edit_dist<=quer->match_dist) matching_word=true;
				}

				cur_doc_str[id]=dt;
			}

			quer->str[iq]=qt;

			if(!matching_word)
			{
				// This query has a word that does not match any word in the document
				matching_query=false;
			}
		}

		if(matching_query)
		{
			// This query matches the document
			query_ids.push_back(quer->query_id);
		}
	}

	Document doc;
	doc.doc_id=doc_id;
	doc.num_res=query_ids.size();
	doc.query_ids=0;
	if(doc.num_res) doc.query_ids=(unsigned int*)malloc(doc.num_res*sizeof(unsigned int));
	for(i=0;i<doc.num_res;i++) doc.query_ids[i]=query_ids[i];
	// Add this result to the set of undelivered results
	docs.push_back(doc);

	return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////

ErrorCode GetNextAvailRes(DocID* p_doc_id, unsigned int* p_num_res, QueryID** p_query_ids)
{
	// Get the first undeliverd resuilt from "docs" and return it
	*p_doc_id=0; *p_num_res=0; *p_query_ids=0;
	if(docs.size()==0) return EC_NO_AVAIL_RES;
	*p_doc_id=docs[0].doc_id; *p_num_res=docs[0].num_res; *p_query_ids=docs[0].query_ids;
	docs.erase(docs.begin());
	return EC_SUCCESS;
}

///////////////////////////////////////////////////////////////////////////////////////////////

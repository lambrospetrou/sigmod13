#include <stdlib.h>

typedef struct _ListInt ListInt;
struct _ListInt{
	ListInt* next;
	unsigned int data;
};

ListInt* ListInt_Constructor(){
	ListInt *l = (ListInt*)malloc(sizeof(ListInt));
	if( !l ) err_mem("error constructing ListPtr");
	memset(l, 0, sizeof(ListInt));
	return l;
}

void ListInt_Destructor(ListInt* l){
	for( ListInt* t; l; l=t ){
		t=l->next;
		free(l);
	}
}

ListInt* insertBack( ListInt** l )

ListInt* insertSorted(ListInt** l, unsigned int val){
	if( !l || !*l ){
		ListInt* n =ListInt_Constructor();
		n->data = val;
		return n;
	}

	// handle head case
	if( (*l)->data >= val ){
		ListInt* n =ListInt_Constructor();
		n->data = val;
		n->next = *l;
		*l = n;
		return n;
	}

	for( ; (*l)->next && (*l)->next<val; (*l)=(*l)->next );
	ListInt* n =ListInt_Constructor();
	n->data = val;
	n->next = (*l)->next;
	(*l)->next = n;
	return n;
}

ListInt* getVal(ListInt* l, unsigned int val){
	for( ; l && l->data<val ; l=l->next );
	return l && l->data==val ? l : NULL ;
}

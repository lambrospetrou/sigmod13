#include <stdlib.h>

typedef struct _ListPtr ListPtr;
struct _ListPtr{
	ListPtr* next;
	void* data;
};

ListPtr* ListPtr_Constructor(){
	ListPtr *l = (ListPtr*)malloc(sizeof(ListPtr));
	if( !l ) err_mem("error constructing ListPtr");
	memset(l, 0, sizeof(ListPtr));
	return l;
}


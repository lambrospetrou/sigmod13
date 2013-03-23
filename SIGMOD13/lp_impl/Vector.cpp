#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>

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

int main(){
	int i;
	Vector* v = Vector_Constructor();
	for(i=0; i<10000000; i++){
		Vector_push_back(v,i);
	}


	fprintf(stdout, "Size: %llu\n", Vector_size(v));

	for(i=0; i<10000000; i++){
		fprintf( stdout, "[%d]: %u\n" ,i, *Vector_get(v,i) );
	}

}
















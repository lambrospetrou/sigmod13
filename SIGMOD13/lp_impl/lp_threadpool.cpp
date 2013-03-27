#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>

typedef struct _lp_tpjob lp_tpjob;
struct _lp_tpjob{
    void *args;
    void *(*func)(int, void *);
    lp_tpjob *next;    
};

typedef struct{
   int workers_ids;   
   int nthreads;
   int pending_jobs;
   lp_tpjob *jobs_head;
   lp_tpjob *jobs_tail;
   
   pthread_cond_t cond_jobs;
   pthread_mutex_t mutex_pool;
   
   pthread_t *worker_threads;
   
}lp_threadpool;

void lp_threadpool_addjob( lp_threadpool* pool, void *(*func)(int, void *), void* args ){
     lp_tpjob *njob = (lp_tpjob*)malloc( sizeof(lp_tpjob) );
     if( !njob ){
         perror( "Could not create a lp_tpjob...\n" );
         return;
     } 
     njob->args = args;
     njob->func = func;
     njob->next = 0;
     
     // ENTER POOL CRITICAL SECTION
     pthread_mutex_lock( &pool->mutex_pool );
     //////////////////////////////////////
     
     // empty job queue
     if( pool->pending_jobs == 0 ){
         pool->jobs_head = njob;
         pool->jobs_tail = njob;
     }else{
         // add new job to the tail of the queue
         pool->jobs_tail->next = njob;
         pool->jobs_tail = njob;
     }
     
     pool->pending_jobs++;
     
     //fprintf( stderr, "job added [%d]\n", pool->pending_jobs );
     
     // EXIT POOL CRITICAL SECTION
     pthread_mutex_unlock( &pool->mutex_pool );
     //////////////////////////////////////
     
     // signal any worker_thread that new job is available
     pthread_cond_signal( &pool->cond_jobs );
}

lp_tpjob* lp_threadpool_fetchjob( lp_threadpool* pool ){
    lp_tpjob* job;
    // lock pool
    pthread_mutex_lock( &pool->mutex_pool );
    
       while( pool->pending_jobs == 0 )
           pthread_cond_wait( &pool->cond_jobs, &pool->mutex_pool );
       
       // available job pending    
        --pool->pending_jobs;
        job = pool->jobs_head;
        pool->jobs_head = pool->jobs_head->next;
        // if no more jobs available
        if( pool->jobs_head == 0 ){
            pool->jobs_tail = 0;
        }
        
    //fprintf( stderr, "job removed - remained[%d]\n", pool->pending_jobs );
        
    // pool unlock
    pthread_mutex_unlock( &pool->mutex_pool );

    return job;
}
// returns an id from 1 to number of threads (eg. threads=12, ids = 1,2,3,4,5,6,7,8,9,10,11,12)
int lp_threadpool_uniquetid( lp_threadpool* pool ){ 
   // lock pool
   int _tid;
   pthread_mutex_lock( &pool->mutex_pool );
   _tid = pool->workers_ids;
   pool->workers_ids = ( ( pool->workers_ids + 1 ) % (pool->nthreads+1) )  ;
   pthread_mutex_unlock( &pool->mutex_pool );
   return _tid;
}

void* lp_tpworker_thread( void* _pool ){

   lp_threadpool* pool = ((lp_threadpool*)_pool);
   int _tid=lp_threadpool_uniquetid( pool );
   int i;

   //fprintf( stderr, "thread[%d] entered worker_thread infite\n", _tid );
   
   for(;;){

       // fetch next job - blocking method
       lp_tpjob* njob = lp_threadpool_fetchjob( pool );
       
       // execute the function passing in the thread_id - the TID starts from 1 - POOL_THREADS
       njob->func( _tid , njob->args ); 
     
       // release memory the job holds
       free( njob );
   } 

}

lp_threadpool* lp_threadpool_init( int threads ){
    lp_threadpool* pool = (lp_threadpool*)malloc( sizeof(lp_threadpool) );
    pool->workers_ids = 1; // threads start from 1 to NTHREADS
    pool->nthreads = threads;
    pool->pending_jobs = 0;
    pool->jobs_head=0;
    pool->jobs_tail=0;
    
    pthread_cond_init( &pool->cond_jobs, NULL );
    pthread_mutex_init( &pool->mutex_pool, NULL );
    
    // lock pool in order to prevent worker threads to start before initializing the whole pool
    pthread_mutex_lock( &pool->mutex_pool );
    
    pthread_t *worker_threads = (pthread_t*)malloc(sizeof(pthread_t)*threads);
    for( int i=0; i<threads; i++ ){
        pthread_create( &worker_threads[i], NULL, reinterpret_cast<void* (*)(void*)>(lp_tpworker_thread), pool );
        //fprintf( stderr, "[%p] thread[%d] started\n", worker_threads[i] );
    }
  
    pool->worker_threads = worker_threads;
    
    // unlock pool for workers
    pthread_mutex_unlock( &pool->mutex_pool );
    
    return pool;
}

void lp_threadpool_destroy(lp_threadpool* pool){
     free( pool->worker_threads );
     for( lp_tpjob* j=pool->jobs_head, *t=0; j; j=t ){
         t = j->next;
         free( j );
     }
     free( pool );
}


#define NTHREADS 24
#define WORKERS NTHREADS+1

sem_t sems[WORKERS];
void* dummy( int tid, void* args ){
    fprintf( stderr, "dummy execute by thread[%d]\n", tid );
    
    // SYNCHRONIZE THREADS
    if( tid < NTHREADS ){
        sem_wait( &sems[tid+1] ); 
    }  
    sem_post( &sems[tid] );
   
    fprintf( stderr, "dummy exit by thread[%d], signaled semaphore[%d]\n", tid, tid );
}


int main(){
   
    lp_threadpool* pool = lp_threadpool_init(NTHREADS);   
   
    for( int i=0; i<WORKERS; i++ ){
       sem_init( &sems[i], 0, 0 );
    }

    for( int i=0; i<NTHREADS; i++ ){
        lp_threadpool_addjob( pool, dummy, NULL );
    }
    dummy( 0, NULL );

    sem_wait( &sems[0] );
    
    fprintf( stderr, "all dummy calls finished..\n" );
    
    return 0;
}




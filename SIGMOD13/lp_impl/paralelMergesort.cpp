
/*

 //for parallel implementation
void parallelMergesort(QueryArrayList* lyst, unsigned int size, unsigned int tlevel);
void *parallelMergesortHelper(void *threadarg);


struct thread_data{
	QueryArrayList* lyst;
	QueryArrayList* back;
	unsigned int low;
	unsigned int high;
	unsigned int level;
};


//merge the elements from the sorted sublysts in [low, mid] and (mid, high]
void mergeP(QueryArrayList* lyst, QueryArrayList* back, unsigned int low, unsigned int mid, unsigned int high)
{
	unsigned int ai = low, bi = mid + 1, i = low;
	while (ai <= mid && bi <= high)
	{
		if ( compareQueryNodes(lyst->at(ai), lyst->at(bi)) )
		{
			back->at(i) = lyst->at(ai);
			ai++;
		}else
		{
			back->at(i) = lyst->at(bi);
			bi++;
		}
		i++;
	}
	if (ai > mid)
	{
		for( unsigned int sz=high-bi+1; sz>0 ; i++, bi++ ,sz-- ){
			back->at(i) = lyst->at(bi);
		}
		//memcpy(&back[i], &lyst[bi], (high-bi+1)*sizeof(double));
	}else
	{
		for( unsigned int sz=mid-ai+1; sz>0; i++, ai++, sz-- ){
					back->at(i) = lyst->at(ai);
				}
		//memcpy(&back[i], &lyst[ai], (mid-ai+1)*sizeof(double));
	}
	for( unsigned int sz=high-low+1; sz>0; low++,sz-- ){
				lyst->at(low) = back->at(low);
			}
	//memcpy(&lyst[low], &back[low], (high-low+1)*sizeof(double));
}

//the actual C mergesort method, with back list to avoid
//NlogN memory (2N instead).
void mergesortHelper(QueryArrayList* lyst, QueryArrayList* back, unsigned int low, unsigned int high)
{
	if (low == high) return;
	unsigned int mid = low + (high-low)/2;
	mergesortHelper(lyst, back, low, mid);
	mergesortHelper(lyst, back, mid+1, high);
	mergeP(lyst, back, low, mid, high);
}


//parallel mergesort top level:
//instantiate parallelMergesortHelper thread, and that's
//basically it.

void parallelMergesort(QueryArrayList* lyst, unsigned int size, unsigned int tlevel)
{
	int rc;
	void *status;

	QueryArrayList* back = new QueryArrayList();
	back->resize(lyst->size());

	//Want joinable threads (usually default).
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

	//pthread function can take only one argument, so struct.
	struct thread_data td;
	td.lyst = lyst;
	td.back = back;
	td.low = 0;
	td.high = size - 1;
	td.level = tlevel;

	//The top-level thread.
	pthread_t theThread;
	rc = pthread_create(&theThread, &attr, parallelMergesortHelper,
						(void *) &td);
	if (rc)
	{
    	printf("ERROR; return code from pthread_create() is %d\n", rc);
    	exit(-1);
    }

	//Now join the thread (wait, as joining blocks) and quit.
	pthread_attr_destroy(&attr);
	rc = pthread_join(theThread, &status);
	if (rc)
	{
		printf("ERROR; return code from pthread_join() is %d\n", rc);
		exit(-1);
	}
	//printf("Main: completed join with top thread having a status of %ld\n",
	//		(long)status);

	delete back;

}


//parallelMergesortHelper
//-if the level is still > 0, then make
//parallelMergesortHelper threads to solve the left and
//right-hand sides, then merge results after joining
// and quit.

void *parallelMergesortHelper(void *threadarg)
{
	unsigned int mid;
	int  t, rc;
	void *status;

	struct thread_data *my_data;
	my_data = (struct thread_data *) threadarg;

	//fyi:
	//printf("Thread responsible for [%d, %d], level %d.\n",
	//		my_data->low, my_data->high, my_data->level);

	if (my_data->level <= 0 || my_data->low == my_data->high)
	{
		//We have plenty of threads, finish with sequential.
		mergesortHelper(my_data->lyst, my_data->back,
						my_data->low, my_data->high);
		pthread_exit(NULL);
	}

	//Want joinable threads (usually default).
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);


	//At this point, we will create threads for the
	//left and right sides.  Must create their data args.
	struct thread_data thread_data_array[2];
	mid = (my_data->low + my_data->high)/2;

	for (t = 0; t < 2; t ++)
	{
		thread_data_array[t].lyst = my_data->lyst;
		thread_data_array[t].back = my_data->back;
		thread_data_array[t].level = my_data->level - 1;
	}
	thread_data_array[0].low = my_data->low;
	thread_data_array[0].high = mid;
	thread_data_array[1].low = mid+1;
	thread_data_array[1].high = my_data->high;

	//Now, instantiate the threads.
	pthread_t threads[2];
	for (t = 0; t < 2; t ++)
	{
		rc = pthread_create(&threads[t], &attr, parallelMergesortHelper,
							(void *) &thread_data_array[t]);
		if (rc)
		{
    		printf("ERROR; return code from pthread_create() is %d\n", rc);
    		exit(-1);
    	}
	}

	pthread_attr_destroy(&attr);
	//Now, join the left and right threads and merge.
	for (t = 0; t < 2; t ++)
	{
		rc = pthread_join(threads[t], &status);
		if (rc)
		{
			printf("ERROR; return code from pthread_join() is %d\n", rc);
			exit(-1);
		}
	}

	//Call the sequential merge now that the left and right
	//sides are sorted.
	mergeP(my_data->lyst, my_data->back, my_data->low, mid, my_data->high);

	pthread_exit(NULL);
}

*/

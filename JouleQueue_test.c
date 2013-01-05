#ifndef _JouleTest
#define _JouleTest

#include <sys/time.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include "minunit.h"
#include "common.h"
#include "JouleQueue.h"
#include "tls.h"

#define TESTNUMTHREADS 4
#define MAXWORK 100000000 // avoid static memory fault


typedef struct test_t {
    int  work[MAXWORK];
    int  work_counter;
} test_t; // record each threads work done

typedef struct test_init_t{
    jq_t  q;
    int   low;
    int   high;
} test_init_t;

//static int             thread_index = 0;  // num we give to thread locals
static int             threads_done = 0;
static test_t          test_work[TESTNUMTHREADS];
static pthread_t       test_threads[TESTNUMTHREADS];
static test_init_t     test_threads_value[TESTNUMTHREADS];
//static pthread_mutex_t test_mutex = PTHREAD_MUTEX_INITIALIZER;
static int             work_to_do[MAXWORK];
static DECLARE_THREAD_LOCAL(key,int);


/* for JouleQueue */
void * test_engine(int id, void * arg){
    
    //log_success("Data is: %d id:%d",*(int*)arg,id);
    int index = test_work[id].work_counter;
    test_work[id].work[index] = *(int*)arg;
    test_work[id].work_counter++;
    assert(test_work[id].work_counter < MAXWORK);
        

    return SUCCESS;
}


/* our test thread init func */
void * test_add(void * arg){

    test_init_t * stuff = arg;
    jq_t q = stuff->q;
    int low = stuff->low;
    int high = stuff->high;
    log_success("Start low; %d, High: %d",low, high);
    for(int i =low; i < high; i++){

        int * data_new = &work_to_do[i];
        *data_new = i;
        int status = enqueue(q, NULL, (void *) data_new);
        
    }

    log_success("Done %d",threads_done);
    threads_done++;

    return SUCCESS;
}

int compare (const void * a, const void * b)
{
  return ( *(int*)a - *(int*)b );
}
int test_jq(){
    memset(&test_work,0,sizeof(test_t)*TESTNUMTHREADS);
    INIT_THREAD_LOCAL(key);
    //srand(time(NULL));
    int status; 
    jq_t q; 
    jq_config_t config = malloc(sizeof(jq_config_t));
    config->app_engine = test_engine;
    config->max_threads = TESTNUMTHREADS;
    config->max_backoff = JQ_DEFAULT_MAX_BACKOFF;
    config->max_retries = JQ_DEFAULT_RETRIES;

    q = malloc(sizeof(queue_t)); 
    status = jq_init(q,config); 
    mu_falsey(status);
    sleep(1);
    int range = 10000;
    int low = 1;
    int high = range-1;
    for(int i =0; i < TESTNUMTHREADS; i++){
        test_threads_value[i].q = q;
        test_threads_value[i].low = low;
        test_threads_value[i].high = high;
        pthread_create(&test_threads[i],NULL, 
                test_add,(void*) &test_threads_value[i]); 
        high +=range;
        low +=range;
        assert(high < MAXWORK);
        
    }

    struct timeval t;
    struct timespec timeout;
    while(1){
        pthread_mutex_lock(&q->mutex);
        log_warn("running %d", q->running);
        gettimeofday(&t, NULL);
        TIMEVAL_TO_TIMESPEC(&t, &timeout);
        timeout.tv_sec += 1;
        while(pthread_cond_wait(&q->cv, &q->mutex));
        jq_thread_t *link = q->thread_list;
        while(link != NULL){
            log_info("Traverseing id:%d, pthread_t: %p state: %d", link->jqid, link->ptid, link->tstate);
            link = link->next;
        }
        if(q->running == 0) break;
        pthread_mutex_unlock(&q->mutex);
    }
    for(int i=0; i< TESTNUMTHREADS; i++){
        qsort(&test_work[i],MAXWORK,sizeof(int), compare);
    }

    for(int j=0; j< MAXWORK; j++){
        for(int i=0; i< TESTNUMTHREADS; i++){
            int x = test_work[i].work[j];
            for(int k=i+1; k< TESTNUMTHREADS-1; k++){
                int y = test_work[k].work[j];
                    if(x !=0 && y!=0){
                        mu_neq(x,y);
                    }
            }
            if(test_work[i].work[j] != 0 ){
                //log_info("Work Done BY:%d %d",i, test_work[i].work[j]);
            }
        }
    }

    sleep(2);
        jq_thread_t *link = q->thread_list;
        while(link != NULL){
             volatile int tstate = link->tstate;
            log_info("Traverseing id:%d, pthread_t: %p state: %d", link->jqid, link->ptid, tstate);
            link = link->next;
        }
    return SUCCESS;
}

int all_tests() {
    MU_SUITE_BEGIN;
 //   mu_run_test(test_init);
  //  mu_run_test(test_enqueue);
    //sleep(2);
   // mu_run_test(test_dequeue);
   // mu_run_test(test_queue);
   // mu_run_test(test_contention);
    mu_run_test(test_jq);
    MU_SUITE_END;
}

RUN_TESTS(all_tests)
#endif

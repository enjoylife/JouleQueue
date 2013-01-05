/* File:  JouleQueue.c
 * Version: 1.0
 * Purpose: Implementation of a fast thread  conscious worker queue.
 * Author: Matthew Clemens
 * Copyright: Modified BSD, see LICENSE for more details 
*/
#ifndef _JouleQueue
#define _JouleQueue
#include <stdlib.h>
#include <string.h> //memset
#include <pthread.h> 
#include <assert.h>
#include <sys/time.h>
#include <unistd.h>
#include "JouleQueue.h"
#include "common.h"


// passed to threads on initialization
typedef struct start_type {
    jq_t q;
    jq_thread_t * thread; // pass our specfic thread data
} start_type_t;

static void * default_app_engine(int jqid, void * arg);
static void * thread_initilizer(void * arg);

int set_opt(jq_t q, jq_config_t c, void * opt){
    return SUCCESS;
}

/* Creates and waits for complete setup of threads, then gets out of their way. */
int jq_init(jq_t q, jq_config_t  config){

    // pthread stuff
    int status;
    status = pthread_attr_init (&q->attr);
    checkp(status);
    status = pthread_attr_setdetachstate (
        &q->attr, PTHREAD_CREATE_DETACHED);
    checkp(status);
    status = pthread_mutex_init (&q->mutex, NULL); 
    checkp(status);
    status = pthread_cond_init (&q->cv, NULL); 
    checkp(status);

    q->counter = 0;               
    q->running = 0;                    
    q->quit = 0; //bool                    

    // thread list 
    job_t * new = malloc(sizeof(job_t));       
    new->data = 0;
    new->next = NULL;
    // first node is always a dummy node
    q->head = q->tail = new;

    // config stuff
    if(config != NULL && config->app_engine !=NULL) {
        q->app_engine = config->app_engine;       
    } else {
        q->app_engine = default_app_engine;
    }

    if(config != NULL && config->max_threads <= JQ_MAX_THREAD_COUNT 
            && config->max_threads > 0){
        q->parallelism =  config->max_threads;
    } else {
        q->parallelism = JQ_DEFAULT_THREAD_COUNT;
    }
    if(config !=NULL && config->max_backoff <= JQ_MAX_BACKOFF){
        q->max_backoff = config->max_backoff;
    } else {
        q->max_backoff = JQ_DEFAULT_MAX_BACKOFF;
    }

    if(config !=NULL && config->max_retries <= JQ_MAX_RETRIES){
        q->max_retries = config->max_retries;
    } else {
        q->max_retries = JQ_DEFAULT_RETRIES;
    }

    log_info("para %d", q->parallelism);
    log_info("backoff %d", q->max_backoff);
    log_info("retries %d", q->max_retries);
    // we need to hold on to mutex now or else a speedy thread might signal 
    // before were done seting up
    pthread_mutex_lock(&q->mutex);

    // setup each thread
    q->thread_list = NULL;
    start_type_t  * start = malloc(sizeof(start_type_t));
    for(int i = 0; i< q->parallelism; i++){
        jq_thread_t * node = (jq_thread_t *) malloc(sizeof(jq_thread_t));
        jq_stat_t * stats = malloc(sizeof(jq_stat_t));
        checkm(node);

        node->jqid = i;
        node->tstate = TS_ALIVE;
        node->stats = *stats;

        node->next = q->thread_list;
        q->thread_list = node;

        memset(node->hazards, 0, MAX_HAZ_POINTERS);
        //node->rtotal = 0;
        //memset(node->rarray, 0, MAX_WASTE);

        start->q = q;
        start->thread = node; //pass in this threads stuff;

        pthread_create(&node->ptid, NULL, thread_initilizer, (void*)start);
        //if(config!=NULL && config->startup_func)
        //log_info("Pid %p", node->ptid);

    }
    // wait for them to be all setup, and allow them to continue initilizing
    while(pthread_cond_wait(&q->cv, &q->mutex));
    pthread_mutex_unlock(&q->mutex);
    log_info("threads ready %d", q->running);
    // commence work
    return SUCCESS;
}


int dequeue(queue_t * q, jq_thread_t * h, void ** data){
    job_t *head, *tail, *next, *changed;
    for(int i = 0;;i++){
        head = q->head;
        h->hazards[0] = head;
        if(q->head != head) continue;
        tail = q->tail;
        next = head->next;
        h->hazards[1] = next;
        if(q->head != head) continue;
        if(next == NULL) return EMPTY;
        if(head == tail) {
            while(1){ // help other threads
                changed = SYNC_CAS(&q->tail,tail, next);
                if(changed == tail){ 
                    //log_success("Made change #1: %d",i);
                    break;
                }else {
                    h->stats.contention++;
                    //log_info("Contention #1: %d",i);
                }
            }
            continue;
        }

        *data = next->data;
        changed = SYNC_CAS(&q->head,head,next);
        if( changed == head){
            //log_success("Made change #2: %d",i);
            break;
        }
    }
    remove_box(head);
    //log_warn("END DEQUEUE",1);
    return SUCCESS;
}

void * thread_initilizer(void * arg){

    // For backoff
    struct timeval t;
    struct timespec timeout;

    // setup thread specifc data
    start_type_t * start = (start_type_t *) arg;
    jq_t q = start->q;
    jq_thread_t * this = start->thread;

    // Lock; we can't finish before main
    pthread_mutex_lock(&q->mutex); 
    int new = SYNC_ADD(&q->running,1);
    pthread_mutex_unlock(&q->mutex);

    if(new == q->parallelism){ // were all ready tell main
        pthread_cond_signal(&q->cv);
    }

    int retries = 0;
    // testing
    void * data = malloc(sizeof(void*));
    int status;
    while(!q->quit){
        status = dequeue(q,this,&data);
        switch(status){
        case SUCCESS:
            q->app_engine(this->jqid, data);
            this->stats.work_done++;
            continue;
        case EMPTY: // TODO: add stats
            retries++;
            gettimeofday(&t, NULL);
            TIMEVAL_TO_TIMESPEC(&t, &timeout);
            unsigned int seed = (this->jqid * retries);
            int backoff = rand_r(&seed) & ((1<<q->max_backoff)-1);
            timeout.tv_sec +=  backoff;
            //status  = pthread_cond_timedwait(&q->cv, &q->mutex, &timeout);
            log_warn("RETRY #%d Sleeping: %d",retries, backoff);
            sleep(backoff);
            if(retries >= q->max_retries){ // TODO: robust handling 
                //pthread_mutex_lock(&q->mutex);
                this->tstate = TS_TERMINATED; 
                SYNC_ADD(&q->running,-1);
                //pthread_mutex_unlock(&q->mutex);
                pthread_cond_signal(&q->cv);
                return (void*)SUCCESS;
            }
            break;
        case FAILURE:
            log_warn("FAILURE",1);
            break;
        default:
            log_warn("UNKONN",1);
            break;

        }
    }
    assert(0);
    return SUCCESS;
}

int queue_destroy(queue_t * q){
    pthread_attr_destroy(&q->attr);
    pthread_mutex_destroy(&q->mutex);
    pthread_cond_destroy(&q->cv);
    jq_thread_t *link = q->thread_list;
    jq_thread_t * prev;
    while(link != NULL){

        prev = link;
        link = link->next;
        free(prev);
    }
    return SUCCESS;
}

int enqueue(queue_t * q, jq_thread_t * h, void * data){

    job_t * new = malloc(sizeof(job_t));
    checkm(new);
    new->data = data;
    new->next = NULL;
    job_t  * tail, *  next, * changed;
    for(int i=0;;i++){
        tail = q->tail;
        //h->hazards[0] = h == NULL ? tail : NULL;
        if(tail != q->tail) continue;
        next = tail->next;
        if(q->tail != tail) continue;
        if(next != NULL){

            while(1){ // help other threads
                changed = SYNC_CAS(&q->tail,tail, next);
                if(changed == tail){ 
                    //log_success("Made change #1: %d",i);
                    break;
                }else {
                    pthread_yield_np();
                    //log_info("Contention #1: %d",i);
                    break;
                }

            //log_warn("looping %d",i);
            }
                sched_yield();
                continue;

        }
        changed = SYNC_CAS(&tail->next,NULL,new);
        if( changed == NULL){
            assert(tail->next == new);
            assert(tail->next->data == new->data);
            assert(&(tail->next->data) == &new->data);
            tail = q->tail;
            //log_success("Made change #2: %d",i);
            break;
        }
        //while(1){
            changed = SYNC_CAS(&q->tail,tail,new);
                if(changed == tail){ 
                    //log_success("Made change #3: %d",i);
                    break;
                }else {
                    //log_info("Contention #3: %d",i);
                }
       // log_warn("looping %d",i);
        //}
        
    }
    return SUCCESS;
}

int remove_box(job_t * box){
    free(box);
    return SUCCESS;
}


int job_traverse(job_t *box){
    job_t * link = box;
    log_info("Traversing",1);
    while(link != NULL){

        log_info("Data %d", *(int*)(link->data));
        link = link->next;
    }
    return SUCCESS;
}

int jq_thread_traverse(jq_t q){
    jq_thread_t *link = q->thread_list;
    while(link != NULL){
        log_info("Traverseing id:%d, pthread_t: %p", link->jqid, link->ptid);
        link = link->next;
    }
    return SUCCESS;
}

static void * default_app_engine( int jqid,void * arg){
    log_success("Data Loc: %p Worker %d",arg, jqid);
    return SUCCESS;
}
#endif // _JouleQueue

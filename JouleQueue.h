/* File:  JouleQueue.h
 * Version: 1.0
 * Purpose: Public datatypes and functions for supporting the JouleQueue api
 * Author: Matthew Clemens
 * Copyright: Modified BSD, see LICENSE for more details 
*/
/* What is JouleQueue?
 * It is a thread conscious worker queue with the ablity to have a
 * work processing flow behave in chain gang esque configuration.
 * In other words multiple threads may be simultaneously consuming and 
 * producing work and this work may be then consumed by other JouleQueues
 * waiting on work to be handed off to them.
*/

#ifndef _JouleQueue_h
#define _JouleQueue_h

#include <pthread.h> 
#include "common.h"

#define JQ_DEFAULT_THREAD_COUNT 1
#define JQ_MAX_THREAD_COUNT 10

#define JQ_DEFAULT_MAX_BACKOFF 2
#define JQ_MAX_BACKOFF 3

#define JQ_DEFAULT_RETRIES 4
#define JQ_MAX_RETRIES 8

#define MAX_HAZ_POINTERS 2
#define MAX_WASTE 40

typedef void * work_unit_t;
typedef void *(*pthread_func)(void*);
typedef void *(*app_func)(int, void*);

typedef enum TSTATE {           /* Thread states */
    TS_ALIVE,                   /* Thread is alive */
    TS_TERMINATED,              /* Thread terminated, not yet joined */
    TS_JOINED                   /* Thread terminated, and joined */
} TSTATE;

// per JouleQueue information / Statistics
typedef struct jq_stat {
    unsigned long long work_done;
    unsigned long long contention;
    unsigned long long found_empty;
} jq_stat_t;


typedef struct config {
    int      max_threads;
    int      max_backoff;
    int      max_retries;
    app_func app_engine;
} config_t;


// to hold the data to be worked on
typedef struct job {
    void *  data;
    struct job * next;
} job_t;

// per thread stuff
typedef struct jq_thread {
    pthread_t             ptid;   // pthread_create
    int                   jqid;   // internal id
    enum TSTATE           tstate; // what this thread is doing
    struct jq_thread *    next;   // linked list 
    jq_stat_t             stats;

    // hazard stuff
    job_t *         hazards[MAX_HAZ_POINTERS]; // held memory
    //int            rtotal; //occupancy
    //job_t *        rarray[];         // to be released
    //int            rcurrent;

} jq_thread_t;

typedef struct queue {
    job_t *          head;               // first job is head->next->data 
    job_t *          tail;               // most recent enqueue 
    jq_thread_t *    thread_list;        // specfic to each thread
    jq_stat_t *      stats;              // not lock protected
    pthread_func     thread_initilizer;  // inits thread locals
    pthread_func     stat_engine;        // reads thread specifics, racey
    app_func         app_engine;         // configurable, no assumptions
    pthread_func     thread_destroyer;   
    pthread_mutex_t  mutex;             
    pthread_cond_t   cv;                 // wait for work 
    pthread_attr_t   attr;               // create detached threads 
    int              quit;               // set when workq should quit 
    int              parallelism;        // number of threads required 
    int              counter;            // current number of threads 
    int              running;               // number of running threads 
    int              max_backoff;
    int              max_retries;
} queue_t;

typedef queue_t* jq_t;
typedef config_t * jq_config_t;


int jq_init(jq_t, jq_config_t);
int jq_add(jq_t, void * );

int jq_nq(jq_t, void *);
int jq_dq(jq_t, void *);
int jq_stat(jq_t, jq_stat_t );
int jq_link(jq_t, jq_t, jq_config_t);
int jq_config(jq_t, jq_config_t, int);
int jq_thread_traverse(jq_t);

int haz_init(jq_thread_t * haz , int num_threads );
int queue_init(queue_t * q, int threads, pthread_func app_engine);
int queue_destroy(queue_t * q);

int enqueue(queue_t * q, jq_thread_t * h, void * data);
//int enqueue_haz(queue_t * q, work_unit_t *data);
int job_traverse(job_t *box);
int remove_box(job_t * box);
int dequeue(queue_t * q, jq_thread_t * h, void ** data);

int jq_config_init(jq_config_t );

#endif // _JouleQueue_h


/* File: minunit.h
 * Version: 1.0
 * Purpose: Definitions for testing 
 * Author: Matthew Clemens
 * Copyright: BSD 
*/


#ifndef _minunit_h
#define _minunit_h

#include <stdio.h>
#include <stdlib.h>
#include "common.h"


#define MU_SUITE_BEGIN int mu_message =0 

#define mu_truthy(test) if(!test){_log_it(TESTFAIL,"",1); mu_message++;return FAILURE;}

#define mu_success(test) if(test !=SUCCESS){_log_it(TESTFAIL,"",1); mu_message++;return FAILURE;}

#define mu_falsey(test) if(test){_log_it(TESTFAIL,"",1); mu_message++;return FAILURE;}

#define mu_eq(test, test2) if(test !=test2){_log_it(TESTFAIL,"",1);mu_message++;return FAILURE; }
#define mu_neq(test, test2) if(test == test2){_log_it(TESTFAIL,"",1); mu_message++;return FAILURE;}

#define mu_run_test(test) printf("\n-----%s", " " #test "\n"); mu_message = test(); tests_run++; 

#define MU_SUITE_END return mu_message


#define RUN_TESTS(name) int main(int argc, char *argv[]) {\
    argc = 1; \
    fprintf( stderr, "------\n" "[" BOLDCYAN "RUNNING:" RESET "]"" %s \n", argv[0]);\
        name(); \
        if (mu_message != 0) {\
            fprintf( stderr,"\n\n[" BOLDRED "FAILED: %d" RESET "]" "\n", mu_message);\
        }\
        else {\
            fprintf(stderr, "\n\n[" BOLDGREEN "ALL TESTS PASSED" RESET "]" "\n");\
        }\
    fprintf(stderr, "Tests run: %d\n", tests_run);\
      pthread_exit((void*)1);\
}
int tests_run;
int mu_message;

#endif

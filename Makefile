#CFLAGS= -pthread -g -D NDEBUG -std=c99 -pedantic  $(DEBUGFLAGS)
CFLAGS= -pthread -g -O3 -std=c99 -pedantic -Wall -Wformat=0 $(DEBUGFLAGS)
CC=gcc
#CC=c99

SOURCES= JouleQueue_test.c
PROGRAMS=$(SOURCES:.c=)
all:	${PROGRAMS}


JouleQueue_test: JouleQueue.h JouleQueue.c JouleQueue_test.c common.h
	${CC} ${CFLAGS} ${LDFLAGS} -o $@  $^

clean:
	@rm -rf $(PROGRAMS) *.o
	@rm -rf  *.dSYM
recompile:	clean all

* File:  JouleQueue.md
* Version: 1.0
* Purpose: Documentation and architecture overview of JouleQueue.
* Author: Matthew Clemens
* Copyright: Modified BSD, see LICENSE for more details 

JouleQueue (worker queue)
======================
> "derived unit of energy/work."

## Datatypes

```c
jq_t
jq_config_t
jq_stat_t
```

## API
```c
jq_init(jq_t q, jq_config_t config);
jq_nq(jq_t q, void * data);
jq_dq(jq_t q, void * data);
jq_stat(jq_t q, jq_stat_t stat);
jq_link(jq_t q, jq_t q2, jq_config_t config);
jq_config(jq_config_t c, ...);
```

## Architecture

We utilize a non blocking queue, which threads pick jobs off of. From there the jobs are consumed by the individual threads. They pick up jobs effecinetly by utilizing a few tricks. Firstly they dont contend over scarce resources. And if their is contention, they do it in the most effecinet way currently known. This done with tricks involving toxic garbaje pointer lookups, and inter-program caching.
Once the threads are off and running on these jobs, they also utilize the latest polling and asyncronous io on file descriptors. They can stay actively doing this for a set amount of time, or they can be put to sleep on a condition to be broadcasted by a signal handler. The signal handler takes a look at the non-blocking queue and a set of thread globals, waking up on work to be done, or shutting down on program close. 

Configurable options on the JouleQueue are the functions which work on the jobs themselves, the timeing and memory parameters, and the performance enhancements. The set's and get's on this JouleQueue are through the `jq_config(queue, config, CONFIG)` function. 

Detailed stats are taken for the program's running times, summs and work. The interaction with this is in the `jq_stat(queue, stats)` function.  Stat snapshots are taken by utilizing Posix semaphores.

We use mutexs for rarely changed inter-variables, aka num of threads sleeping, etc. 

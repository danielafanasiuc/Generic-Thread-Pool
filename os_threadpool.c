// SPDX-License-Identifier: BSD-3-Clause

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <unistd.h>

#include "os_threadpool.h"
#include "log/log.h"
#include "utils.h"

/* Create a task that would be executed by a thread. */
os_task_t *create_task(void (*action)(void *), void *arg, void (*destroy_arg)(void *))
{
	os_task_t *t;

	t = malloc(sizeof(*t));
	DIE(t == NULL, "malloc");

	t->action = action;		// the function
	t->argument = arg;		// arguments for the function
	t->destroy_arg = destroy_arg;	// destroy argument function

	return t;
}

/* Destroy task. */
void destroy_task(os_task_t *t)
{
	if (t->destroy_arg != NULL)
		t->destroy_arg(t->argument);
	free(t);
}

/* Put a new task to threadpool task queue. */
void enqueue_task(os_threadpool_t *tp, os_task_t *t)
{
	assert(tp != NULL);
	assert(t != NULL);

	pthread_mutex_lock(&tp->lock);

	list_add(&tp->head, &t->list);

	pthread_mutex_unlock(&tp->lock);
	// signal a single waiting thread to wake and execute the task
	pthread_cond_signal(&tp->queue_condition);
}

/*
 * Check if queue is empty.
 * This function should be called in a synchronized manner.
 */
static int queue_is_empty(os_threadpool_t *tp)
{
	return list_empty(&tp->head);
}

/*
 * Get a task from threadpool task queue.
 * Block if no task is available.
 * Return NULL if work is complete, i.e. no task will become available,
 * i.e. all threads are going to block.
 */

os_task_t *dequeue_task(os_threadpool_t *tp)
{
	os_task_t *t;

	pthread_mutex_lock(&tp->lock);
	while (!tp->is_enabled)
		pthread_cond_wait(&tp->queue_condition, &tp->lock);

	// consider this thread as a NOT working thread
	tp->worker_threads_count--;

	// if the queue is empty and there are no worker threads
	// signal the completion condition
	if (queue_is_empty(tp) && tp->worker_threads_count == 0) {
		pthread_mutex_unlock(&tp->lock);
		pthread_cond_signal(&tp->completed_condition);
		// return in order to not a create a race condition
		// between the broadcast and wait of the thread
		// (if broadcast calls before the wait, the thread will wait infinetely)
		return NULL;
	}

	// If no task in queue, pthread_cond_wait() waits for a signal
	// and unlocks the mutex
	// Whenever the signal is set, pthread_cond_wait() locks back the mutex
	while (queue_is_empty(tp)) {
		pthread_cond_wait(&tp->queue_condition, &tp->lock);
		// if wait_for_completion() broadcasts for all waiting threads
		// and thus is_completed is set to one, return from all threads
		if (tp->is_completed == 1) {
			pthread_mutex_unlock(&tp->lock);
			return NULL;
		}
	}

	// if the signal for the queue_condition variable is set,
	// consider the thread a working one
	tp->worker_threads_count++;


	// remove task from queue
	os_list_node_t *first_node = tp->head.next;

	t = list_entry(first_node, os_task_t, list);
	list_del(first_node);

	pthread_mutex_unlock(&tp->lock);
	return t;
}

/* Loop function for threads */
static void *thread_loop_function(void *arg)
{
	os_threadpool_t *tp = (os_threadpool_t *) arg;

	while (1) {
		os_task_t *t;

		t = dequeue_task(tp);
		if (t == NULL)
			break;
		t->action(t->argument);
		destroy_task(t);
	}

	return NULL;
}

/* Wait completion of all threads. This is to be called by the main thread. */
void wait_for_completion(os_threadpool_t *tp)
{
	pthread_mutex_lock(&tp->lock);

	// if there are working threads, wait
	if (tp->worker_threads_count > 0)
		pthread_cond_wait(&tp->completed_condition, &tp->lock);

	tp->is_completed = 1;

	pthread_mutex_unlock(&tp->lock);
	// signal all waiting threads
	pthread_cond_broadcast(&tp->queue_condition);

	/* Join all worker threads. */
	for (unsigned int i = 0; i < tp->num_threads; i++)
		pthread_join(tp->threads[i], NULL);
}

/* Create a new threadpool. */
os_threadpool_t *create_threadpool(unsigned int num_threads)
{
	os_threadpool_t *tp = NULL;
	int rc;

	tp = malloc(sizeof(*tp));
	DIE(tp == NULL, "malloc");

	list_init(&tp->head);

	DIE(pthread_mutex_init(&tp->lock, NULL) != 0, "mutex_init");
	DIE(pthread_cond_init(&tp->queue_condition, NULL) != 0, "queue_condition_init");
	DIE(pthread_cond_init(&tp->completed_condition, NULL) != 0, "complete_condition_init");

	tp->worker_threads_count = num_threads;
	tp->is_enabled = 0;
	tp->is_completed = 0;

	tp->num_threads = num_threads;
	tp->threads = malloc(num_threads * sizeof(*tp->threads));
	DIE(tp->threads == NULL, "malloc");

	for (unsigned int i = 0; i < num_threads; ++i) {
		rc = pthread_create(&tp->threads[i], NULL, &thread_loop_function, (void *) tp);
		DIE(rc < 0, "pthread_create");
	}

	return tp;
}

/* Destroy a threadpool. Assume all threads have been joined. */
void destroy_threadpool(os_threadpool_t *tp)
{
	os_list_node_t *n, *p;

	pthread_mutex_destroy(&tp->lock);
	pthread_cond_destroy(&tp->queue_condition);
	pthread_cond_destroy(&tp->completed_condition);

	list_for_each_safe(n, p, &tp->head) {
		list_del(n);
		destroy_task(list_entry(n, os_task_t, list));
	}

	free(tp->threads);
	free(tp);
}

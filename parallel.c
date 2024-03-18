// SPDX-License-Identifier: BSD-3-Clause

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <time.h>

#include "os_graph.h"
#include "os_threadpool.h"
#include "log/log.h"
#include "utils.h"

#define NUM_THREADS		4

static int sum;
static os_graph_t *graph;
static os_threadpool_t *tp;

pthread_mutex_t graph_lock;

typedef struct {
	unsigned int idx;
} graph_task_arg_t;

static void process_node(void *_arg)
{
	graph_task_arg_t *arg = (graph_task_arg_t *) _arg;

	os_node_t *node;

	node = graph->nodes[arg->idx];

	pthread_mutex_lock(&graph_lock);
	sum += node->info;
	graph->visited[arg->idx] = DONE;

	for (unsigned int i = 0; i < node->num_neighbours; ++i) {
		if (graph->visited[node->neighbours[i]] == NOT_VISITED) {
			graph->visited[node->neighbours[i]] = PROCESSING;
			graph_task_arg_t *neighbour_arg = malloc(sizeof(graph_task_arg_t));

			neighbour_arg->idx = node->neighbours[i];

			os_task_t *task = create_task(process_node, neighbour_arg, free);

			enqueue_task(tp, task);
		}
	}

	pthread_mutex_unlock(&graph_lock);

	pthread_mutex_lock(&tp->lock);
	if (!tp->is_enabled) {
		tp->is_enabled = 1;
		pthread_cond_broadcast(&tp->queue_condition);
	}
	pthread_mutex_unlock(&tp->lock);
}

int main(int argc, char *argv[])
{
	FILE *input_file;

	if (argc != 2) {
		fprintf(stderr, "Usage: %s input_file\n", argv[0]);
		exit(EXIT_FAILURE);
	}

	input_file = fopen(argv[1], "r");
	DIE(input_file == NULL, "fopen");

	graph = create_graph_from_file(input_file);

	DIE(pthread_mutex_init(&graph_lock, NULL) != 0, "graph_mutex_init");

	tp = create_threadpool(NUM_THREADS);

	graph_task_arg_t *first_arg = malloc(sizeof(graph_task_arg_t));

	first_arg->idx = 0;

	process_node(first_arg);

	wait_for_completion(tp);
	destroy_threadpool(tp);

	printf("%d", sum);

	pthread_mutex_destroy(&graph_lock);

	return 0;
}

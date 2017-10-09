#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define RECORD_SIZE sizeof(movies) / sizeof(movies[0])

typedef enum { false, true } bool;
enum PlanNodeType { select_node_t, scan_node_t };

typedef struct record {
	unsigned int id;
	char *title;
	char *genres;
} Record;

Record movies[] = {
	1, "fight club", "psychological thriller",
	2, "the pianist", "drama"
};

// empty string vs NULL
Record empty_record = { 1, "", ""};

typedef struct scan_node_state {
	unsigned int current_record_idx;
} ScanNodeState;

typedef struct select_node_state {
	bool (*filter)(Record*);
} SelectNodeState;

typedef struct PlanNode PlanNode;
struct PlanNode {
	Record *(*next)(void *self);
	PlanNode *child;
	enum PlanNodeType nodeType;
	union {
		SelectNodeState *selectNodeState;
		ScanNodeState *scanNodeState; 
	};
};


Record *scanNext(void *self);

PlanNode *makeScanNode(void) {
	PlanNode *node = malloc(sizeof(PlanNode));

	if (node != NULL) {
		node->next = &scanNext;
		node->scanNodeState = malloc(sizeof(ScanNodeState));
		node->scanNodeState->current_record_idx = 0;
	}
	return node;
}

Record *scanNext(void *self) {
	PlanNode *scan_node = self;
	unsigned int current_record_idx = scan_node->scanNodeState->current_record_idx;

	if (current_record_idx < RECORD_SIZE)
		return &movies[scan_node->scanNodeState->current_record_idx++];
	else 
		return NULL;
}

// select node 
Record *selectNext(void *self);

PlanNode *makeSelectNode(bool (*predicate)(Record *)) {

	PlanNode *node = malloc(sizeof(PlanNode));
	if (node != NULL) {
		node->next = &selectNext;
		node->selectNodeState = malloc(sizeof(SelectNodeState));
		node->selectNodeState->filter = predicate;
	}
	return node;
}

Record *selectNext(void *self) {
	PlanNode *select_node = self;
	PlanNode *child_node = select_node->child;

	for (;;) {
		Record *current_record = child_node->next(child_node);

		if (current_record != NULL) {
			if (select_node->selectNodeState->filter(current_record)) {
				return current_record;
			}
		} else {
			return NULL;
		}

	}

	return NULL;
}

bool filterPredicate(Record *source) {
	return (strcmp(source->title, "the pianist") == 0) ? true : false;
}

bool fn(Record *source) {
	return (strcmp(source->title, "fight club") == 0) ? true : false;
}

int main(void) {
// [
//   ["AVERAGE"],
//   ["PROJECTION", ["rating"]],
//   ["SELECTION", ["movie_id", "EQUALS", "5000"]],
//   ["FILESCAN", ["ratings"]]
// ]

	PlanNode *node[] = { makeSelectNode(fn), makeScanNode() };
	PlanNode *root = node[0];
	root->child = node[1];
	printf("should be fight club: %s\n", root->next(root)->title );

	PlanNode *node2[] = { makeSelectNode(filterPredicate), makeScanNode() };
	PlanNode *root2 = node2[0];
	root2->child = node2[1];
	printf("should be the pianist: %s\n", root2->next(root2)->title );

} 
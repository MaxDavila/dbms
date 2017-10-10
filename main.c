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
	double rating;
} Record;

Record movies[] = {
	1, "fight club", "psychological thriller", 4.5,
	2, "the pianist", "drama", 5.0
};

// empty string vs NULL
Record empty_record = { 1, "", ""};

typedef struct scan_node_state {
	unsigned int current_record_idx;
} ScanNodeState;

typedef struct select_node_state {
	bool (*filter)(Record*);
} SelectNodeState;

typedef struct project_node_state {
	char **proj_fields;
	int proj_field_count;
} ProjectionNodeState;

typedef struct PlanNode PlanNode;
struct PlanNode {
	Record *(*next)(void *self);
	PlanNode *child;
	enum PlanNodeType nodeType;
	union {
		SelectNodeState *selectNodeState;
		ScanNodeState *scanNodeState; 
		ProjectionNodeState *projectNodeState;
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


Record *projectNext(void *self);
PlanNode *makeProjectNode(char *fields[], int field_count) {

	PlanNode *node = malloc(sizeof(PlanNode));
	if (node != NULL) {
		node->next = &projectNext;
		node->projectNodeState = malloc(sizeof(ProjectionNodeState));
		node->projectNodeState->proj_fields = fields;
		node->projectNodeState->proj_field_count = field_count;
	}
	return node;
}


Record *project(Record *source, char *proj_fields[], int field_count) {
	// new record with filtered columns 
	Record *projected = malloc(sizeof(Record));

	for (int i = 0; i < field_count; i++) {
		if (strcmp(proj_fields[i], "id") == 0) {
			projected->id = source->id;
		} else if (strcmp(proj_fields[i], "title") == 0) {
			projected->title = source->title;
		} else if (strcmp(proj_fields[i], "genres") == 0) {
			projected->genres = source->genres;
		} else if (strcmp(proj_fields[i], "rating") == 0) {
			projected->rating = source->rating;
		}
	}
	return projected;
}

Record *projectNext(void *self) {
	PlanNode *project_node = self;
	PlanNode *child_node = project_node->child;
	ProjectionNodeState *project_node_state = project_node->projectNodeState;

	Record *record = child_node->next(child_node);
	if (record != NULL) {
		printf("HIT\n");
		record = project(record, project_node_state->proj_fields, project_node_state->proj_field_count);
	}
	return record;
}




Record *averageNext(void *self) {
	PlanNode *avg_node = self;
	PlanNode *child_node = avg_node->child;
	Record *record;
	unsigned int count = 0;
	unsigned int sum = 0;

	while ((record = child_node->next(child_node)) != NULL) {
		count++;
		sum += record->rating;
	}

	Record *newRecord = malloc(sizeof(Record));
	newRecord->rating = sum / count;
	return newRecord;
}

PlanNode *makeAverageNode() {

	PlanNode *node = malloc(sizeof(PlanNode));
	if (node != NULL) {
		node->next = &averageNext;
	}
	return node;
}


bool filterPredicate(Record *source) {
	return (strcmp(source->title, "the pianist") == 0) ? true : false;
}

bool fn(Record *source) {
	return (source->id == 1) ? true : false;
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

	char *proj_fields[] = { "rating" };
	PlanNode *node2[] = { makeAverageNode(), makeProjectNode(proj_fields, 1), makeSelectNode(filterPredicate), makeScanNode() };
	PlanNode *root2 = node2[0];
	root2->child = node2[1];
	root2->child->child = node2[2];
	root2->child->child->child = node2[3];
	printf("should be 5.0: %f\n", root2->next(root2)->rating );
	// printf("should be null: %s\n", root2->next(root2)->title );


} 
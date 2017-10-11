#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define RECORD_SIZE sizeof(movies) / sizeof(movies[0])

typedef enum { false, true } bool;
enum PlanNodeType { select_node_t, scan_node_t };
enum RecordType { movie_t, rating_t };

typedef struct record {
    enum RecordType recordType;
} Record;

typedef struct movie {
    Record record;
	unsigned int id;
	char *title;
	char *genres;
	double rating;
} Movie;

typedef struct rating {
    Record record;
    unsigned int user_id;
    unsigned int movie_id;
    double rating;
} Rating;

Movie movies[] = {
        movie_t , 1, "fight club", "psychological thriller", 4.5,
        movie_t , 2, "the pianist", "drama", 5.0,
        movie_t , 3, "jumanji", "adventure", 3.5
};

Rating ratings[] = {
        rating_t, 1, 1, 4.5,
        rating_t, 1, 2, 5.0,
        rating_t, 1, 3, 3.5
};

typedef struct scan_node_state {
	unsigned int current_record_idx;
    enum RecordType recordType;
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

PlanNode *makeScanNode(enum RecordType recordType) {
	PlanNode *node = malloc(sizeof(PlanNode));

	if (node != NULL) {
		node->next = &scanNext;
		node->scanNodeState = malloc(sizeof(ScanNodeState));
		node->scanNodeState->current_record_idx = 0;
        node->scanNodeState->recordType = recordType;
	}
	return node;
}

Record *scanNext(void *self) {
	PlanNode *scan_node = self;
	unsigned int current_record_idx = scan_node->scanNodeState->current_record_idx;

	if (current_record_idx < RECORD_SIZE)
		return (Record *) &movies[scan_node->scanNodeState->current_record_idx++];
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

Movie *projectMovie(Movie *source, Movie *projection, char *proj_fields[], int field_count) {
    for (int i = 0; i < field_count; i++) {
        if (strcmp(proj_fields[i], "id") == 0) {
            projection->id = source->id;
        } else if (strcmp(proj_fields[i], "title") == 0) {
            projection->title = source->title;
        } else if (strcmp(proj_fields[i], "genres") == 0) {
            projection->genres = source->genres;
        } else if (strcmp(proj_fields[i], "rating") == 0) {
            projection->rating = source->rating;
        }
    }
    return projection;
}

Rating *projectRating(Rating *source, Rating *projection, char *proj_fields[], int field_count) {
    for (int i = 0; i < field_count; i++) {
        if (strcmp(proj_fields[i], "user_id") == 0) {
            projection->user_id = source->user_id;
        } else if (strcmp(proj_fields[i], "movie_id") == 0) {
            projection->movie_id = source->movie_id;
        } else if (strcmp(proj_fields[i], "rating") == 0) {
            projection->rating = source->rating;
        }
    }
    return projection;
}

void *project(Record *src, char *proj_fields[], int field_count) {
	// new record with filtered columns 
    switch (src->recordType) {
        case movie_t: {
            Movie *projection = malloc(sizeof(Movie));
            Movie *source = (Movie *) src;
            projectMovie(source, projection, proj_fields, field_count);
            return projection;
        }
        case rating_t: {
            Rating *projection = malloc(sizeof(Rating));
            Rating *source = (Rating *) src;
            projectRating(source, projection, proj_fields, field_count);
            return projection;
        }
    }
}

Record *projectNext(void *self) {
	PlanNode *project_node = self;
	PlanNode *child_node = project_node->child;
	ProjectionNodeState *project_node_state = project_node->projectNodeState;

	Record *record = child_node->next(child_node);
	if (record != NULL) {
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
		sum += (( Movie *) record)->rating;
	}

	Movie *newRecord = malloc(sizeof(Record));
	newRecord->rating = sum / count;
	return (Record *) newRecord;
}

PlanNode *makeAverageNode() {

	PlanNode *node = malloc(sizeof(PlanNode));
	if (node != NULL) {
		node->next = &averageNext;
	}
	return node;
}


bool filterPredicate(Record *source) {
	return (strcmp(((Movie *) source)->title, "the pianist") == 0) ? true : false;
}

bool fn(Record *source) {
	return (((Movie*) source)->id == 1) ? true : false;
}

bool fnRating(Record *source) {
    return (((Rating*) source)->rating == 3.5) ? true : false;
}

int main(void) {
// [
//   ["AVERAGE"],
//   ["PROJECTION", ["rating"]],
//   ["SELECTION", ["movie_id", "EQUALS", "5000"]],
//   ["FILESCAN", ["ratings"]]
// ]

    // makeScanNode(RecordType);
	PlanNode *node[] = { makeSelectNode(fn), makeScanNode(movie_t) };
	PlanNode *root = node[0];
	root->child = node[1];
	printf("should be fight club: %s\n", ((Movie *) root->next(root))->title );

	 char *proj_fields[] = { "rating" };
	 PlanNode *node2[] = { makeAverageNode(), makeProjectNode(proj_fields, 1), makeSelectNode(filterPredicate), makeScanNode(movie_t) };
	 PlanNode *root2 = node2[0];
	 root2->child = node2[1];
	 root2->child->child = node2[2];
	 root2->child->child->child = node2[3];
	 printf("should be 5.0: %f\n", ((Movie *) root2->next(root2))->rating );
//	 printf("should be null: %s\n", ((Movie *) root2->next(root2))->title );

    PlanNode *node3[] = { makeSelectNode(fnRating), makeScanNode(rating_t) };
    PlanNode *root3 = node[0];
    root3->child = node3[1];
    printf("rating should be 3.5%1.f\n", ((Rating *) root->next(root3))->rating );
}
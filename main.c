#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define RECORD_SIZE sizeof(movies) / sizeof(movies[0])

typedef enum { false, true } bool;
enum PlanNodeType { select_node_t, scan_node_t };
enum RecordType { movie_t, rating_t, movie_rating_t };

typedef struct record {
    enum RecordType record_type;
} Record;

typedef struct movie {
    Record record;
	unsigned int id;
	char *title;
	char *genres;
} Movie;

typedef struct rating {
    Record record;
    unsigned int user_id;
    unsigned int movie_id;
    double rating;
} Rating;

typedef struct joined_record {
    Record record;
    Record inner;
    Record outer;
} JoinedRecord;

Movie movies[] = {
        movie_t , 1, "fight club", "psychological thriller",
        movie_t , 2, "the pianist", "drama",
        movie_t , 3, "jumanji", "adventure"
};

Rating ratings[] = {
        rating_t, 1, 1, 4.5,
        rating_t, 1, 2, 5.0,
        rating_t, 1, 3, 3.5
};


typedef struct PlanNode PlanNode;
struct PlanNode {
	Record *(*next)(void *self);
	PlanNode *left_tree;
    PlanNode *right_tree;
    void *(*reset)(void);
	enum PlanNodeType nodeType;
};

typedef struct scan_node {
	PlanNode plan_node;
	unsigned int current_record_idx;
    enum RecordType record_type;
} ScanNode;

typedef struct select_node {
	PlanNode plan_node;
	bool (*filter)(Record*);
} SelectNode;

typedef struct project_node {
	PlanNode plan_node;
	char **proj_fields;
	int proj_field_count;
} ProjectNode;

typedef struct nested_loop_join_node {
    PlanNode plan_node;
    bool (*join)(Record *r, Record *s);
    Record *current_outer_record;
} NestedLoopJoinNode;


Record *scanNext(void *self);

ScanNode *makeScanNode(enum RecordType record_type) {
	ScanNode *node = malloc(sizeof(ScanNode));

	if (node != NULL) {
		node->plan_node.next = &scanNext;
		node->current_record_idx = 0;
        node->record_type = record_type;
	}
	return node;
}

Record *scanNext(void *self) {
	ScanNode *scan_node = self;
	unsigned int current_record_idx = scan_node->current_record_idx;
	switch (scan_node->record_type) {
        case movie_t: {
        	if (current_record_idx < RECORD_SIZE)
				return (Record *) &movies[scan_node->current_record_idx++];
			else
				return NULL;
    	}
    	case rating_t: {
    		if (current_record_idx < RECORD_SIZE)
				return (Record *) &ratings[scan_node->current_record_idx++];
			else
				return NULL;
    	}
        case movie_rating_t: {
            return NULL;
        }
	}

}

// select node
Record *selectNext(void *self);

SelectNode *makeSelectNode(bool (*predicate)(Record *)) {

	SelectNode *node = malloc(sizeof(SelectNode));
	if (node != NULL) {
		node->plan_node.next = &selectNext;
		node->filter = predicate;
	}
	return node;
}

Record *selectNext(void *self) {
	SelectNode *select_node = self;
	PlanNode *child_node = ((PlanNode *) self)->left_tree;

	for (;;) {
		Record *current_record = child_node->next(child_node);
		if (current_record != NULL) {
			if (select_node->filter(current_record)) {
				return current_record;
			}
		} else {
			return NULL;
		}

	}

	return NULL;
}


Record *projectNext(void *self);
ProjectNode *makeProjectNode(char *fields[], int field_count) {

	ProjectNode *node = malloc(sizeof(ProjectNode));

	if (node != NULL) {
		node->plan_node.next = &projectNext;
		node->proj_fields = fields;
		node->proj_field_count = field_count;
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
    switch (src->record_type) {
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
        case movie_rating_t: {
            return NULL;
        }
    }
}

Record *projectNext(void *self) {
	PlanNode *plan_node = self;
	PlanNode *child_node = plan_node->left_tree;
	ProjectNode *project_node = (ProjectNode *) self;

	Record *record = child_node->next(child_node);
	if (record != NULL) {
		record = project(record, project_node->proj_fields, project_node->proj_field_count);
	}
	return record;
}

Record *averageNext(void *self) {
	PlanNode *avg_node = self;
	PlanNode *child_node = avg_node->left_tree;
	Record *record;
	unsigned int count = 0;
	double sum = 0;

	// TODO: derive type and field to average from record-> type
	while ((record = child_node->next(child_node)) != NULL) {
		count++;
		sum += (( Rating *) record)->rating;
	}

	Rating *newRecord = malloc(sizeof(Record));
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

Record *nestedLoopNext(void *self);

NestedLoopJoinNode *makeNestedLoopJoinNode(bool (*theta_fn)(Record *r, Record *s)) {

    NestedLoopJoinNode *node = malloc(sizeof(NestedLoopJoinNode));
    if (node != NULL) {
        node->plan_node.next = &nestedLoopNext;
        node->join = theta_fn;
    }
    return node;
}

// It scans the inner relation to join with current outer tuple.
Record *nestedLoopNext(void *self) {
    NestedLoopJoinNode *nested_loop_node = self;
    PlanNode *outer_node = ((PlanNode *) self)->left_tree;
    PlanNode *inner_node = ((PlanNode *) self)->right_tree;
    
    Record *inner;
    Record *outer = (nested_loop_node->current_outer_record == NULL) ? outer_node->next(outer_node) : nested_loop_node->current_outer_record;

    // we've run out of records to scan once the outernode returns no more tuples NULL
    if (outer == NULL) 
        return NULL;

    // how do i represent inner + outer
    while ((inner = inner_node->next(inner_node)) != NULL) {
        JoinedRecord *joined_record = malloc(sizeof(JoinedRecord));
        joined_record->inner = *inner;
        joined_record->outer = *outer; 
        return (Record *) joined_record;
    }

    // TODO: reset inner relation
    inner_node->reset();
    nested_loop_node->current_outer_record = outer_node->next(outer_node);

    return NULL;
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

	PlanNode *node[] = { (PlanNode *) makeSelectNode(fn), (PlanNode *) makeScanNode(movie_t) };
	PlanNode *root = node[0];
	root->left_tree = node[1];
	printf("should be fight club: %s\n", ((Movie *) root->next(root))->title );

	char *proj_fields[] = { "title" };
	PlanNode *node2[] = { (PlanNode *) makeProjectNode(proj_fields, 1), (PlanNode *) makeSelectNode(filterPredicate),
		(PlanNode *) makeScanNode(movie_t) };
	PlanNode *root2 = node2[0];
	root2->left_tree = node2[1];
    root2->left_tree->left_tree = node2[2];
	printf("should be the pianist: %s\n", ((Movie *) root2->next(root2))->title );

	char *proj_fields3[] = { "rating" };
	PlanNode *node3[] = { (PlanNode *) makeAverageNode(), (PlanNode *) makeProjectNode(proj_fields3, 1),
	 (PlanNode *) makeSelectNode(fnRating), (PlanNode *) makeScanNode(rating_t) };
	PlanNode *root3 = node3[0];
	root3->left_tree = node3[1];
	root3->left_tree->left_tree = node3[2];
	root3->left_tree->left_tree->left_tree = node3[3];
	printf("should be 3.5: %f\n", ((Rating *) root3->next(root3))->rating );

    // PlanNode *node3[] = { (PlanNode *) makeSelectNode(fnRating), (PlanNode *) makeScanNode(rating_t) };
    // PlanNode *root3 = node3[0];
    // root3->child = node3[1];
    // printf("rating should be 3.5 %f\n", ((Rating *) root3->next(root3))->rating );

}
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
} Movie;

typedef struct rating {
    Record record;
    unsigned int user_id;
    unsigned int movie_id;
    double rating;
} Rating;


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
    PlanNode *child;
    enum PlanNodeType nodeType;
};      

typedef struct scan_node {
    PlanNode planNode;
    unsigned int current_record_idx;
    enum RecordType recordType;
} ScanNode;

typedef struct select_node {
    PlanNode planNode;
    bool (*filter)(Record*);
} SelectNode;

typedef struct project_node {
    PlanNode planNode;
    char **proj_fields;
    int proj_field_count;
} ProjectNode;

Record *scanNext(void *self);

ScanNode *makeScanNode(enum RecordType recordType) {
    ScanNode *node = malloc(sizeof(ScanNode));

    if (node != NULL) {
        node->planNode.next = &scanNext;
        node->current_record_idx = 0;
        node->recordType = recordType;
    }
    return node;
}

Record *scanNext(void *self) {
    ScanNode *scan_node = self;
    unsigned int current_record_idx = scan_node->current_record_idx;
    switch (scan_node->recordType) {
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
    }
    
}

// select node 
Record *selectNext(void *self);

SelectNode *makeSelectNode(bool (*predicate)(Record *)) {

    SelectNode *node = malloc(sizeof(SelectNode));
    if (node != NULL) {
        node->planNode.next = &selectNext;
        node->filter = predicate;
    }
    return node;
}

Record *selectNext(void *self) {
    SelectNode *select_node = self;
    PlanNode *child_node = ((PlanNode *) self) ->child;

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
        node->planNode.next = &projectNext;
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
    PlanNode *plan_node = self;
    PlanNode *child_node = plan_node->child;
    ProjectNode *project_node = (ProjectNode *) self;

    Record *record = child_node->next(child_node);
    if (record != NULL) {
        record = project(record, project_node->proj_fields, project_node->proj_field_count);
    }
    return record;
}

Record *averageNext(void *self) {
    PlanNode *avg_node = self;
    PlanNode *child_node = avg_node->child;
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

    PlanNode *node[] = { (PlanNode *) makeSelectNode(fn), (PlanNode *) makeScanNode(movie_t) };
    PlanNode *root = node[0];
    root->child = node[1];
    printf("should be fight club: %s\n", ((Movie *) root->next(root))->title );

    char *proj_fields[] = { "title" };
    PlanNode *node2[] = { (PlanNode *) makeProjectNode(proj_fields, 1), (PlanNode *) makeSelectNode(filterPredicate),
        (PlanNode *) makeScanNode(movie_t) };
    PlanNode *root2 = node2[0];
    root2->child = node2[1];
    root2->child->child = node2[2];
    printf("should be the pianist: %s\n", ((Movie *) root2->next(root2))->title );

    char *proj_fields3[] = { "rating" };
    PlanNode *node3[] = { (PlanNode *) makeAverageNode(), (PlanNode *) makeProjectNode(proj_fields3, 1),
     (PlanNode *) makeSelectNode(fnRating), (PlanNode *) makeScanNode(rating_t) };
    PlanNode *root3 = node3[0];
    root3->child = node3[1];
    root3->child->child = node3[2];
    root3->child->child->child = node3[3];
    printf("should be 3.5: %f\n", ((Rating *) root3->next(root3))->rating );

    // PlanNode *node3[] = { (PlanNode *) makeSelectNode(fnRating), (PlanNode *) makeScanNode(rating_t) };
    // PlanNode *root3 = node3[0];
    // root3->child = node3[1];
    // printf("rating should be 3.5 %f\n", ((Rating *) root3->next(root3))->rating );

}
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define RECORD_SIZE sizeof(movies) / sizeof(movies[0])

typedef enum { false, true } bool;

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


Record scanNext(void *self);
typedef struct scan_node {
	Record (*next)(void *self);
	unsigned int current_record_idx;
} ScanNode;

ScanNode *makeScanNode(void) {
	ScanNode *node = malloc(sizeof(ScanNode));
	if (node != NULL) {
		node->next = &scanNext;
		node->current_record_idx = 0;
	}
	return node;
}

Record scanNext(void *self) {
	ScanNode *scan_node = self;
	unsigned int current_record_idx = scan_node->current_record_idx;

	if (current_record_idx < RECORD_SIZE)
		return movies[scan_node->current_record_idx++];
	else 
		return empty_record;
}


// select node 
Record selectNext(void *self);
bool filterPredicate(Record target, char *fields[]);

typedef struct select_node {
	Record (*next)(void *self);
	ScanNode *scan_node;
	bool (*filter)(Record, char *f[]);
	char **query;
} SelectNode;

SelectNode *makeSelectNode(char *query[], int query_size) {
	SelectNode *node = malloc(sizeof(SelectNode));
	if (node != NULL) {
		node->next = &selectNext;
		node->scan_node = makeScanNode();
		node->filter = &filterPredicate;
		node->query = query;
	}
	return node;
}

typedef struct record_list {
	Record current;
	Record *next;
} RecordList;

Record selectNext(void *self) {
	SelectNode *select_node = self;
	ScanNode *scan_node = select_node->scan_node;
	Record current_record = scan_node->next(scan_node);

	printf("YOLO: %s\n", *(select_node->query));
	printf("filter: %s\n", select_node->filter(current_record, select_node->query) ? "yes" : "no");
	if (select_node->filter(current_record, select_node->query))
		return current_record;
	else 
		return empty_record;
}

bool filterPredicate(Record source, char *fields[]) {
	return (strcmp(source.title, fields[2]) == 0) ? true : false;
}

typedef union {
	SelectNode *selectNode;
	ScanNode *fileScanNode; 
} PlanNode;



int main(void) {
// [
//   ["AVERAGE"],
//   ["PROJECTION", ["rating"]],
//   ["SELECTION", ["movie_id", "EQUALS", "5000"]],
//   ["FILESCAN", ["ratings"]]
// ]
	ScanNode *scanner = makeScanNode();  
	printf("movie: %s\n", scanner->next(scanner).title);
	printf("movie: %s\n", scanner->next(scanner).title);
	printf("movie: %s\n", scanner->next(scanner).title);
	printf("movie: %d\n", scanner->next(scanner).title == 0);
	printf("movie: %d\n", strcmp(scanner->next(scanner).title, "the pianist"));
	printf("should be fail: %s\n", false ? "pass" : "fail");

	char *query[] = { "title", "EQUALS", "the pianist" };
	int query_size = 3;
	printf("%s\n", query[1]);


	SelectNode *select_node = makeSelectNode(query, query_size); 
	printf("select_node 1: %s\n", select_node->next(select_node).title );
	printf("select_node 2: %s\n", select_node->next(select_node).title );

	PlanNode node1[] = { 
		
		{ .fileScanNode = { makeScanNode() } }, 
		{ .selectNode = { makeSelectNode(query, query_size) } }
	};
	printf("after: %s\n", node1[1].selectNode->next(node1[1].selectNode).title );
	printf("after: %s\n", node1[1].selectNode->next(node1[1].selectNode).title );
	printf("after: %s\n", node1[1].selectNode->next(node1[1].selectNode).title );

} 
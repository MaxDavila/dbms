#include <stdio.h>
#include <serializer.h>

typedef enum { false, true } bool;
enum PlanNodeType { select_node_t, scan_node_t };

typedef struct PlanNode PlanNode;
struct PlanNode {
    Tuple *(*next)(void *self);
    PlanNode *left_tree;
    PlanNode *right_tree;
    void *(*reset)(void);
    enum PlanNodeType nodeType;
};

typedef struct scan_node {
    PlanNode plan_node;
    TupleId *current_tuple_id;
    char *table_name;
    Schema schema;
} ScanNode;

typedef struct select_node {
    PlanNode plan_node;
    bool (*filter)(Tuple*);
} SelectNode;


Tuple *scanNext(void *self) {
    ScanNode *node = self;
    // TupleId current_tuple_id = scan_node->current_tuple_id;
    FILE *heap_fp = fopen(node->table_name, "rb+");

    if (heap_fp == NULL)
        exit(EXIT_FAILURE);


    // if current tuple id has not been set. Set it to point to the first record
    if (node->current_tuple_id == NULL) {
        node->current_tuple_id = malloc(sizeof(TupleId));
        node->current_tuple_id->block_idx = 0;
        node->current_tuple_id->offset = 8;
    } else {
        // otherwise increment current tuple offset by tuple size
        node->current_tuple_id->offset += node->schema.size;
        if (node->schema.size > (BLOCK_SIZE - node->current_tuple_id->offset)) {
            node->current_tuple_id->block_idx++;
            node->current_tuple_id->offset = 8;
        }
    }

    // TODO: remove duplication from heap file
    // rad first 8 bytes so we can check for EOF
    char first_bytes[8];
    fseek(heap_fp, 0, SEEK_SET);
    fread(first_bytes, 8, 1, heap_fp);   
    int last_block_idx = 0;
    int last_tuple_offset = 0;
    deserialize_into_int(first_bytes, 0, &last_block_idx);
    deserialize_into_int(first_bytes, 4, &last_tuple_offset);

    if (node->current_tuple_id->block_idx == last_block_idx && node->current_tuple_id->offset == last_tuple_offset) {
        // EOF
        return NULL;
    } else {
        Buffer *buffer = new_buffer_of_size(node->schema.size);
        Tuple *tuple = malloc(sizeof(Tuple));
        tuple->tuple_id = *node->current_tuple_id;
        tuple->schema = node->schema;
        tuple->buffer = buffer;

        heap_read_raw_tuple(*tuple, heap_fp);
        return tuple;
    }
  
    fclose(heap_fp);
}

ScanNode *makeScanNode(char table_name[], Schema schema) {
    ScanNode *node = malloc(sizeof(ScanNode));

    if (node != NULL) {
        node->plan_node.next = &scanNext;
        node->table_name = table_name;
        node->schema = schema;
        node->current_tuple_id = NULL;
    }
    return node;
}



Tuple *selectNext(void *self) {
    SelectNode *node = self;
    PlanNode *child_node = ((PlanNode *) self)->left_tree;

    for (;;) {
        Tuple *tuple = child_node->next(child_node);
        if (tuple != NULL) {
            if (node->filter(tuple)) {
                return tuple;
            }
        } else {
            return NULL;
        }

    }

    return NULL;
}


SelectNode *makeSelectNode(bool (*predicate)(Tuple *)) {

    SelectNode *node = malloc(sizeof(SelectNode));
    if (node != NULL) {
        node->plan_node.next = &selectNext;
        node->filter = predicate;
    }
    return node;
}


bool fn(Tuple *source) {
    char query[100] = "Money Train (1995)";
    char field[] = "title";
    // get char length from schema. Pass field to select node (schema, title)
    Buffer *buffer = new_buffer_of_size(my_char.size);
    serialize_char_array(query, my_char.size, buffer);

    char tuple_title[my_char.size];
    memcpy(tuple_title, source->buffer->data + 4, my_char.size);
    return memcmp(buffer->data, tuple_title, my_char.size) == 0 ? true : false;
}

int main(void) {

    // TODO fix duplication
    char *movie_fields[] = { "id", "title", "genres" };
    DbType *movie_types[] = { &my_unsigned_int, &my_char, &my_char };
    Schema movie_schema = { "movies",  movie_fields, 3, 204, movie_types };

    PlanNode *node[] = { (PlanNode *) makeScanNode("data/movies.table", movie_schema) };
    PlanNode *root = node[0];
    print_hex_memory(root->next(root)->buffer->data);

    printf("------------------\n");
    PlanNode *node2[] = { (PlanNode *) makeSelectNode(fn), (PlanNode *) makeScanNode("data/movies.table", movie_schema) };
    PlanNode *root2 = node2[0];
    root2->left_tree = node2[1];
    print_hex_memory(root2->next(root2)->buffer->data);
    printf("------------------\n");

    // predicate tests
    // Tuple *source = root->next(root);
    char query[100] = "Money Train (1995)";
    // char field[] = "title";
    // // get char length from schema. Pass field to select node (schema, title)
    Buffer *query_buffer = new_buffer_of_size(100);
    serialize_char_array(query, my_char.size, query_buffer);

    // char tuple_title[100];
    // memcpy(tuple_title, source->buffer->data + 4, 100);
    print_hex_memory(query_buffer->data);
    // printf("result %s\n", memcmp(query_buffer->data, tuple_title, 100) == 0 ? "true" : "false");
    // print_hex_memory(tuple_title);


}
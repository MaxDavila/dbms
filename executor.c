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

typedef struct project_node {
    PlanNode plan_node;
    Schema projected_schema;
} ProjectNode;

// SCAN node

Tuple *scanNext(void *self) {
    ScanNode *node = self;
    // TupleId current_tuple_id = scan_node->current_tuple_id;
    FILE *heap_fp = fopen(node->table_name, "rb+");

    if (heap_fp == NULL)
        exit(EXIT_FAILURE);


    // TODO: remove duplication from heap file
    // rad first 8 bytes so we can check for EOF
    char first_bytes[8];
    fseek(heap_fp, 0, SEEK_SET);
    fread(first_bytes, 8, 1, heap_fp);   
    int last_block_idx = 0;
    int last_tuple_offset = 0;
    deserialize_into_int(first_bytes, 0, &last_block_idx);
    deserialize_into_int(first_bytes, 4, &last_tuple_offset);

    // if current tuple id has not been set. Set it to point to the first record
    if (node->current_tuple_id == NULL) {
        node->current_tuple_id = malloc(sizeof(TupleId));
        node->current_tuple_id->block_idx = 0;
        node->current_tuple_id->offset = 8;

    // if at EOF
    } else if (node->current_tuple_id->block_idx == last_block_idx && node->current_tuple_id->offset == last_tuple_offset) {
        return NULL;
    } else {
        // otherwise increment current tuple offset by tuple size
        node->current_tuple_id->offset += node->schema.size;
        if (node->schema.size > (BLOCK_SIZE - node->current_tuple_id->offset)) {
            node->current_tuple_id->block_idx++;
            node->current_tuple_id->offset = 8;
        }
    }




    Buffer *buffer = new_buffer_of_size(node->schema.size);
    Tuple *tuple = malloc(sizeof(Tuple));
    tuple->tuple_id = *node->current_tuple_id;
    tuple->schema = node->schema;
    tuple->buffer = buffer;

    heap_read_raw_tuple(*tuple, heap_fp);
    return tuple;
  
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



// SELECT node
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


// PROJECT node

// assumes projected schema fields is an ordered subset of src schema
// i.e src_schema.fields = { "id", "title", "genre"}
// cannot be projected_schema.fields = { "genre", "title" }
void *project(Tuple *src_tuple, Schema projected_schema) {
    
    Schema src_schema = src_tuple->schema;
    int offset = 0;

    Tuple *projected_tuple = malloc(sizeof(Tuple));
    projected_tuple->schema = projected_schema;
    projected_tuple->buffer = new_buffer_of_size(projected_schema.size);

    for (int i = 0; i < src_schema.field_count; i++) {
        for (int j = 0; j < projected_schema.field_count; j++) {

            if (strcmp(projected_schema.fields[j], src_schema.fields[i]) == 0) {
                memcpy(projected_tuple->buffer->data + projected_tuple->buffer->offset, src_tuple->buffer->data + offset, src_schema.types[i]->size);

                projected_tuple->buffer->offset += src_schema.types[i]->size;
                break;
            }
        }
        offset += src_schema.types[i]->size;
    }
    return projected_tuple;
}

Tuple *projectNext(void *self) {
    PlanNode *plan_node = self;
    PlanNode *child_node = plan_node->left_tree;
    ProjectNode *project_node = (ProjectNode *) self;

    Tuple *tuple = child_node->next(child_node);
    if (tuple != NULL) {

        tuple = project(tuple, project_node->projected_schema);
    }
    return tuple;
}


ProjectNode *makeProjectNode(Schema new_schema) {

    ProjectNode *node = malloc(sizeof(ProjectNode));

    if (node != NULL) {
        node->plan_node.next = &projectNext;
        node->projected_schema = new_schema;
    }
    return node;
}

// AVERAGE node

Tuple *averageNext(void *self) {
    PlanNode *avg_node = self;
    PlanNode *child_node = avg_node->left_tree;
    Tuple *tuple;
    unsigned int count = 0;
    double sum = 0;
    double tuple_value = 0;

    // assuming schema contains only 1 type 
    while ((tuple = child_node->next(child_node)) != NULL) {
        count++;
        switch (tuple->schema.types[0]->type) {
            case mdb_unsigned_int: {
                int value = 0;
                deserialize_into_int(tuple->buffer->data, 0, &value);
                tuple_value = value;

                break;
            }
            case mdb_double: {
                printf("my_double\n");
                break;
            }
            case mdb_unsigned_char: {
                printf("mdb_unsigned_char\n");
                break;
            }
        }

        sum += tuple_value;

    }
    double average = sum / count;

    DbType *types[] = { &my_double };
    Schema avg_schema = { NULL, NULL, 1, 8, types };

    Tuple *result = malloc(sizeof(Tuple));
    result->schema = avg_schema;

    result->buffer = new_buffer_of_size(my_double.size);
    serialize_double(average, result->buffer);

    return result;
}

PlanNode *makeAverageNode() {

    PlanNode *node = malloc(sizeof(PlanNode));
    if (node != NULL) {
        node->next = &averageNext;
    }
    return node;
}

bool fn(Tuple *source) {
    char query[100] = "Money Train (1995)";
    char field[] = "title";
    // get char length from schema. Pass field to select node (schema, title)
    Buffer *buffer = new_buffer_of_size(my_char.size);
    serialize_char_array(query, my_char.size, buffer);

    char tuple_field[my_char.size];
    memcpy(tuple_field, source->buffer->data + 4, my_char.size);
    return memcmp(buffer->data, tuple_field, my_char.size) == 0 ? true : false;
}

bool fn2(Tuple *source) {
    int query = 10;
    char field[] = "id";

    Buffer *buffer = new_buffer_of_size(my_unsigned_int.size);
    serialize_int(query, buffer);

    char tuple_field[my_unsigned_int.size];
    memcpy(tuple_field, source->buffer->data, my_unsigned_int.size);
    return memcmp(buffer->data, tuple_field, my_unsigned_int.size) == 0 ? true : false;
}


void print_debug_tuple(Tuple *tuple) {

    Schema schema = tuple->schema;
    tuple->buffer->offset = 0;

    for (int i = 0; i < schema.field_count; i++) {

        switch (schema.types[i]->type) {
            case mdb_unsigned_int: {
                int value = 0;
                deserialize_into_int(tuple->buffer->data, tuple->buffer->offset, &value);
                printf("%s %d | ", schema.fields[i], value);
                break;
            }
            case mdb_double: {
                double value = 0;
                deserialize_into_double(tuple->buffer->data, tuple->buffer->offset, &value);
                printf("%s %f | ", schema.fields[i], value);
                break;
            }
            case mdb_unsigned_char: {
                size_t size = schema.types[i]->size;
                char *value = malloc(size);
                deserialize_into_char_array(tuple->buffer->data, value, tuple->buffer->offset, size);
                printf("%s %s | ", schema.fields[i], value);
                break;
            }
        }
        tuple->buffer->offset += schema.types[i]->size;

    }
    printf("\n");
}
int main(void) {

    // TODO fix duplication
    char *movie_fields[] = { "id", "title", "genres" };
    DbType *movie_types[] = { &my_unsigned_int, &my_char, &my_char };
    Schema movie_schema = { "movies",  movie_fields, 3, 204, movie_types };

    PlanNode *node[] = { (PlanNode *) makeScanNode("data/movies.table", movie_schema) };
    PlanNode *root = node[0];
    Tuple *result = root->next(root);
    print_debug_tuple(result);
    print_hex_memory(result->buffer->data);

    printf("------------------\n");
    PlanNode *node2[] = { (PlanNode *) makeSelectNode(fn2), (PlanNode *) makeScanNode("data/movies.table", movie_schema) };
    PlanNode *root2 = node2[0];
    root2->left_tree = node2[1];

    Tuple *result2 = root2->next(root2);
    print_debug_tuple(result2);
    print_hex_memory(result2->buffer->data);
    printf("------------------\n");

    char *proj_fields[] = { "id", "title" };
    DbType *proj_field_types[] = { &my_unsigned_int, &my_char};
    Schema proj_fields_schema = { "movies", proj_fields, 2, 104, proj_field_types };
    PlanNode *node3[] = { (PlanNode *) makeProjectNode(proj_fields_schema), (PlanNode *) makeSelectNode(fn2), (PlanNode *) makeScanNode("data/movies.table", movie_schema) };
    PlanNode *root3 = node3[0];
    root3->left_tree = node3[1];
    root3->left_tree->left_tree = node3[2];

    Tuple *result3 = root3->next(root3);
    print_debug_tuple(result3);
    print_hex_memory(result3->buffer->data);
    printf("------------------\n");

    char *proj_fields2[] = { "id" };
    DbType *proj_field_types2[] = { &my_unsigned_int};
    Schema proj_fields_schema2 = { "movies", proj_fields2, 1, 4, proj_field_types2 };
    PlanNode *node4[] = { (PlanNode *) makeAverageNode(), (PlanNode *) makeProjectNode(proj_fields_schema2), (PlanNode *) makeSelectNode(fn2), (PlanNode *) makeScanNode("data/movies.table", movie_schema) };
    PlanNode *root4 = node4[0];
    root4->left_tree = node4[1];
    root4->left_tree->left_tree = node4[2];
    root4->left_tree->left_tree->left_tree = node4[3];

    // Tuple *result4 = root4->next(root4);
    // print_debug_tuple(result4);
    // print_hex_memory(result4->buffer->data);

    char *res = root4->next(root4)->buffer->data;
    printf("result should be 10  %f\n", * ((double *) res));
    printf("------------------\n");

    // predicate tests
    // Tuple *source = root->next(root);
    // char query[100] = "Money Train (1995)";
    // char field[] = "title";
    // // get char length from schema. Pass field to select node (schema, title)
    // Buffer *query_buffer = new_buffer_of_size(100);
    // serialize_char_array(query, my_char.size, query_buffer);

    // char tuple_title[100];
    // memcpy(tuple_title, source->buffer->data + 4, 100);
    // print_hex_memory(query_buffer->data);
    // printf("result %s\n", memcmp(query_buffer->data, tuple_title, 100) == 0 ? "true" : "false");
    // print_hex_memory(tuple_title);


}
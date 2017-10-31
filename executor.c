#include <stdio.h>
#include <serializer.h>


void reset_next(void *self) {
    PlanNode *node = self;
    node->left_tree->reset(node->left_tree);
    node->right_tree->reset(node->right_tree);
}

void reset_scan(void *self) {
    ScanNode *node = self;
    node->current_tuple_id = NULL; 
}


// SCAN node
Tuple *scanNext(void *self) {
    ScanNode *node = self;
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
    if (node->schema.size > (BLOCK_SIZE - last_tuple_offset))
        last_tuple_offset -= node->schema.size;

    // if current tuple id has not been set. Set it to point to the first record
    if (node->current_tuple_id == NULL) {
        node->current_tuple_id = malloc(sizeof(TupleId));
        node->current_tuple_id->block_idx = 0;
        node->current_tuple_id->offset = 8;

    // otherwise increment current tuple offset by tuple size
    } else {
        node->current_tuple_id->offset += node->schema.size;
        if (node->schema.size > (BLOCK_SIZE - node->current_tuple_id->offset)) {
            node->current_tuple_id->block_idx++;
            node->current_tuple_id->offset = 8;
        }
    }

    // if at EOF
    if (node->current_tuple_id->block_idx == last_block_idx && node->current_tuple_id->offset == last_tuple_offset) {
        return NULL;
    }

    Buffer *buffer = new_buffer_of_size(node->schema.size);
    Tuple *tuple = malloc(sizeof(Tuple));
    tuple->tuple_id = *node->current_tuple_id;
    tuple->schema = node->schema;
    tuple->buffer = buffer;

    heap_read_raw_tuple(*tuple, heap_fp);
    fclose(heap_fp);

    return tuple;

}

ScanNode *makeScanNode(char table_name[], Schema schema) {
    ScanNode *node = malloc(sizeof(ScanNode));

    if (node != NULL) {
        node->plan_node.next = &scanNext;
        node->table_name = table_name;
        node->schema = schema;
        node->current_tuple_id = NULL;
        node->plan_node.reset = &reset_scan;
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
            free(tuple);
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

// NESTED loop join
void merge_schemas(Schema outer, Schema inner, Schema *joined) {

    joined->field_count = outer.field_count + inner.field_count;
    joined->size = outer.size + inner.size;

    // join schema fields and types
    char **joined_fields = (char**) calloc(outer.field_count + inner.field_count, sizeof(char*));
    DbType **joined_types = (DbType**) calloc(outer.field_count + inner.field_count, sizeof(DbType*));

    for (int i = 0; i < outer.field_count; i++) {
        joined_fields[i] = (char*) calloc(strlen(outer.fields[i]), sizeof(char));
        strcpy(joined_fields[i], outer.fields[i]);

        joined_types[i] = outer.types[i];
    }

    int offset = outer.field_count;
    for (int i = 0; i < inner.field_count; i++) {
        joined_fields[i + offset] = (char*) calloc(strlen(inner.fields[i]), sizeof(char));
        strcpy(joined_fields[i + offset], inner.fields[i]);

        joined_types[i + offset] = inner.types[i];
    }
    joined->fields = joined_fields;
    joined->types = joined_types;
}

// It scans the inner relation to join with current outer tuple.
Tuple *nestedLoopNext(void *self) {
    NestedLoopJoinNode *nested_loop_node = self;
    PlanNode *outer_node = ((PlanNode *) self)->left_tree;
    PlanNode *inner_node = ((PlanNode *) self)->right_tree;

    Tuple *outer = (nested_loop_node->current_outer_tuple == NULL) ? outer_node->next(outer_node) : nested_loop_node->current_outer_tuple;
    Tuple *inner;

    // we've run out of records to scan once the outernode returns no more tuples NULL
    if (outer == NULL)
        return NULL;
   

    for (;;) {
        while ((inner = inner_node->next(inner_node)) != NULL) {
            Tuple *joined_tuple = malloc(sizeof(Tuple));
            Schema *joined_schema = malloc(sizeof(Schema));

            merge_schemas(outer->schema, inner->schema, joined_schema);
            joined_tuple->schema = *joined_schema;

            bool matched = nested_loop_node->join(outer, inner);

            if (matched) {
                Buffer *merged_data = new_buffer_of_size(outer->buffer->size + inner->buffer->size);
                memcpy(merged_data->data, outer->buffer->data, outer->buffer->size);
                memcpy(merged_data->data + outer->buffer->size, inner->buffer->data, inner->buffer->size);

                joined_tuple->buffer = merged_data;
                return joined_tuple;

            } else {

                // free(outer);
                free(inner);
                free(joined_schema);
                free(joined_tuple);
            }
        }

        // reset inner relation
        inner_node->reset(inner_node);

        nested_loop_node->current_outer_tuple = outer_node->next(outer_node);
        outer = nested_loop_node->current_outer_tuple;

        if (outer == NULL)
            return NULL;
    }
}

NestedLoopJoinNode *makeNestedLoopJoinNode(bool (*theta_fn)(Tuple *r, Tuple *s)) {

    NestedLoopJoinNode *node = malloc(sizeof(NestedLoopJoinNode));
    if (node != NULL) {
        node->plan_node.next = &nestedLoopNext;
        node->join = theta_fn;
        node->plan_node.reset = &reset_next;
    }
    return node;
}


bool theta(Tuple *r, Tuple *s) {
    // r.id = s.movie_id
    // TODO: derive compare fields dynamically

    char r_field[my_unsigned_int.size];
    memcpy(r_field, r->buffer->data, my_unsigned_int.size);

    char s_field[my_unsigned_int.size];
    memcpy(s_field, s->buffer->data + 4, my_unsigned_int.size);
    return memcmp(r_field, s_field, my_unsigned_int.size) == 0 ? true : false;
}

bool fn(Tuple *source) {
    char query[100] = "Money Train (1995)";
    char field[] = "title";

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
    int matched = memcmp(buffer->data, tuple_field, my_unsigned_int.size);

    free(buffer);
    return matched == 0 ? true : false;
}

int main(void) {

    // TODO fix duplication
    char *movie_fields[] = { "id", "title", "genres" };
    DbType *movie_types[] = { &my_unsigned_int, &my_char, &my_char };
    Schema movie_schema = { "movies",  movie_fields, 3, 204, movie_types };

    char *rating_fields[] = { "user_id", "movie_id", "rating", "timestamp" };
    DbType *rating_types[] = { &my_unsigned_int, &my_unsigned_int, &my_double, &my_char };
    Schema rating_schema = { "ratings",  rating_fields, 4, 116, rating_types };

    PlanNode *node[] = { (PlanNode *) makeScanNode("data/movies.table", movie_schema) };
    PlanNode *root = node[0];
    Tuple *result = root->next(root);
    print_debug_tuple(result);
    // print_hex_memory(result->buffer->data);

    printf("------------------\n");
    PlanNode *node2[] = { (PlanNode *) makeSelectNode(fn), (PlanNode *) makeScanNode("data/movies.table", movie_schema) };
    PlanNode *root2 = node2[0];
    root2->left_tree = node2[1];

    Tuple *result2 = root2->next(root2);
    print_debug_tuple(result2);
    // print_hex_memory(result2->buffer->data);
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
    // print_hex_memory(result3->buffer->data);
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

    // merge schema tests
    Schema *merged_schema = malloc(sizeof(Schema));
    merge_schemas(proj_fields_schema, proj_fields_schema2, merged_schema);
    printf("merged_schema field should be id  %s\n", merged_schema->fields[0]);
    printf("merged_schema field should be title %s\n", merged_schema->fields[1]);
    printf("merged_schema field should be id %s\n", merged_schema->fields[2]);

    printf("merged_schema type should be int  %d\n", merged_schema->types[0]->size);
    printf("merged_schema type should be char %d\n", merged_schema->types[1]->size);
    printf("merged_schema type should be int %d\n", merged_schema->types[2]->size);

    printf("------------------\n");

    PlanNode *node5[] = { (PlanNode *) makeNestedLoopJoinNode(theta), (PlanNode *) makeScanNode("data/movies.table", movie_schema), (PlanNode *) makeScanNode("data/ratings.table", rating_schema) };
    PlanNode *root5 = node5[0];
    root5->left_tree = node5[1];
    root5->right_tree = node5[2];

    Tuple *tuple10;
    while ((tuple10 = root5->next(root5)) != NULL) {
        print_debug_tuple(tuple10);
    }
}
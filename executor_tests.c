#include <stdio.h>
#include <string.h>

int main(void) {

    char *movie_fields[] = { "id", "title", "genres" };
    DbType *movie_types[] = { &my_unsigned_int, &my_char, &my_char };
    Schema movie_schema = { "movies",  movie_fields, 3, 204, movie_types };

    char *rating_fields[] = { "user_id", "movie_id", "rating", "timestamp" };
    DbType *rating_types[] = { &my_unsigned_int, &my_unsigned_int, &my_double, &my_char };
    Schema rating_schema = { "ratings",  rating_fields, 4, 116, rating_types };

    PlanNode *node[] = { (PlanNode *) makeScanNode("data/movies.table", movie_schema) };
    PlanNode *root = build_tree(node, 1);
    execute(build_tree(node, 1));

    printf("------------------\n");
    PlanNode *node2[] = { (PlanNode *) makeSelectNode(fn), (PlanNode *) makeScanNode("data/movies.table", movie_schema) };
    PlanNode *root2 = build_tree(node2, 2);
    execute(build_tree(node2, 2));

    printf("------------------\n");

    char *proj_fields[] = { "id", "title" };
    DbType *proj_field_types[] = { &my_unsigned_int, &my_char};
    Schema proj_fields_schema = { "movies", proj_fields, 2, 104, proj_field_types };
    PlanNode *node3[] = { (PlanNode *) makeProjectNode(proj_fields_schema), (PlanNode *) makeSelectNode(fn2), (PlanNode *) makeScanNode("data/movies.table", movie_schema) };
    PlanNode *root3 = build_tree(node3, 3);
    Tuple *result3 = root3->next(root3);
    print_debug_tuple(result3);
    // print_hex_memory(result3->buffer->data);
    printf("------------------\n");

    char *proj_fields2[] = { "id" };
    DbType *proj_field_types2[] = { &my_unsigned_int};
    Schema proj_fields_schema2 = { "movies", proj_fields2, 1, 4, proj_field_types2 };
    PlanNode *node4[] = { (PlanNode *) makeAverageNode(), (PlanNode *) makeProjectNode(proj_fields_schema2), (PlanNode *) makeSelectNode(fn2), (PlanNode *) makeScanNode("data/movies.table", movie_schema) };
    PlanNode *root4 = build_tree(node4, 4);

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
    execute(root5);
}
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "serializer.h" 

DbType my_unsigned_int = { mdb_unsigned_int, 4 };
DbType my_char = { mdb_unsigned_char, 100 };
DbType my_double = { mdb_double, 8 };

char *movie_fields[] = { "id", "title", "genres" };
DbType *movie_types[] = { &my_unsigned_int, &my_char, &my_char };
Schema movie_schema = { "movies",  movie_fields, 3, 204, movie_types };

char *rating_fields[] = { "user_id", "movie_id", "rating", "timestamp" };
DbType *rating_types[] = { &my_unsigned_int, &my_unsigned_int, &my_double, &my_char };
Schema rating_schema = { "ratings",  rating_fields, 4, 116, rating_types };

void print_hex_memory(void *mem) {
  int i;
  unsigned char *p = (unsigned char *)mem;
  for (i=0;i<256;i++) {
    printf("0x%02x ", p[i]);
    if ((i%16==0) && i)
      printf("\n");
  }
  printf("\n");
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
                free(value);

                break;
            }
        }
        tuple->buffer->offset += schema.types[i]->size;

    }
    printf("\n");
}


void parse_row(char *line, ssize_t line_length, char *output[]) {
    char *new_string;
    int in = 0;
    int needle = 0;
    int start = 0;
    int output_i = 0;

    while (needle < line_length) {

        if ((line[needle] == ',' || line[needle] == '\n') && !in) {
            // allocate and copy string up until needle

            size_t new_size = needle - start;
            new_string = malloc(new_size);
            memcpy(new_string, line + start, new_size);

            // strip out last char if it's a quotation mark
            if (new_string[new_size - 1] == '\"')
                new_string[new_size - 1] = '\0';

            output[output_i] = malloc(new_size);
            output[output_i] = new_string;

            start = needle + 1;
            output_i++;
        } else if (line[needle] == '\"' && !in) {
            in = 1;
            start++;

        } else if (line[needle] == '\"' && in) {
            in = 0;
        }
        needle++;
    }
}
void serialize_row(Schema schema, Buffer *buffer, char *row[]) {
    for (int i = 0; i < schema.field_count; i++) {

        switch (schema.types[i]->type) {
            case (mdb_unsigned_int): {

                serialize_int(atoi(row[i]), buffer);
                break;
            }
            case (mdb_unsigned_char): {
                // need to zero out and properly size the char array before serializing
                // TODO: size should not be hardcoded

                // skipping rows longer than predetermined schema length
                if (strlen(row[i]) >= my_char.size)
                    return;

                char temp[100] = {0};
                strcpy(temp, row[i]);
                serialize_char_array(temp, my_char.size, buffer);

                break;
            }
            case (mdb_double): {
                serialize_double(strtod(row[i], NULL), buffer);
                break;
            }
        }
    }
} 

void heap_write(FILE *heap_fp, Buffer *serialization_buffer) {

    // if file is empty, write first block
    fseek(heap_fp, 0, SEEK_END);
    if (ftell(heap_fp) == 0) {
        Buffer *temp_buffer = new_buffer();
        serialize_int(0, temp_buffer);
        serialize_int(8, temp_buffer);

        fwrite(temp_buffer->data, 1, 8, heap_fp);

        // zero out the rest of the block 
        char empty[BLOCK_SIZE - 8] = {0};
        fwrite(empty, 1, BLOCK_SIZE - 8, heap_fp);
        free(temp_buffer);
    }

    // read first 8 bytes from the first block
    char first_bytes[8];
    fseek(heap_fp, 0, SEEK_SET);
    fread(first_bytes, 8, 1, heap_fp);        
    
    // get the block idx and offset of next available place to write
    int block_idx = 0;
    int tuple_offset = 0;
    deserialize_into_int(first_bytes, 0, &block_idx);
    deserialize_into_int(first_bytes, 4, &tuple_offset);

    // if we don't have space for the record, move to the next block
    if (serialization_buffer->size > (BLOCK_SIZE - tuple_offset)) {
       block_idx++;
       tuple_offset = 0;

       // zero out the first 8 bytes of the next block
       fseek(heap_fp, block_idx * BLOCK_SIZE, SEEK_SET);  
       char empty[8] = {0};
       fwrite(empty, 1, 8, heap_fp);
       tuple_offset+= 8;
    }

    // write the record itself
    // TODO: handle case when buffer < schema.size
    fseek(heap_fp, block_idx * BLOCK_SIZE + tuple_offset, SEEK_SET);  
    fwrite(serialization_buffer->data, 1, serialization_buffer->size, heap_fp);
    // update block header to point to next available record
    tuple_offset += serialization_buffer->size;
    
    Buffer *temp_buffer = new_buffer();
    serialize_int(block_idx, temp_buffer);
    serialize_int(tuple_offset, temp_buffer);
    fseek(heap_fp, 0, SEEK_SET);
    fwrite(temp_buffer->data, 1, 8, heap_fp);
    free(temp_buffer);
}

void heap_read(struct Element *tuple, FILE *heap_fp, TupleId tuple_id, Schema schema) {
    Buffer *buffer = new_buffer_of_size(BLOCK_SIZE);
    int offset = tuple_id.block_idx * BLOCK_SIZE + tuple_id.offset;
    fseek(heap_fp, offset, SEEK_SET);
    fread(buffer->data, schema.size, 1, heap_fp);
    
    deserialize_int(buffer, &tuple[0]);
    deserialize_char_array(buffer, &tuple[1], my_char.size);
    deserialize_char_array(buffer, &tuple[2], my_char.size);

    printf("TUPLE DATA %d, %s, %s\n",  tuple[0].i, tuple[1].str, tuple[2].str);
}


void heap_read_raw_tuple(Tuple tuple, FILE *heap_fp) {
    TupleId tuple_id = tuple.tuple_id;

    int offset = tuple_id.block_idx * BLOCK_SIZE + tuple_id.offset;
    fseek(heap_fp, offset, SEEK_SET);
    fread(tuple.buffer->data, tuple.schema.size, 1, heap_fp);
}



// int main(void) {

//     FILE *csv_fp;
//     FILE *heap_fp;

//     char *read_buffer = NULL;
//     size_t buffer_len = 0;
//     ssize_t line_length;
//     Schema schema = rating_schema;

//     csv_fp = fopen("data/movielens/ratings_small.csv", "r");
//     heap_fp = fopen("data/ratings.table", "wb+");
//     if (csv_fp == NULL)
//         exit(EXIT_FAILURE);

//     getline(&read_buffer, &buffer_len, csv_fp); // skip headers

//     while ((line_length = getline(&read_buffer, &buffer_len, csv_fp)) != -1) {
//         char *parsed_row[4];
//         parse_row(read_buffer, line_length, parsed_row);

//         Buffer *serialization_buffer = new_buffer_of_size(schema.size);
//         serialize_row(schema, serialization_buffer, parsed_row);
//         heap_write(heap_fp, serialization_buffer);
//         free(serialization_buffer);


//         // printf("OUTPUT 1%s\n", parsed_row[0]);
//         // printf("OUTPUT 2 %s\n", parsed_row[1]);
//         // printf("OUTPUT 3 %s\n", parsed_row[2]);
//         // printf("OUTPUT 4 %s\n", parsed_row[3]);

//     }

//     // attempt to read a record given a block id and offset
//     TupleId tuple_id = {0, 8};
//     struct Element *arr = malloc(sizeof(struct Element) * schema.field_count);
//     for (int i = 0; i < schema.field_count; i++) {
//         if (schema.types[i]->type == mdb_unsigned_char) {
//             arr[i].str = malloc(my_char.size);
//         }
//     }
//     heap_read(arr, heap_fp, tuple_id, schema);

//     // test read raw tuple
//     Buffer *buffer = new_buffer_of_size(schema.size);
//     Tuple tuple = { tuple_id, schema, buffer };
//     heap_read_raw_tuple(tuple, heap_fp);
//     print_hex_memory(tuple.buffer->data);

//     fclose(csv_fp);
//     fclose(heap_fp);

//     if (read_buffer)
//         free(read_buffer);
//     exit(EXIT_SUCCESS);
// }
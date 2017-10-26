#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "serializer.h" 

#define BLOCK_SIZE 4096

typedef enum { mdb_unsigned_int, mdb_unsigned_char, mdb_double } dbType;

typedef struct db_type {
    dbType type;
    int size;
} DbType;

DbType my_unsigned_int = { mdb_unsigned_int, 4 };
DbType my_char = { mdb_unsigned_char, 100 };
DbType my_double = { mdb_double, 8 };

typedef struct schema {
    char *table_name;
    char **fields;
    int field_count;
    int size;
    DbType **types;
} Schema;

char *movie_fields[] = { "id", "title", "genres" };
DbType *movie_types[] = { &my_unsigned_int, &my_char, &my_char };
Schema movie_schema = { "movies",  movie_fields, 3, 204, movie_types };

void print_hex_memory(void *mem) {
  int i;
  unsigned char *p = (unsigned char *)mem;
  for (i=0;i<128;i++) {
    printf("0x%02x ", p[i]);
    if ((i%16==0) && i)
      printf("\n");
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
                break;
            }
        }
    }
} 

int main(void) {

    FILE *csv_fp;
    FILE *heap_fp;

    char *buffer = NULL;
    size_t buffer_len = 0;
    ssize_t line_length;

    csv_fp = fopen("data/movielens/movies.csv", "r");
    heap_fp = fopen("data/movies.table", "wb+");
    if (csv_fp == NULL)
        exit(EXIT_FAILURE);

    getline(&buffer, &buffer_len, csv_fp); // skip headers

    while ((line_length = getline(&buffer, &buffer_len, csv_fp)) != -1) {
        char *parsed_row[3];

        parse_row(buffer, line_length, parsed_row);

        Buffer *serialization_buffer = new_buffer_of_size(movie_schema.size);
        serialize_row(movie_schema, serialization_buffer, parsed_row);

        // write to heap file

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

        // // read the block into a buffer
        // Buffer *block_buffer = new_buffer_of_size(BLOCK_SIZE);
        // fseek(heap_fp, block_idx * BLOCK_SIZE + tuple_offset, SEEK_SET);
        // fread(block_buffer->data, BLOCK_SIZE - 8, 1, heap_fp);

        // if we don't have space for the record, move to the next block
        if (serialization_buffer->offset > (BLOCK_SIZE - tuple_offset)) {
           block_idx++;
           tuple_offset = 0;

           // zero out the first 8 bytes of the next block
           fseek(heap_fp, block_idx * BLOCK_SIZE, SEEK_SET);  
           char empty[BLOCK_SIZE - 8] = {0};
           fwrite(empty, 1, BLOCK_SIZE - 8, heap_fp);
        }

        // struct Element *arr = malloc(sizeof(struct Element) * 3);
        // serialization_buffer->offset = 0;

        // deserialize_int(serialization_buffer, &arr[0]);
        // arr[1].str = malloc(100);
        // arr[2].str = malloc(100);

        // deserialize_char_array(serialization_buffer, &arr[1], 100);
        // deserialize_char_array(serialization_buffer, &arr[2], 100);

        // printf("serialization_buffer data %s what\n",  arr[1].str);
        // printf("serialization_buffer data %s what\n",  arr[2].str);

        // write the record itself
        // TODO: handle case when buffer < schema.size
        fseek(heap_fp, block_idx * BLOCK_SIZE + tuple_offset, SEEK_SET);  
        fwrite(serialization_buffer->data, 1, movie_schema.size, heap_fp);

        // update block header to point to next available record
        tuple_offset += movie_schema.size;
        
        Buffer *temp_buffer = new_buffer();
        serialize_int(block_idx, temp_buffer);
        serialize_int(tuple_offset, temp_buffer);
        fseek(heap_fp, 0, SEEK_SET);
        fwrite(temp_buffer->data, 1, 8, heap_fp);
        free(temp_buffer);

        // get next free block id
        /*
                
        fwrite(buffer, length);
        1) when file is empty
            - write block header (next_free_block(4), next_free_offset(4))
        2) when file is not empty
            - read first 4 bytes
            - read the next 4 bytes
            - resolve address
            - fseek to address
            - if record_size > block_size - offset  
                - block_i ++
                - offset = 0;
                - write 8 first null bytes of next block
            - fwrite record
            - update block header to point to next available record
                - update next free block, 
                - update offset
                - return both 
        */


        // free buffer

        printf("OUTPUT %s\n", parsed_row[0]);
        printf("OUTPUT2 %s\n", parsed_row[1]);
        printf("OUTPUT3 %s\n", parsed_row[2]);

    }


    fclose(csv_fp);
    fclose(heap_fp);

    if (buffer)
        free(buffer);
    exit(EXIT_SUCCESS);
}

/*
    tuple desc = type, length
    schema = new schema ('table_name', [id, title, genre], [mdb_unsigned_int, mdb_unsigned_char(100), mdb_unsigned_char(100)])
    row = [1, pianist, sad];
    serialize(row, schema, buffer);
     for field in schema.fields:
        // this or mdb_unsigned_int.serialize(to get rid of branches )
        switch(type) {
            case(mdb_unsigned_int):
                serialize_int(row[i], buffer);
                break;
                ..
        }
    fwrite(buffer, length)

    block = new Block(fread(block, length)); 
    bytes = get_tuple(block, BLOCK_LENGTH);
    tuple = new Tuple(bytes, schema);
    tuple = join(tupleA, tupleB); work on tuples
    Result *result_arr = generic struct
    deserialize(tuple.get_bytes(), schema, result_arr)
        for field, i in schema.fields
            switch(type) {
                case(mdb_unsigned_int):
                    result_arr[i].type = mdb_unsigned_int;
                    deserialize_int(src_buffer, result_arr[i]);
                    break;
                    ..
            }
    new Record(buffer);
*/



// read movie lens csv into heapfile
// open csv
// skip row 1
// for each row
  // serialize
  // find next available record_id (block_id, offset)
    // read first 4 bytes of header 
    // read next 4 bytes to determine offset
    
  // write record
  // update next available record_id
  // return record_id

// schema that can serialize / deserialize

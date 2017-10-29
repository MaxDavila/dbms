#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DEFAULT_BUFFER_SIZE 32
#define BLOCK_SIZE 4096

typedef struct buffer {
    void            *data;
    unsigned int    offset;
    size_t          size;
} Buffer;

struct Element {
  union {
    char            *str;
    unsigned int    i;
    double          d;
  };
};

typedef enum { mdb_unsigned_int, mdb_unsigned_char, mdb_double } dbType;

typedef struct db_type {
    dbType type;
    int size;
} DbType;

typedef struct schema {
    char *table_name;
    char **fields;
    int field_count;
    int size;
    DbType **types;
} Schema;

typedef struct tuple_id {
    unsigned int block_idx;
    unsigned int offset;
} TupleId;

typedef struct tuple {
    TupleId tuple_id;
    Schema schema;
    Buffer *buffer;
} Tuple;


extern DbType my_unsigned_int;
extern DbType my_char;
extern DbType my_double;

extern char *movie_fields[];
extern DbType *movie_types[];
extern Schema movie_schema;

Buffer *new_buffer();
Buffer *new_buffer_of_size(int size);
void serialize_int(int x, Buffer *b);
void serialize_double(double x, Buffer *b);
void serialize_char_array(char *src, int size, Buffer *b);
// void serialize_row(Schema schema, Buffer *buffer, char *row[]);

void deserialize_int(Buffer *src, struct Element *dst);
void deserialize_into_int(char *src, int offset, int *dst);
void deserialize_double(Buffer *src, struct Element *dst);
void deserialize_char_array(Buffer *src, struct Element *dst, int size);

// heap file
void heap_read(struct Element *tuple, FILE *heap_fp, TupleId tuple_id, Schema schema);
void heap_read_raw_tuple(Tuple tuple, FILE *heap_fp);

// utils
void print_hex_memory(void *mem);
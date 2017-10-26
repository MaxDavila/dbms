#include <stdio.h>
#include <stdlib.h>

#define INITIAL_SIZE 32

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

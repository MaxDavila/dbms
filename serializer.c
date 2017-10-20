#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define INITIAL_SIZE 32

typedef struct buffer {
    void            *data;
    unsigned int    next;
    size_t          size;
} Buffer;

// enum ElementType { et_str, et_int, et_dbl };
struct Element {
  // ElementType type;
  union {
    char            *str;
    unsigned int    i;
    double          d;
  };
};


Buffer *new_buffer() {
    Buffer *b = malloc(sizeof(Buffer));

    b->data = malloc(INITIAL_SIZE);
    b->size = INITIAL_SIZE;
    b->next = 0;

    return b;
}

void reserve_space(Buffer *b, size_t bytes) {
    if((b->next + bytes) > b->size) {
        // double size
        b->data = realloc(b->data, b->size * 2);
        b->size *= 2;
    }
}

void serialize_int(int x, Buffer *b) {
    reserve_space(b, sizeof(int));

    memcpy(((char *)b->data) + b->next, &x, sizeof(unsigned int));
    b->next += sizeof(unsigned int);
}

void serialize_char_array(char *src, int size, Buffer *b) {
    reserve_space(b, size);
    memcpy(((char *)b->data) + b->next, src, size);
    b->next += size;
}


void deserialize_int(Buffer *src, struct Element *dst) {
    memcpy(&dst->i, ((char *) src->data) + src->next, sizeof(unsigned int));
    src->next += sizeof(unsigned int);
}

void deserialize_char_array(Buffer *src, struct Element *dst, int size) {
    memcpy(dst->str, ((char *) src->data) + src->next, size);
    src->next += size;
}

    
/*
    tuple desc = type, length
    schema = new schema ('table_name', [id, title, genre], [my_unsigned_int, my_unsigned_char(100), my_unsigned_char(100)])
    row = [1, pianist, sad];
    serialize(row, schema, buffer);
     for field in schema.fields:
        // this or my_unsigned_int.serialize(to get rid of branches )
        switch(type) {
            case(my_unsigned_int):
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
                case(my_unsigned_int):
                    result_arr[i].type = my_unsigned_int;
                    deserialize_int(src_buffer, result_arr[i]);
                    break;
                    ..
            }
    new Record(buffer);
*/



int main(void) {
    struct Element *arr = malloc(sizeof(struct Element) * 3);

    char *string = { "hello"};
    Buffer *src = new_buffer();

    serialize_int(2147483640, src);
    serialize_char_array(string, 5, src);

    src->next = 0;    
    deserialize_int(src, &arr[0]);
    arr[1].str = malloc(100);
    deserialize_char_array(src, &arr[1], 100);
    char *address = src->data;

    printf("address %x\n", address[8]); 

    printf("deserialized int %d\n", arr[0].i);
    printf("deserialized char %s\n",arr[1].str); 

    printf("size %zu\n", src->size);
    printf("src next %u\n", src->next); 

}

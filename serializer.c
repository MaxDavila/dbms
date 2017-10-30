#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <serializer.h>

Buffer *new_buffer() {
    Buffer *b = malloc(sizeof(Buffer));
    b->data = malloc(DEFAULT_BUFFER_SIZE);
    b->size = DEFAULT_BUFFER_SIZE;
    b->offset = 0;

    return b;
}

Buffer *new_buffer_of_size(int size) {
    Buffer *b = malloc(sizeof(Buffer));
    b->data = malloc(size);
    b->size = size;
    b->offset = 0;

    return b;
}

void _reserve_space(Buffer *b, size_t bytes) {
    while ((b->offset + bytes) > b->size) {
        // double size
        b->data = realloc(b->data, b->size * 2);
        b->size *= 2;
    }
}

void serialize_int(int x, Buffer *b) {
    _reserve_space(b, sizeof(int));

    memcpy(((char *)b->data) + b->offset, &x, sizeof(unsigned int));
    b->offset += sizeof(unsigned int);
}

void serialize_double(double x, Buffer *b) {
    _reserve_space(b, sizeof(double));

    memcpy(((char *)b->data) + b->offset, &x, sizeof(double));
    b->offset += sizeof(double);
}

void serialize_char_array(char *src, int size, Buffer *b) {
    _reserve_space(b, size);
    memcpy(((char *)b->data) + b->offset, src, size);
    b->offset += size;
}

void deserialize_int(Buffer *src, struct Element *dst) {
    memcpy(&dst->i, ((char *) src->data) + src->offset, sizeof(unsigned int));
    src->offset += sizeof(unsigned int);
}

void deserialize_into_int(char *src, int offset, int *dst) {
    memcpy(dst, src + offset, sizeof(unsigned int));
}

void deserialize_double(Buffer *src, struct Element *dst) {
    memcpy(&dst->d, ((char *) src->data) + src->offset, sizeof(double));
    src->offset += sizeof(double);
}

void deserialize_into_double(char *src, int offset, double *dst) {
    memcpy(dst, src + offset, sizeof(double));
}

void deserialize_char_array(Buffer *src, struct Element *dst, int size) {
    memcpy(dst->str, ((char *) src->data) + src->offset, size);
    src->offset += size;
}

void deserialize_into_char_array(char *src, char *dst, int offset, int size) {
    memcpy(dst, src + offset, size);
}

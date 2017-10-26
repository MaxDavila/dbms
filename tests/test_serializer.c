#include <stdio.h>
#include "../serializer.h"

    
int main(void) {
    struct Element *arr = malloc(sizeof(struct Element) * 3);

    char *string = { "hello"};
    Buffer *src = new_buffer();

    serialize_int(2147483640, src);
    serialize_char_array(string, 200, src);
    serialize_double(4.9, src);

    src->offset = 0;    
    deserialize_int(src, &arr[0]);
    arr[1].str = malloc(200);
    deserialize_char_array(src, &arr[1], 200);
    deserialize_double(src, &arr[2]);

    char *address = src->data;
    printf("Runnig serializer tests\n"); 

    printf("address %x\n", address[8]); 

    printf("deserialized int %d\n", arr[0].i);
    printf("deserialized char %s\n",arr[1].str); 
    printf("deserialized double %1.1f\n",arr[2].d); 

    printf("size %zu\n", src->size);
    printf("src offset %u\n", src->offset); 

    printf("Finished serializer tests\n"); 

}
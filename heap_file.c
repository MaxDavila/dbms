#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void parse_line(char *line, ssize_t line_length, char *output[]) {
    char *new_string;
    int in = 0;
    int needle = 0;
    int start = 0;
    int output_i = 0;

    while (needle < line_length) {

        if ((line[needle] == ',' || line[needle] == '\n') && !in) {

            size_t new_size = needle - start;
            new_string = malloc(new_size);
            memcpy(new_string, line + start, new_size);
            output[output_i++] = new_string;
            start = needle + 1;
        } else if (line[needle] == '\"' && !in) {
            needle++;
            start++;

            in = 1;
        } else if (line[needle] == '\"' && in) {
            needle++;
            start++;
            in = 0;
        }

        needle++;
    }
}

int main(void) {
    FILE *fp;
    char *buffer = NULL;
    size_t buffer_len = 0;
    ssize_t line_length;

    fp = fopen("movielens/movies.csv", "r");
    if (fp == NULL)
        exit(EXIT_FAILURE);

    getline(&buffer, &buffer_len, fp); // read headers

    char *output[3];
    while ((line_length = getline(&buffer, &buffer_len, fp)) != -1) {
        printf("Retrieved line of length %zu :\n", line_length);
        printf("%s\n", buffer);
        parse_line(buffer, line_length, output);
    }


    printf("OUTPUT %s\n", output[0]);
    printf("OUTPUT2 %s\n", output[1]);
    printf("OUTPUT3 %s\n", output[2]);

    fclose(fp);
    if (buffer)
        free(buffer);
    exit(EXIT_SUCCESS);
}
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
// took admitedly longer than expected on parsing my csv file, today i'll keep working on my heap file
#include "cs165_api.h"
#include "utils.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/mman.h>
#include <unistd.h>

// MMAP includes
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

// limits for
#include <limits.h>
// library for int64
#include <stdint.h>
// library for select_parallel
#include <assert.h>
#include <pthread.h>
#include <math.h>
//Library for load files
#include <message.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <sys/sysinfo.h>
#include <time.h>

Db* current_db;

// Driver program to test above functions
int main(){

  //struct Status ret_status;
  int fd;
  struct stat mmapstat;

  clock_t t;
  t = clock();
  const char* filename = "tests/data2_generated.csv";

  FILE *fp = fopen(filename, "r");
  if(fp==NULL){
    log_err("Failed to open load file: %s\n", filename);
    return -1;
  }

  log_info("before malloc %f ms to execute \n", ((double) clock() - t)/CLOCKS_PER_SEC*1000);
  int* int_buffer_array = (int*) malloc(10000000*sizeof(int));
  log_info("after malloc %f ms to execute \n", ((double) clock() - t)/CLOCKS_PER_SEC*1000);

  size_t len = 0;
  ssize_t read_line = 0;

  //log_info("-- The number of columns is %ld --\n", columns_filled);
  // Extract characters from file and store in character c
  // int total_num_rows = 0;  // Line counter (result)
  // char c;
  // for (c = getc(fp); c != EOF; c = getc(fp)){
  //   if (c == '\n'){ // Increment count if this character is newline
  //       total_num_rows = total_num_rows + 1;
  //   }
  // }
  // total_num_rows--;

  char* line = NULL;
  int columns_filled = 4;
  size_t max_num_rows  = (size_t) floor(MAX_MESSAGE_SIZE/columns_filled/sizeof(int));
  //log_info("-- MAXIMUM NUM ROWS: %ld --\n", max_num_rows);
  //int* int_buffer_array = (int*) malloc(10000000*sizeof(int));
  int   offset = 0;
  char* buffer_allocated = (char*) malloc(LOAD_BUFFER_SIZE*sizeof(char));
  char* buffer = buffer_allocated;
  int num_rows = 0;
  char* number;

  while (0 < fread(buffer+offset, sizeof(char), LOAD_BUFFER_SIZE-offset, fp)) {
    while((line = strsep(&buffer, "\n"))!=NULL && buffer != NULL){
      for(int colum=0; colum<columns_filled; colum++){
         number = strsep(&line, ",");
         int_buffer_array[num_rows*columns_filled+colum] = atoi(number);
       }
       num_rows++;
    };

    buffer = buffer_allocated;
    offset = strlen(line);
    for(int i=0; i<offset; i++){
      buffer[i] = line[i];
    }

  }


  log_info("after scan %f ms to execute \n", ((double) clock() - t)/CLOCKS_PER_SEC*1000);

  fseek(fp, 0, SEEK_SET);

  log_info("after reset file pointer %f ms to execute \n", ((double) clock() - t)/CLOCKS_PER_SEC*1000);

  //printf("%d", total_num_rows);

  // for (off_t i = 0; i < mmapstat.st_size; i++)
  // {
  //   printf("Found character %c at %ji\n", map[i], (intmax_t)i);
  // }

  //Write the array to column
  // int* temp_array = (int*) malloc(sizeof(int)*len_allocated);
  // for (size_t i=0;i<len_occupied;i++){
  //   temp_array[i] = temp[i];
  // }

  // Don't forget to free the mmapped memory
  // if (munmap(buffer, mmapstat.st_size) == -1){
  //   close(fd);
  //   strcpy(ret_status.error_message, "read_column_file: munmmap failure");
  //   ret_status.code=ERROR;
  //   //return ret_status;
  // }

  // Un-mmaping doesn't close the file, so we still need to do that.
  close(fd);
  //*data = temp_array;

  //t = clock() - t;
  //double time_taken = ((double)t)/CLOCKS_PER_SEC*1000;
  log_info("db_startup() took %f ms to execute \n", ((double) clock() - t)/CLOCKS_PER_SEC*1000);

  //ret_status.code=OK;
  //return ret_status;

}

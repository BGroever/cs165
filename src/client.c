/* This line at the top is necessary for compilation on the lab machine and many other Unix machines.
Please look up _XOPEN_SOURCE for more details. As well, if your code does not compile on the lab
machine please look into this as a a source of error. */
#define _XOPEN_SOURCE

/**
 * client.c
 *  CS165 Fall 2018
 *
 * This file provides a basic unix socket implementation for a client
 * used in an interactive client-server database.
 * The client receives input from stdin and sends it to the server.
 * No pre-processing is done on the client-side.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include "cs165_api.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "common.h"
#include "message.h"
#include "utils.h"
#include <math.h>

// MMAP includes
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/mman.h>
#include <unistd.h>

#define DEFAULT_STDIN_BUFFER_SIZE 1024


/**
 * read_load_file()
 *
 * Reads load file
 * return '\0' if file does not present
 *
 **/

 void wait_for_response_from_server(int client_socket){

     message recv_message;
     //char *output_str = NULL;
     int len = 0;
     // Always wait for server response (even if it is just an OK message)
     if ((len = recv(client_socket, &(recv_message), sizeof(message), 0)) > 0) {
         if ((recv_message.status == OK_PRINT || recv_message.status == OK_NO_PRINT) &&
             (int) recv_message.length > 0) {
             // Calculate number of bytes in response package
             int num_bytes = (int) recv_message.length;
             char payload[num_bytes + 1];
             // Receive the payload and print it out
             if ((len = recv(client_socket, payload, num_bytes, 0)) > 0) {
                 payload[num_bytes] = '\0';
                 if(recv_message.status == OK_PRINT){
                   printf("%s\n", payload);
                 }
             }
         }
     } else {
         if (len < 0) {
             log_err("Failed to receive message.");
         } else {
             log_info("-- Server closed connection\n");
         }
         exit(1);
     }

 }

void send_read_buffer_to_server(char* read_buffer, int client_socket){

  message send_message;
  send_message.payload = read_buffer;
  send_message.length  = strlen(read_buffer);
  send_message.status  = OK;

  // send the query to server, server waits in parse_load for responds
  // of column values
  if (send(client_socket, &send_message, sizeof(message), 0) == -1) {
    log_err("Failed to send message header.");
    exit(1);
  }

  // Send the payload (query) to server
  if (send(client_socket, send_message.payload, send_message.length, 0) == -1) {
    log_err("Failed to send query payload.");
    exit(1);
  }

}

/** This function sends the file to the server
 **
 **
 ** return -1 when an error occured
 **
 **/

int send_file_to_server(char* read_buffer, int client_socket){

  send_read_buffer_to_server(read_buffer, client_socket);

  char* filename = get_filename(&read_buffer[0]);
  filename = trim_quotes(filename);

  // FILE *fp = fopen(filename, "r");
  // if(fp==NULL){
  //   log_err("Failed to open load file: %s\n", filename);
  //   return -1;
  // }
  //
  // char* line = NULL;
  // size_t len = 0;
  //
  // if(getline(&line, &len, fp) == -1){
  //   log_err("Failed to read from file: %s\n", filename);
  //   return -1;
  // }

  //const char* filename = "tests/data2_generated.csv";
  int fd;
  struct stat mmapstat;
  if ((fd = open(filename, O_RDONLY, (mode_t) 0666)) == -1) {
    log_err("send_file_to_server: open failure");
    //ret_status.code=ERROR;
   }

  // Get current size of the file
  if (stat(filename, &mmapstat) == -1) {
   close(fd);
   log_err("send_file_to_server: stat failure");
   //ret_status.code=ERROR;
  }

  // Check if file is empty
  if (mmapstat.st_size == 0){
    close(fd);
    log_err("send_file_to_server: file is empty");
    //ret_status.code=ERROR;
  }

  char* buffer_allocated = mmap(NULL, mmapstat.st_size, PROT_READ, MAP_SHARED, fd, 0);
  if (buffer_allocated == MAP_FAILED) {
     close(fd);
     log_err("send_file_to_server: mmap failure");
     //ret_status.code=ERROR;
  }

  char* buffer = (char*) strchr(buffer_allocated, '\n');
  message send_message;
  send_message.payload = buffer_allocated;
  send_message.length = (int)(buffer-buffer_allocated);
  send_message.status  = OK;

  if (send(client_socket, &send_message, sizeof(message), 0) == -1) {
    log_err("Failed to send message header.");
    exit(1);
  }

  // Send the payload (query) to server
  if (send(client_socket, send_message.payload, send_message.length, 0) == -1) {
    log_err("Failed to send query payload.");
    exit(1);
  }
  //free(line);
  //line = NULL;

  int success = -1;
  recv(client_socket, &success, sizeof(success), 0);
  if(success==0){
    log_err("Server faild to parse load file");
    return -1;
  }
  int columns_filled = 0;
  recv(client_socket, &columns_filled, sizeof(int), 0);

  //log_info("-- The number of columns is %ld --\n", columns_filled);

  int max_num_rows  = (int) floor(MAX_MESSAGE_SIZE/columns_filled/sizeof(int));
  log_info("-- MAXIMUM NUM ROWS: %ld --\n", max_num_rows);
  int* int_buffer_array = (int*) malloc(max_num_rows*columns_filled*sizeof(int));
  //char* number = NULL;

  // size_t num_rows = 0;
  // int done = 0;
  int_message send_int_message;
  // size_t read_line = 0;
  // int   offset = 0;
  // char* buffer = (char*) calloc(LOAD_BUFFER_SIZE+1, sizeof(char));
  // char* buffer_allocated = buffer;

  int num_rows=0;
  //int columns_filled=4;
  char* prior = NULL;
  int done = 0;
  //char* buffer = (char*) strchr(buffer_allocated, '\n');


  while (1){

      //read_line = fread(buffer+offset, sizeof(char), LOAD_BUFFER_SIZE-offset, fp);

      //buffer[LOAD_BUFFER_SIZE] = '\0';

      // Send the payload (query) to server
      if(send(client_socket, &done, 1, 0) == -1){
        log_err("Failed to send query payload.");
        exit(1);
      }
      if(done == 1){
        break;
      }

      do{
        for(int colum=0; colum<columns_filled; colum++){
           prior  = buffer + 1;
           buffer = (char*) strchr(buffer, ',');
           int_buffer_array[num_rows*columns_filled+colum] = strtol(prior, &buffer, 10);
           //printf("%d, ", int_buffer_array[num_rows*columns_filled+colum]);
         }

         buffer = (char*) strchr(buffer, '\n');
         //printf("\n");
         //printf("%ld", prior-buffer);
         //index = (ssize_t)(prior-buffer);
         //printf("%ld, %ld\n", (size_t) (buffer-buffer_allocated), (size_t) mmapstat.st_size);
         num_rows++;

      }while(num_rows < max_num_rows && (buffer+1-buffer_allocated) < mmapstat.st_size);

      if((buffer+1-buffer_allocated) >= mmapstat.st_size){
        done = 1;
      }

      //printf("%d\n", done);

      send_int_message.payload = int_buffer_array;
      send_int_message.length  = (num_rows*columns_filled)*sizeof(int);
      send_int_message.status  = OK;
      //log_info("-- %ld rows in %ld bytes send --\n", num_rows, send_int_message.length);

      if (send(client_socket, &send_int_message, sizeof(int_message), 0) == -1) {
        log_err("Failed to send message header.");
        exit(1);
      }

      // Send the payload (query) to server
      if (send(client_socket, send_int_message.payload, send_int_message.length, 0) == -1) {
        log_err("Failed to send query payload.");
        exit(1);
      }

      num_rows = 0;
  }

  // Don't forget to free the mmapped memory
  if (munmap(buffer_allocated, mmapstat.st_size) == -1){
    log_err("read_column_file: munmmap failure");
  }

  //free(buffer_allocated);
  free(int_buffer_array);
  close(fd);
  return 1;

}

/** This function print the output from the server
 **
 **
 ** return -1 when an error occured
 **
 **/

int print_from_server(char* read_buffer, int client_socket){
  // send command to server
  send_read_buffer_to_server(read_buffer, client_socket);
  // receive number of column and column length
  size_t data_length;
  size_t num_columns;
  recv(client_socket, &data_length, sizeof(data_length), 0);
  recv(client_socket, &num_columns, sizeof(num_columns), 0);
  // receive data types
  DataType datatypes[num_columns];
  recv(client_socket, &datatypes, sizeof(datatypes), 0);
  // allocate memory for the data
  Intermediate intermediates[num_columns];
  for(size_t i=0; i<num_columns; i++){
    if(datatypes[i]==INT){
      intermediates[i].type = INT;
      intermediates[i].data.ints = (int*) malloc(data_length*sizeof(int));
      intermediates[i].len_data = data_length;
    } else if(datatypes[i]==DOUBLE){
      intermediates[i].type = DOUBLE;
      intermediates[i].data.doubles = (double*) malloc(data_length*sizeof(double));
      intermediates[i].len_data = data_length;
    } else if(datatypes[i]==LONG){
      intermediates[i].type = LONG;
      intermediates[i].data.longs = (long*) malloc(data_length*sizeof(long));
      intermediates[i].len_data = data_length;
    }
  }
  // receive column data
  int done = 0;
  size_t size_payload;
  size_t index;
  size_t size_element;
  for(size_t i=0; i<num_columns; i++){
    if(intermediates[i].type==INT){
      //Calculate the maximum numbers of elements
      size_t max_num_elements = (size_t) floor(MAX_MESSAGE_SIZE/sizeof(int));
      int recv_buffer[max_num_elements];
      size_element = sizeof(int);
      // receive data inside the column
      done = 0;
      index = 0;
      while(done==0){
        //receive size of payload
        recv(client_socket, &size_payload, sizeof(size_payload), 0);
        //receive payload
        recv(client_socket, &recv_buffer, size_payload*size_element, 0);
        //save data in array
        for(size_t j=index; j<(index+size_payload); j++){
          intermediates[i].data.ints[j] = recv_buffer[j-index];
        }
        //receive done message
        index = index+size_payload;
        recv(client_socket, &done, sizeof(int), 0);
      }
      //free(recv_buffer);
    } else if(intermediates[i].type==DOUBLE){
      //Calculate the maximum numbers of elements
      size_t max_num_elements = (size_t) floor(MAX_MESSAGE_SIZE/sizeof(double));
      double recv_buffer[max_num_elements];
      size_element = sizeof(double);
      // receive data inside the column
      done = 0;
      index = 0;
      while(done==0){
        //receive size of payload
        recv(client_socket, &size_payload, sizeof(size_payload), 0);
        //receive payload
        recv(client_socket, &recv_buffer, size_payload*size_element, 0);
        //save data in array
        for(size_t j=index; j<(index+size_payload); j++){
          intermediates[i].data.doubles[j] = recv_buffer[j-index];
        }
        //receive done message
        index = index+size_payload;
        recv(client_socket, &done, sizeof(int), 0);
      }
    } else if(intermediates[i].type==LONG){
      //Calculate the maximum numbers of elements
      size_t max_num_elements = (size_t) floor(MAX_MESSAGE_SIZE/sizeof(long));
      long recv_buffer[max_num_elements];
      size_element = sizeof(long);
      // receive data inside the column
      done = 0;
      index = 0;
      while(done==0){
        //receive size of payload
        recv(client_socket, &size_payload, sizeof(size_payload), 0);
        //receive payload
        recv(client_socket, &recv_buffer, size_payload*size_element, 0);
        //save data in array
        for(size_t j=index; j<(index+size_payload); j++){
          intermediates[i].data.longs[j] = recv_buffer[j-index];
        }
        //receive done message
        index = index+size_payload;
        recv(client_socket, &done, sizeof(int), 0);
      }
    }
  }

  // Print values
  // int k = 0;
  for(size_t i = 0; i < data_length; i++){
    for(size_t j = 0; j < num_columns; j++){
      if(intermediates[j].type == INT){
        if(j == (num_columns-1)){
          //k+=sprintf(buffer+k,"%d",intermediates[j]->data.ints[i]);
          printf("%d", intermediates[j].data.ints[i]);
        }else{
          //k+=sprintf(buffer+k,"%d, ",intermediates[j]->data.ints[i]);
          printf("%d,", intermediates[j].data.ints[i]);
        }
      }else if(intermediates[j].type == DOUBLE){
        if(j == (num_columns-1)){
          //k+=sprintf(buffer+k,"%0.2f",intermediates[j]->data.doubles[i]);
          printf("%0.2f",intermediates[j].data.doubles[i]);
        }else{
          //k+=sprintf(buffer+k,"%0.2f, ",intermediates[j]->data.doubles[i]);
          printf("%0.2f,",intermediates[j].data.doubles[i]);
        }
      }else if(intermediates[j].type == LONG){
        if(j == (num_columns-1)){
          //k+=sprintf(buffer+k,"%0.2f",intermediates[j]->data.doubles[i]);
          printf("%ld",intermediates[j].data.longs[i]);
        }else{
          //k+=sprintf(buffer+k,"%0.2f, ",intermediates[j]->data.doubles[i]);
          printf("%ld,",intermediates[j].data.longs[i]);
        }
      }
    }
    printf("\n");
    //k+=sprintf(buffer+k,"\n");
  }
  printf("\n");

  // deallocate memory
  for(size_t i=0; i<num_columns; i++){
    if(intermediates[i].type==INT){
      free(intermediates[i].data.ints);
    }else if(intermediates[i].type==DOUBLE){
      free(intermediates[i].data.doubles);
    }else if(intermediates[i].type==LONG){
      free(intermediates[i].data.longs);
    }
  }

  return 1;

}

/**
 * connect_client()
 *
 * This sets up the connection on the client side using unix sockets.
 * Returns a valid client socket fd on success, else -1 on failure.
 *
 **/
int connect_client() {
    int client_socket;
    size_t len;
    struct sockaddr_un remote;

    log_info("-- Attempting to connect...\n");

    if ((client_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    remote.sun_family = AF_UNIX;
    strncpy(remote.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    len = strlen(remote.sun_path) + sizeof(remote.sun_family) + 1;
    if (connect(client_socket, (struct sockaddr *)&remote, len) == -1) {
        log_err("client connect failed: ");
        return -1;
    }

    log_info("-- Client connected at socket: %d.\n", client_socket);
    return client_socket;
}

/**
 * Getting Started Hint:
 *      What kind of protocol or structure will you use to deliver your results from the server to the client?
 *      What kind of protocol or structure will you use to interpret results for final display to the user?
 *
**/
int main(void){

    int client_socket = connect_client();
    if (client_socket < 0) {
        exit(1);
    }

    //message send_message;
    //message recv_message;

    // Always output an interactive marker at the start of each command if the
    // input is from stdin. Do not output if piped in from file or from other fd
    char* prefix = "";
    if (isatty(fileno(stdin))) {
        prefix = "db_client > ";
    }

    char *output_str = NULL;
    //int len = 0;

    // Continuously loop and wait for input. At each iteration:
    // 1. output interactive marker
    // 2. read from stdin until eof.
    char read_buffer[DEFAULT_STDIN_BUFFER_SIZE];
    //send_message.payload = read_buffer;
    //send_message.status = 0;

    while (printf("%s", prefix), output_str = fgets(read_buffer, DEFAULT_STDIN_BUFFER_SIZE, stdin), !feof(stdin)) {
        if (output_str == NULL) {
            log_err("fgets failed.\n");
            break;
        }
        // Send message to the server
        // When loading a file or printing open up a sepearate channel
        if(strncmp(read_buffer, "load(", 5)==0){
            if(send_file_to_server(read_buffer, client_socket)==-1){
              log_err("error when sending file to server");
            }
            wait_for_response_from_server(client_socket);
        }else if(strncmp(read_buffer, "print(", 6)==0){
            if(print_from_server(read_buffer, client_socket)==-1){
              log_err("error when printing from server");
            }
            wait_for_response_from_server(client_socket);
        }else{
            if(strlen(read_buffer)>1){
              send_read_buffer_to_server(read_buffer, client_socket);
              wait_for_response_from_server(client_socket);
            }
        }
    }
    close(client_socket);
    return 0;
}

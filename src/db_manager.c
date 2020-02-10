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
#include <stdio.h>
#include <time.h>

#define SetBit(A,k) (A[(k/32)] |= (1 << (k%32)))
#define TestBit(A,k) (A[(k/32)] & (1 << (k%32)))
#define ClearBit(A,k) (A[(k/32)] &= ~(1 << (k%32)))

Db *current_db;
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
BatchCollection* batch;
size_t iter = 0;

Status relational_insert(Table *table, int* values){

   struct Status ret_status;

   //Check if all columns have been initialized
   for(size_t i=0; i < table -> col_count; i++){
     if (table -> columns[i] == NULL){
       ret_status.code = ERROR;
       strcpy(ret_status.error_message, "relational_insert: not fully initialized");
       return ret_status;
     }
   }

   // Check if new memory needs to be allocated
   if(table->len_allocated <= table->len_occupied){
     for(size_t i=0; i < table -> col_count; i++){
        table->columns[i]->data = (int*) realloc(table->columns[i]->data, 2*(table->len_allocated)*sizeof(int));
        if(table->columns[i]->data == NULL){
          ret_status.code = ERROR;
          strcpy(ret_status.error_message, "relational_insert: realloc insert table failed");
          return ret_status;
        }
     }
     if(table->deletes!=NULL){
       table->deletes = (int*) realloc(table->deletes, (2*(table->len_allocated)/32+1)*sizeof(int));
       if(table->deletes == NULL){
         ret_status.code = ERROR;
         strcpy(ret_status.error_message, "relational_insert: realloc table deletes failed!");
         return ret_status;
       }
     }
     table->len_allocated = 2*(table->len_allocated);
   }

   // Copy data from values to database
   for(size_t i=0; i < table -> col_count; i++){
     table->columns[i]->data[table->len_occupied] = values[i];
   }

   if(table->deletes!=NULL){
     ClearBit(table->deletes, table->len_occupied);
   }

   table->len_occupied += 1;

   ret_status.code=OK;
   return ret_status;

}

Status create_column(Table *table, char *name, bool sorted){

   struct Status ret_status;

   // Check if table name is not too long
   if (strlen(name) > MAX_SIZE_NAME){
      ret_status.code = ERROR;
      strcpy(ret_status.error_message, "create_column: column name is too long!");
      return ret_status;
   }


  // Check if column called 'name' already exists inside the tbl
   for (size_t i=0; i < table->col_count; i++){
     if(table -> columns[i] != NULL){
       if (strcmp(table -> columns[i] -> name, name) == 0){
         ret_status.code = ERROR;
         sprintf(ret_status.error_message, "%s%s%s", "create_column: ", name, " already exist in db!");
         return ret_status;
       }
     }
   }

   // Find the position to insert column and check if there is a free spot inside table
   size_t insert_index = 0;
   while(table->columns[insert_index] != NULL && insert_index < table->col_count){
     insert_index += 1;
   }
   if (insert_index == table->col_count){
     ret_status.code = ERROR;
     strcpy(ret_status.error_message, "create_column: tbl is full!");
     return ret_status;
   }

   // create_column malloc 1
   Column* column = (Column*) malloc(sizeof(Column));

   // Check if malloc 1 worked
   if (column == NULL){
     ret_status.code = ERROR;
     strcpy(ret_status.error_message, "create_column: malloc column failed!");
     return ret_status;
   }

   //Initilization of column values
   strcpy(column -> name, name);
   column -> sorted = sorted;
   column -> secondary_index = NULL;
   table->columns[insert_index] = column;

   // create_table malloc 2
   int* data = (int*) malloc(sizeof(int)*INITIAL_COL_SIZE);
   table -> len_allocated = INITIAL_COL_SIZE;

   // Check if malloc 2 worked
   if (data == NULL){
     ret_status.code = ERROR;
     strcpy(ret_status.error_message, "create_column: malloc data failed!");
     return ret_status;
   }

   column -> data = data;
   ret_status.code=OK;
   return ret_status;

 }

Status create_table(Db* db, const char* name, size_t num_columns) {

  struct Status ret_status;

  // Check if table name is not too long
  if (strlen(name) > MAX_SIZE_NAME){
     ret_status.code = ERROR;
     strcpy(ret_status.error_message, "create_table: tbl name is too long!");
     return ret_status;
  }


 // Check if table called 'name' already exists inside the db
  for (size_t i=0; i< db->tables_capacity; i++){
    if(db -> tables[i] != NULL){
      if (strcmp(db -> tables[i] -> name, name) == 0){
        ret_status.code = ERROR;
        sprintf(ret_status.error_message, "%s%s%s", "create_table: ", name, " already exist in db!");
        return ret_status;
      }
    }
  }

  // Find the position to insert table and check if there is a free spot inside database
  size_t insert_index = 0;
  while(db->tables[insert_index] != NULL && insert_index < db->tables_capacity){
    insert_index += 1;
  }

  if (insert_index == db->tables_capacity){
    ret_status.code = ERROR;
    strcpy(ret_status.error_message, "create_table: db is full!");
    return ret_status;
  }

  //size_t size = (size_t) MAX_SIZE_NAME + 3*sizeof(size_t) + sizeof(Column**);
  Table* table = (Table*) malloc(sizeof(Table));

  // Check if malloc worked
  if (table == NULL){
    ret_status.code = ERROR;
    strcpy(ret_status.error_message, "create_table: malloc 1 failed!");
    return ret_status;
  }

  strcpy(table -> name, name);
  table -> col_count = num_columns;
  table -> len_indexed = 0;
  table -> len_allocated = 0;
  table -> len_occupied = 0;
  table -> primary_index = NULL;
  table -> deletes = NULL;
  table -> select_queries_concession = 0;
  table -> dirty = false;
  table -> changed = false;

  // create_table malloc 2
  Column** columns = (Column**) malloc(num_columns*sizeof(Column*));

  // Check if malloc 2 worked
  if (table == NULL){
    ret_status.code = ERROR;
    strcpy(ret_status.error_message, "create_table: malloc 2 failed!");
    free(table);
    return ret_status;
  }

  // Initilization of column addresses to null vectors
  for (size_t i=0; i<num_columns; i++){
    *(columns+i) = (Column*) NULL;
  }

  table -> columns = columns;
  db -> tables[insert_index] = table;

  ret_status.code=OK;
  return ret_status;

}

Status create_db(const char* db_name) {

  // Initilization, set status and compute size of database
	struct Status ret_status;

  if (current_db!=NULL){
    ret_status = deallocate();
    if(ret_status.code==ERROR){
      return ret_status;
    }
    system("rm -rf cs165.db");
  }

  // Check if the db name is valid
  if (strlen(db_name) > MAX_SIZE_NAME){
    ret_status.code = ERROR;
    strcpy(ret_status.error_message, "create_db: db name is too long!");
    return ret_status;
  }

  // create_db malloc 1
  current_db = (Db*) malloc(sizeof(Db));

  // Check if malloc above worked
  if (current_db == NULL){
    ret_status.code = ERROR;
    strcpy(ret_status.error_message, "create_db: malloc 1 failed!");
    return ret_status;
  }

  // Initilization of db values
  strcpy(current_db -> name, db_name);
  current_db -> tables_capacity = MAX_TABLES_PER_DATABASE;
  current_db -> tables_size			= MAX_TABLES_PER_DATABASE;

  // create_db malloc 2
  Table** tables = (Table**) malloc(MAX_TABLES_PER_DATABASE*sizeof(Table*));

  // Check if malloc above worked
  if (tables == NULL){
    ret_status.code = ERROR;
    strcpy(ret_status.error_message, "create_db: malloc 2 failed!");
    free(current_db);
    return ret_status;
  }

  // Initilization of table addresses to null vectors
  for (int i=0; i<MAX_TABLES_PER_DATABASE; i++){
    *(tables+i) = (Table*) NULL;
  }

  Intermediate** intermediates = (Intermediate**) malloc(MAX_INTERMEDIATES*sizeof(Intermediate*));

  // Check if malloc above worked
  if (intermediates == NULL){
    ret_status.code = ERROR;
    strcpy(ret_status.error_message, "create_db: malloc 3 failed!");
    free(current_db);
    return ret_status;
  }

  // Initilization of intermediate addresses to null vectors
  for (int i=0; i<MAX_INTERMEDIATES; i++){
    *(intermediates+i) = (Intermediate*) NULL;
  }

  current_db -> intermediates = intermediates;
  current_db -> tables = tables;

  ret_status.code = OK;
  return ret_status;

}

Status print_handle(Intermediate** intermediates, size_t num_columns, int client_socket){

  struct Status ret_status;

  // Raise an error when elements are equal to null
  for(size_t i=0; i<num_columns; i++){
    if(intermediates[i] == NULL){
      ret_status.code=ERROR;
      strcpy(ret_status.error_message, "print_handle: null pointer");
      return ret_status;
    }
  }

  //Check the length of each intermediate
  size_t data_length = intermediates[0] -> len_data;
  for(size_t i=1; i<num_columns; i++){
    if(intermediates[i]->len_data != data_length){
      ret_status.code=ERROR;
      strcpy(ret_status.error_message, "print_handle: uneven data length");
      return ret_status;
    }
  }

  // send data_length and number of columns to client
  if(send(client_socket, &data_length, sizeof(data_length), 0)==-1){
    strcpy(ret_status.error_message, "print_handle: failed to send data_length");
    return ret_status;
  }
  if(send(client_socket, &num_columns, sizeof(num_columns), 0)==-1){
    strcpy(ret_status.error_message, "print_handle: failed to send num_columns");
    return ret_status;
  }

  // send data types to client
  DataType datatypes[num_columns];
  for(size_t i=0; i<num_columns; i++){
    datatypes[i] = intermediates[i]->type;
  }
  if(send(client_socket, &datatypes, sizeof(datatypes), 0)==-1){
    strcpy(ret_status.error_message, "print_handle: failed to send datatypes");
    return ret_status;
  }

  // send column data
  int done;
  size_t start_scan;
  size_t end_scan;
  size_t size_payload;
  for(size_t i=0; i<num_columns; i++){
    if(datatypes[i]==INT){
      // calculate maximum number of elements
      size_t max_num_elements = (size_t) floor(MAX_MESSAGE_SIZE/sizeof(int));
      int* send_buffer = (int*) malloc(max_num_elements*sizeof(int));

      // receive data inside the column
      done = 0;
      start_scan = 0;
      end_scan   = 0;
      while(done==0){
        //determine size of payload
        if((data_length-start_scan)>max_num_elements){
          size_payload = max_num_elements;
        } else {
          size_payload = data_length - end_scan;
          done = 1;
        }
        end_scan = start_scan + size_payload;
        //send size of payload
        if(send(client_socket, &size_payload, sizeof(size_payload), 0)==-1){
          strcpy(ret_status.error_message, "print_handle: failed to send payload size");
          return ret_status;
        }
        //send payload
        for(size_t j=start_scan; j<end_scan; j++){
          send_buffer[j-start_scan] = intermediates[i]->data.ints[j];
        }
        if(send(client_socket, send_buffer, size_payload*sizeof(int), 0)==-1){
          strcpy(ret_status.error_message, "print_handle: failed to send data");
          return ret_status;
        }
        //send done message
        start_scan  =  end_scan;
        if(send(client_socket, &done, sizeof(int), 0)==-1){
          strcpy(ret_status.error_message, "print_handle: failed to send done msg");
          return ret_status;
        }
      }
      free(send_buffer);
    } else if(datatypes[i]==DOUBLE){
      // calculate maximum number of elements
      size_t max_num_elements = (size_t) floor(MAX_MESSAGE_SIZE/sizeof(double));
      double* send_buffer = (double*) malloc(max_num_elements*sizeof(double));

      // receive data inside the column
      done = 0;
      start_scan = 0;
      end_scan   = 0;
      while(done==0){
        //determine size of payload
        if((data_length-start_scan)>max_num_elements){
          size_payload = max_num_elements;
        } else {
          size_payload = data_length - end_scan;
          done = 1;
        }
        end_scan = start_scan + size_payload;
        //send size of payload
        if(send(client_socket, &size_payload, sizeof(size_payload), 0)==-1){
          strcpy(ret_status.error_message, "print_handle: failed to send payload size");
          return ret_status;
        }
        //send payload
        for(size_t j=start_scan; j<end_scan; j++){
          send_buffer[j-start_scan] = intermediates[i]->data.doubles[j];
        }
        if(send(client_socket, send_buffer, size_payload*sizeof(double), 0)==-1){
          strcpy(ret_status.error_message, "print_handle: failed to send data");
          return ret_status;
        }
        //send done message
        start_scan  =  end_scan;
        if(send(client_socket, &done, sizeof(int), 0)==-1){
          strcpy(ret_status.error_message, "print_handle: failed to send done msg");
          return ret_status;
        }
      }
      free(send_buffer);
    } else if(datatypes[i]==LONG){
      // calculate maximum number of elements
      size_t max_num_elements = (size_t) floor(MAX_MESSAGE_SIZE/sizeof(long));
      long* send_buffer = (long*) malloc(max_num_elements*sizeof(long));

      // receive data inside the column
      done = 0;
      start_scan = 0;
      end_scan   = 0;
      while(done==0){
        //determine size of payload
        if((data_length-start_scan)>max_num_elements){
          size_payload = max_num_elements;
        } else {
          size_payload = data_length - end_scan;
          done = 1;
        }
        end_scan = start_scan + size_payload;
        //send size of payload
        if(send(client_socket, &size_payload, sizeof(size_payload), 0)==-1){
          strcpy(ret_status.error_message, "print_handle: failed to send payload size");
          return ret_status;
        }
        //send payload
        for(size_t j=start_scan; j<end_scan; j++){
          send_buffer[j-start_scan] = intermediates[i]->data.longs[j];
        }
        if(send(client_socket, send_buffer, size_payload*sizeof(long), 0)==-1){
          strcpy(ret_status.error_message, "print_handle: failed to send data");
          return ret_status;
        }
        //send done message
        start_scan  =  end_scan;
        if(send(client_socket, &done, sizeof(int), 0)==-1){
          strcpy(ret_status.error_message, "print_handle: failed to send done msg");
          return ret_status;
        }
      }
      free(send_buffer);
    }
  }

  ret_status.code=OK;
  return ret_status;

}


Status db_shutdown(){

  struct Status ret_status;

  // remove previous files
  if( access( "cs165.db", F_OK ) == -1 ){
    system("mkdir cs165.db");
  }

  // create file
  FILE *fptr = fopen("cs165.db/meta.txt", "w");
  if (fptr==NULL){
    ret_status.code=ERROR;
    strcpy(ret_status.error_message, "shutdown: could not open file");
    return ret_status;
  }

  // write database name to file
  if(fprintf(fptr,"Database: %s\n", current_db -> name) <= 0){
    ret_status.code=ERROR;
    strcpy(ret_status.error_message, "shutdown: could not write database name");
    return ret_status;
  }

  // write tables and columns to file
  for(size_t i=0; i < current_db -> tables_capacity; i++){

    if(current_db->tables[i] != NULL){

      // write table name, col_count, len_occupied, len_allocated to file
      if(fprintf(fptr,"Tbl%ld:\t%s\n",  i, current_db->tables[i]-> name) <= 0){
        strcpy(ret_status.error_message, "shutdown: could not write table name");
        ret_status.code=ERROR; return ret_status;
      }
      if(fprintf(fptr,"col_count:\t%lu\n", current_db->tables[i]-> col_count)<= 0){
        strcpy(ret_status.error_message, "shutdown: could not write col_count");
        ret_status.code=ERROR; return ret_status;
      }
      if(fprintf(fptr,"len_indexed:\t%lu\n", current_db->tables[i]-> len_indexed) <= 0){
        strcpy(ret_status.error_message, "shutdown: could not write len_indexed");
        ret_status.code=ERROR; return ret_status;
      }
      if(fprintf(fptr,"len_occupied:\t%lu\n", current_db->tables[i]-> len_occupied) <= 0){
        strcpy(ret_status.error_message, "shutdown: could not write len_occupied");
        ret_status.code=ERROR; return ret_status;
      }
      if(fprintf(fptr,"len_allocated:\t%lu\n", current_db->tables[i]-> len_allocated) <= 0){
        strcpy(ret_status.error_message, "shutdown: could not write len_allocated");
        ret_status.code=ERROR; return ret_status;
      }
      // write primary index to file
      if(current_db->tables[i]->primary_index != NULL){
        if(fprintf(fptr,"p_index:\t%u\n", current_db->tables[i]->primary_index->type) <= 0){
          strcpy(ret_status.error_message, "shutdown: could not write primary index");
          ret_status.code=ERROR; return ret_status;
        }
        if(fprintf(fptr,"p_index:\t%d\n", current_db->tables[i]->primary_index->col_index) <= 0){
          strcpy(ret_status.error_message, "shutdown: could not write primary index");
          ret_status.code=ERROR; return ret_status;
        }
      }else{
        if(fprintf(fptr,"p_index:\t-1\n") <= 0){
          strcpy(ret_status.error_message, "shutdown: could not write primary index");
          ret_status.code=ERROR; return ret_status;
        }
        if(fprintf(fptr,"p_index:\t-1\n") <= 0){
          strcpy(ret_status.error_message, "shutdown: could not write primary index");
          ret_status.code=ERROR; return ret_status;
        }
      }

      //printf("col_count = %lu\n", current_db->tables[i]->col_count);
      for(size_t j=0; j < (current_db->tables[i]->col_count); j++){
        //printf("i,j = %lu, %lu\n", i, j);
        if(current_db->tables[i]->columns[j] != NULL){
          if(fprintf(fptr,"Col%ld:\t%s\n",  j, current_db->tables[i]->columns[j]->name) <= 0){
            strcpy(ret_status.error_message, "shutdown: could not write col name");
            ret_status.code=ERROR; return ret_status;
          }
          if(fprintf(fptr,"Col%ld:\t%d\n",  j, current_db->tables[i]->columns[j]->sorted) <= 0){
            strcpy(ret_status.error_message, "shutdown: could not write sorted");
            ret_status.code=ERROR; return ret_status;
          }
          if(current_db->tables[i]->columns[j]->data != NULL && current_db->tables[i]->changed == true){
            char filename[MAX_SIZE_NAME];
            sprintf(filename, "cs165.db/%lu_%lu.txt", i, j);
            //printf("%s\n", filename);
            ret_status = write_column_file(filename,current_db->tables[i]->columns[j]->data, current_db->tables[i]->len_occupied);
            if(ret_status.code==ERROR){
              return ret_status;
            }
          } // close if data pointer != NULL
          if(current_db->tables[i]->columns[j] != NULL){
            if(current_db->tables[i]->columns[j]->secondary_index!= NULL){
              if(fprintf(fptr,"sec_index:\t%u\n", current_db->tables[i]->columns[j]->secondary_index->type) <= 0){
                strcpy(ret_status.error_message, "shutdown: could not write secondary index");
                ret_status.code=ERROR; return ret_status;
              }
              if(current_db->tables[i]->columns[j]->secondary_index->data != NULL && current_db->tables[i]->changed == true){
                char filename[MAX_SIZE_NAME];
                sprintf(filename, "cs165.db/%lu_%lu_sec_data.txt", i, j);
                ret_status = write_column_file(filename,current_db->tables[i]->columns[j]->secondary_index->data,current_db->tables[i]->len_indexed);
                if(ret_status.code==ERROR){
                  return ret_status;
                }
              }
              if(current_db->tables[i]->columns[j]->secondary_index->pos != NULL && current_db->tables[i]->changed == true){
                char filename[MAX_SIZE_NAME];
                sprintf(filename, "cs165.db/%lu_%lu_sec_pos.txt", i, j);
                ret_status = write_column_file(filename,current_db->tables[i]->columns[j]->secondary_index->pos,current_db->tables[i]->len_indexed);
                if(ret_status.code==ERROR){
                  return ret_status;
                }
              }
            }else{
              if(fprintf(fptr,"sec_index:\t-1\n") <= 0){
                strcpy(ret_status.error_message, "shutdown: could not write secondary index");
                ret_status.code=ERROR; return ret_status;
              }
            } // close secondary index if
          } // close if column pointer != NULL
        } // close if column pointer != NULL
      } //close for loop column pointers
    } //close if table pointer != NULL
  } // close for loop table pointers

  fclose(fptr);
  ret_status = deallocate();
  if(ret_status.code==ERROR){
    return ret_status;
  }
  ret_status.code=OK;
  return ret_status;

}

Status write_column_file(char* filename, int* data, size_t length){

  // Reference (examples) for mmap and and munmap
  //https://www.tutorialspoint.com/inter_process_communication/inter_process_communication_memory_mapping.htm
  //https://gist.github.com/marcetcheverry/991042

  struct Status ret_status;
  int fd;
  struct stat mmapstat;

  if ((fd = open(filename, O_CREAT|O_RDWR|O_TRUNC, (mode_t) 0666)) == -1) {
      strcpy(ret_status.error_message, "write_column_file: open failure");
      ret_status.code=ERROR;
      return ret_status;
   }

  // Get current size of the file
  if (stat(filename, &mmapstat) == -1) {
     close(fd);
     strcpy(ret_status.error_message, "write_column_file: stat failure");
     ret_status.code=ERROR;
     return ret_status;
  }
  //printf("File size is: %ld\n", mmapstat.st_size);

  // Move the file size
  //Table* tbl = current_db->tables[0];
  //size_t filesize = length*sizeof(int);
  if (lseek(fd, length*sizeof(int), SEEK_SET) == -1){
    close(fd);
    strcpy(ret_status.error_message, "write_column_file: lseek failure");
    ret_status.code=ERROR;
    return ret_status;
  }

  // Something needs to be written to file to make new size effective
  if (write(fd, "", 1) == -1){
    close(fd);
    strcpy(ret_status.error_message, "write_column_file: write '\0'");
    ret_status.code=ERROR;
    return ret_status;
  }

  // Check the size of the file again
  if (stat(filename, &mmapstat) == -1) {
    close(fd);
    strcpy(ret_status.error_message, "write_column_file: stat failure");
    ret_status.code=ERROR;
    return ret_status;
  }
  //printf("File size is: %ld\n", mmapstat.st_size);

  int* temp = mmap(NULL, length*sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (temp == MAP_FAILED) {
     close(fd);
     strcpy(ret_status.error_message, "write_column_file: mmap failure");
     ret_status.code=ERROR;
     return ret_status;
  }

  for (size_t i=0; i<length; i++){
    temp[i] = data[i];
  }

  // Don't forget to free the mmapped memory
  if (munmap(temp, length*sizeof(int)) == -1){
    close(fd);
    strcpy(ret_status.error_message, "write_column_file: munmmap failure");
    ret_status.code=ERROR;
    return ret_status;
  }

  close(fd);

  ret_status.code=OK;
  return ret_status;

}

// Status write_column_file(char* filename, int* data, size_t length){
//
//   struct Status ret_status;
//
//   FILE *f = fopen(filename, "w");  // Try changing to "a"
//   if (f==NULL){
//     ret_status.code=ERROR;
//     strcpy(ret_status.error_message, "shutdown: could not open file");
//     return ret_status;
//   }
//
//   for(size_t i=0;i<length;i++){
//     fprintf(f, "%d\n", data[i]);
//   }
//   fclose(f);
//
//   ret_status.code=OK;
//   return ret_status;
//
// }

Status db_startup(){

  struct Status ret_status;

  if( access( "cs165.db", F_OK ) == -1 ){
    ret_status.code=OK;
    log_info("%s\n", "Db file does not exists, newly created");
    return ret_status;
  }

  // create file
  FILE *fptr = fopen("cs165.db/meta.txt", "r");

  if(fptr==NULL){
    ret_status.code=ERROR;
    strcpy(ret_status.error_message, "db_startup: could not open file!");
    return ret_status;
  }
  log_info("meta.txt loaded\n");

  char buf[MAX_SIZE_NAME*2];

  //Create database if database name exist
  if(fscanf(fptr,"%*s %s ",buf)==1){
    ret_status = create_db(buf);
    if(ret_status.code==ERROR){
      return ret_status;
    }
  }else{
    ret_status.code=ERROR;
    strcpy(ret_status.error_message, "db_startup: database name does not exists in file!");
    return ret_status;
  }

  int number_secondary_index = 0;

  //Read all i tables
  size_t i = 0;
  while(fscanf(fptr,"%*s %s ",buf)==1){
    char tbl_name[MAX_SIZE_NAME];
    size_t col_count;
    size_t len_indexed;
    size_t len_occupied;
    size_t len_allocated;
    strcpy(tbl_name, buf);
    int prim_type = -1;
    int prim_column_index = -1;
    int secondary_type = -1;
    if(fscanf(fptr,"%*s %s ",buf)==1){
      col_count = (size_t) atoi(buf);
    }else{
      ret_status.code=ERROR;
      strcpy(ret_status.error_message, "db_startup: read col_count failed");
      return ret_status;
    }
    if(fscanf(fptr,"%*s %s ",buf)==1){
      len_indexed = (size_t) atoi(buf);
    }else{
      ret_status.code=ERROR;
      strcpy(ret_status.error_message, "db_startup: read len_indexed failed");
      return ret_status;
    }
    if(fscanf(fptr,"%*s %s ",buf)==1){
      len_occupied = (size_t) atoi(buf);
    }else{
      ret_status.code=ERROR;
      strcpy(ret_status.error_message, "db_startup: read len_occupied failed");
      return ret_status;
    }
    if(fscanf(fptr,"%*s %s ",buf)==1){
      len_allocated = (size_t) atoi(buf);
    }else{
      ret_status.code=ERROR;
      strcpy(ret_status.error_message, "db_startup: read len_allocated failed");
      return ret_status;
    }

    ret_status = create_table(current_db, tbl_name, col_count);
    if(ret_status.code==ERROR){
      return ret_status; // Pass error of create_table to function caller
    }

    if(fscanf(fptr,"%*s %s ",buf)==1){
      prim_type = (int) atoi(buf);
    }else{
      ret_status.code=ERROR;
      strcpy(ret_status.error_message, "db_startup: read primary index type");
      return ret_status;
    }
    if(fscanf(fptr,"%*s %s ",buf)==1){
      prim_column_index = (int) atoi(buf);
    }else{
      ret_status.code=ERROR;
      strcpy(ret_status.error_message, "db_startup: read primary index column index");
      return ret_status;
    }

    number_secondary_index = 0;
    Column** sec_column = (Column**) malloc(col_count*sizeof(Column*));
    IndexType* sec_type = (IndexType*) malloc(col_count*sizeof(IndexType));
    size_t* col_number = (size_t*) malloc(col_count*sizeof(size_t));


    for(size_t j = 0; j < col_count; j++){
      char col_name[100];
      bool sorted;

      if(fscanf(fptr,"%*s %s ",buf)==1){
        strcpy(col_name, buf);
      }else{
        ret_status.code=ERROR;
        strcpy(ret_status.error_message, "db_startup: read column name failed");
        return ret_status;
      }

      if(fscanf(fptr,"%*s %s ",buf)==1){
        sorted = (atoi(buf)==1);
      }else{
        ret_status.code=ERROR;
        strcpy(ret_status.error_message, "db_startup: read sorted failed");
        return ret_status;
      }

      ret_status = create_column(current_db->tables[i], col_name, sorted);
      if(ret_status.code==ERROR){
        return ret_status;  // if read_column fn failed pass error to db_startup fn caller
      }

      //free inital column size
      free(current_db->tables[i]->columns[j]->data);

      char filename[MAX_SIZE_NAME];
      sprintf(filename, "cs165.db/%lu_%lu.txt", i, j);
      log_info("data from column file: '%s' loaded\n", filename);
      ret_status = read_column_file(filename,
                                    &(current_db->tables[i]->columns[j]->data),
                                    len_occupied,
                                    len_allocated);
      if(ret_status.code==ERROR){
        return ret_status;
      }

      if(fscanf(fptr,"%*s %s ",buf)==1){
        secondary_type = (int) atoi(buf);
      }else{
        ret_status.code=ERROR;
        strcpy(ret_status.error_message, "db_startup: read secondary index type");
        return ret_status;
      }
      if(secondary_type != -1){
        sec_column[number_secondary_index] = current_db->tables[i]->columns[j];
        sec_type[number_secondary_index]   = secondary_type;
        col_number[number_secondary_index] = j;
        number_secondary_index++;
      }
    }

    current_db->tables[i]->len_indexed  = len_indexed;
    current_db->tables[i]->len_occupied  = len_occupied;
    current_db->tables[i]->len_allocated = len_allocated;
    if(prim_column_index != -1){
      ret_status = load_primary_index(current_db->tables[i], current_db->tables[i]->columns[prim_column_index], prim_type);
      if(ret_status.code == ERROR){
        return ret_status;
      }
    }
    for(int k=0; k<number_secondary_index; k++){
      ret_status = load_secondary_index(current_db->tables[i], i, col_number[k], sec_column[k], sec_type[k]);
      if(ret_status.code == ERROR){
        return ret_status;
      }
    }
    free(sec_column);
    free(col_number);
    free(sec_type);
    i++;
  }

  log_info("This system has %d processors configured and %d processors available.\n", get_nprocs_conf(), get_nprocs());


  fclose(fptr);

  ret_status.code=OK;
  return ret_status;
}

// Status read_column_file(char* filename, int** data, size_t len_occupied, size_t len_allocated){
//
//     struct Status ret_status;
//
//     FILE *f = fopen(filename, "r");
//     if(f==NULL){
//       strcpy(ret_status.error_message, "read_column_file: open failure");
//       ret_status.code=ERROR;
//       return ret_status;
//     }
//
//     //Write the array to column
//     int* temp_array = (int*) malloc(sizeof(int)*len_allocated);
//     if(temp_array==NULL){
//       strcpy(ret_status.error_message, "read_column_file: malloc failure");
//       ret_status.code=ERROR;
//       return ret_status;
//     }
//
//     for(size_t i=0;i<len_occupied;i++){
//       fscanf(f, "%d", (temp_array+i));
//     }
//
//     *data = temp_array;
//
//     fclose(f);
//
//     ret_status.code=OK;
//     return ret_status;
//
// }

Status read_column_file(char* filename, int** data, size_t len_occupied, size_t len_allocated){

  struct Status ret_status;
  int fd;
  struct stat mmapstat;

  if ((fd = open(filename, O_CREAT|O_RDWR, (mode_t) 0666)) == -1) {
    strcpy(ret_status.error_message, "read_column_file: open failure");
    ret_status.code=ERROR;
    return ret_status;
   }

  // Get current size of the file
  if (stat(filename, &mmapstat) == -1) {
   close(fd);
   strcpy(ret_status.error_message, "read_column_file: stat failure");
   ret_status.code=ERROR;
   return ret_status;
  }

  // Check if file is empty
  if (mmapstat.st_size == 0){
    close(fd);
    strcpy(ret_status.error_message, "read_column_file: file is empty");
    ret_status.code=ERROR;
    return ret_status;
  }
  //printf("File size is: %ld\n", mmapstat.st_size);


  int* temp = mmap(NULL, mmapstat.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (temp == MAP_FAILED) {
     close(fd);
     strcpy(ret_status.error_message, "read_column_file: mmap failure");
     ret_status.code=ERROR;
     return ret_status;
  }

  // for (off_t i = 0; i < mmapstat.st_size; i++)
  // {
  //   printf("Found character %c at %ji\n", map[i], (intmax_t)i);
  // }

  //Write the array to column
  int* temp_array = (int*) malloc(sizeof(int)*len_allocated);
  for (size_t i=0;i<len_occupied;i++){
    temp_array[i] = temp[i];
  }

  // Don't forget to free the mmapped memory
  if (munmap(temp, mmapstat.st_size) == -1){
    close(fd);
    strcpy(ret_status.error_message, "read_column_file: munmmap failure");
    ret_status.code=ERROR;
    return ret_status;
  }

  // Un-mmaping doesn't close the file, so we still need to do that.
  close(fd);
  *data = temp_array;

  ret_status.code=OK;
  return ret_status;

}

Status select_pre(int* positions, int* values, size_t length, char* handle, int min, int max, SelectBound bound){

  struct Status ret_status;

  // Scan through data vector
  int* hit_res = malloc(length*sizeof(int));
  size_t j = 0;
  int  hit = 0;
  if(bound==UPPER_AND_LOWER){
    for(size_t i=0; i<length; i++){
      hit = (min <= values[i] && values[i] < max);
      hit_res[j] = positions[i]*hit;
      j = j + hit;
    }
  }else if(bound==UPPER){
    for(size_t i=0; i<length; i++){
      hit = (values[i] < max);
      hit_res[j] = positions[i]*hit;
      j = j + hit;
    }
  }else if(bound==LOWER){
    for(size_t i=0; i<length; i++){
      hit = (min <= values[i]);
      hit_res[j] = positions[i]*hit;
      j = j + hit;
    }
  }else{
    strcpy(ret_status.error_message, "select_db: invalid null");
    return ret_status;
  }

  Intermediate* results = NULL;
  ret_status = get_intermediate(handle, &results);
  if(ret_status.code==ERROR){
    return ret_status;
  }

  results -> data.ints = realloc(hit_res, j * sizeof(int));
  results -> type = INT;
  results -> len_data = j;

  ret_status.code=OK;
  return ret_status;
}


Status fetch_col(Table *table, char* col_name, Intermediate* src_intermediate, char* dest_handle){

  struct Status ret_status;

  Intermediate* dest_intermediate = NULL;
  ret_status = get_intermediate(dest_handle, &dest_intermediate);
  if(ret_status.code==ERROR){
    return ret_status;
  }

  int* pos = src_intermediate -> data.ints;

  // Find column name in table
  size_t i=0;
  int index=-1;
  while(index == -1 && i<table->col_count){
    if(table->columns[i]!=NULL){
      if(strcmp(table->columns[i]->name, col_name)==0){
        index = i;
      }
    }
    i++;
  }

  // check if colum name found in table
  if(table->columns[index]==NULL){
    ret_status.code = ERROR;
    sprintf(ret_status.error_message, "%s%s%s", "select_db: ", col_name, " not found in table");
    return ret_status;
  }

  // Scan through data vector
  dest_intermediate -> data.ints = malloc((src_intermediate->len_data)*sizeof(int));
  for(size_t i=0; i<src_intermediate->len_data; i++){
    dest_intermediate -> data.ints[i] = table->columns[index]->data[pos[i]];
  }

  dest_intermediate -> len_data     = src_intermediate->len_data;
  dest_intermediate -> type         = INT;

  ret_status.code=OK;
  return ret_status;

}

Status get_intermediate(char* handle, Intermediate **ret_intermediate){


  struct Status ret_status;

  // Check if intermediates have been initialized
  if(current_db -> intermediates == NULL){
    strcpy(ret_status.error_message, "get_intermediate: db uninitialized");
    ret_status.code=ERROR;
    return ret_status;
  }

  // Check if intermediate with name "handle" exists
  int found  = 0;
  size_t index = 0;
  size_t num_slots = MAX_INTERMEDIATES;
  while(index < num_slots && found==0){
    if(current_db -> intermediates[index] != NULL){
      if(strcmp(current_db -> intermediates[index]->handle,handle)==0){
        found = 1;
      }
    }
    index+=1;
  }

  // Return intermediate if it is found, free exisisting memory and
  if(index != num_slots){
    if(current_db -> intermediates[index-1] -> type == INT){
      if(current_db -> intermediates[index-1] -> data.ints!=NULL){
        int* pt = current_db -> intermediates[index-1] -> data.ints;
        free(pt);
      }
    }
    if(current_db -> intermediates[index-1] -> type == DOUBLE){
      if(current_db -> intermediates[index-1] -> data.doubles!=NULL){
        double* pt = current_db -> intermediates[index-1] -> data.doubles;
        free(pt);
      }
    }
    if(current_db -> intermediates[index-1] -> type == LONG){
      if(current_db -> intermediates[index-1] -> data.longs!=NULL){
        long* pt = current_db -> intermediates[index-1] -> data.longs;
        free(pt);
      }
    }
    current_db -> intermediates[index-1] -> len_data = 0;
    *ret_intermediate = current_db -> intermediates[index-1];
    ret_status.code=OK;
    //log_err("gi: %s ow.\n", handle);
    return ret_status;
  }

  // Search for an empty slot in intermediates array
  found  = 0;
  index  = 0;
  while(index < num_slots && current_db -> intermediates[index] != NULL){
    index+=1;
  }

  // return error when intermediate array table is full
  if(index == num_slots+1){
    strcpy(ret_status.error_message, "get_intermediate: intermediate array is full");
    ret_status.code=ERROR;
    return ret_status;
  }

  // if intermediate array is not full create new intermediate object
  Intermediate* temp = (Intermediate*) malloc(sizeof(Intermediate));
  if(temp == NULL){
    strcpy(ret_status.error_message, "get_intermediate: malloc failed");
    ret_status.code=ERROR;
    return ret_status;
  }
  current_db -> intermediates[index] = temp;
  strcpy(temp -> handle, handle);
  temp -> data.ints       = NULL;
  temp -> len_data        = 0;
  *ret_intermediate       = temp;


  ret_status.code=OK;
  //log_err("gi: %s alloc.\n", handle);
  return ret_status;

}

void deallocate_index(Index* index){
  if(index -> type == BTREE){
    if(index -> root != NULL){
      dealloc_btree(index -> root);
    }
  }
  if(index->pos != NULL){
    free(index->pos);
  }
  if(index -> place == SECONDARY && index -> data != NULL){
    free(index -> data);
  }
  free(index);
}

Status deallocate(){

  struct Status ret_status;

  //Deallocate deletes
  for(size_t i=0; i<current_db->tables_capacity;i++){
    if(current_db->tables[i]!=NULL){
      if(current_db->tables[i]->deletes!=NULL){
        free(current_db->tables[i]->deletes);
      }
    } // close table if pointer
  } // close table for loop

  //Deallocate indicies
  for(size_t i=0; i<current_db->tables_capacity;i++){
    if(current_db->tables[i]!=NULL){
      //Deallocate primary_index
      if(current_db->tables[i]->primary_index!=NULL){
        deallocate_index(current_db->tables[i]->primary_index);
      }
      //Deallocate secondary index
      for(size_t j=0;j<current_db->tables[i]->col_count;j++){
        if(current_db->tables[i]->columns[j]->secondary_index!=NULL){
          deallocate_index(current_db->tables[i]->columns[j]->secondary_index); //free secondary indicies
        } // close column if pointer
      } // close column for loop
    } // close table if pointer
  } // close table for loop


  //Deallocate columns
  for(size_t i=0; i<current_db->tables_capacity;i++){
    if(current_db->tables[i]!=NULL){
      for(size_t j=0;j<current_db->tables[i]->col_count;j++){
        if(current_db->tables[i]->columns[j]!=NULL){
          free(current_db->tables[i]->columns[j]->data); //free column data
          free(current_db->tables[i]->columns[j]); //free column meta data
        } // close column if pointer
      } // close column for loop
      free(current_db->tables[i]->columns); // free columns pointers
      free(current_db->tables[i]); // free table meta data
    } // close table if pointer
  } // close table for loop


  //Deallocate intermediates
  for(size_t i=0; i<MAX_INTERMEDIATES; i++){
    if(current_db->intermediates[i]!=NULL){
      if(current_db->intermediates[i]->type==INT){
        free(current_db->intermediates[i]->data.ints); //free int intermediate data
      }else if(current_db->intermediates[i]->type==DOUBLE){
        free(current_db->intermediates[i]->data.doubles); //free doubles intermediate data
      }
      else if(current_db->intermediates[i]->type==LONG){
        free(current_db->intermediates[i]->data.doubles); //free doubles intermediate data
      }
      free(current_db->intermediates[i]); //free intermediates[i] meta data
    }
  }

  free(current_db->intermediates); //free intermediates meta data
  free(current_db->tables); // free table pointers
  free(current_db); // free meta data from database
  free(batch);

  ret_status.code=OK;
  return ret_status;

}

Status aggregate(char* dest_handle, AggregateType aggregate, size_t length, int* ints, long* longs){

  struct Status ret_status;

  Intermediate* dest_intermediate = NULL;
  ret_status = get_intermediate(dest_handle, &dest_intermediate);
  if(ret_status.code==ERROR){
    strcpy(ret_status.error_message, "aggregate: get_intermediate failed");
    return ret_status;
  }

  if(aggregate==AVG){
    if(ints!=NULL){
      dest_intermediate -> type = DOUBLE;
      dest_intermediate -> data.doubles = (double*) malloc(sizeof(double));
      if(dest_intermediate -> data.doubles == NULL){
        strcpy(ret_status.error_message, "aggregate: malloc failed");
        ret_status.code=ERROR;
        return ret_status;
      }
      int64_t sum = 0;
      for(size_t i=0; i < length; i++){
        sum += (int64_t) ints[i];
      }
      if(length > 0){
        dest_intermediate -> data.doubles[0] = ((double) sum/length);
      }else{
        dest_intermediate -> data.doubles[0] = 0;
      }
    }else if(longs!=NULL){
      dest_intermediate -> type = DOUBLE;
      dest_intermediate -> data.doubles = (double*) malloc(sizeof(double));
      if(dest_intermediate -> data.doubles == NULL){
        strcpy(ret_status.error_message, "aggregate: malloc failed");
        ret_status.code=ERROR;
        return ret_status;
      }
      int64_t sum = 0;
      for(size_t i=0; i < length; i++){
        sum += (int64_t) longs[i];
      }
      if(length > 0){
        dest_intermediate -> data.doubles[0] = ((double) sum/length);
      }else{
        dest_intermediate -> data.doubles[0] = 0;
      }
    }
  }

  if(aggregate==SUM){
    if(ints!=NULL){
      dest_intermediate -> type = LONG;
      dest_intermediate -> data.longs = (long*) malloc(sizeof(long));
      if(dest_intermediate -> data.longs == NULL){
        strcpy(ret_status.error_message, "aggregate: malloc failed");
        ret_status.code=ERROR;
        return ret_status;
      }
      long sum = 0;
      for(size_t i=0; i < length; i++){
        sum += (long) ints[i];
      }
      dest_intermediate -> data.longs[0] = sum;
    }else if(longs!=NULL){
      dest_intermediate -> type = LONG;
      dest_intermediate -> data.longs = (long*) malloc(sizeof(long));
      if(dest_intermediate -> data.longs == NULL){
        strcpy(ret_status.error_message, "aggregate: malloc failed");
        ret_status.code=ERROR;
        return ret_status;
      }
      long sum = 0;
      for(size_t i=0; i < length; i++){
        sum += (long) longs[i];
      }
      dest_intermediate -> data.longs[0] = sum;
    }
  }

  if(aggregate==MAX){
    if(ints!=NULL){
      dest_intermediate -> type = INT;
      dest_intermediate -> data.ints = (int*) malloc(sizeof(int));
      if(dest_intermediate -> data.ints == NULL){
        strcpy(ret_status.error_message, "aggregate: malloc failed");
        ret_status.code=ERROR;
        return ret_status;
      }
      int max = INT_MIN;
      for(size_t i=0; i < length; i++){
        max = ints[i] > max ? ints[i] : max;
      }
      dest_intermediate -> data.ints[0] = max;
    }else if(longs!=NULL){
      dest_intermediate -> type = INT;
      dest_intermediate -> data.ints = (int*) malloc(sizeof(int));
      if(dest_intermediate -> data.ints == NULL){
        strcpy(ret_status.error_message, "aggregate: malloc failed");
        ret_status.code=ERROR;
        return ret_status;
      }
      long max = LONG_MIN;
      for(size_t i=0; i < length; i++){
        max = longs[i] > max ? longs[i] : max;
      }
      dest_intermediate -> data.ints[0] = (int) max;
    }
  }

  if(aggregate==MIN){
    if(ints!=NULL){
      dest_intermediate -> type = INT;
      dest_intermediate -> data.ints = (int*) malloc(sizeof(int));
      if(dest_intermediate -> data.ints == NULL){
        strcpy(ret_status.error_message, "aggregate: malloc failed");
        ret_status.code=ERROR;
        return ret_status;
      }
      int min = INT_MAX;
      for(size_t i=0; i < length; i++){
        min = ints[i] < min ? ints[i] : min;
      }
      dest_intermediate -> data.ints[0] = min;
    }else if(longs!=NULL){
      dest_intermediate -> type = INT;
      dest_intermediate -> data.ints = (int*) malloc(sizeof(int));
      if(dest_intermediate -> data.ints == NULL){
        strcpy(ret_status.error_message, "aggregate: malloc failed");
        ret_status.code=ERROR;
        return ret_status;
      }
      long min = LONG_MAX;
      for(size_t i=0; i < length; i++){
        min = longs[i] < min ? longs[i] : min;
      }
      dest_intermediate -> data.ints[0] = (int) min;
    }
  }

  if(aggregate==COUNT){
    dest_intermediate -> type = INT;
    dest_intermediate -> data.ints = (int*) malloc(sizeof(int));
    if(dest_intermediate -> data.ints == NULL){
      strcpy(ret_status.error_message, "aggregate: malloc failed");
      ret_status.code=ERROR;
      return ret_status;
    }
    dest_intermediate -> data.ints[0] = (int) length;
  }

  dest_intermediate -> len_data = 1;

  ret_status.code=OK;
  return ret_status;

}

Status operate_column(char* dest_handle, OperationType operation, size_t length, int* data1, int* data2){

  struct Status ret_status;
  Intermediate* dest_intermediate = NULL;
  ret_status = get_intermediate(dest_handle, &dest_intermediate);
  if(ret_status.code==ERROR || dest_intermediate == NULL){
    strcpy(ret_status.error_message, "operate: get_intermediate failed");
    return ret_status;
  }
  dest_intermediate -> type = LONG;
  dest_intermediate -> len_data = length;
  dest_intermediate -> data.longs = (long*) malloc(length*sizeof(long));
  if(dest_intermediate -> data.longs == NULL){
    strcpy(ret_status.error_message, "operate: malloc failed");
    ret_status.code=ERROR;
    return ret_status;
  }

  if(operation==ADD){
    for(size_t i=0; i < length; i++){
      dest_intermediate -> data.longs[i] = (long) (data1[i] + data2[i]);
    }
  }
  if(operation==SUB){
    for(size_t i=0; i < length; i++){
      dest_intermediate -> data.longs[i] = (long) (data1[i] - data2[i]);
    }
  }

  ret_status.code=OK;
  return ret_status;
}

Status operate_inter(char* dest_handle, OperationType operation, size_t length, Intermediate* data1, Intermediate* data2){

  struct Status ret_status;
  Intermediate* dest_intermediate = NULL;
  ret_status = get_intermediate(dest_handle, &dest_intermediate);
  if(ret_status.code==ERROR || dest_intermediate == NULL){
    strcpy(ret_status.error_message, "operate: get_intermediate failed");
    return ret_status;
  }
  dest_intermediate -> type = LONG;
  dest_intermediate -> len_data = length;
  dest_intermediate -> data.longs = (long*) malloc(length*sizeof(long));
  if(dest_intermediate -> data.longs == NULL){
    strcpy(ret_status.error_message, "operate: malloc failed");
    ret_status.code=ERROR;
    return ret_status;
  }

  if(data1->type == INT && data2->type == INT){
    if(operation==ADD){
      for(size_t i=0; i < length; i++){
        dest_intermediate -> data.longs[i] = ( (long) data1->data.ints[i] + (long) data2->data.ints[i]);
      }
    }
    if(operation==SUB){
      for(size_t i=0; i < length; i++){
        dest_intermediate -> data.longs[i] = ( (long) data1->data.ints[i] - (long) data2->data.ints[i]);
      }
    }
  }else if(data1->type == INT && data2->type == LONG){
    if(operation==ADD){
      for(size_t i=0; i < length; i++){
        dest_intermediate -> data.longs[i] = ( (long) data1->data.ints[i] + data2->data.longs[i]);
      }
    }
    if(operation==SUB){
      for(size_t i=0; i < length; i++){
        dest_intermediate -> data.longs[i] = ( (long) data1->data.ints[i] - data2->data.longs[i]);
      }
    }
  }else if(data1->type == LONG && data2->type == INT){
    if(operation==ADD){
      for(size_t i=0; i < length; i++){
        dest_intermediate -> data.longs[i] = ( data1->data.longs[i] + (long) data2->data.ints[i]);
      }
    }
    if(operation==SUB){
      for(size_t i=0; i < length; i++){
        dest_intermediate -> data.longs[i] = ( data1->data.longs[i] - (long) data2->data.ints[i]);
      }
    }
  }else if(data1->type == LONG && data2->type == LONG){
    if(operation==ADD){
      for(size_t i=0; i < length; i++){
        dest_intermediate -> data.longs[i] = ( data1->data.longs[i] + data2->data.longs[i]);
      }
    }
    if(operation==SUB){
      for(size_t i=0; i < length; i++){
        dest_intermediate -> data.longs[i] = ( data1->data.longs[i] - data2->data.longs[i]);
      }
    }
  }

  ret_status.code=OK;
  return ret_status;
}

Status load_file(Table* table, int client_socket, size_t columns_filled){

    Status ret_status;
    int_message recv_int_message;

    int max_num_rows   = (size_t) floor(MAX_MESSAGE_SIZE/columns_filled/sizeof(int));
    int* int_buffer_array = (int*) malloc(max_num_rows*columns_filled*sizeof(int));

    int done = 0;
    int total_rows = 0;
    while(1){

        recv(client_socket, &done, 1, 0);
        if(done==1){
          break;
        }

        recv(client_socket, &recv_int_message, sizeof(int_message), 0);
        recv(client_socket, int_buffer_array, recv_int_message.length, 0);
        //log_info("%d bytes received\n", recv_int_message.length);
        max_num_rows = (int) recv_int_message.length / (double) (sizeof(int)*columns_filled);

        int num_rows = 0;
        while(num_rows < max_num_rows) {

             //Check if new memory needs to be allocated
             if (table->len_allocated <= table->len_occupied){
               for(size_t i=0; i < table -> col_count; i++){
                  table->columns[i]->data = (int*) realloc(table->columns[i]->data, 2*(table->len_allocated)*sizeof(int));
                  if(table->columns[i]->data == NULL){
                    ret_status.code = ERROR;
                    strcpy(ret_status.error_message, "load_file: realloc failed");
                    return ret_status;
                  }
               }
               if(table->deletes!=NULL){
                 table->deletes = (int*) realloc(table->deletes, (2*(table->len_allocated)/32+1)*sizeof(int));
                 if(table->deletes == NULL){
                   ret_status.code = ERROR;
                   strcpy(ret_status.error_message, "relational_insert: realloc table deletes failed!");
                   return ret_status;
                 }
               }
               table->len_allocated = 2*(table->len_allocated);
             }

             // Copy data from values to database
             for(size_t i=0; i < table -> col_count; i++){
               table -> columns[i]-> data[table -> len_occupied] = int_buffer_array[num_rows*columns_filled+i];
             }

             if(table->deletes!=NULL){
               ClearBit(table->deletes, table -> len_occupied);
             }
             table -> len_occupied += 1;

             num_rows++;
        }
        total_rows += num_rows;
    }
    log_info("%d rows received\n", total_rows);

    free(int_buffer_array);

    ret_status = reindex(table);
    if(ret_status.code == ERROR){
      return ret_status;
    }

    ret_status.code = OK;
    return ret_status;

}

/* Milestone 2 */

// void* select_col_parallel(void* arguments){
//     BatchCollection* datastruct = (BatchCollection*) arguments;
//     int* data         = datastruct -> data;
//     size_t length     = datastruct -> length;
//     char handle[MAX_SIZE_NAME];
//     strcpy(handle, datastruct->handle);
//     int min           = datastruct -> min;
//     int max           = datastruct -> max;
//     SelectBound bound  = datastruct -> bound;
//     int* hit_res = malloc(length*sizeof(int));
//     if(data==hit_res){
//       datastruct -> ret_status.code = ERROR;
//       strcpy(datastruct -> ret_status.error_message, "select_col_prallel: malloc failed");
//       return &(datastruct -> ret_status);
//     }
//     size_t j = 0;
//
//     if(bound==UPPER_AND_LOWER){
//       for(size_t i=0; i<length; i++){
//         hit_res[j] = i;
//         j += (min <= data[i] && data[i] < max);
//       }
//     }else if(bound==UPPER){
//       for(size_t i=0; i<length; i++){
//         hit_res[j] = i;
//         j += (data[i] < max);
//       }
//     }else if(bound==LOWER){
//       for(size_t i=0; i<length; i++){
//         hit_res[j] = i;
//         j += (min <= data[i]);
//       }
//     }else{
//       strcpy(datastruct -> ret_status.error_message, "select_db: invalid null");
//       return &(datastruct -> ret_status);
//     }
//     Intermediate* results = NULL;
//     pthread_mutex_lock(&lock);
//     datastruct -> ret_status = get_intermediate(handle, &results);
//     pthread_mutex_unlock(&lock);
//     if(datastruct -> ret_status.code==ERROR){
//       return &(datastruct -> ret_status);
//     }
//     results -> data.ints = realloc(hit_res, j * sizeof(int));
//     results -> type = INT;
//     results -> len_data = j;
//     datastruct -> ret_status.code=OK;
//     return &(datastruct -> ret_status);
// }
//
// Status select_parallel(BatchCollection* datastruct, size_t num_queries){
//
//     Status ret_status;
//     pthread_t threads[NUM_THREADS];
//     int result_code;
//     // run NUM_THREADS queries in parallel
//     size_t iter = (size_t) floor(num_queries/NUM_THREADS);
//     for(size_t j = 0; j < iter; j++){
//       for(size_t i = 0; i < NUM_THREADS; i++){
//         result_code = pthread_create(&threads[i], NULL, select_col_parallel, datastruct+NUM_THREADS*j+i);
//         //printf("%ld\n", NUM_THREADS*j+i);
//         assert(!result_code);
//       }
//       for(size_t i = 0; i < NUM_THREADS; i++) {
//         result_code = pthread_join(threads[i], (void**) &ret_status);
//         assert(!result_code);
//         if(ret_status.code == ERROR){
//           return ret_status;
//         }
//       }
//     }
//
//     // handle the remainder
//     size_t offset = num_queries - NUM_THREADS * iter;
//     for(size_t i = 0; i < offset; i++) {
//         result_code = pthread_create(&threads[i], NULL, select_col_parallel, datastruct+NUM_THREADS*iter+i);
//         assert(!result_code);
//         //printf("%ld\n", NUM_THREADS*iter+i);
//     }
//     for(size_t i = 0; i < offset; i++) {
//         result_code = pthread_join(threads[i], (void**) &ret_status);
//         assert(!result_code);
//         if(ret_status.code == ERROR){
//           return ret_status;
//         }
//     }
//
//     ret_status.code = OK;
//     return ret_status;
//
// }

/* Milestone 3 */

// void quicksort(int* data, int* pos, int first,int last){
//    int i, j, pivot, temp;
//    if(first<last){
//       pivot=first;
//       i=first;
//       j=last;
//       while(i<j){
//          while(data[i]<=data[pivot]&&i<last)
//             i++;
//          while(data[j]>data[pivot])
//             j--;
//          if(i<j){
//             temp=data[i];
//             data[i]=data[j];
//             data[j]=temp;
//
//             temp=pos[i];
//             pos[i]=pos[j];
//             pos[j]=temp;
//          }
//       }
//
//       temp=data[pivot];
//       data[pivot]=data[j];
//       data[j]=temp;
//
//       temp=pos[pivot];
//       pos[pivot]=pos[j];
//       pos[j]=temp;
//
//       quicksort(data,pos,first,j-1);
//       quicksort(data,pos,j+1,last);
//    }
// }

// void sortArray(int* data, int first,int last){
//    int i, j, pivot, temp;
//    if(first<last){
//       pivot=first;
//       i=first;
//       j=last;
//       while(i<j){
//          while(data[i]<=data[pivot]&&i<last)
//             i++;
//          while(data[j]>data[pivot])
//             j--;
//          if(i<j){
//             temp=data[i];
//             data[i]=data[j];
//             data[j]=temp;
//          }
//       }
//       temp=data[pivot];
//       data[pivot]=data[j];
//       data[j]=temp;
//
//       sortArray(data,first,j-1);
//       sortArray(data,j+1,last);
//    }
// }

// sort the other columns according to the to-be-sorted-column
// int* sortColumnByPosition(int* arr, int* pos, size_t len_occupied, size_t len_allocated){
//   int* temp = (int*) malloc(len_allocated*sizeof(int));
//   for(size_t i=0;i<len_occupied;i++){
//     temp[i] = arr[pos[i]];
//   }
//   free(arr);
//   return temp;
// }

// propagte order of each column, in sort every column in table by t
// Status sortTableByColumn(Table* table, Column* sortedColumn){
//   Status ret_status;
//   ret_status.code = OK;
//   size_t len_occupied  = table->len_occupied;
//   size_t len_allocated = table->len_allocated;
//   int* pos = (int*) malloc(len_occupied * sizeof(int));
//   if(pos==NULL){
//     ret_status.code = ERROR;
//     strcpy(ret_status.error_message, "sortTableByColumn: pos malloc failed");
//     return ret_status;
//   }
//   for(size_t i = 0; i < len_occupied; i++){
//     pos[i] = i;
//   }
//
//   clock_t t;
//   t = clock();
//   quicksort(sortedColumn->data, pos, 0, len_occupied-1);
//   t = clock() - t;
//   double time_taken = (((double)t)/CLOCKS_PER_SEC)*1000;
//   log_info("sorting took took %f ms to execute in sortTableByColumn() \n", time_taken);
//
//   sortedColumn -> sorted = true;
//   for(size_t i=0; i<table->col_count; i++){
//     Column* column = table->columns[i];
//     if( column != sortedColumn){
//       column -> data = sortColumnByPosition(column->data, pos, len_occupied, len_allocated);
//     }
//   }
//   free(pos);
//   return ret_status;
// }

int expN(int N,size_t exp){
  int result = 1;
  while(exp != 0){
    result *= N;
    exp--;
  }
  return result;
}

int getNumElements(int N, int depth){
  return expN(N,NUM_LAYERS_BTREE-1-depth);
}

size_t getNodeSize(int N, int depth){
  if(depth==0){
    return N-1;
  }else{
    return expN(N, depth+1) - expN(N,depth);
  }
}

void build_btree(Node* node,int N,int depth){
    if(NUM_LAYERS_BTREE-depth > 1){
        node -> child = (Node*) malloc(sizeof(*node));
        node -> child -> data = NULL;
        node -> data  = (int*)  malloc(getNodeSize(N,depth)*sizeof(int));
        build_btree(node -> child,N,++depth);
    } else {
        node -> child = NULL;
    }
}

void dealloc_btree(Node* node){
    if(node->child!=NULL){
        dealloc_btree(node -> child);
    }
    if(node->data != NULL){
      free(node->data);
    }
    free(node);
}

int initialize_btree(Node* node,int* data,int N,int target_depth,int depth,size_t offset,size_t counter,size_t len_occupied){
  size_t num_elements = getNumElements(N,depth);
  for(int j=0; j<N; j++){
    if(target_depth>depth){
        counter = initialize_btree(node->child,data,N,target_depth,depth+1,num_elements*j+offset,counter,len_occupied);
    }else{
      if(j>0){
        size_t data_index = num_elements*j+offset;
        if(len_occupied > data_index){
          node->data[counter] = data[data_index];
        }else{
          node->data[counter] = INT_MAX;
        }
        counter = counter + 1;
      }
    }
  }
  return counter;
}

size_t binary_search_upper(Node* node,int N,int depth,size_t start_index,size_t offset,int value){
  size_t index = start_index;
  int counter  = 0;
  int num_elements = getNumElements(N,depth);
  while(index < getNodeSize(N,depth) && node->data[index] <= value){
    index+=1;
    counter+=1;
  }
  if(NUM_LAYERS_BTREE-2 > depth){
    return binary_search_upper(node->child,N,depth+1,start_index*N+counter*(N-1),num_elements*counter+offset,value);
  }else{
    return num_elements*counter+offset+N;
  }
}

size_t binary_search_lower(Node* node,int N,int depth,size_t start_index,size_t offset,int value){
  size_t index = start_index;
  int counter  = 0;
  int num_elements = getNumElements(N,depth);
  while(index < getNodeSize(N,depth) && node->data[index] < value){
    index+=1;
    counter+=1;
  }
  if(NUM_LAYERS_BTREE-2 > depth){
    return binary_search_lower(node->child,N,depth+1,start_index*N+counter*(N-1),num_elements*counter+offset,value);
  }else{
    return num_elements*counter+offset;
  }
}

// Qunatile funcion
int getQuantile(int* data,int quantile,size_t len_indexed){
  double index = round(len_indexed*quantile)/100;
  return (int) round(0.5*(data[(int)floor(index)]+data[(int)ceil(index)]));
}

/*
 *
 *  main select function
 *
 */
Status select_col(Table* table,char* col_name,char* handle,int min,int max,SelectBound bound){

  //log_err("select: %s of size: %ld\n",table->name, table->len_occupied);

  struct Status ret_status;

  // Find column name in table
  size_t i=0;
  int col_index=-1;
  while(col_index == -1 && i<table->col_count){
    if(table->columns[i]!=NULL){
      if(strcmp(table->columns[i]->name, col_name)==0){
        col_index = i;
      }
    }
    i++;
  }

  // check if colum name found in table
  if(col_index==-1){
    ret_status.code = ERROR;
    sprintf(ret_status.error_message, "%s%s%s", "select_db: ", col_name, " not found in table");
    return ret_status;
  }

  Column* column = table->columns[col_index];
  if(table->dirty == true){
    return select_full_scan(table,column->data,handle,min,max,bound);
  }

  table -> select_queries_concession += 1;
  if(table->primary_index!=NULL){ // check if primary index exists
    if(table->primary_index->data == column->data){ // check if we select from primary index column
      if(table->primary_index->type == BTREE){
        ret_status = select_btree(table->primary_index->root,table,column->data,handle,min,max,bound,NULL, col_index);
        return ret_status;
      }else if(table->primary_index->type == SORTED){
        ret_status = select_sorted_col(table,column->data,handle,min,max,bound,NULL, col_index);
        return ret_status;
      }
    }
  }

  if(column->secondary_index!=NULL){ //check if secondary index exists
    // SORTED COLUMN optimizer
    if(column->secondary_index->type == SORTED){
      if(bound == LOWER){
        size_t Bmin = table->len_occupied - findB(table->len_occupied,SORTED);
        if(column->data[Bmin]<min){
          ret_status = select_sorted_col(table,column->secondary_index->data,handle,min,max,bound,column->secondary_index->pos, col_index);
        }else{
          ret_status = select_full_scan(table,column->data,handle,min,max,bound);
        }
      }else if(bound == UPPER || bound==UPPER_AND_LOWER){
        size_t Bmax = findB(table->len_occupied,SORTED);
        if(max < column->data[Bmax]){
          ret_status = select_sorted_col(table,column->secondary_index->data,handle,min,max,bound,column->secondary_index->pos, col_index);
        }else{
          ret_status = select_full_scan(table,column->data,handle,min,max,bound);
        }
      }
      return ret_status;
    }

    // BTREE optimizer
    if(column->secondary_index->type == BTREE){
      if(bound == LOWER){
        size_t Bmin = table->len_indexed - findB(table->len_indexed,BTREE);
        if(column->secondary_index->data[Bmin]<min){
          ret_status = select_btree(column->secondary_index->root,table,column->secondary_index->data,handle,min,max,bound,column->secondary_index->pos, col_index);
        }else{
          ret_status = select_full_scan(table,column->data,handle,min,max,bound);
        }
      }else if(bound == UPPER){
        size_t Bmax = findB(table->len_indexed,BTREE);
        if(max < column->secondary_index->data[Bmax]){
          ret_status = select_btree(column->secondary_index->root,table,column->secondary_index->data,handle,min,max,bound,column->secondary_index->pos, col_index);
        }else{
          ret_status = select_full_scan(table,column->data,handle,min,max,bound);
        }
      }else if(bound == UPPER_AND_LOWER){
        size_t Bmax = binary_search_upper(column->secondary_index->root,column->secondary_index->root->N,0,0,0,max);
        size_t Bmin = binary_search_lower(column->secondary_index->root,column->secondary_index->root->N,0,0,0,min);
        if((Bmax - Bmin)<findB(table->len_indexed,BTREE)){
          ret_status = select_btree(column->secondary_index->root,table,column->secondary_index->data,handle,min,max,bound,column->secondary_index->pos, col_index);
        }else{
          ret_status = select_full_scan(table,column->data,handle,min,max,bound);
        }
      }
      return ret_status;
    }
  }
  ret_status = select_full_scan(table,column->data,handle,min,max,bound);
  return ret_status;
}

Status select_full_scan_from_array(int* data,size_t start_index,size_t length_data,int min,int max,SelectBound bound,IntermediateField* field, size_t* length_field){

    Status ret_status;
    ret_status.code = OK;

    size_t j = *length_field;
    //int hit  = 0;
    int* hit_res = field->ints;

    if(bound==UPPER_AND_LOWER){
      for(size_t i=start_index; i<length_data; i++){
        hit_res[j] = i;
        j += (min <= data[i] && data[i] < max);
      }
    }else if(bound==UPPER){
      for(size_t i=start_index; i<length_data; i++){
        hit_res[j] = i;
        j += (data[i] < max);
      }
    }else if(bound==LOWER){
      for(size_t i=start_index; i<length_data; i++){
        hit_res[j] = i;
        j += (min <= data[i]);
      }
    }else{
      strcpy(ret_status.error_message, "select_full_scan_from_array: invalid bound");
      ret_status.code = ERROR;
      return ret_status;
    }

    *length_field = j;
    field->ints = hit_res;

    return ret_status;
}

Status select_full_scan(Table* table,int* data,char* handle,int min,int max, SelectBound bound){

    struct Status ret_status;
    ret_status.code = OK;

    Intermediate* results = NULL;
    ret_status = get_intermediate(handle, &results);
    if(ret_status.code==ERROR){
      return ret_status;
    }

    results -> type = INT;
    results -> data.ints = (int*) malloc(table->len_occupied*sizeof(int));
    if(results -> data.ints == NULL){
      strcpy(ret_status.error_message, "select_full_scan: malloc failed");
      ret_status.code = ERROR;
      return ret_status;
    }
    ret_status = select_full_scan_from_array(data, 0, table->len_occupied,min,max,bound,&(results->data),&(results->len_data));
    if(ret_status.code==ERROR){
      return ret_status;
    }

    if(table -> dirty == true){
      ret_status = remove_deletes_from_select(table, results);
    }

    //Shrink allocated memory
    results -> data.ints = realloc(results -> data.ints, results->len_data * sizeof(int));

    return ret_status;

}


// Select from sorted column
Status select_sorted_col(Table* table,int* data,char* handle,int min,int max,SelectBound bound, int* pos, int col_index){

    struct Status ret_status;
    ret_status.code=OK;

    // Scan through data vector
    int* hit_res = malloc((table->len_occupied)*sizeof(int));
    size_t j = 0;
    if(bound==UPPER_AND_LOWER){
        size_t i=0;
        while(i<table->len_indexed && data[i] < min){
          i++;
        }
        while(i<table->len_indexed && data[i] < max){
          hit_res[j] = i;
          i++; j++;
        }
    }else if(bound==UPPER){
        size_t i=0;
        while(i<table->len_indexed && data[i] < max){
          hit_res[j] = i;
          j++; i++;
        }
    }else if(bound==LOWER){
        size_t i=table->len_indexed-1;
        while(min <= data[i]){
          hit_res[j] = i;
          i--; j++;
        }
    }else{
      strcpy(ret_status.error_message, "select_db: invalid null");
      return ret_status;
    }

    if(pos!=NULL){
      for(size_t i=0; i<j; i++){
        hit_res[i] = pos[hit_res[i]];
      }
    }

    Intermediate* results = NULL;
    ret_status = get_intermediate(handle, &results);
    if(ret_status.code==ERROR){
      return ret_status;
    }

    results -> data.ints = hit_res;
    results -> type = INT;
    results -> len_data = j;

    ret_status = select_full_scan_from_array(table->columns[col_index]->data, table->len_indexed, table->len_occupied,min,max,bound,&(results->data),&(results->len_data));
    if(ret_status.code==ERROR){
      return ret_status;
    }

    if(table -> dirty == true){
      ret_status = remove_deletes_from_select(table, results);
    }

    //Shrink allocated memory
    results -> data.ints = realloc(results -> data.ints, results->len_data * sizeof(int));

    return ret_status;

}

// select from btree
Status select_btree(Node* root, Table* table, int* data, char* handle, int min, int max, SelectBound bound, int* pos, int col_index){

    struct Status ret_status;
    ret_status.code=OK;

    size_t len_occupied = table->len_occupied;
    size_t len_indexed  = table->len_indexed;

    // Scan through data vector
    int* hit_res = malloc(len_occupied*sizeof(int));
    size_t j = 0;

    if(bound==UPPER_AND_LOWER){
      size_t i = binary_search_lower(root,root->N,0,0,0,min);
      if(root->N < (int) i){
         i = i - root->N;
      }
      while(i<len_indexed && data[i] < max){
        hit_res[j] = i;
        j = j + (min <= data[i]); i++;
      }
    }else if(bound==UPPER){
      size_t upper_limit = binary_search_upper(root,root->N,0,0,0,max);
      if(len_indexed < upper_limit){
        upper_limit = len_indexed;
      }
      for(size_t i=0; i<upper_limit; i++){
        hit_res[j] = i;
        j += (data[i] < max);
      }
    }else if(bound==LOWER){
      size_t lower_limit = binary_search_lower(root,root->N,0,0,0,min);
      for(size_t i=lower_limit; i<len_indexed; i++){
        hit_res[j] = i;
        j += (min <= data[i]);
      }
    }else{
      strcpy(ret_status.error_message, "select_db: invalid bound");
      return ret_status;
    }

    if(pos!=NULL){
      for(size_t i=0; i<j; i++){
        hit_res[i] = pos[hit_res[i]];
      }
    }

    Intermediate* results = NULL;
    ret_status = get_intermediate(handle, &results);
    if(ret_status.code==ERROR){
      return ret_status;
    }

    results -> data.ints = hit_res;
    results -> type = INT;
    results -> len_data = j;

    ret_status = select_full_scan_from_array(table->columns[col_index]->data, len_indexed, len_occupied,min,max,bound,&(results->data),&(results->len_data));
    if(ret_status.code==ERROR){
      return ret_status;
    }

    if(table -> dirty == true){
      ret_status = remove_deletes_from_select(table, results);
    }

    //Shrink allocated memory
    results -> data.ints = realloc(results -> data.ints, results->len_data * sizeof(int));

    return ret_status;

}

Status create_primary_index(Table* table, Column* column, IndexType type){

  Status ret_status;
  ret_status.code = OK;

  //check if table already has a primary index
  if(table->primary_index == NULL){
    table->primary_index = (Index*) malloc(sizeof(Index));
  }else{
    strcpy(ret_status.error_message, "create_primary_index: multiple primary indicies are not implemented yet");
    ret_status.code = ERROR;
    return ret_status;
  }

  int col_index;
  //finding the column_index
  for(size_t i=0; i<table->col_count; i++){
    if(table->columns[i]==column){
      col_index = i;
      break;
    }
  }

  Index* temp = table->primary_index;
  temp -> type = type;
  temp -> place = PRIMARY;
  temp -> col_index = col_index;

  if(column->sorted==false || table->len_indexed != table->len_occupied){
    sortTableByColumn(table, column);
  }

  size_t len_occupied = table->len_occupied;

  if(type==BTREE){
    int N = (int) ceil(pow((double)len_occupied,(double)1/NUM_LAYERS_BTREE));
    Node* root = (Node*) malloc(sizeof(Node));
    root -> child = NULL;
    root -> data  = NULL;
    if(len_occupied>0){
      root -> N = N;
      build_btree(root, N, 0);
      for(size_t i=0; i<NUM_LAYERS_BTREE-1; i++){
        initialize_btree(root,column->data,N,i,0,0,0,len_occupied);
      }
    }
    temp -> root = root;
    temp -> pos = NULL;
    temp -> data = column -> data;
  }else if(type==SORTED){
    temp -> root = NULL;
    temp -> pos = NULL;
    temp -> data = column->data;
  }

  table -> len_indexed = table->len_occupied;
  return ret_status;

}

Status create_secondary_index(Table* table, Column* column, IndexType type){

  Status ret_status;
  ret_status.code = OK;

  if(column->secondary_index!=NULL){
    strcpy(ret_status.error_message, "create_secondary_index: multiple secondary indicies are not implemented yet");
    ret_status.code = ERROR;
    return ret_status;
  }

  Index* temp = (Index*) malloc(sizeof(Index));

  if(temp==NULL){
    strcpy(ret_status.error_message, "create_secondary_index: index malloc failed");
    ret_status.code = ERROR;
    return ret_status;
  }

  size_t len_occupied  = table->len_occupied;
  //size_t len_allocated = table->len_allocated;
  temp -> type = type;
  temp -> place = SECONDARY;
  //temp -> pos  = (int*) malloc(len_occupied*sizeof(int));
  int** ptrs = (int**) malloc(len_occupied*sizeof(int*));
  temp -> data = (int*) malloc(len_occupied*sizeof(int));
  temp -> pos  = (int*) malloc(len_occupied*sizeof(int));
  if(ptrs==NULL || temp->data==NULL){
    strcpy(ret_status.error_message, "create_secondary_index: pos or data malloc failed");
    ret_status.code = ERROR;
    return ret_status;
  }

  // copy data
  for(size_t i=0; i<len_occupied; i++){
    ptrs[i] = column->data + i;
    //temp -> data[i] = column->data[i];
  }

  // Sorting new column and row data
  clock_t t;
  t = clock();
  qsort(ptrs, len_occupied, sizeof(int*), cmpptrs);
  t = clock() - t;
  double time_taken = (((double)t)/CLOCKS_PER_SEC)*1000;
  log_info("sorting took took %f ms to execute in create_secondary_index()\n", time_taken);

  // copy data
  for(size_t i=0; i<len_occupied; i++){
    temp -> data[i] = *(ptrs[i]);
    temp -> pos[i] = (int) (ptrs[i] - column -> data);
  }

  free(ptrs);

  if(type==BTREE){
    Node* root = (Node*) malloc(sizeof(Node));
    int N = (int) ceil(pow((double)len_occupied,(double)1/NUM_LAYERS_BTREE));
    root -> N = N;
    root -> child = NULL;
    root -> data  = NULL;
    if(len_occupied>0){
      build_btree(root, N, 0);
      for(size_t i=0; i<NUM_LAYERS_BTREE-1; i++){
        initialize_btree(root,temp->data,root->N,i,0,0,0,len_occupied);
      }
    }
    temp -> root = root;
  }else if(type==SORTED){
    temp -> root = NULL;
  }

  table -> len_indexed = table->len_occupied;
  column->secondary_index = temp;
  return ret_status;
}


// Create indicies
Status create_index(Table* table, Column* column, IndexType type, IndexPlace place){
  struct Status ret_status;
  if(place==PRIMARY){
    for(size_t i=0; i<table->col_count; i++){
      if(table->columns[i]!=NULL){
        if(table->columns[i]->secondary_index!=NULL){
          strcpy(ret_status.error_message, "create_primary_index: table has sec. index, primary needs to be created first");
          ret_status.code = ERROR;
          return ret_status;
        }
      }
    }
    ret_status = create_primary_index(table,column,type);
    return ret_status;
  }else if(place==SECONDARY){
    ret_status = create_secondary_index(table,column,type);
    return ret_status;
  }else{
    strcpy(ret_status.error_message, "create_index: index type not found");
    ret_status.code = ERROR;
    return ret_status;
  }
}

double function_btree(size_t N, double B){
  return log(N) + B * log(B) - N;
}

double derivative_btree(double B){
  return 1 + log(B);
}

double function_sorted(size_t N, double B){
  return B + B * log(B) - N;
}

double derivative_sorted(double B){
  return 2 + log(B);
}

size_t findB(size_t N, IndexType type){
  double B = 0.5*N;
  if(type == BTREE){
    double error = function_btree(N,B);
    while(error>0.1){
      B = B - function_btree(N,B) / derivative_btree(B);
      error = function_btree(N,B);
    }
  }else if(type==SORTED){
    double error = function_sorted(N,B);
    while(error>0.1){
      B = B - function_sorted(N,B) / derivative_sorted(B);
      error = function_sorted(N,B);
    }
  }
  return (size_t) round(B);
}


Status join_nested(Intermediate* pos1, Intermediate* val1, Intermediate* pos2, Intermediate* val2, char* handle1, char* handle2){

  Status ret_status;
  ret_status.code = OK;

  size_t len_allocated = pos1->len_data;
  int* join_pos1 = (int*) malloc(len_allocated*sizeof(int));
  int* join_pos2 = (int*) malloc(len_allocated*sizeof(int));
  if(join_pos1==NULL||join_pos2==NULL){
    strcpy(ret_status.error_message,"join_nested: malloc joinpos 1 or 2 or both failed");
    return ret_status;
  }

  size_t k=0;
  for(size_t i=0; i<pos1->len_data; i++){
    for(size_t j=0; j<pos2->len_data; j++){
      if(val1->data.ints[i]==val2->data.ints[j]){
        join_pos1[k] = pos1->data.ints[i];
        join_pos2[k] = pos2->data.ints[j];
        k++;
        if(len_allocated<=k){
          join_pos1 = (int*) realloc(join_pos1, 2*len_allocated*sizeof(int));
          join_pos2 = (int*) realloc(join_pos2, 2*len_allocated*sizeof(int));
          if(join_pos1==NULL||join_pos2==NULL){
            strcpy(ret_status.error_message,"join_nested: realloc joinpos 1 or 2 or both failed");
            return ret_status;
          }
          len_allocated = 2*len_allocated;
        }
      }
    }
  }

  Intermediate* inter_handle1 = NULL;
  ret_status = get_intermediate(handle1, &inter_handle1);

  Intermediate* inter_handle2 = NULL;
  ret_status = get_intermediate(handle2, &inter_handle2);

  inter_handle1->type = INT;
  inter_handle2->type = INT;
  inter_handle1 -> len_data = k;
  inter_handle2 -> len_data = k;
  inter_handle1 -> data.ints = (int*) realloc(join_pos1, k*sizeof(int));;
  inter_handle2 -> data.ints = (int*) realloc(join_pos2, k*sizeof(int));;
  return ret_status;

}

Status join_blocked_nested(Intermediate* pos1, Intermediate* val1, Intermediate* pos2, Intermediate* val2, char* handle1, char* handle2){

  Status ret_status;
  ret_status.code = OK;

  size_t len_allocated = pos1->len_data;
  int* join_pos1 = (int*) malloc(len_allocated*sizeof(int));
  int* join_pos2 = (int*) malloc(len_allocated*sizeof(int));
  if(join_pos1==NULL||join_pos2==NULL){
    strcpy(ret_status.error_message,"join_nested: malloc joinpos 1 or 2 or both failed");
    return ret_status;
  }

  size_t k=0;
  for(int ii=0; ii < (int) pos1->len_data; ii+= NESTED_LOOP_JOIN_BLOCK_SIZE){
    for(int jj=0; jj < (int) pos2->len_data; jj+= NESTED_LOOP_JOIN_BLOCK_SIZE){
      int i_size = ii+NESTED_LOOP_JOIN_BLOCK_SIZE < (int) pos1->len_data ? NESTED_LOOP_JOIN_BLOCK_SIZE : (int) pos1->len_data - ii;
      int j_size = jj+NESTED_LOOP_JOIN_BLOCK_SIZE < (int) pos2->len_data ? NESTED_LOOP_JOIN_BLOCK_SIZE : (int) pos2->len_data - jj;
      for(int i=ii; i < ii+i_size; i++){
        for(int j=jj; j < jj+j_size; j++){
          if(val1->data.ints[i]==val2->data.ints[j]){
            join_pos1[k] = pos1->data.ints[i];
            join_pos2[k] = pos2->data.ints[j];
            k++;
            if(len_allocated<=k){
              join_pos1 = (int*) realloc(join_pos1, 2*len_allocated*sizeof(int));
              join_pos2 = (int*) realloc(join_pos2, 2*len_allocated*sizeof(int));
              if(join_pos1==NULL||join_pos2==NULL){
                strcpy(ret_status.error_message,"join_nested: realloc joinpos 1 or 2 or both failed");
                return ret_status;
              }
              len_allocated = 2*len_allocated;
            }
          }
        }
      }
    }
  }

  Intermediate* inter_handle1 = NULL;
  ret_status = get_intermediate(handle1, &inter_handle1);

  Intermediate* inter_handle2 = NULL;
  ret_status = get_intermediate(handle2, &inter_handle2);

  inter_handle1->type = INT;
  inter_handle2->type = INT;
  inter_handle1 -> len_data = k;
  inter_handle2 -> len_data = k;
  inter_handle1 -> data.ints = (int*) realloc(join_pos1, k*sizeof(int));;
  inter_handle2 -> data.ints = (int*) realloc(join_pos2, k*sizeof(int));;
  return ret_status;

}

int hash(int x, size_t SIZE){
  return x & ((int)(SIZE-1));
}

Partition* partitioning(Intermediate* inter, Intermediate* pos, Status* ret_status){

  Partition* partitions = (Partition*) malloc(HASH_JOIN_PARITION_NUM*sizeof(Partition));
  if(partitions==NULL){
    strcpy(ret_status->error_message,"partitioning: malloc patitions failed");
    return partitions;
  }

  // allocate memory for partitions
  for(size_t i=0; i < HASH_JOIN_PARITION_NUM; i++){
    partitions[i].data = (int*) malloc(INIT_PARTION_DATA*sizeof(int));
    partitions[i].pos  = (int*) malloc(INIT_PARTION_DATA*sizeof(int));
    if(partitions[i].data==NULL || partitions[i].pos==NULL){
      strcpy(ret_status->error_message,"partitioning: malloc patitions[i].data failed");
      return partitions;
    }
    partitions[i].len_occupied  = 0;
    partitions[i].len_allocated = INIT_PARTION_DATA;
  }

  // put elements in the right parition
  int key;
  for(size_t i=0; i < inter->len_data; i++){
    key = hash(inter->data.ints[i], HASH_JOIN_PARITION_NUM);
    if(partitions[key].len_allocated <= partitions[key].len_occupied){
      partitions[key].data = (int*) realloc(partitions[key].data, 2*(partitions[key].len_allocated)*sizeof(int));
      partitions[key].pos  = (int*) realloc(partitions[key].pos,  2*(partitions[key].len_allocated)*sizeof(int));
      if(partitions[key].data==NULL || partitions[key].pos==NULL){
        strcpy(ret_status->error_message,"partitioning: realloc failed");
        return partitions;
      }
      partitions[key].len_allocated = 2*partitions[key].len_allocated;
    }
    partitions[key].data[partitions[key].len_occupied] = inter->data.ints[i];
    partitions[key].pos[ partitions[key].len_occupied] = pos  ->data.ints[i];
    partitions[key].len_occupied += 1;
  }

  return partitions;

}


Status build_and_probe(Partition partion_val1,int** pt_pos1,Partition partion_val2,int** pt_pos2,size_t* len_occupied,size_t* len_allocated){

  Status ret_status;
  ret_status.code = OK;
  int* pos1 = *pt_pos1;
  int* pos2 = *pt_pos2;
  Partition* smaller_p;
  Partition* bigger_p;

  // pos1 is assicated to the smaller parititon
  if(partion_val1.len_occupied < partion_val2.len_occupied){
    smaller_p = &partion_val1;
    bigger_p  = &partion_val2;
    pos1 = *pt_pos1;
    pos2 = *pt_pos2;
  }else{
    smaller_p = &partion_val2;
    bigger_p  = &partion_val1;
    pos1 = *pt_pos2;
    pos2 = *pt_pos1;
  }

  size_t SIZE = 2*(smaller_p -> len_occupied);
  int key;

  if(SIZE==0){
    return ret_status;
  }

  HashItem* HashTable = (HashItem*) malloc(SIZE*sizeof(HashItem));
  for(size_t i=0; i<SIZE; i++){
    HashTable[i].state = FREE;
  }

  struct timespec start, end;
  clock_gettime(CLOCK_MONOTONIC, &start);
  // 2. pass insert elements in hash table_length
  for(size_t i=0; i<smaller_p->len_occupied; i++){
    key = hash(smaller_p -> data[i], SIZE);
    //move in array until an free HashItem
    while(HashTable[key].state != FREE) {
       //go to next cell
       ++key;
       //wrap around the table
       key %= SIZE;
    }
    HashTable[key].state = OCCUPIED;
    HashTable[key].data = smaller_p->data[i];
    HashTable[key].pos  = smaller_p->pos[i];
  }

  clock_gettime(CLOCK_MONOTONIC, &end);
  double time_taken = 1000*(end.tv_sec - start.tv_sec)+1e-6*(end.tv_nsec - start.tv_nsec);
  //log_info("build_and_probe 2. pass took %f ms to execute \n", time_taken);

  //printf("len_allocated start: %ld\n", *len_allocated);

  // 3. pass probe hash table
  clock_gettime(CLOCK_MONOTONIC, &start);
  size_t k = 0;
  for(size_t i=0; i<bigger_p->len_occupied; i++){
    key = hash(bigger_p -> data[i], SIZE);
    //move in array until an free HashItem
    while(HashTable[key].state != FREE) {
        if(bigger_p->data[i]==HashTable[key].data){
          if((*len_allocated) <= (*len_occupied + k)){
            pos1 = (int*) realloc(pos1, (2*(*len_allocated)*sizeof(int)));
            pos2 = (int*) realloc(pos2, (2*(*len_allocated)*sizeof(int)));
            if(pos1==NULL || pos2==NULL){
              ret_status.code = ERROR;
              strcpy(ret_status.error_message,"partitioning: realloc failed");
              return ret_status;
            }
            *len_allocated = 2*(*len_allocated);
          }
          pos1[*len_occupied  + k] = HashTable[key].pos;
          pos2[*len_occupied  + k] = bigger_p -> pos[i];
          k++;
        }
       //go to next cell
       ++key;
       //wrap around the table
       key %= SIZE;
    }
  }

  //printf("len_allocated end: %ld\n", *len_allocated);

  clock_gettime(CLOCK_MONOTONIC, &end);
  time_taken = 1000*(end.tv_sec - start.tv_sec)+1e-6*(end.tv_nsec - start.tv_nsec);
  //log_info("build_and_probe 3. pass took %f ms to execute \n", time_taken);

  // return realloced pointers
  *len_occupied  = *len_occupied  + k;
  if(partion_val1.len_occupied < partion_val2.len_occupied){
    *pt_pos1 = pos1;
    *pt_pos2 = pos2;
  }else{
    *pt_pos2 = pos1;
    *pt_pos1 = pos2;
  }
  free(HashTable);
  return ret_status;

}

Status joined_blocked_nested_loop(Partition partion_val1,int** pt_pos1,Partition partion_val2,int** pt_pos2,size_t* len_occupied,size_t* len_allocated){

  Status ret_status;
  ret_status.code = OK;
  int* pos1 = *pt_pos1;
  int* pos2 = *pt_pos2;
  Partition* smaller_p;
  Partition* bigger_p;

  // pos1 is assicated to the smaller parititon
  if(partion_val1.len_occupied < partion_val2.len_occupied){
    smaller_p = &partion_val1;
    bigger_p  = &partion_val2;
    pos1 = *pt_pos1;
    pos2 = *pt_pos2;
  }else{
    smaller_p = &partion_val2;
    bigger_p  = &partion_val1;
    pos1 = *pt_pos2;
    pos2 = *pt_pos1;
  }

  size_t k=0;
  for(int jj=0; jj < (int) smaller_p->len_occupied; jj+= NESTED_LOOP_JOIN_BLOCK_SIZE){
    int j_size = jj+NESTED_LOOP_JOIN_BLOCK_SIZE < (int) smaller_p->len_occupied ? NESTED_LOOP_JOIN_BLOCK_SIZE : (int) smaller_p->len_occupied - jj;
    for(int j=jj; j < jj+j_size; j++){
      for(int i=0; i < (int) bigger_p->len_occupied; i++){
        if(smaller_p->data[j]==bigger_p->data[i]){
          pos1[*len_occupied  + k] = smaller_p -> pos[j];
          pos2[*len_occupied  + k] = bigger_p  -> pos[i];
          k++;
          if((*len_allocated) <= (*len_occupied + k)){
            pos1 = (int*) realloc(pos1, 2*(*len_allocated)*sizeof(int));
            pos2 = (int*) realloc(pos2, 2*(*len_allocated)*sizeof(int));
            if(pos1==NULL||pos2==NULL){
              strcpy(ret_status.error_message,"join_nested: realloc joinpos 1 or 2 or both failed");
              return ret_status;
            }
            *len_allocated = 2*(*len_allocated);
          }
        }
      }
    }
  }

  // return realloced pointers
  *len_occupied  = *len_occupied  + k;
  if(partion_val1.len_occupied < partion_val2.len_occupied){
    *pt_pos1 = pos1;
    *pt_pos2 = pos2;
  }else{
    *pt_pos2 = pos1;
    *pt_pos1 = pos2;
  }
  return ret_status;

}

Status join_parition_blocked_loop(Intermediate* pos1, Intermediate* val1, Intermediate* pos2, Intermediate* val2, char* handle1, char* handle2){

  Status ret_status;
  ret_status.code = OK;

  //struct timespec start, end;
  //clock_gettime(CLOCK_MONOTONIC, &start);

  // 1. pass paritioning
  Partition* partions_val1 = partitioning(val1, pos1, &ret_status);
  if(ret_status.code==ERROR){
    return ret_status;
  }

  Partition* partions_val2 = partitioning(val2, pos2, &ret_status);
  if(ret_status.code==ERROR){
    return ret_status;
  }

  //clock_gettime(CLOCK_MONOTONIC, &end);
  //double time_taken = 1000*(end.tv_sec - start.tv_sec)+1e-6*(end.tv_nsec - start.tv_nsec);
  //log_info("join_hash partitioning took %f ms to execute \n\n", time_taken);


  size_t len_occupied  = 0;
  size_t len_allocated = 1*(pos1->len_data);
  int* ints_handle1 = (int*) malloc(len_allocated*sizeof(int));
  int* ints_handle2 = (int*) malloc(len_allocated*sizeof(int));
  if(ints_handle1==NULL || ints_handle2==NULL){
    strcpy(ret_status.error_message,"join_hash: malloc inter_handle 1 failed");
    return ret_status;
  }

  // 2. and 3 pass building hash tables and probing
  //struct timespec start_inner, end_inner;
  //clock_gettime(CLOCK_MONOTONIC, &start);
  for(size_t i=0; i < HASH_JOIN_PARITION_NUM; i++){
    //clock_gettime(CLOCK_MONOTONIC, &start_inner);
    ret_status = joined_blocked_nested_loop(partions_val1[i], &ints_handle1, partions_val2[i], &ints_handle2, &len_occupied, &len_allocated);
    if(ret_status.code==ERROR){
      return ret_status;
    }
    //printf("len_occupied: %ld\n", len_occupied);
    //printf("len_allocated: %ld\n", len_allocated);
    //clock_gettime(CLOCK_MONOTONIC, &end_inner);
    //time_taken = 1000*(end_inner.tv_sec - start_inner.tv_sec)+1e-6*(end_inner.tv_nsec - start_inner.tv_nsec);
    //log_info("join_hash build_and_probe took %f ms to execute \n\n", time_taken);
  }

  //clock_gettime(CLOCK_MONOTONIC, &end);
  //time_taken = 1000*(end.tv_sec - start.tv_sec)+1e-6*(end.tv_nsec - start.tv_nsec);
  //log_info("join_hash build_and_probe took %f ms to execute \n\n", time_taken);

  // free partition data
  for(size_t i=0; i < HASH_JOIN_PARITION_NUM; i++){
    free(partions_val1[i].data);
    free(partions_val2[i].data);
    free(partions_val1[i].pos);
    free(partions_val2[i].pos);
  }
  free(partions_val1);
  free(partions_val2);

  Intermediate* inter_handle1 = NULL;
  ret_status = get_intermediate(handle1, &inter_handle1);
  Intermediate* inter_handle2 = NULL;
  ret_status = get_intermediate(handle2, &inter_handle2);
  inter_handle1->type = INT;
  inter_handle2->type = INT;
  inter_handle1->data.ints = (int*) realloc(ints_handle1, len_occupied*sizeof(int));
  inter_handle2->data.ints = (int*) realloc(ints_handle2, len_occupied*sizeof(int));
  inter_handle1->len_data = len_occupied;
  inter_handle2->len_data = len_occupied;
  return ret_status;

}


Status join_hash(Intermediate* pos1, Intermediate* val1, Intermediate* pos2, Intermediate* val2, char* handle1, char* handle2){

  Status ret_status;
  ret_status.code = OK;

  struct timespec start, end;
  clock_gettime(CLOCK_MONOTONIC, &start);

  // 1. pass paritioning
  Partition* partions_val1 = partitioning(val1, pos1, &ret_status);
  if(ret_status.code==ERROR){
    return ret_status;
  }

  Partition* partions_val2 = partitioning(val2, pos2, &ret_status);
  if(ret_status.code==ERROR){
    return ret_status;
  }

  clock_gettime(CLOCK_MONOTONIC, &end);
  double time_taken = 1000*(end.tv_sec - start.tv_sec)+1e-6*(end.tv_nsec - start.tv_nsec);
  //log_info("join_hash partitioning took %f ms to execute \n\n", time_taken);


  size_t len_occupied  = 0;
  size_t len_allocated = 1*(pos1->len_data);
  int* ints_handle1 = (int*) malloc(len_allocated*sizeof(int));
  int* ints_handle2 = (int*) malloc(len_allocated*sizeof(int));
  if(ints_handle1==NULL || ints_handle2==NULL){
    strcpy(ret_status.error_message,"join_hash: malloc inter_handle 1 failed");
    return ret_status;
  }

  // 2. and 3 pass building hash tables and probing
  struct timespec start_inner, end_inner;
  clock_gettime(CLOCK_MONOTONIC, &start);
  for(size_t i=0; i < HASH_JOIN_PARITION_NUM; i++){
    clock_gettime(CLOCK_MONOTONIC, &start_inner);
    ret_status = build_and_probe(partions_val1[i], &ints_handle1, partions_val2[i], &ints_handle2, &len_occupied, &len_allocated);
    if(ret_status.code==ERROR){
      return ret_status;
    }
    clock_gettime(CLOCK_MONOTONIC, &end_inner);
    time_taken = 1000*(end_inner.tv_sec - start_inner.tv_sec)+1e-6*(end_inner.tv_nsec - start_inner.tv_nsec);
    //log_info("join_hash build_and_probe took %f ms to execute \n\n", time_taken);
  }

  clock_gettime(CLOCK_MONOTONIC, &end);
  time_taken = 1000*(end.tv_sec - start.tv_sec)+1e-6*(end.tv_nsec - start.tv_nsec);
  //log_info("join_hash build_and_probe took %f ms to execute \n\n", time_taken);

  // free partition data
  for(size_t i=0; i < HASH_JOIN_PARITION_NUM; i++){
    free(partions_val1[i].data);
    free(partions_val2[i].data);
    free(partions_val1[i].pos);
    free(partions_val2[i].pos);
  }
  free(partions_val1);
  free(partions_val2);

  Intermediate* inter_handle1 = NULL;
  ret_status = get_intermediate(handle1, &inter_handle1);
  Intermediate* inter_handle2 = NULL;
  ret_status = get_intermediate(handle2, &inter_handle2);
  inter_handle1->type = INT;
  inter_handle2->type = INT;
  inter_handle1->data.ints = (int*) realloc(ints_handle1, len_occupied*sizeof(int));
  inter_handle2->data.ints = (int*) realloc(ints_handle2, len_occupied*sizeof(int));
  inter_handle1->len_data = len_occupied;
  inter_handle2->len_data = len_occupied;
  return ret_status;

}

//* Milestone 5 addons *//

// reindex after load file
Status reindex(Table* table){

  Status ret_status;
  ret_status.code = OK;
  table->changed = true;

  // update primary index
  if(table->primary_index != NULL){
    IndexType type  = table -> primary_index -> type;
    int col_index = table -> primary_index -> col_index;
    deallocate_index(table -> primary_index);
    table -> primary_index = NULL;
    ret_status = create_primary_index(table,table->columns[col_index], type);
    if(ret_status.code == ERROR){
      return ret_status;
    }
  }

  // update secondary indicies
  for(size_t i=0; i < table->col_count; i++){
    if(table->columns[i]->secondary_index != NULL){
      IndexType type  = table -> columns[i] -> secondary_index -> type;
      deallocate_index(table -> columns[i] -> secondary_index);
      table -> columns[i] -> secondary_index = NULL;
      ret_status = create_secondary_index(table,table->columns[i], type);
      if(ret_status.code == ERROR){
        return ret_status;
      }
    }
  }

  table -> len_indexed = table -> len_occupied;
  return ret_status;

}

Status relational_update(Table* table, int* data, Intermediate* pos, int value){
  Status ret_status;
  ret_status.code = OK;
  table->changed = true;

  if(table->dirty == false){
    table -> deletes = (int*) malloc( (table->len_allocated/32+1) * sizeof(int));
    for(size_t i=0; i < table -> len_occupied; i++){
      ClearBit(table->deletes, i);
    }
  }
  table -> dirty = true;
  table -> select_queries_concession = 0;
  for(size_t i=0; i < pos -> len_data; i++){
    data[pos->data.ints[i]] = value;
  }
  return ret_status;
}

Status remove_deletes_from_select(Table* table, Intermediate* pos){
  Status ret_status;
  ret_status.code = OK;
  size_t k = 0;
  int hit;
  for(size_t i=0; i < pos -> len_data; i++){
    hit = TestBit(table->deletes, pos->data.ints[i]) == 0;
    pos->data.ints[k] = pos->data.ints[i];
    k = k + hit;
  }
  pos->len_data = k;
  return ret_status;
}

Status relational_delete(Table* table, Intermediate* pos){
  Status ret_status;
  ret_status.code = OK;
  table->changed = true;

  if(table->dirty == false){
    table -> deletes = (int*) malloc( (table->len_allocated/32+1) * sizeof(int));
    for(size_t i=0; i < table -> len_occupied; i++){
      ClearBit(table->deletes, i);
    }
  }
  table -> dirty = true;
  table -> select_queries_concession = 0;
  for(size_t i=0; i < pos -> len_data; i++){
    SetBit(table->deletes, pos->data.ints[i]);
  }
  return ret_status;
}

//* Additional code for milestone 2 *//


void* select_col_parallel_with_counter(void* arguments){

    BatchProcess* process = (BatchProcess*) arguments;
    BatchCollection* datastruct = process -> batch;
    BatchCollection* temp = NULL;
    int num_queries = process -> num_queries;
    int index;

    while(1){
        Intermediate* results = NULL;
        char handle[MAX_SIZE_NAME];
        // receive net work-load and get
        index = 0;
        pthread_mutex_lock(&lock);
        index = process -> counter;
        process -> counter += 1;
        if(index >= num_queries){
          pthread_mutex_unlock(&lock);
          return NULL;
        }
        temp = datastruct + index;
        strcpy(handle, temp->handle);
        temp -> ret_status = get_intermediate(handle, &results);
        if(temp -> ret_status.code==ERROR){
          return NULL;
        }
        // if(strcmp(handle,"s1_1")==0 || strcmp(handle,"s1_2")==0){
        //   for(int i=0; i < MAX_INTERMEDIATES; i++){
        //     if(current_db->intermediates[i]!=NULL){
        //       log_err("inter %d: %s\n", i, current_db->intermediates[i]->handle);
        //     }
        //   }
        // }
        pthread_mutex_unlock(&lock);
        // temp = datastruct + index;
        // if(index >= num_queries){
        //   return NULL;
        // }
        int* data          = temp -> data;
        size_t length      = temp -> length;
        int min            = temp -> min;
        int max            = temp -> max;
        SelectBound bound  = temp -> bound;
        int* hit_res = malloc(length*sizeof(int));
        if(data==hit_res){
          temp -> ret_status.code = ERROR;
          strcpy(temp -> ret_status.error_message, "select_col_prallel: malloc failed");
          return NULL;
        }
        size_t j = 0;

        if(bound==UPPER_AND_LOWER){
          for(size_t i=0; i<length; i++){
            hit_res[j] = i;
            j += (min <= data[i] && data[i] < max);
          }
        }else if(bound==UPPER){
          for(size_t i=0; i<length; i++){
            hit_res[j] = i;
            j += (data[i] < max);
          }
        }else if(bound==LOWER){
          for(size_t i=0; i<length; i++){
            hit_res[j] = i;
            j += (min <= data[i]);
          }
        }else{
          strcpy(temp -> ret_status.error_message, "select_db: invalid null");
          return NULL;
        }

        results -> data.ints = realloc(hit_res, j * sizeof(int));
        results -> type = INT;
        results -> len_data = j;
        temp    -> ret_status.code=OK;
    }
}

Status select_parallel_with_counter(BatchCollection* datastruct, size_t num_queries){
    Status ret_status;
    int result_code;
    pthread_t threads[NUM_THREADS];
    BatchProcess* process = (BatchProcess*) malloc(sizeof(BatchProcess));
    process -> counter = 0;
    process -> batch = datastruct;
    process -> num_queries = num_queries;
    for(size_t i = 0; i < NUM_THREADS; i++){
      result_code = pthread_create(&threads[i], NULL, select_col_parallel_with_counter, process);
      assert(!result_code);
    }
    for(size_t i = 0; i < NUM_THREADS; i++) {
      result_code = pthread_join(threads[i], NULL);
      assert(!result_code);
    }
    for(size_t i = 0; i < num_queries; i++) {
      ret_status = datastruct[i].ret_status;
      if(ret_status.code == ERROR){
        return ret_status;
      }
    }
    ret_status.code = OK;
    free(process);
    return ret_status;
}

//* Additional code for milestone 3 *//

Status load_primary_index(Table* table, Column* column, IndexType type){

  Status ret_status;
  ret_status.code = OK;

  //check if table already has a primary index
  if(table->primary_index == NULL){
    table->primary_index = (Index*) malloc(sizeof(Index));
  }else{
    strcpy(ret_status.error_message, "load_primary_index: multiple primary indicies are not implemented yet");
    ret_status.code = ERROR;
    return ret_status;
  }

  int col_index;
  //finding the column_index
  for(size_t i=0; i<table->col_count; i++){
    if(table->columns[i]==column){
      col_index = i;
      break;
    }
  }

  Index* temp = table->primary_index;
  temp -> type = type;
  temp -> place = PRIMARY;
  temp -> col_index = col_index;

  size_t len_indexed = table->len_indexed;

  if(type==BTREE){
    int N = (int) ceil(pow((double) len_indexed,(double)1/NUM_LAYERS_BTREE));
    Node* root = (Node*) malloc(sizeof(Node));
    root -> child = NULL;
    root -> data  = NULL;
    if(len_indexed>0){
      root -> N = N;
      build_btree(root, N, 0);
      for(size_t i=0; i<NUM_LAYERS_BTREE-1; i++){
        initialize_btree(root,column->data,N,i,0,0,0,len_indexed);
      }
    }
    temp -> root = root;
    temp -> pos  = NULL;
    temp -> data = column -> data;
  }else if(type==SORTED){
    temp -> root = NULL;
    temp -> pos  = NULL;
    temp -> data = column->data;
  }

  return ret_status;

}

Status load_secondary_index(Table* table, size_t i, size_t j, Column* column, IndexType type){

  Status ret_status;
  ret_status.code = OK;

  Index* temp = (Index*) malloc(sizeof(Index));
  if(temp==NULL){
    strcpy(ret_status.error_message, "load_secondary_index: index malloc failed");
    ret_status.code = ERROR;
    return ret_status;
  }

  size_t len_indexed   = table->len_indexed;
  temp -> type = type;
  temp -> place = SECONDARY;

  char filename[MAX_SIZE_NAME];
  sprintf(filename, "cs165.db/%ld_%lu_sec_pos.txt", i, j);
  log_info("data from column file: '%s' loaded\n", filename);
  ret_status = read_column_file(filename,
                                &(temp->pos),
                                len_indexed,
                                len_indexed);

  sprintf(filename, "cs165.db/%ld_%lu_sec_data.txt", i, j);
  log_info("data from column file: '%s' loaded\n", filename);
  ret_status = read_column_file(filename,
                                &(temp->data),
                                len_indexed,
                                len_indexed);

  if(type==BTREE){
    Node* root = (Node*) malloc(sizeof(Node));
    int N = (int) ceil(pow((double)len_indexed,(double)1/NUM_LAYERS_BTREE));
    root -> N = N;
    root -> child = NULL;
    root -> data  = NULL;
    if(len_indexed>0){
      build_btree(root, N, 0);
      for(size_t i=0; i<NUM_LAYERS_BTREE-1; i++){
        initialize_btree(root,temp->data,root->N,i,0,0,0,len_indexed);
      }
    }
    temp -> root = root;
  }else if(type==SORTED){
    temp -> root = NULL;
  }

  column->secondary_index = temp;
  return ret_status;

}

int cmpptrs (const void * a, const void * b) {
  return ( *(*(int**) a) - *(*(int**) b) );
}

Status sortTableByColumn(Table* table, Column* sortedColumn){
  Status ret_status;
  ret_status.code = OK;
  size_t len_occupied  = table->len_occupied;
  size_t len_allocated = table->len_allocated;
  int** ptrs = (int**) malloc(len_occupied * sizeof(int*));
  if(ptrs==NULL){
    ret_status.code = ERROR;
    strcpy(ret_status.error_message, "sortTableByColumn: ptrs malloc failed");
    return ret_status;
  }
  for(size_t i = 0; i < len_occupied; i++){
    ptrs[i] = sortedColumn->data + i;
  }

  clock_t t;
  t = clock();
  qsort(ptrs, len_occupied, sizeof(int*), cmpptrs);
  t = clock() - t;
  double time_taken = (((double)t)/CLOCKS_PER_SEC)*1000;
  log_info("sorting took took %f ms to execute in sortTableByColumn() \n", time_taken);

  sortedColumn -> sorted = true;
  int* temp_data = (int*) malloc(len_allocated*sizeof(int));
  int* offset =  sortedColumn->data;
  for(size_t i=0; i<table->col_count; i++){
    Column* column = table->columns[i];
    for(size_t i=0; i<len_occupied; i++){
      temp_data[i] = *(ptrs[i] - offset + column->data);
    }
    int* temp = column -> data;
    column -> data = temp_data;
    temp_data = temp;
  }
  free(temp_data);
  free(ptrs);
  return ret_status;
}

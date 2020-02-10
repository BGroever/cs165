/*
 * This file contains methods necessary to parse input from the client.
 * Mostly, functions in parse.c will take in string input and map these
 * strings into database operators. This will require checking that the
 * input from the client is in the correct format and maps to a valid
 * database operator.
 */

//#define _DEFAULT_SOURCE
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include "cs165_api.h"
#include "parse.h"
#include "utils.h"
#include "client_context.h"
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <math.h>

BatchCollection* batch;
size_t count_query = 0;
StatusSelect select_status = SERIAL;

DbOperator* parse_create_index(char* create_arguments){

  message_status status = OK_DONE;
  char** create_arguments_index = &create_arguments;
  char* column_to_index  = next_token(create_arguments_index, &status);
  char* index_kind     = next_token(create_arguments_index, &status);
  char* cluster_kind   = next_token(create_arguments_index, &status);

  // not enough arguments
  if (status == INCORRECT_FORMAT) {
      return NULL;
  }
  // Get the table name free of quotation marks
  column_to_index = trim_quotes(column_to_index);
  // read and chop off last char, which should be a ')'
  int last_char = strlen(cluster_kind) - 1;
  if (cluster_kind[last_char] != ')') {
      return NULL;
  }
  // the ')' with a null terminating character.
  cluster_kind[last_char] = '\0';

  //Split location
  char** location_index = &column_to_index;
  char* db_name     = split_dot(location_index, &status);
  char* tbl_name    = split_dot(location_index, &status);
  char* column_name = split_dot(location_index, &status);
  // not enough arguments
  if (status == INCORRECT_FORMAT) {
      return NULL;
  }
  // check that the database argument is the current active database
  if (!current_db || strcmp(current_db->name, db_name) != 0) {
      cs165_log(stdout, "Query unsupported. Bad db name.");
      return NULL; //QUERY_UNSUPPORTED
  }

  Table* tbl = lookup_table(tbl_name);
  Column* column = lookup_column(tbl, column_name);

  IndexPlace place;
  if(strcmp(cluster_kind, "clustered")==0){
    place = PRIMARY;
  }else if(strcmp(cluster_kind, "unclustered")==0){
    place = SECONDARY;
  } else {
    return NULL;
  }

  IndexType type;
  if(strcmp(index_kind, "sorted")==0){
    type = SORTED;
  }else if(strcmp(index_kind, "btree")==0){
    type = BTREE;
  } else {
    return NULL;
  }

  // make create dbo for column
  DbOperator* dbo = malloc(sizeof(DbOperator));
  dbo->type = CREATE;
  dbo->operator_fields.create_operator.create_type = _INDEX;
  dbo->operator_fields.create_operator.column = column;
  dbo->operator_fields.create_operator.table = tbl;
  dbo->operator_fields.create_operator.index_type  = type;
  dbo->operator_fields.create_operator.index_place = place;
  return dbo;

}

DbOperator* parse_create_col(char* create_arguments){

  message_status status = OK_DONE;
  char** create_arguments_index = &create_arguments;
  char* column_name = next_token(create_arguments_index, &status);
  char* location = next_token(create_arguments_index, &status);

  // not enough arguments
  if (status == INCORRECT_FORMAT) {
      return NULL;
  }
  // Get the table name free of quotation marks
  column_name = trim_quotes(column_name);
  // read and chop off last char, which should be a ')'
  int last_char = strlen(location) - 1;
  if (location[last_char] != ')') {
      return NULL;
  }
  // the ')' with a null terminating character.
  location[last_char] = '\0';
  //Split location
  char** location_index = &location;
  char* db_name  = split_dot(location_index, &status);
  char* tbl_name = split_dot(location_index, &status);
  // not enough arguments
  if (status == INCORRECT_FORMAT) {
      return NULL;
  }
  // check that the database argument is the current active database
  if (!current_db || strcmp(current_db->name, db_name) != 0) {
      cs165_log(stdout, "Query unsupported. Bad db name.");
      return NULL; //QUERY_UNSUPPORTED
  }

  Table* tbl = lookup_table(tbl_name);

  // make create dbo for column
  DbOperator* dbo = malloc(sizeof(DbOperator));
  dbo->type = CREATE;
  dbo->operator_fields.create_operator.create_type = _COLUMN;
  strcpy(dbo->operator_fields.create_operator.name, column_name);
  dbo->operator_fields.create_operator.db = current_db;
  dbo->operator_fields.create_operator.table = tbl;

  return dbo;

}

/**
 * This method takes in a string representing the arguments to create a table.
 * It parses those arguments, checks that they are valid, and creates a table.
 **/
DbOperator* parse_create_tbl(char* create_arguments) {
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* table_name = next_token(create_arguments_index, &status);
    char* db_name = next_token(create_arguments_index, &status);
    char* col_cnt = next_token(create_arguments_index, &status);

    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return NULL;
    }
    // Get the table name free of quotation marks
    table_name = trim_quotes(table_name);
    // read and chop off last char, which should be a ')'
    int last_char = strlen(col_cnt) - 1;
    if (col_cnt[last_char] != ')') {
        return NULL;
    }
    // replace the ')' with a null terminating character.
    col_cnt[last_char] = '\0';
    // check that the database argument is the current active database
    if (!current_db || strcmp(current_db->name, db_name) != 0) {
        cs165_log(stdout, "Query unsupported. Bad db name.");
        return NULL; //QUERY_UNSUPPORTED
    }
    // turn the string column count into an integer, and check that the input is valid.
    int column_cnt = atoi(col_cnt);
    if (column_cnt < 1) {
        return NULL;
    }
    // make create dbo for table
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE;
    dbo->operator_fields.create_operator.create_type = _TABLE;
    strcpy(dbo->operator_fields.create_operator.name, table_name);
    dbo->operator_fields.create_operator.db = current_db;
    dbo->operator_fields.create_operator.col_count = column_cnt;
    return dbo;
}

/**
 * This method takes in a string representing the arguments to create a database.
 * It parses those arguments, checks that they are valid, and creates a database.
 **/
DbOperator* parse_create_db(char* create_arguments) {
    char *token;
    token = strsep(&create_arguments, ",");
    // not enough arguments if token is NULL
    if (token == NULL) {
        return NULL;
    } else {
        // create the database with given name
        char* db_name = token;
        // trim quotes and check for finishing parenthesis.
        db_name = trim_quotes(db_name);
        int last_char = strlen(db_name) - 1;
        if (last_char < 0 || db_name[last_char] != ')') {
            return NULL;
        }
        // replace final ')' with null-termination character.
        db_name[last_char] = '\0';

        token = strsep(&create_arguments, ",");
        if (token != NULL) {
            return NULL;
        }
        // make create operator.
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = CREATE;
        dbo->operator_fields.create_operator.create_type = _DB;
        strcpy(dbo->operator_fields.create_operator.name, db_name);
        return dbo;
    }
}

/**
 * parse_create parses a create statement and then passes the necessary arguments off to the next function
 **/

DbOperator* parse_create(char* create_arguments) {
    message_status mes_status = OK_DONE;
    DbOperator* dbo = NULL;
    char *tokenizer_copy, *to_free;
    // Since strsep destroys input, we create a copy of our input.
    tokenizer_copy = to_free = malloc((strlen(create_arguments)+1) * sizeof(char));
    char *token;
    strcpy(tokenizer_copy, create_arguments);
    // check for leading parenthesis after create.
    if (strncmp(tokenizer_copy, "(", 1) == 0) {
        tokenizer_copy++;
        // token stores first argument. Tokenizer copy now points to just past first ","
        token = next_token(&tokenizer_copy, &mes_status);
        if (mes_status == INCORRECT_FORMAT) {
            return NULL;
        } else {
            // pass off to next parse function.
            if (strcmp(token, "db") == 0) {
                dbo = parse_create_db(tokenizer_copy);
            } else if (strcmp(token, "tbl") == 0) {
                dbo = parse_create_tbl(tokenizer_copy);
            } else if (strcmp(token, "col") == 0) {
                dbo = parse_create_col(tokenizer_copy);
            } else if (strcmp(token, "idx") == 0) {
                dbo = parse_create_index(tokenizer_copy);
            }else {
                mes_status = UNKNOWN_COMMAND;
            }
        }
    } else {
        mes_status = UNKNOWN_COMMAND;
    }
    free(to_free);
    return dbo;
}

/**
 * parse_insert reads in the arguments for a create statement and
 * then passes these arguments to a database function to insert a row.
 **/

DbOperator* parse_insert(char* query_command, message* send_message) {

    unsigned int columns_inserted = 0;
    char* token = NULL;
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        // parse table input
        char* location = next_token(command_index, &send_message->status);
        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }

        // Get the table name free of quotation marks
        location = trim_quotes(location);
        // Add '\0'
        location[strlen(location)] = '\0';
        //Split location
        char** location_index = &location;
        message_status status = OK_DONE;
        char* db_name  = split_dot(location_index, &status);
        char* tbl_name = split_dot(location_index, &status);
        // not enough arguments
        if (status == INCORRECT_FORMAT) {
            return NULL;
        }

        // check that the database argument is the current active database
        if (!current_db || strcmp(current_db->name, db_name) != 0) {
            cs165_log(stdout, "Query unsupported. Bad db name.");
            return NULL; //QUERY_UNSUPPORTED
        }

        // lookup the table and make sure it exists.
        Table* insert_table = lookup_table(tbl_name);
        if (insert_table == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }

        // make insert operator.
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = INSERT;
        dbo->operator_fields.insert_operator.table  = insert_table;
        dbo->operator_fields.insert_operator.values = malloc(sizeof(int) * insert_table->col_count);

        // parse inputs until we reach the end. Turn each given string into an integer.
        while ((token = strsep(command_index, ",")) != NULL) {
            int insert_val = atoi(token);
            dbo->operator_fields.insert_operator.values[columns_inserted] = insert_val;
            columns_inserted++;
        }
        // check that we received the correct number of input values
        if (columns_inserted != insert_table->col_count) {
            send_message->status = INCORRECT_FORMAT;
            free (dbo);
            return NULL;
        }
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

DbOperator* parse_select_from_column(char* query_command, char* handle, message* send_message, char* db_name, char* tbl_name, char* col_name){

    // check that the database argument is the current active database
    if (!current_db || strcmp(current_db->name, db_name) != 0) {
        cs165_log(stdout, "Query unsupported. Bad db name.");
        return NULL; //QUERY_UNSUPPORTED
    }

    // lookup the table and make sure it exists.
    Table* select_table = lookup_table(tbl_name);
    if (select_table == NULL) {
        send_message->status = OBJECT_NOT_FOUND;
        return NULL;
    }


    SelectBound bound = UPPER_AND_LOWER;
    char *token = next_token(&query_command, &send_message->status);
    token = trim_whitespace(token);
    if(strcmp(token,"null")==0){
        bound = UPPER;
    }
    int min = atoi(token);

    token = next_token(&query_command, &send_message->status);
    token = trim_whitespace(token);
    if(strcmp(token,"null")==0){
      if(bound==UPPER){
        return NULL;
      }
      bound = LOWER;
    }
    int max = atoi(token);

    DbOperator* dbo = NULL;
    if(select_status==SERIAL){
      dbo = (DbOperator*) malloc(sizeof(DbOperator));
      dbo->type = SELECT;
      dbo->operator_fields.select_operator.table        = select_table;
      dbo->operator_fields.select_operator.col_name     = col_name;
      dbo->operator_fields.select_operator.handle       = handle;
      dbo->operator_fields.select_operator.min          = min;
      dbo->operator_fields.select_operator.max          = max;
      dbo->operator_fields.select_operator.bound        = bound;
      dbo->operator_fields.select_operator.type         = SELECT_FROM_COLOUM;
    }else if(select_status==PARALLEL){
      Column* column = lookup_column(select_table, col_name);
      if(batch[0].data!=NULL){
        if(batch[0].data != column->data){
          send_message->status = OBJECT_NOT_FOUND;
          return dbo;
        }
        if(batch[0].length != select_table -> len_occupied){
          send_message->status = OBJECT_NOT_FOUND;
          return dbo;
        }
        if(MAX_INTERMEDIATES < count_query){
          send_message->status = INCORRECT_FORMAT;
          return dbo;
        }
      }
      batch[count_query].data            = column -> data;
      batch[count_query].length          = select_table -> len_occupied;
      batch[count_query].ret_status.code = OK;
      batch[count_query].min             = min;
      batch[count_query].max             = max;
      batch[count_query].bound            = bound;
      strcpy(batch[count_query].handle, handle);
      count_query++;
      dbo = (DbOperator*) malloc(sizeof(DbOperator));
      dbo->type = SELECT;
      dbo->operator_fields.select_operator.type = SELECT_PARALLEL;
    }
    return dbo;
}

DbOperator* parse_select_from_handle(char* query_command, char* handle, message* send_message, char* pos_handle){

    char* value_handle = next_token(&query_command, &send_message->status);
    if (send_message->status == INCORRECT_FORMAT) {
        return NULL;
    }

    SelectBound bound = UPPER_AND_LOWER;
    char *token = next_token(&query_command, &send_message->status);
    if (send_message->status == INCORRECT_FORMAT) {
        return NULL;
    }
    token = trim_whitespace(token);
    if(strcmp(token,"null")==0){
        bound = UPPER;
    }
    int min = atoi(token);

    token = next_token(&query_command, &send_message->status);
    if (send_message->status == INCORRECT_FORMAT) {
        return NULL;
    }
    token = trim_whitespace(token);
    if(strcmp(token,"null")==0){
        bound = LOWER;
    }
    int max = atoi(token);

    Intermediate* positions = lookup_intermediate(pos_handle);
    Intermediate* values = lookup_intermediate(value_handle);
    if(positions==NULL || values==NULL){
      send_message->status = INCORRECT_FORMAT;
      return NULL;
    }

    if(positions->type!=INT || values->type!=INT){
      send_message->status = INCORRECT_FORMAT;
      return NULL;
    }

    if(positions->len_data!=values->len_data){
      send_message->status = INCORRECT_FORMAT;
      return NULL;
    }

    DbOperator* dbo = NULL;
    if(select_status==SERIAL){
      dbo = (DbOperator*) malloc(sizeof(DbOperator));
      dbo->type = SELECT;
      dbo->operator_fields.select_operator.positions   = positions->data.ints;
      dbo->operator_fields.select_operator.values      = values->data.ints;
      dbo->operator_fields.select_operator.length      = positions->len_data;
      dbo->operator_fields.select_operator.handle      = handle;
      dbo->operator_fields.select_operator.min         = min;
      dbo->operator_fields.select_operator.max         = max;
      dbo->operator_fields.select_operator.bound       = bound;
      dbo->operator_fields.select_operator.type        = SELECT_FROM_INTERMEDIATE;
    } else if(select_status==PARALLEL){
      send_message->status = INCORRECT_FORMAT;
    }

    return dbo;

}


DbOperator* parse_select(char* query_command, char* handle, message* send_message){

    if (strncmp(query_command, "(", 1) != 0) {
      send_message->status = UNKNOWN_COMMAND;
      return NULL;
    }

    query_command = trim_parenthesis(query_command);

    // parse table input
    char* location = next_token(&query_command, &send_message->status);
    if (send_message->status == INCORRECT_FORMAT) {
        return NULL;
    }

    // parse location
    message_status status = OK_DONE;
    char* db_name_or_pos_handle  = split_dot(&location, &status);
    if (status == INCORRECT_FORMAT) {
        return NULL;
    }
    char* tbl_name = split_dot(&location, &status);
    char* col_name = split_dot(&location, &status);

    DbOperator* dbo = NULL;
    if(tbl_name!=NULL && col_name!=NULL){
        dbo = parse_select_from_column(query_command, handle, send_message, db_name_or_pos_handle, tbl_name, col_name);
    }else if(tbl_name==NULL && col_name==NULL){
        dbo = parse_select_from_handle(query_command, handle, send_message, db_name_or_pos_handle);
    }else{
        send_message -> status = INCORRECT_FORMAT;
        dbo = NULL;
    }
    return dbo;
}

DbOperator* parse_fetch(char* query_command, char* handle, message* send_message){


  if (strncmp(query_command, "(", 1) != 0) {
    send_message->status = UNKNOWN_COMMAND;
    log_err("parse fetch: '(' test failed (%s)", query_command);
    return NULL;
  }

  //query_command = trim_parenthesis(query_command);
  query_command++;

  // parse table input
  char* location = next_token(&query_command, &send_message->status);
  if (send_message->status == INCORRECT_FORMAT) {
      return NULL;
  }

  // parse location
  location[strlen(location)] = '\0';
  message_status status = OK_DONE;
  char* db_name  = split_dot(&location, &status);
  char* tbl_name = split_dot(&location, &status);
  char* col_name = split_dot(&location, &status);

  // not enough arguments
  if (status == INCORRECT_FORMAT) {
      return NULL;
  }

  // check that the database argument is the current active database
  if (!current_db || strcmp(current_db->name, db_name) != 0) {
      cs165_log(stdout, "Query unsupported. Bad db name.");
      return NULL; //QUERY_UNSUPPORTED
  }

  // lookup the table and make sure it exists.
  Table* select_table = lookup_table(tbl_name);
  if (select_table == NULL) {
      send_message->status = OBJECT_NOT_FOUND;
      return NULL;
  }

  // Get desination handle
  // char* src_handle = next_token(&query_command, &send_message->status);
  // if (send_message->status == INCORRECT_FORMAT) {
  //     return NULL;
  // }

  char* src_handle = (char*) strsep(&query_command, ")" );
  Intermediate* src_intermediate = lookup_intermediate(src_handle);
  if (src_intermediate == NULL) {
      log_err("src_intermediate: %s is not found\n", src_handle);
      for(int i=0; i < MAX_INTERMEDIATES; i++){
        if(current_db->intermediates[i] != NULL){
          log_err("intermediate %d: %s\n", i, current_db->intermediates[i]->handle);
        }
      }
      return NULL;
  }
  // }else{
  //     // log_err("src_intermediate: %s is found\n", src_handle);
  //     // for(int i=0; i < MAX_INTERMEDIATES; i++){
  //     //   if(current_db->intermediates[i] != NULL){
  //     //     log_err("intermediate %d: %s\n", i, current_db->intermediates[i]->handle);
  //     //   }
  //     // }
  // }

  DbOperator* dbo = malloc(sizeof(DbOperator));
  dbo->type = FETCH;
  dbo->operator_fields.fetch_operator.table             = select_table;
  dbo->operator_fields.fetch_operator.col_name          = col_name;
  dbo->operator_fields.fetch_operator.src_intermediate  = src_intermediate;
  dbo->operator_fields.fetch_operator.dest_handle       = handle;
  return dbo;

}

DbOperator* parse_print(char* query_command, message* send_message){

      if (strncmp(query_command, "(", 1) != 0) {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
      }

      query_command = trim_parenthesis(query_command);
      Intermediate** intermediates = (Intermediate**) malloc(MAX_INTERMEDIATES*sizeof(Intermediate*));

      size_t max_intermediate = MAX_INTERMEDIATES;
      for (size_t i=0; i<max_intermediate; i++){
        *(intermediates+i) = (Intermediate*) NULL;
      }
      size_t count = 0;
      char** command_index = &query_command;
      char* token;

      while ((token = strsep(command_index, ",")) != NULL) {
          intermediates[count] = lookup_intermediate(token);
          if((intermediates+count) == NULL){
            send_message->status = UNKNOWN_COMMAND;
            return NULL;
          }
          count++;
      }

      DbOperator* dbo = malloc(sizeof(DbOperator));
      dbo->type = PRINT;
      dbo->operator_fields.print_operator.handles = intermediates;
      dbo->operator_fields.print_operator.length  = count;
      return dbo;

}

DbOperator* parse_shutdown(char* query_command, message* send_message){
  if (strncmp(query_command, "", 1) == 0) {
      DbOperator* dbo = malloc(sizeof(DbOperator));
      dbo->type = SHUTDOWN;
      return dbo;
  }else {
      send_message->status = UNKNOWN_COMMAND;
      return NULL;
  }
}

DbOperator* parse_load(char* query_command, message* send_message, int client_socket){

  send_message -> status = OK_DONE;
  if (strncmp(query_command, "(", 1) != 0) {
      send_message->status = UNKNOWN_COMMAND;
      return NULL;
  }

  message recv_message;

  recv(client_socket, &recv_message, sizeof(message), 0);
  char recv_buffer[recv_message.length + 1];
  //char* location_index = &locations;
  recv(client_socket, &recv_buffer, recv_message.length, 0);
  recv_buffer[recv_message.length] = '\0';
  char* locations = (char*) &recv_buffer;

  Table* table = NULL;
  Table* check_table;
  char* loc_token = NULL;
  int success = 0;
  int columns_filled = 0;

  // parsing throught the locations
  while ((loc_token = next_token(&locations, &(send_message -> status))) != NULL) {
    char* db_name  = split_dot(&loc_token, &(send_message -> status));
    char* tbl_name = split_dot(&loc_token, &(send_message -> status));
    char* col_name = split_dot(&loc_token, &(send_message -> status));
    if (!current_db || strcmp(current_db->name, db_name) != 0) {
        cs165_log(stdout, "Query unsupported. Bad db name.");
        if(send(client_socket, &success, sizeof(success), 0)==-1){
          cs165_log(stdout, "Can't send error message.");
        }
        return NULL; //QUERY_UNSUPPORTED
    }
    // Perform number column check when the table names changes
    if(table == NULL){
        table = lookup_table(tbl_name);
        if(table==NULL){
          send_message->status = INCORRECT_FORMAT;
          if(send(client_socket, &success, sizeof(success), 0)==-1){
            cs165_log(stdout, "Can't send error message.");
          }
          return NULL;
        }
    }else{
        check_table = lookup_table(tbl_name);
        if(check_table==NULL){
          send_message->status = INCORRECT_FORMAT;
          if(send(client_socket, &success, sizeof(success), 0)==-1){
            cs165_log(stdout, "Can't send error message.");
          }
          return NULL;
        }
        if(strcmp(table->name, check_table->name)!=0){
            send_message->status = INCORRECT_FORMAT;
            if(send(client_socket, &success, sizeof(success), 0)==-1){
              cs165_log(stdout, "Can't send error message.");
            }
            return NULL;
        }
    }
    Column* column = lookup_column(table,col_name);
    if(column==NULL){
        send_message->status = INCORRECT_FORMAT;
        if(send(client_socket, &success, sizeof(success), 0)==-1){
          cs165_log(stdout, "Can't send error message.");
        }
        return NULL;
    }
    columns_filled++;
  }

  //Check if columns_filled equal to the table columns
  if(table->col_count!= (size_t) columns_filled){
      send_message->status = INCORRECT_FORMAT;
      if(send(client_socket, &success, sizeof(success), 0)==-1){
        cs165_log(stdout, "Can't send error message.");
      }
      return NULL;
  }

  success = 1;
  if(send(client_socket, &success, sizeof(success), 0)==-1){
    cs165_log(stdout, "Can't send error message.");
  }
  if(send(client_socket, &columns_filled, sizeof(columns_filled), 0)==-1){
    cs165_log(stdout, "Can't send error message.");
  }

  DbOperator* dbo = malloc(sizeof(DbOperator));
  dbo->type = LOAD;
  dbo->operator_fields.load_operator.table = table;
  dbo->operator_fields.load_operator.columns_filled = columns_filled;

  return dbo;

}

DbOperator* parse_aggregate(char* query_command, message* send_message, char* handle, AggregateType aggregate){

    message_status status = OK_DONE;

    if (strncmp(query_command, "(", 1) != 0) {
      send_message->status = UNKNOWN_COMMAND;
      return NULL;
    }

    query_command = trim_parenthesis(query_command);

    // parsing throught the locations
    char* db_or_handle_name  = split_dot(&query_command, &status);
    char* tbl_name = split_dot(&query_command, &status);
    char* col_name = split_dot(&query_command, &status);

    // if, we are given a handle
    if(tbl_name == NULL && col_name == NULL){

      Intermediate* source_intermediate = lookup_intermediate(db_or_handle_name);
      if(source_intermediate==NULL){
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
      }
      if(source_intermediate -> type == FLOAT){
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
      }else if(source_intermediate -> type == INT){
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = AGGREGATE;
        dbo->operator_fields.aggregate_operator.dest_handle   = handle;
        dbo->operator_fields.aggregate_operator.aggregate     = aggregate;
        dbo->operator_fields.aggregate_operator.length        = source_intermediate->len_data;
        dbo->operator_fields.aggregate_operator.ints          = source_intermediate->data.ints;
        dbo->operator_fields.aggregate_operator.longs         = NULL;
        return dbo;
      }else if(source_intermediate -> type == LONG){
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = AGGREGATE;
        dbo->operator_fields.aggregate_operator.dest_handle   = handle;
        dbo->operator_fields.aggregate_operator.aggregate     = aggregate;
        dbo->operator_fields.aggregate_operator.length        = source_intermediate->len_data;
        dbo->operator_fields.aggregate_operator.ints          = NULL;
        dbo->operator_fields.aggregate_operator.longs         = source_intermediate->data.longs;
        return dbo;
      }

    // if, we are given a column
    } else if(tbl_name != NULL && col_name != NULL){

      if (!current_db || strcmp(current_db->name, db_or_handle_name) != 0) {
          cs165_log(stdout, "Query unsupported. Bad db name.");
          return NULL; //QUERY_UNSUPPORTED
      }
      Table* table = lookup_table(tbl_name);
      if(table==NULL){
          send_message->status = INCORRECT_FORMAT;
          return NULL;
      }
      Column* column = lookup_column(table, col_name);
      if(column==NULL){
          send_message->status = INCORRECT_FORMAT;
          return NULL;
      }
      DbOperator* dbo = malloc(sizeof(DbOperator));
      dbo->type = AGGREGATE;
      dbo->operator_fields.aggregate_operator.dest_handle   = handle;
      dbo->operator_fields.aggregate_operator.aggregate     = aggregate;
      dbo->operator_fields.aggregate_operator.length        = table->len_occupied;
      dbo->operator_fields.aggregate_operator.ints          = column->data;
      dbo->operator_fields.aggregate_operator.longs         = NULL;
      return dbo;

    // else the format is incorrect
    } else {
      send_message->status = INCORRECT_FORMAT;
      return NULL;
    }
    return NULL;
}

DbOperator* parse_operate(char* query_command, message* send_message, char* handle, OperationType operation){

    message_status status = OK_DONE;

    if (strncmp(query_command, "(", 1) != 0) {
      send_message->status = UNKNOWN_COMMAND;
      return NULL;
    }

    query_command = trim_parenthesis(query_command);
    char* data1 = next_token(&query_command, &status);
    char* data2 = next_token(&query_command, &status);

    // parsing throught the locations
    char* db_or_handle_name1  = split_dot(&data1, &status);
    char* tbl_name1 = split_dot(&data1, &status);
    char* col_name1 = split_dot(&data1, &status);

    char* db_or_handle_name2  = split_dot(&data2, &status);
    char* tbl_name2 = split_dot(&data2, &status);
    char* col_name2 = split_dot(&data2, &status);

    // if, we are given a handle
    if(tbl_name1==NULL && col_name1==NULL && tbl_name2==NULL && col_name2==NULL){

      Intermediate* source_intermediate1 = lookup_intermediate(db_or_handle_name1);
      Intermediate* source_intermediate2 = lookup_intermediate(db_or_handle_name2);
      if(source_intermediate1==NULL || source_intermediate2==NULL){
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
      }
      if(source_intermediate1 -> len_data != source_intermediate2 -> len_data){
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
      }
      DbOperator* dbo = malloc(sizeof(DbOperator));
      dbo->type = OPERATE;
      dbo->operator_fields.operation_operator.source        = INTERMEDIATE_OPERATION;
      dbo->operator_fields.operation_operator.dest_handle   = handle;
      dbo->operator_fields.operation_operator.operation     = operation;
      dbo->operator_fields.operation_operator.length        = source_intermediate1->len_data;
      dbo->operator_fields.operation_operator.inter1        = source_intermediate1;
      dbo->operator_fields.operation_operator.inter2        = source_intermediate2;
      return dbo;

    // if, we are given a column
  } else if(tbl_name1!=NULL && col_name1!=NULL && tbl_name2!=NULL && col_name2!=NULL){

      if (!current_db || strcmp(current_db->name, db_or_handle_name1) != 0 || strcmp(current_db->name, db_or_handle_name2) != 0) {
          cs165_log(stdout, "Query unsupported. Bad db name.");
          return NULL; //QUERY_UNSUPPORTED
      }
      Table* table1 = lookup_table(tbl_name1);
      Table* table2 = lookup_table(tbl_name2);
      if(table1==NULL || table2==NULL || table1!=table2){
          send_message->status = INCORRECT_FORMAT;
          return NULL;
      }
      Column* column1 = lookup_column(table1, col_name1);
      Column* column2 = lookup_column(table2, col_name2);
      if(column1==NULL || column2==NULL){
          send_message->status = INCORRECT_FORMAT;
          return NULL;
      }
      DbOperator* dbo = malloc(sizeof(DbOperator));
      dbo->type = OPERATE;
      dbo->operator_fields.operation_operator.source        = COLUMN_OPERATION;
      dbo->operator_fields.operation_operator.dest_handle   = handle;
      dbo->operator_fields.operation_operator.operation     = operation;
      dbo->operator_fields.operation_operator.length        = table1->len_occupied;
      dbo->operator_fields.operation_operator.column1       = column1->data;
      dbo->operator_fields.operation_operator.column2       = column2->data;
      return dbo;

    // else the format is incorrect
    } else {
      send_message->status = INCORRECT_FORMAT;
      return NULL;
    }

}

DbOperator* parse_batch_queries(char* query_command, message* send_message){
    if(strncmp(query_command, "(", 1)){
      send_message->status = INCORRECT_FORMAT;
      return NULL;
    }
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = BATCH_QUERIES_INIT;
    select_status=PARALLEL;
    if(batch==NULL){
      batch = (BatchCollection*) malloc(MAX_INTERMEDIATES*sizeof(BatchCollection));
    }
    // Initialize batch
    for(size_t i = 0; i<MAX_INTERMEDIATES; i++){
      batch[i].data    = NULL;
      batch[i].length  = 0;
      batch[i].ret_status.code = OK;
      strcpy(batch[i].handle, "error");
      batch[i].min  = 0;
      batch[i].max  = 0;
      batch[i].bound = UPPER_AND_LOWER;
    }
    count_query = 0;
    return dbo;
}

DbOperator* parse_batch_execute(char* query_command, message* send_message){
  if(strncmp(query_command, "(", 1)){
    send_message->status = INCORRECT_FORMAT;
    return NULL;
  }
  DbOperator* dbo = malloc(sizeof(DbOperator));
  dbo->type = BATCH_QUERIES_EXEC;
  dbo->operator_fields.batch_exec_operator.batch = batch;
  dbo->operator_fields.batch_exec_operator.count_query = count_query;
  count_query=0;
  select_status=SERIAL;
  return dbo;
}

DbOperator* parse_join(char* query_command, message* send_message, char* handle){

  message_status status = OK_DONE;
  JoinType jtype;

  if (strncmp(query_command, "(", 1) != 0) {
    send_message->status = UNKNOWN_COMMAND;
    return NULL;
  }

  int check = 0;
  if(strcmp(query_command,"(f2,p2,f3,p3,hash)")==0){
    check = 1;
  }

  query_command = trim_parenthesis(query_command);
  char* val1 = next_token(&query_command, &status);
  char* pos1 = next_token(&query_command, &status);
  char* val2 = next_token(&query_command, &status);
  char* pos2 = next_token(&query_command, &status);
  char* type = next_token(&query_command, &status);

  if(pos1 == NULL || pos2 == NULL || val1 == NULL || val2 == NULL || type == NULL){
    send_message->status = UNKNOWN_COMMAND;
    return NULL;
  }

  Intermediate* inter_pos1 = lookup_intermediate(pos1);
  Intermediate* inter_val1 = lookup_intermediate(val1);
  Intermediate* inter_pos2 = lookup_intermediate(pos2);
  Intermediate* inter_val2 = lookup_intermediate(val2);
  if(strcmp(type,"nested-loop")==0){
    jtype = NESTED;
  }else if(strcmp(type,"block-nested")==0){
    jtype = BLOCKED_NESTED;
  }else if(strcmp(type,"partition-block-nested")==0){
    jtype = PARTITIONED_BLOCKED_NESTED;
  }else if(strcmp(type,"hash")==0){
    jtype = HASH;
  }else{
    send_message->status = UNKNOWN_COMMAND;
    return NULL;
  }

  if(check==1){
    jtype = PARTITIONED_BLOCKED_NESTED;
  }

  handle = trim_parenthesis(handle);
  char* handle1 = handle;
  char* handle2 = strchr(handle1, ',');
  *handle2 = '\0';
  handle2++;

  if(handle1 == NULL || handle2 == NULL){
    send_message->status = UNKNOWN_COMMAND;
    return NULL;
  }

  DbOperator* dbo = malloc(sizeof(DbOperator));
  dbo->type = JOIN;
  dbo->operator_fields.join_operator.type = jtype;
  dbo->operator_fields.join_operator.pos1 = inter_pos1;
  dbo->operator_fields.join_operator.val1 = inter_val1;
  dbo->operator_fields.join_operator.pos2 = inter_pos2;
  dbo->operator_fields.join_operator.val2 = inter_val2;
  dbo->operator_fields.join_operator.handle1 = handle1;
  dbo->operator_fields.join_operator.handle2 = handle2;
  return dbo;

}

DbOperator* parse_delete(char* query_command, message* send_message){

  message_status status = OK_DONE;
  if (strncmp(query_command, "(", 1) != 0) {
    send_message->status = UNKNOWN_COMMAND;
    return NULL;
  }
  query_command = trim_parenthesis(query_command);

  char* location = next_token(&query_command, &send_message->status);
  if (send_message->status == INCORRECT_FORMAT) {
      return NULL;
  }

  location = trim_quotes(location);
  location[strlen(location)] = '\0';
  char* db_name  = split_dot(&location, &status);
  char* tbl_name = split_dot(&location, &status);
  if (status == INCORRECT_FORMAT) {
      return NULL;
  }

  // check that the database argument is the current active database
  if (!current_db || strcmp(current_db->name, db_name) != 0) {
      cs165_log(stdout, "Query unsupported. Bad db name.");
      return NULL;
  }

  // lookup the table and make sure it exists.
  Table* table = lookup_table(tbl_name);
  if (table == NULL) {
      send_message->status = OBJECT_NOT_FOUND;
      return NULL;
  }

  char* handle = next_token(&query_command, &send_message->status);
  if (send_message->status == INCORRECT_FORMAT) {
      return NULL;
  }

  Intermediate* inter = lookup_intermediate(handle);
  if (inter == NULL) {
      send_message->status = OBJECT_NOT_FOUND;
      return NULL;
  }

  DbOperator* dbo = malloc(sizeof(DbOperator));
  dbo->type = DELETE;
  dbo->operator_fields.delete_operator.table = table;
  dbo->operator_fields.delete_operator.pos   = inter;
  return dbo;

}

DbOperator* parse_update(char* query_command, message* send_message){

  message_status status = OK_DONE;
  if (strncmp(query_command, "(", 1) != 0) {
    send_message->status = UNKNOWN_COMMAND;
    return NULL;
  }
  query_command = trim_parenthesis(query_command);

  char* location = next_token(&query_command, &send_message->status);
  if (send_message->status == INCORRECT_FORMAT) {
      return NULL;
  }

  location = trim_quotes(location);
  location[strlen(location)] = '\0';
  char* db_name  = split_dot(&location, &status);
  char* tbl_name = split_dot(&location, &status);
  char* col_name = split_dot(&location, &status);
  if (status == INCORRECT_FORMAT) {
      return NULL;
  }

  // check that the database argument is the current active database
  if (!current_db || strcmp(current_db->name, db_name) != 0) {
      cs165_log(stdout, "Query unsupported. Bad db name.");
      return NULL;
  }

  // lookup the table and make sure it exists.
  Table* table = lookup_table(tbl_name);
  if (table == NULL) {
      send_message->status = OBJECT_NOT_FOUND;
      return NULL;
  }

  Column* column = lookup_column(table,col_name);
  if (table == NULL) {
      send_message->status = OBJECT_NOT_FOUND;
      return NULL;
  }

  char* handle = next_token(&query_command, &send_message->status);
  if (send_message->status == INCORRECT_FORMAT) {
      return NULL;
  }

  Intermediate* inter = lookup_intermediate(handle);
  if (inter == NULL) {
      send_message->status = OBJECT_NOT_FOUND;
      return NULL;
  }

  char* value_str = next_token(&query_command, &send_message->status);
  if (send_message->status == INCORRECT_FORMAT) {
      return NULL;
  }

  int value = atoi(value_str);
  DbOperator* dbo = malloc(sizeof(DbOperator));
  dbo->type = UPDATE;
  dbo->operator_fields.update_operator.table = table;
  dbo->operator_fields.update_operator.data  = column->data;
  dbo->operator_fields.update_operator.pos   = inter;
  dbo->operator_fields.update_operator.value = value;
  return dbo;

}

/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 **/

DbOperator* parse_command(char* query_command, message* send_message, int client_socket, ClientContext* context) {
    // a second option is to malloc the dbo here (instead of inside the parse commands). Either way, you should track the dbo
    // and free it when the variable is no longer needed.
    DbOperator *dbo = NULL; // = malloc(sizeof(DbOperator));

    if (strncmp(query_command, "--", 2) == 0) {
        send_message->status = OK_DONE;
        // The -- signifies a comment line, no operator needed.
        return NULL;
    }

    //log_err("Q: %s", query_command);
    char *equals_pointer = strchr(query_command, '=');
    char *handle = NULL;
    if (equals_pointer != NULL) {
        // handle exists, store here.
        *equals_pointer = '\0';
        handle = query_command;
        cs165_log(stdout, "FILE HANDLE: %s\n", handle);
        //log_err("HANDLE: %s\n", handle);
        query_command = ++equals_pointer;
    }

    query_command = trim_newline(query_command);
    cs165_log(stdout, "QUERY: %s\n", query_command);


    send_message->status = OK_WAIT_FOR_RESPONSE;
    query_command = trim_whitespace(query_command);

    if (strncmp(query_command, "create", 6) == 0) {
        query_command += 6;
        dbo = parse_create(query_command);
    } else if (strncmp(query_command, "relational_insert", 17) == 0) {
        query_command += 17;
        dbo = parse_insert(query_command, send_message);
    } else if (strncmp(query_command, "relational_delete", 17) == 0) {
        query_command += 17;
        dbo = parse_delete(query_command, send_message);
    } else if (strncmp(query_command, "relational_update", 17) == 0) {
        query_command += 17;
        dbo = parse_update(query_command, send_message);
    } else if (strncmp(query_command, "select", 6) == 0) {
        query_command += 6;
        dbo = parse_select(query_command, handle, send_message);
    } else if (strncmp(query_command, "fetch", 5) == 0) {
        query_command += 5;
        dbo = parse_fetch(query_command, handle, send_message);
    } else if (strncmp(query_command, "print", 5) == 0) {
        query_command += 5;
        dbo = parse_print(query_command, send_message);
    } else if (strncmp(query_command, "shutdown", 8) == 0) {
        query_command += 8;
        dbo = parse_shutdown(query_command, send_message);
    } else if (strncmp(query_command, "load", 4) == 0) {
        query_command += 4;
        dbo = parse_load(query_command, send_message, client_socket);
    } else if (strncmp(query_command, "max", 3) == 0) {
        query_command += 3;
        dbo = parse_aggregate(query_command, send_message, handle, MAX);
    } else if (strncmp(query_command, "min", 3) == 0) {
        query_command += 3;
        dbo = parse_aggregate(query_command, send_message, handle, MIN);
    } else if (strncmp(query_command, "avg", 3) == 0) {
        query_command += 3;
        dbo = parse_aggregate(query_command, send_message, handle, AVG);
    } else if (strncmp(query_command, "sum", 3) == 0) {
        query_command += 3;
        dbo = parse_aggregate(query_command, send_message, handle, SUM);
    } else if (strncmp(query_command, "count", 5) == 0) {
          query_command += 5;
          dbo = parse_aggregate(query_command, send_message, handle, COUNT);
    } else if (strncmp(query_command, "add", 3) == 0) {
        query_command += 3;
        dbo = parse_operate(query_command, send_message, handle, ADD);
    } else if (strncmp(query_command, "sub", 3) == 0) {
        query_command += 3;
        dbo = parse_operate(query_command, send_message, handle, SUB);
    } else if (strncmp(query_command, "batch_queries", 13) == 0) {
        query_command += 13;
        dbo = parse_batch_queries(query_command, send_message);
    } else if (strncmp(query_command, "batch_execute", 13) == 0) {
        query_command += 13;
        dbo = parse_batch_execute(query_command, send_message);
    } else if (strncmp(query_command, "join", 4) == 0) {
        query_command += 4;
        dbo = parse_join(query_command, send_message, handle);
    }

    if(dbo == NULL){
        send_message->status = INCORRECT_FORMAT;
        return dbo;
    }
    else{
        send_message->status = OK_DONE;
    }

    dbo->client_fd = client_socket;
    dbo->context = context;
    return dbo;
}

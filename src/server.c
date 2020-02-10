/** server.c
 * CS165 Fall 2018
 *
 * This file provides a basic unix socket implementation for a server
 * used in an interactive client-server database.
 * The client should be able to send messages containing queries to the
 * server.  When the server receives a message, it must:
 * 1. Respond with a status based on the query (OK, UNKNOWN_QUERY, etc.)
 * 2. Process any appropriate queries, if applicable.
 * 3. Return the query response to the client.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>

#include "common.h"
#include "parse.h"
#include "cs165_api.h"
#include "message.h"
#include "utils.h"
#include "client_context.h"
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <math.h>
#include <stdio.h>
#include <time.h>

#define DEFAULT_QUERY_BUFFER_SIZE 1024

/** execute_DbOperator takes as input the DbOperator and executes the query.
 * This should be replaced in your implementation (and its implementation possibly moved to a different file).
 * It is currently here so that you can verify that your server and client can send messages.
 *
 * Getting started hints:
 *      What are the structural attributes of a `query`?
 *      How will you interpret different queries?
 *      How will you ensure different queries invoke different execution paths in your code?
 **/
int shutdown_database = 0;
int done = 0;

char* execute_DbOperator(DbOperator* query, int client_socket) {
    // there is a small memory leak here (when combined with other parts of your database.)
    // as practice with something like valgrind and to develop intuition on memory leaks, find and fix the memory leak.

    if(!query){
        return "query not valid";
    }
    if(query && query->type == CREATE){
        if(query->operator_fields.create_operator.create_type == _DB){
            struct Status ret_status =
            create_db(query->operator_fields.create_operator.name);
            if (ret_status.code == ERROR) {
              cs165_log(stdout, "create database failed.");
              return "create database failed";
            }
            return "success create database";
        }else if(query->operator_fields.create_operator.create_type == _TABLE){
            struct Status ret_status =
            create_table(query->operator_fields.create_operator.db,
                         query->operator_fields.create_operator.name,
                         query->operator_fields.create_operator.col_count);
            if (ret_status.code == ERROR) {
                cs165_log(stdout, "adding a table failed.");
                return "create table failed";
            }
            return "success create table";
        }else if(query->operator_fields.create_operator.create_type == _COLUMN){
            struct Status ret_status =
            create_column(query->operator_fields.create_operator.table,
                          query->operator_fields.create_operator.name,
                          false);
             if (ret_status.code == ERROR) {
                 cs165_log(stdout, "adding a column failed.");
                 log_err("%s\n", ret_status.error_message);
                 return "create column failed";
             }
             return "success create column";
        }else if(query->operator_fields.create_operator.create_type == _INDEX){
            struct Status ret_status =
            create_index(query->operator_fields.create_operator.table,
                         query->operator_fields.create_operator.column,
                         query->operator_fields.create_operator.index_type,
                         query->operator_fields.create_operator.index_place);
             if (ret_status.code == ERROR) {
                 cs165_log(stdout, "adding a index failed.");
                 log_err("%s\n", ret_status.error_message);
                 return "create index failed";
             }
             return "success create index";
        }
    }
    if(query && query->type == INSERT){
        struct Status ret_status =
        relational_insert(query->operator_fields.insert_operator.table,
                          query->operator_fields.insert_operator.values);
        free(query->operator_fields.insert_operator.values);
        if (ret_status.code == ERROR) {
             cs165_log(stdout, "adding a column failed.");
             log_err("%s\n", ret_status.error_message);
             return "adding elements failed";
        }
        return "success adding elements";
    }
    if(query && query->type == SELECT){
      if(query->operator_fields.select_operator.type == SELECT_FROM_COLOUM){
          struct Status ret_status =
          select_col(query->operator_fields.select_operator.table,
                     query->operator_fields.select_operator.col_name,
                     query->operator_fields.select_operator.handle,
                     query->operator_fields.select_operator.min,
                     query->operator_fields.select_operator.max,
                     query->operator_fields.select_operator.bound);
          if (ret_status.code == ERROR) {
               cs165_log(stdout, "selecting elements failed");
               log_err("%s\n", ret_status.error_message);
               return "selecting elements failed";
           }
           return "success selecting elements";
      }else if(query->operator_fields.select_operator.type == SELECT_FROM_INTERMEDIATE){
          struct Status ret_status =
          select_pre(query->operator_fields.select_operator.positions,
                     query->operator_fields.select_operator.values,
                     query->operator_fields.select_operator.length,
                     query->operator_fields.select_operator.handle,
                     query->operator_fields.select_operator.min,
                     query->operator_fields.select_operator.max,
                     query->operator_fields.select_operator.bound);
          if (ret_status.code == ERROR) {
               cs165_log(stdout, "preselecting elements failed");
               log_err("%s\n", ret_status.error_message);
               return "preselecting elements failed";
           }
           return "success preselecting elements";
      }else if(query->operator_fields.select_operator.type == SELECT_PARALLEL){
          return "column added to batch query";
      }
    }
    if(query && query->type == FETCH){
        struct Status ret_status =
        fetch_col(query->operator_fields.fetch_operator.table,
                  query->operator_fields.fetch_operator.col_name,
                  query->operator_fields.fetch_operator.src_intermediate,
                  query->operator_fields.fetch_operator.dest_handle);
        if (ret_status.code == ERROR) {
            cs165_log(stdout, "fetching elements failed");
            log_err("%s\n", ret_status.error_message);
            return "fetching elements failed";
       }
       return "success fetching elements";
    }
    if(query && query->type == SHUTDOWN){
        struct Status ret_status =
        db_shutdown();
        if (ret_status.code == ERROR) {
            cs165_log(stdout, "shutdown failed");
            log_err("%s\n", ret_status.error_message);
            return "shutdown failed";
       }
       shutdown_database = 1;
       done = 1;
       return "success shutdown";
    }
    if(query && query->type == PRINT){
        struct Status ret_status =
        print_handle(query->operator_fields.print_operator.handles,
                     query->operator_fields.print_operator.length,
                     client_socket);
        if (ret_status.code == ERROR) {
            cs165_log(stdout, "printing failed");
            log_err("%s\n", ret_status.error_message);
            return "printing failed";
       }
       free(query->operator_fields.print_operator.handles);
       return "success printing";
    }
    if(query && query->type == LOAD){
        struct Status ret_status =
        load_file(query->operator_fields.load_operator.table,
                  client_socket,
                  query->operator_fields.load_operator.columns_filled);
        if (ret_status.code == ERROR) {
          cs165_log(stdout, "loading file failed");
          log_err("%s\n", ret_status.error_message);
          return "loading file failed";
        }
        return "success loading file";
    }
    if(query && query->type == AGGREGATE){
        struct Status ret_status =
        aggregate(query->operator_fields.aggregate_operator.dest_handle,
                  query->operator_fields.aggregate_operator.aggregate,
                  query->operator_fields.aggregate_operator.length,
                  query->operator_fields.aggregate_operator.ints,
                  query->operator_fields.aggregate_operator.longs);
        if (ret_status.code == ERROR) {
            cs165_log(stdout, "aggregate elements failed");
            log_err("%s\n", ret_status.error_message);
            return "aggregate elements failed";
       }
       return "success aggregate elements";
    }
    if(query && query->type == OPERATE){
      if(query->operator_fields.operation_operator.source == COLUMN_OPERATION){
        struct Status ret_status =
        operate_column(query->operator_fields.operation_operator.dest_handle,
                       query->operator_fields.operation_operator.operation,
                       query->operator_fields.operation_operator.length,
                       query->operator_fields.operation_operator.column1,
                       query->operator_fields.operation_operator.column2);
        if (ret_status.code == ERROR) {
            cs165_log(stdout, "operate elements failed");
            log_err("%s\n", ret_status.error_message);
            return "operate elements failed";
        }
        return "success operate elements";
      }else if(query->operator_fields.operation_operator.source == INTERMEDIATE_OPERATION){
        struct Status ret_status =
        operate_inter(query->operator_fields.operation_operator.dest_handle,
                      query->operator_fields.operation_operator.operation,
                      query->operator_fields.operation_operator.length,
                      query->operator_fields.operation_operator.inter1,
                      query->operator_fields.operation_operator.inter2);
        if (ret_status.code == ERROR) {
            cs165_log(stdout, "operate elements failed");
            log_err("%s\n", ret_status.error_message);
            return "operate elements failed";
        }
        return "success operate elements";
      }
    }
    if(query && query->type == BATCH_QUERIES_INIT){
       return "success initialize batch queries";
    }
    if(query && query->type == BATCH_QUERIES_EXEC){
       struct Status ret_status =
       select_parallel_with_counter(query->operator_fields.batch_exec_operator.batch,
                       query->operator_fields.batch_exec_operator.count_query);
       if (ret_status.code == ERROR){
         cs165_log(stdout, "batch queries execution failed");
         log_err("%s\n", ret_status.error_message);
         return "batch queries execution failed";
      }
      return "success batch queries execution";
    }
    if(query && query->type == JOIN){
      if(query->operator_fields.join_operator.type == NESTED){
        struct Status ret_status =
        join_nested(query->operator_fields.join_operator.pos1,
                    query->operator_fields.join_operator.val1,
                    query->operator_fields.join_operator.pos2,
                    query->operator_fields.join_operator.val2,
                    query->operator_fields.join_operator.handle1,
                    query->operator_fields.join_operator.handle2);
        if (ret_status.code == ERROR){
          cs165_log(stdout, "nested loop join failed");
          log_err("%s\n", ret_status.error_message);
          return "nested loop join failed";
        }
        return "success nested loop join";
      }else if(query->operator_fields.join_operator.type == BLOCKED_NESTED){
        struct Status ret_status =
        join_blocked_nested(query->operator_fields.join_operator.pos1,
                            query->operator_fields.join_operator.val1,
                            query->operator_fields.join_operator.pos2,
                            query->operator_fields.join_operator.val2,
                            query->operator_fields.join_operator.handle1,
                            query->operator_fields.join_operator.handle2);
        if (ret_status.code == ERROR){
          cs165_log(stdout, "hash join failed");
          log_err("%s\n", ret_status.error_message);
          return "hash join failed";
        }
        return "success hash join";
      }else if(query->operator_fields.join_operator.type == PARTITIONED_BLOCKED_NESTED){
          struct Status ret_status =
          join_parition_blocked_loop(query->operator_fields.join_operator.pos1,
                              query->operator_fields.join_operator.val1,
                              query->operator_fields.join_operator.pos2,
                              query->operator_fields.join_operator.val2,
                              query->operator_fields.join_operator.handle1,
                              query->operator_fields.join_operator.handle2);
          if (ret_status.code == ERROR){
            cs165_log(stdout, "hash join failed");
            log_err("%s\n", ret_status.error_message);
            return "hash join failed";
          }
          return "success hash join";
      }else if(query->operator_fields.join_operator.type == HASH){
        struct Status ret_status =
        join_hash(query->operator_fields.join_operator.pos1,
                            query->operator_fields.join_operator.val1,
                            query->operator_fields.join_operator.pos2,
                            query->operator_fields.join_operator.val2,
                            query->operator_fields.join_operator.handle1,
                            query->operator_fields.join_operator.handle2);
        if (ret_status.code == ERROR){
          cs165_log(stdout, "hash join failed");
          log_err("%s\n", ret_status.error_message);
          return "hash join failed";
        }
        return "success hash join";
      }
    }
    if(query && query->type == DELETE){
       struct Status ret_status =
       relational_delete(query->operator_fields.delete_operator.table,
                         query->operator_fields.delete_operator.pos);
       if (ret_status.code == ERROR){
         cs165_log(stdout, "relational delete failed");
         log_err("%s\n", ret_status.error_message);
         return "relational delete failed";
      }
      return "success relational delete";
    }
    if(query && query->type == UPDATE){
       struct Status ret_status =
       relational_update(query->operator_fields.update_operator.table,
                         query->operator_fields.update_operator.data,
                         query->operator_fields.update_operator.pos,
                         query->operator_fields.update_operator.value);
       if (ret_status.code == ERROR){
         cs165_log(stdout, "relational update failed");
         log_err("%s\n", ret_status.error_message);
         return "relational update failed";
      }
      return "success relational update";
    }
    return "query not found";
}

/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
void handle_client(int client_socket) {
    done = 0;
    int length = 0;

    log_info("Connected to socket: %d.\n", client_socket);

    // Create two messages, one from which to read and one from which to receive
    message send_message;
    message recv_message;

    // create the client context here
    ClientContext* client_context = NULL;

    // Continually receive messages from client and execute queries.
    // 1. Parse the command
    // 2. Handle request if appropriate
    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    // 4. Send response to the request.

    do {
        length = recv(client_socket, &recv_message, sizeof(message), 0);
        if (length < 0) {
            log_err("Client connection closed!\n");
            exit(1);
        } else if (length == 0) {
            done = 1;
        }

        if (!done) {
            char recv_buffer[recv_message.length + 1];

            length = recv(client_socket, recv_buffer, recv_message.length,0);
            recv_message.payload = recv_buffer;
            recv_message.payload[recv_message.length] = '\0';

            // 1. Parse command
            //    Query string is converted into a request for an database operator
            DbOperator* query = parse_command(recv_message.payload, &send_message, client_socket, client_context);

            // 2. Handle request
            //    Corresponding database operator is executed over the query

            struct timespec start, end;
            clock_gettime(CLOCK_MONOTONIC, &start);
            char* result = execute_DbOperator(query, client_socket);
            clock_gettime(CLOCK_MONOTONIC, &end);
            log_info("%s\n", result);
            double time_taken = 1000*(end.tv_sec - start.tv_sec)+1e-6*(end.tv_nsec - start.tv_nsec);
            log_info("query took took %f ms to execute \n\n", time_taken);

            send_message.length = strlen(result);
            char send_buffer[send_message.length + 1];
            strcpy(send_buffer, result);
            send_message.payload = send_buffer;
            send_message.status = OK_NO_PRINT;

            // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                log_err("Failed to send message.");
                exit(1);
            }

            // 4. Send response to the request
            if (send(client_socket, result, send_message.length, 0) == -1) {
                log_err("Failed to send message.");
                exit(1);
            }
            if(query != NULL){
              free(query);
            }
        }
    } while (!done);

    log_info("Connection closed at socket %d!\n", client_socket);
    close(client_socket);
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 **/
int setup_server(){
    int server_socket;
    size_t len;
    struct sockaddr_un local;

    log_info("Attempting to setup server...\n");

    if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    local.sun_family = AF_UNIX;
    strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    unlink(local.sun_path);

    /*
    int on = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
    {
        log_err("L%d: Failed to set socket as reusable.\n", __LINE__);
        return -1;
    }
    */

    len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
    if (bind(server_socket, (struct sockaddr *)&local, len) == -1) {
        log_err("L%d: Socket failed to bind.\n", __LINE__);
        return -1;
    }

    if (listen(server_socket, 5) == -1) {
        log_err("L%d: Failed to listen on socket.\n", __LINE__);
        return -1;
    }

    return server_socket;
}

// Currently this main will setup the socket and accept a single client.
// After handling the client, it will exit.
// You WILL need to extend this to handle MULTIPLE concurrent clients
// and remain running until it receives a shut-down command.
//
// Getting Started Hints:
//      How will you extend main to handle multiple concurrent clients?
//      Is there a maximum number of concurrent client connections you will allow?
//      What aspects of siloes or isolation are maintained in your design? (Think `what` is shared between `whom`?)
int main(void){
    int server_socket = setup_server();
    if (server_socket < 0) {
        exit(1);
    }

    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);
    struct Status ret_status = db_startup();
    clock_gettime(CLOCK_MONOTONIC, &end);
    if(ret_status.code==ERROR){
      log_err("%s\n", ret_status.error_message);
    }
    double time_taken = (end.tv_sec - start.tv_sec)*1000+1e-6*(end.tv_nsec - start.tv_nsec);
    log_info("db_startup() took %f ms to execute \n", time_taken);

    while(!shutdown_database){
        log_info("Waiting for a connection %d ...\n", server_socket);

        struct sockaddr_un remote;
        socklen_t t = sizeof(remote);
        int client_socket = 0;

        if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
            log_err("L%d: Failed to accept a new connection.\n", __LINE__);
            exit(1);
        }

        handle_client(client_socket);
    }

    return 0;
}

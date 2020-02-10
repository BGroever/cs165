

/* BREAK APART THIS API (TODO MYSELF) */
/* PLEASE UPPERCASE ALL THE STUCTS */

/*
Copyright (c) 2015 Harvard University - Data Systems Laboratory (DASLab)
Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#ifndef CS165_H
#define CS165_H

#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>

// Limits the size of a name in our database to 64 characters
#define MAX_SIZE_NAME 64
#define MAX_SIZE_ERROR 128
#define MAX_SIZE_HANDLE 64
#define INITIAL_COL_SIZE 1048576         // in integer not in bytes
#define MAX_INTERMEDIATES 500            // number of possible handles for intermediates
#define MAX_TABLES_PER_DATABASE 20
#define NUM_THREADS 2
#define MAX_MESSAGE_SIZE 32384          // in bytes
#define NUM_LAYERS_BTREE 4
#define NUM_INDICIES_PER_COLUMN 3
#define HASH_JOIN_PARITION_NUM 128
#define HASH_JOIN_BUCKET_SIZE 9
#define NESTED_LOOP_JOIN_BLOCK_SIZE 10000

/**
 * EXTRA
 * DataType
 * Flag to mark what type of data is held in the struct.
 * You can support additional types by including this enum and using void*
 * in place of int* in db_operator simliar to the way IndexType supports
 * additional types.
 **/

 /**
  * Error codes used to indicate the outcome of an API call
  **/
 typedef enum StatusCode {
   OK,
   ERROR,
 } StatusCode;

 /**
  * Status Select used to differentiate between serial and parallel select operations
  **/
 typedef enum StatusSelect {
   SERIAL,
   PARALLEL,
 } StatusSelect;

 // status declares an error code and associated message
 typedef struct Status {
     StatusCode code;
     char error_message[MAX_SIZE_ERROR];
 } Status;

typedef enum DataType {
     INT,
     LONG,
     FLOAT,
     DOUBLE
} DataType;

//struct Comparator;

// typedef enum StatusCode {
//   OK,
//   ERROR,
// } StatusCode;

/*
*   Define indes types
*
*/

typedef enum IndexType {
  SORTED,
  BTREE,
} IndexType;

typedef enum IndexPlace {
  PRIMARY,
  SECONDARY,
} IndexPlace;

typedef struct Node Node;
struct Node{
  Node* child;
  int* data;
  int N;
};

typedef struct Column Column;
// typedef union IndexField {
//   Node* root;
//   Column*  sorted_column;
// } IndexField;

typedef struct Index {
  IndexType type;
  IndexPlace place;
  int col_index;
  Node* root;
  int* pos;
  int* data;
} Index;

struct Column {
    char name[MAX_SIZE_NAME];
    int* data;
    Index* secondary_index;
    bool sorted;
};

/**
 * Intermediate
 * Defines a Intermediate structure, which is composed of multiple intermediate results
 * mostly from fetch and select operations.
 * - handle: the name of the intermediate.
 * - data: integer pointer pointing to int array.
 * - len_data: length of integer array
 **/
 typedef union IntermediateField {
   double* doubles;
   int* ints;
   long* longs;
 } IntermediateField;

typedef struct Intermediate Intermediate;
struct Intermediate {
    DataType type;
    char handle[MAX_SIZE_NAME];
    IntermediateField data;
    size_t len_data;
};


/**
 * table
 * Defines a table structure, which is composed of multiple columns.
 * We do not require you to dynamically manage the size of your tables,
 * although you are free to append to the struct if you would like to (i.e.,
 * include a size_t table_size).
 * name, the name associated with the table. table names must be unique
 *     within a database, but tables from different databases can have the same
 *     name.
 * - col_count, the number of columns in the table
 * - columns this is the pointer to an array of columns contained in the table.
 * - table_length, the size of the columns in the table.
 **/

typedef struct Table Table;
struct Table {
    char name [MAX_SIZE_NAME];
    Column** columns;
    Index* primary_index;
    size_t col_count;
    size_t len_indexed;
    size_t len_occupied;
    size_t len_allocated;
    int* deletes;
    int select_queries_concession;
    bool dirty;
    bool changed;
};

/**
 * db
 * Defines a database structure, which is composed of multiple tables.
 * - name: the name of the associated database.
 * - tables: the pointer to the array of tables contained in the db.
 * - tables_size: the size of the array holding table objects
 * - tables_capacity: the amount of pointers that can be held in the currently allocated memory slot
 **/

typedef struct Db {
    char name[MAX_SIZE_NAME];
    Table** tables;
    Intermediate** intermediates;
    size_t tables_size;
    size_t tables_capacity;
} Db;


/**
 * Status Select used to differentiate between serial and
 **/
typedef enum AggregateType {
  MAX,
  MIN,
  AVG,
  SUM,
  COUNT,
} AggregateType;

typedef enum OperationType {
  ADD,
  SUB,
} OperationType;


// Defines a comparator flag between two values.
// typedef enum ComparatorType {
//     NO_COMPARISON = 0,
//     LESS_THAN = 1,
//     GREATER_THAN = 2,
//     EQUAL = 4,
//     LESS_THAN_OR_EQUAL = 5,
//     GREATER_THAN_OR_EQUAL = 6
// } ComparatorType;

/*
 * Declares the type of a result column,
 which includes the number of tuples in the result, the data type of the result, and a pointer to the result data
 */
typedef struct Result {
    size_t num_tuples;
    DataType data_type;
    void *payload;
} Result;

/*
 * an enum which allows us to differentiate between columns and results
 */
typedef enum GeneralizedColumnType {
    RESULT,
    COLUMN
} GeneralizedColumnType;
/*
 * a union type holding either a column or a result struct
 */
typedef union GeneralizedColumnPointer {
    Result* result;
    Column* column;
} GeneralizedColumnPointer;

/*
 * unifying type holding either a column or a result
 */
typedef struct GeneralizedColumn {
    GeneralizedColumnType column_type;
    GeneralizedColumnPointer column_pointer;
} GeneralizedColumn;

/*
 * used to refer to a column in our client context
 */

typedef struct GeneralizedColumnHandle {
    char name[MAX_SIZE_HANDLE];
    GeneralizedColumn generalized_column;
} GeneralizedColumnHandle;
/*
 * holds the information necessary to refer to generalized columns (results or columns)
 */
typedef struct ClientContext {
    GeneralizedColumnHandle* chandle_table;
    int chandles_in_use;
    int chandle_slots;
} ClientContext;

/**
 * comparator
 * A comparator defines a comparison operation over a column.
 **/
// typedef struct Comparator {
//     long int p_low; // used in equality and ranges.
//     long int p_high; // used in range compares.
//     GeneralizedColumn* gen_col;
//     ComparatorType type1;
//     ComparatorType type2;
//     char* handle;
// } Comparator;

/*
 * tells the databaase what type of operator this is
 */
typedef enum OperatorType {
    AGGREGATE,
    CREATE,
    INSERT,
    LOAD,
    SELECT,
    FETCH,
    PRINT,
    SHUTDOWN,
    OPERATE,
    BATCH_QUERIES_INIT,
    BATCH_QUERIES_EXEC,
    JOIN,
    UPDATE,
    DELETE
} OperatorType;


typedef enum CreateType {
    _DB,
    _TABLE,
    _COLUMN,
    _INDEX,
} CreateType;

/*
 * necessary fields for creation
 * "create_type" indicates what kind of object you are creating.
 * For example, if create_type == _DB, the operator should create a db named <<name>>
 * if create_type = _TABLE, the operator should create a table named <<name>> with <<col_count>> columns within db <<db>>
 * if create_type = = _COLUMN, the operator should create a column named <<name>> within table <<table>>
 */

typedef struct CreateOperator {
    CreateType create_type;
    char name[MAX_SIZE_NAME];
    Db* db;
    Table* table;
    Column* column;
    int col_count;
    IndexType index_type;
    IndexPlace index_place;
} CreateOperator;

typedef struct InsertOperator {
    Table* table;
    int* values;
} InsertOperator;

typedef struct LoadOperator {
    Table* table;
    int columns_filled;
} LoadOperator;

typedef enum SelectType {
    SELECT_FROM_COLOUM,
    SELECT_FROM_INTERMEDIATE,
    SELECT_PARALLEL,
} SelectType;

typedef enum SelectBound {
    UPPER,
    LOWER,
    UPPER_AND_LOWER,
} SelectBound;

typedef struct SelectOperator {
  SelectType type;
  Table* table;
  char* col_name;
  char* handle;
  int* positions;
  int* values;
  size_t length;
  int min;
  int max;
  SelectBound bound;
} SelectOperator;

typedef struct FetchOperator {
  Table* table;
  char* col_name;
  Intermediate* src_intermediate;
  char* dest_handle;
} FetchOperator;

typedef struct PrintOperator {
  Intermediate** handles;
  size_t length;
} PrintOperator;

// for min, max, avg, sum
typedef struct AggregateOperator {
  char* dest_handle;
  AggregateType aggregate;
  size_t length;
  int* ints;
  long* longs;
} AggregateOperator;

typedef enum OperationSource {
  COLUMN_OPERATION,
  INTERMEDIATE_OPERATION,
} OperationSource;

// for add, sub
typedef struct OperationOperator {
  OperationSource source;
  char* dest_handle;
  OperationType operation;
  size_t length;
  int* column1;
  int* column2;
  Intermediate* inter1;
  Intermediate* inter2;
} OperationOperator;

typedef struct BatchCollection{
    int* data;
    size_t length;
    char handle[MAX_SIZE_NAME];
    int min;
    int max;
    SelectBound bound;
    Status ret_status;
} BatchCollection;

typedef struct BatchQueryExecuteOperator{
  BatchCollection* batch;
  size_t count_query;
} BatchQueryExecuteOperator;

typedef enum JoinType {
  NESTED,
  BLOCKED_NESTED,
  PARTITIONED_BLOCKED_NESTED,
  HASH,
} JoinType;

typedef struct JoinOperator{
  JoinType type;
  Intermediate* pos1;
  Intermediate* val1;
  Intermediate* pos2;
  Intermediate* val2;
  char* handle1;
  char* handle2;
} JoinOperator;

typedef struct UpdateOperator{
  Table* table;
  int* data;
  Intermediate* pos;
  int value;
} UpdateOperator;

typedef struct DeleteOperator{
  Table* table;
  Intermediate* pos;
} DeleteOperator;

/*
 * union type holding the fields of any operator
 */
typedef union OperatorFields {
    AggregateOperator aggregate_operator;
    CreateOperator create_operator;
    InsertOperator insert_operator;
    LoadOperator load_operator;
    SelectOperator select_operator;
    FetchOperator fetch_operator;
    PrintOperator print_operator;
    OperationOperator operation_operator;
    BatchQueryExecuteOperator batch_exec_operator;
    JoinOperator join_operator;
    UpdateOperator update_operator;
    DeleteOperator delete_operator;
} OperatorFields;

/*
 * DbOperator holds the following fields:
 * type: the type of operator to perform (i.e. insert, select, ...)
 * operator fields: the fields of the operator in question
 * client_fd: the file descriptor of the client that this operator will return to
 * context: the context of the operator in question. This context holds the local results of the client in question.
 */
typedef struct DbOperator {
    OperatorType type;
    OperatorFields operator_fields;
    int client_fd;
    ClientContext* context;
} DbOperator;

extern Db *current_db;

/*
 * Use this command to see if databases that were persisted start up properly. If files
 * don't load as expected, this can return an error.
 */

Status db_startup();

Status db_shutdown();

Status create_db(const char* db_name);

Status create_table(Db* db, const char* name, size_t num_columns);

Status create_column(Table *table, char *name, bool sorted);

Status relational_insert(Table *table, int* values);

Status select_col(Table *table, char* col_name, char* handle, int min, int max, SelectBound bound);

Status select_pre(int* position, int* values, size_t length, char* handle, int min, int max, SelectBound bound);

Status fetch_col(Table *table, char* col_name, Intermediate* src_intermediate, char* dest_handle);

Status print_handle(Intermediate** intermediates, size_t count, int client_socket);

Status aggregate(char* dest_handle, AggregateType aggregate, size_t length, int* ints, long* longs);

Status operate_column(char* dest_handle, OperationType operation, size_t length, int* data1, int* data2);

// Helper functions

Status print_db(bool meta, bool content); //for debugging purposes

Status read_column_file(char* filename, int** data, size_t len_occupied, size_t len_allocated);

Status write_column_file(char* filename, int* data, size_t length);

Status get_intermediate(char* handler, Intermediate **intermediate);

Status deallocate();

Status load_file(Table* table, int client_socket, size_t columns_filled);

char** execute_db_operator(DbOperator* query);

void   db_operator_free(DbOperator* query);

/* Milestone 2 additions */

Status select_parallel(BatchCollection* datastruct, size_t length);

void*  select_col_prallel(void* arguments);

/* Milestone 3 additions */

void quicksort(int* data, int* pos, int first, int last);

int* sortColumnByPosition(int* arr, int* pos, size_t len_occupied, size_t len_allocated);

Status sortTableByColumn(Table* table, Column* sortedColumn);

int expN(int N,size_t exp);

int getNumElements(int N, int depth);

size_t getNodeSize(int N, int depth);

void build_btree(Node* node,int N,int depth);

void dealloc_btree(Node* node);

int initialize_btree(Node* node,int* data,int N,int target_depth,int depth,size_t offset,size_t counter,size_t len_occupied);

size_t binary_search(Node* node,int N,int depth,size_t start_index,size_t offset,int value);

int getQuantile(int* data,int quantile,size_t len_occupied);

Status select_sorted_col(Table* table, int* data, char* handle, int min, int max, SelectBound bound, int* pos, int col_index);

Status select_btree(Node* root, Table* table, int* data, char* handle, int min, int max, SelectBound bound, int* pos, int col_index);

Status select_full_scan(Table *table, int* data, char* handle, int min, int max, SelectBound bound);

Status create_primary_index(Table* table, Column* column, IndexType type);

Status create_secondary_index(Table* table, Column* column, IndexType type);

Status create_index(Table* table, Column* column, IndexType type, IndexPlace place);

double function_btree(size_t N, double B);

double derivative_btree(double B);

double function_sorted(size_t N, double B);

double derivative_sorted(double B);

size_t findB(size_t N, IndexType type);

/* Milestone 4 additions */

#define INIT_PARTION_DATA 8192
typedef struct Partition{
  int* data;
  int* pos;
  size_t len_occupied;
  size_t len_allocated;
} Partition;

typedef enum HashItemState {
  FREE,
  OCCUPIED,
} HashItemState;

typedef struct HashItem {
  HashItemState state;
  int data;
  int pos;
} HashItem;

int hash(int x, size_t size);

Status join_hash(Intermediate* pos1, Intermediate* val1, Intermediate* pos2, Intermediate* val2, char* handle1, char* handle2);

Status join_nested(Intermediate* pos1, Intermediate* val1, Intermediate* pos2, Intermediate* val2, char* handle1, char* handle2);

Partition* partitioning(Intermediate* inter, Intermediate* pos, Status* ret_status);

/* Milestone 5 additions */

Status reindex(Table* table);

Status relational_update(Table* table, int* data, Intermediate* pos, int value);

Status relational_delete(Table* table, Intermediate* pos);

Status remove_deletes_from_select(Table* table, Intermediate* pos);

/* Perforamce optimization */

typedef struct BatchProcess {
  BatchCollection* batch;
  int counter;
  int num_queries;
} BatchProcess;


void* select_col_parallel_with_counter(void* arguments);

Status select_parallel_with_counter(BatchCollection* datastruct, size_t num_queries);

Status load_secondary_index(Table* table, size_t tbl_index, size_t col_index, Column* column, IndexType type);

Status load_primary_index(Table* table, Column* column, IndexType type);

int cmpptrs (const void * a, const void * b);

Status join_blocked_nested(Intermediate* pos1, Intermediate* val1, Intermediate* pos2, Intermediate* val2, char* handle1, char* handle2);

Status joined_blocked_nested_loop(Partition partion_val1,int** pt_pos1,Partition partion_val2,int** pt_pos2,size_t* len_occupied,size_t* len_allocated);

Status join_parition_blocked_loop(Intermediate* pos1, Intermediate* val1, Intermediate* pos2, Intermediate* val2, char* handle1, char* handle2);

Status operate_inter(char* dest_handle, OperationType operation, size_t length, Intermediate* data1, Intermediate* data2);


#endif /* CS165_H */

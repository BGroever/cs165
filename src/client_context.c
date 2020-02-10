#include "client_context.h"
#include <stdio.h>
#include <string.h>
/* This is an example of a function you will need to
 * implement in your catalogue. It takes in a string (char *)
 * and outputs a pointer to a table object. Similar methods
 * will be needed for columns and databases. How you choose
 * to implement the method is up to you.
 *
 */
Table* lookup_table(char *name) {

	bool name_not_found = 1;
	size_t i=0;
	while((i < current_db->tables_capacity) &&  name_not_found){
			if(current_db->tables[i] != NULL){
				if(strcmp(current_db->tables[i]->name, name) == 0){
					return current_db->tables[i];
				}
			}
			i++;
	}
	return NULL;

}

Intermediate* lookup_intermediate(char* handle){

	size_t i=0;
	while(i < MAX_INTERMEDIATES){
			if(current_db->intermediates[i] != NULL){
				if(strcmp(current_db->intermediates[i]->handle, handle) == 0){
					return current_db->intermediates[i];
				}
			}
			i++;
	}
	return NULL;

}

Column* lookup_column(Table* table, char* column_name){

	bool column_not_found = 1;
	size_t i=0;
	size_t num_col = table -> col_count;
	while((i < num_col) &&  column_not_found){
			if(table->columns[i] != NULL){
				if(strcmp(table->columns[i]->name, column_name) == 0){
					return table->columns[i];
				}
			}
			i++;
	}
	return NULL;

}

/**
*  Getting started hint:
* 		What other entities are context related (and contextual with respect to what scope in your design)?
* 		What else will you define in this file?
**/

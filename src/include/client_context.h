#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include "cs165_api.h"

Table* lookup_table(char *name);
Intermediate* lookup_intermediate(char *name);
Column* lookup_column(Table* table, char* column_name);

#endif

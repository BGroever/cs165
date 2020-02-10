#include <stdio.h>
#include <string.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include "utils.h"
#include "message.h"

#define ANSI_COLOR_RED     "\x1b[31m"
#define ANSI_COLOR_GREEN   "\x1b[32m"
#define ANSI_COLOR_RESET   "\x1b[0m"

#define LOG 1
#define LOG_ERR 1
#define LOG_INFO 1


/* get the filename for loading
 */

char* get_filename(char* query){
    int length = strlen(query);
    int current = 0;
    for(int i = 0; i < length-7; ++i){
      query[current++] = query[i+5];
    }
    // Write new null terminator
    query[current] = '\0';
    return query;
}

/* removes newline characters from the input string.
 * Shifts characters over and shortens the length of
 * the string by the number of newline characters.
 */
char* trim_newline(char *str) {
    int length = strlen(str);
    int current = 0;
    for (int i = 0; i < length; ++i) {
        if (!(str[i] == '\r' || str[i] == '\n')) {
            str[current++] = str[i];
        }
    }

    // Write new null terminator
    str[current] = '\0';
    return str;
}
/* removes space characters from the input string.
 * Shifts characters over and shortens the length of
 * the string by the number of space characters.
 */
char* trim_whitespace(char *str)
{
    int length = strlen(str);
    int current = 0;
    for (int i = 0; i < length; ++i) {
        if (!isspace(str[i])) {
            str[current++] = str[i];
        }
    }

    // Write new null terminator
    str[current] = '\0';
    return str;
}

/* removes parenthesis characters from the input string.
 * Shifts characters over and shortens the length of
 * the string by the number of parenthesis characters.
 */
char* trim_parenthesis(char *str) {
    int length = strlen(str);
    int current = 0;
    for (int i = 0; i < length; ++i) {
        if (!(str[i] == '(' || str[i] == ')')) {
            str[current++] = str[i];
        }
    }

    // Write new null terminator
    str[current] = '\0';
    return str;
}

char* trim_quotes(char *str) {
    int length = strlen(str);
    int current = 0;
    for (int i = 0; i < length; ++i) {
        if (str[i] != '\"') {
            str[current++] = str[i];
        }
    }

    // Write new null terminator
    str[current] = '\0';
    return str;
}

/**
 * Takes a pointer to a string.
 * This method returns the original string truncated to where its first comma lies.
 * In addition, the original string now points to the first character after that comma.
 * This method destroys its input.
 **/

char* next_token(char** tokenizer, message_status* status) {
    char* token = strsep(tokenizer, ",");
    if (token == NULL) {
        *status= INCORRECT_FORMAT;
    }
    return token;
}

char* split_dot(char** tokenizer, message_status* status) {
    char* token = strsep(tokenizer, ".");
    if (token == NULL) {
        *status= INCORRECT_FORMAT;
    }
    return token;
}

char* split_break(char** tokenizer, message_status* status) {
    char* token = strsep(tokenizer, "\n");
    if (token == NULL) {
        *status= INCORRECT_FORMAT;
    }
    return token;
}
/* The following three functions will show output on the terminal
 * based off whether the corresponding level is defined.
 * To see log output, define LOG.
 * To see error output, define LOG_ERR.
 * To see info output, define LOG_INFO
 */
void cs165_log(FILE* out, const char *format, ...) {
#ifdef LOG
    va_list v;
    va_start(v, format);
    vfprintf(out, format, v);
    va_end(v);
#else
    (void) out;
    (void) format;
#endif
}

void log_err(const char *format, ...) {
#ifdef LOG_ERR
    va_list v;
    va_start(v, format);
    fprintf(stderr, ANSI_COLOR_RED);
    vfprintf(stderr, format, v);
    fprintf(stderr, ANSI_COLOR_RESET);
    va_end(v);
#else
    (void) format;
#endif
}

void log_info(const char *format, ...) {
#ifdef LOG_INFO
    va_list v;
    va_start(v, format);
    fprintf(stdout, ANSI_COLOR_GREEN);
    vfprintf(stdout, format, v);
    fprintf(stdout, ANSI_COLOR_RESET);
    fflush(stdout);
    va_end(v);
#else
    (void) format;
#endif
}

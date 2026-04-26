#ifndef ListMap_h
#define ListMap_h

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdint.h>
#include <inttypes.h>

enum 
{
    // Fixed key-slot capacity used by this educational implementation.
    listMapCapacity = 20
};

struct key_value
{
    char* key;
    enum
    {
        // Plain string value (SET/GET).
        LIST_MAP_VALUE_STRING,
        // List value (LPUSH/RPUSH/LPOP/LRANGE/LLEN/BLPOP).
        LIST_MAP_VALUE_LIST,
        // Stream value (XADD).
        LIST_MAP_VALUE_STREAM,
    } type;
    union
    {
        char* value;
        struct
        {
            // Generic dynamic vector used for list items and serialized stream rows.
            char** items;
            size_t count;
            size_t capacity;
        } list;
    } data;
    // 0 means no expiration, otherwise absolute epoch milliseconds.
    int64_t expires_at_ms;
};

struct ListMap
{
    struct key_value kvPairs[listMapCapacity];
    size_t count;
};

struct ListMap* newListMap(void);

bool listMapInsert(struct ListMap* collection, const char* key, const char* value, int64_t expires_at_ms);

bool listMapAppend(struct ListMap* collection, const char* key, const char* value, int64_t now_ms, size_t* list_count);

bool listMapPrepend(struct ListMap* collection, const char* key, const char* value, int64_t now_ms, size_t* list_count);

struct key_value* listMapFindEntry(struct ListMap* collection, const char* key);

const char* listMapValueForKey(struct ListMap* collection, const char* key, int64_t now_ms);

void freeListMap(struct ListMap* collection);

void deleteKey(struct ListMap* collection, const char* key);

int64_t incrementKey(struct ListMap* collection, const char* key, int64_t now_ms);

#endif

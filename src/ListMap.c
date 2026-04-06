#include "../headers/ListMap.h"

// Frees the active value payload of an entry (string/list/stream).
static void freeListValue(struct key_value* entry)
{
    if (entry->type == LIST_MAP_VALUE_LIST || entry->type == LIST_MAP_VALUE_STREAM)
    {
        for (size_t i = 0 ; i < entry->data.list.count ; ++i)
        {
            free(entry->data.list.items[i]);
        }
        free(entry->data.list.items);
        entry->data.list.items = NULL;
        entry->data.list.count = 0;
        entry->data.list.capacity = 0;
        return;
    }
    free(entry->data.value);
    entry->data.value = NULL;
}

// Frees both key and payload for one key/value entry.
static void freeKeyValue(struct key_value* entry)
{
    free(entry->key);
    entry->key = NULL;
    freeListValue(entry);
    entry->expires_at_ms = 0;
}

// Linear search over fixed-capacity storage.
struct key_value* listMapFindEntry(struct ListMap* collection, const char* key)
{
    for (size_t i = 0 ; i < collection->count ; ++i)
    {
        if (strcmp(collection->kvPairs[i].key, key) == 0)
        {
            return &collection->kvPairs[i];
        }
    }
    return NULL;
}

// Grows list backing storage using geometric growth.
static bool ensureListCapacity(struct key_value* entry, size_t needed_capacity)
{
    if (entry->data.list.capacity >= needed_capacity)
    {
        return true;
    }

    size_t new_capacity = entry->data.list.capacity == 0 ? 4 : entry->data.list.capacity * 2;
    while (new_capacity < needed_capacity)
    {
        new_capacity *= 2;
    }

    char** new_items = realloc(entry->data.list.items, new_capacity * sizeof(*new_items));
    if (new_items == NULL)
    {
        return false;
    }

    entry->data.list.items = new_items;
    entry->data.list.capacity = new_capacity;
    return true;
}

// Appends one duplicated value to a list-like entry.
static bool appendToList(struct key_value* entry, const char* value, size_t* list_count)
{
    char* new_item = strdup(value);
    if (new_item == NULL)
    {
        return false;
    }
    if (!ensureListCapacity(entry, entry->data.list.count + 1))
    {
        free(new_item);
        return false;
    }

    entry->data.list.items[entry->data.list.count++] = new_item;
    if (list_count != NULL)
    {
        *list_count = entry->data.list.count;
    }
    return true;
}

// Prepends one duplicated value to a list-like entry.
static bool prependToList(struct key_value* entry, const char* value, size_t* list_count)
{
    char* new_item = strdup(value);
    if (new_item == NULL)
    {
        return false;
    }
    if (!ensureListCapacity(entry, entry->data.list.count + 1))
    {
        free(new_item);
        return false;
    }

    if (entry->data.list.count > 0)
    {
        memmove(&entry->data.list.items[1], &entry->data.list.items[0], entry->data.list.count * sizeof(*entry->data.list.items));
    }
    entry->data.list.items[0] = new_item;
    entry->data.list.count++;

    if (list_count != NULL)
    {
        *list_count = entry->data.list.count;
    }
    return true;
}

// Creates an empty map with zeroed storage.
struct ListMap* newListMap(void)
{
    struct ListMap* ret = (struct ListMap*)calloc(1, sizeof * ret);
    ret->count = 0;
    return ret;
}

// Inserts or replaces a string key.
bool listMapInsert(struct ListMap* collection, const char* key, const char* value, int64_t expires_at_ms)
{
    struct key_value* entry = listMapFindEntry(collection, key);
    if (entry != NULL)
    {
        char* new_value = strdup(value);
        if (new_value == NULL)
        {
            return false;
        }
        freeListValue(entry);
        entry->type = LIST_MAP_VALUE_STRING;
        entry->data.value = new_value;
        entry->expires_at_ms = expires_at_ms;
        return true;
    }
    if (collection->count == listMapCapacity)
    {
        return false;
    }
    entry = &collection->kvPairs[collection->count];
    entry->key = strdup(key);
    entry->type = LIST_MAP_VALUE_STRING;
    entry->data.value = strdup(value);
    if (entry->key == NULL || entry->data.value == NULL)
    {
        free(entry->key);
        free(entry->data.value);
        entry->key = NULL;
        entry->data.value = NULL;
        return false;
    }
    entry->expires_at_ms = expires_at_ms;
    collection->count++;
    return true;
}

// Appends into a list key, creating it when missing.
bool listMapAppend(struct ListMap* collection, const char* key, const char* value, int64_t now_ms, size_t* list_count)
{
    struct key_value* entry = listMapFindEntry(collection, key);

    if (entry != NULL && entry->expires_at_ms != 0 && entry->expires_at_ms <= now_ms)
    {
        // Expired key behaves as absent and is removed lazily.
        deleteKey(collection, key);
        entry = NULL;
    }

    if (entry == NULL)
    {
        if (collection->count == listMapCapacity)
        {
            return false;
        }

        entry = &collection->kvPairs[collection->count];
        entry->key = strdup(key);
        if (entry->key == NULL)
        {
            return false;
        }
        entry->type = LIST_MAP_VALUE_LIST;
        entry->data.list.items = NULL;
        entry->data.list.count = 0;
        entry->data.list.capacity = 0;
        entry->expires_at_ms = 0;

        if (!appendToList(entry, value, list_count))
        {
            free(entry->key);
            entry->key = NULL;
            return false;
        }

        collection->count++;
        return true;
    }

    if (entry->type != LIST_MAP_VALUE_LIST)
    {
        return false;
    }

    return appendToList(entry, value, list_count);
}

// Prepends into a list key, creating it when missing.
bool listMapPrepend(struct ListMap* collection, const char* key, const char* value, int64_t now_ms, size_t* list_count)
{
    struct key_value* entry = listMapFindEntry(collection, key);

    if (entry != NULL && entry->expires_at_ms != 0 && entry->expires_at_ms <= now_ms)
    {
        // Expired key behaves as absent and is removed lazily.
        deleteKey(collection, key);
        entry = NULL;
    }

    if (entry == NULL)
    {
        if (collection->count == listMapCapacity)
        {
            return false;
        }

        entry = &collection->kvPairs[collection->count];
        entry->key = strdup(key);
        if (entry->key == NULL)
        {
            return false;
        }
        entry->type = LIST_MAP_VALUE_LIST;
        entry->data.list.items = NULL;
        entry->data.list.count = 0;
        entry->data.list.capacity = 0;
        entry->expires_at_ms = 0;

        if (!prependToList(entry, value, list_count))
        {
            free(entry->key);
            entry->key = NULL;
            return false;
        }

        collection->count++;
        return true;
    }

    if (entry->type != LIST_MAP_VALUE_LIST)
    {
        return false;
    }

    return prependToList(entry, value, list_count);
}

// Returns string value for key, evicting expired keys on read.
const char* listMapValueForKey(struct ListMap* collection, const char* key, int64_t now_ms)
{
    struct key_value* entry = listMapFindEntry(collection, key);
    if (entry == NULL)
    {
        return NULL;
    }
    if (entry->expires_at_ms != 0 && entry->expires_at_ms <= now_ms)
    {
        deleteKey(collection, key);
        return NULL;
    }
    if (entry->type != LIST_MAP_VALUE_STRING)
    {
        return NULL;
    }
    return entry->data.value;
}

// Frees the entire map and all owned heap allocations.
void freeListMap(struct ListMap* listMap)
{
    if (listMap == NULL)
    {
        return;
    }
    for (size_t i = 0 ; i < listMap->count ; ++i)
    {
        freeKeyValue(&listMap->kvPairs[i]);
    }
    free(listMap);
}

// Removes a key and compacts the fixed array by shifting left.
void deleteKey(struct ListMap* collection, const char* key)
{
    for (size_t i = 0 ; i < collection->count ; ++i)
    {
        if (strcmp(collection->kvPairs[i].key, key) == 0)
        {
            freeKeyValue(&collection->kvPairs[i]);
            // Shift remaining elements left
            for (size_t j = i ; j < collection->count - 1 ; ++j)
            {
                collection->kvPairs[j] = collection->kvPairs[j + 1];
            }
            collection->count--;
            memset(&collection->kvPairs[collection->count], 0, sizeof(collection->kvPairs[collection->count]));
            break;
        }
    }
}

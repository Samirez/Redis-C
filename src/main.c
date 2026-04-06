#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <string.h>
#include <strings.h>
#include <errno.h>
#include <unistd.h>
#include <pthread.h>
#include <stdint.h>
#include <sys/time.h>
#include <time.h>
#include "../headers/ListMap.h"

typedef struct
{
	// Shared accept-loop state passed to the background accept thread.
	int server_fd;
	socklen_t client_addr_len;
	struct sockaddr_in client_addr;
} server_thread_params_t;


typedef struct StreamEntry
{
	// Temporary representation used while parsing XADD arguments.
	char* id;
	char** fields;
	int field_count;
	struct StreamEntry* next;
} StreamEntry;

// Frees a temporary StreamEntry and all memory it owns.
static void free_stream_entry(StreamEntry *stream)
{
	if (stream == NULL)
	{
		return;
	}
	if (stream->fields != NULL)
	{
		for (int i = 0; i < stream->field_count; ++i)
		{
			free(stream->fields[i]);
		}
		free(stream->fields);
	}
	free(stream->id);
	free(stream);
}

// Parses a stream ID in the form "<milliseconds>-<sequence>".
static bool parse_stream_id_parts(const char *id, int64_t *ms_out, int64_t *seq_out)
{
	char *endptr;
	const char *dash = strchr(id, '-');
	if (dash == NULL || dash == id || *(dash + 1) == '\0')
	{
		return false;
	}

	errno = 0;
	*ms_out = strtoll(id, &endptr, 10);
	if (errno != 0 || endptr != dash || *ms_out < 0)
	{
		return false;
	}

	errno = 0;
	*seq_out = strtoll(dash + 1, &endptr, 10);
	if (errno != 0 || *endptr != '\0' || *seq_out < 0)
	{
		return false;
	}

	return true;
}

// Lexicographic compare for stream IDs by numeric (ms, seq) tuple.
// On parse failure, sets *ok to false so callers do not confuse errors with equality.
static int compare_stream_ids(const char *lhs, const char *rhs, bool *ok)
{
	int64_t lhs_ms;
	int64_t lhs_seq;
	int64_t rhs_ms;
	int64_t rhs_seq;

	if (!parse_stream_id_parts(lhs, &lhs_ms, &lhs_seq) || !parse_stream_id_parts(rhs, &rhs_ms, &rhs_seq))
	{
		*ok = false;
		return 0;
	}
	*ok = true;

	if (lhs_ms < rhs_ms)
	{
		return -1;
	}
	if (lhs_ms > rhs_ms)
	{
		return 1;
	}
	if (lhs_seq < rhs_seq)
	{
		return -1;
	}
	if (lhs_seq > rhs_seq)
	{
		return 1;
	}
	return 0;
}

// Global in-memory key/value store guarded by a single mutex.
struct ListMap *listmap = NULL;
pthread_mutex_t listmap_mutex = PTHREAD_MUTEX_INITIALIZER;
// Used by BLPOP waiters so LPUSH/RPUSH can wake blocked clients.
pthread_cond_t listmap_cond = PTHREAD_COND_INITIALIZER;

// Helper used for expirations and timeouts.
static int64_t current_time_millis(void)
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

// Reads a RESP bulk-string token from the command input and advances cursor.
static bool parse_bulk_string(const char *input, const char **cursor, char *output, size_t output_size)
{
	const char *start = *cursor != NULL ? *cursor : input;
	const char *dollar = strstr(start, "$");
	const char *line_end;
	const char *value_start;
	int value_len;

	if (dollar == NULL)
	{
		return false;
	}
	line_end = strstr(dollar, "\r\n");
	if (line_end == NULL)
	{
		return false;
	}
	value_len = atoi(dollar + 1);
	if (value_len < 0 || (size_t)value_len >= output_size)
	{
		return false;
	}
	value_start = line_end + 2;
	if (strlen(value_start) < (size_t)value_len)
	{
		return false;
	}
	snprintf(output, output_size, "%.*s", value_len, value_start);
	*cursor = value_start + value_len;
	return true;
}

// Core RESP command dispatcher.
// Variable-size responses are written into the caller-provided buffer so each
// client thread has isolated response storage.
const char *resp_parse(const char *input, char *buffer, size_t buffer_size)
{
	if (input[0] != '*')
	{
		return "+PONG\r\n";
	}

	char *dollar = strstr(input, "$");
	if (dollar == NULL)
	{
		return "+PONG\r\n";
	}
	int cmd_len = atoi(dollar + 1);
	char *cmd_start = strstr(dollar, "\r\n") + 2;

	if (strncasecmp(cmd_start, "echo", 4) == 0)
	{
		// ECHO returns the exact payload as a bulk string.
		char *next_dollar = strstr(cmd_start + 1, "$");
		if (next_dollar == NULL)
		{
			return "+PONG\r\n";
		}
		int arg_len = atoi(next_dollar + 1);
		const char *arg_start = strstr(next_dollar, "\r\n") + 2;
		snprintf(buffer, buffer_size, "$%d\r\n%.*s\r\n", arg_len, arg_len, arg_start);
		return buffer;
	}
	else if (strncasecmp(cmd_start, "ping", 4) == 0)
	{
		return "+PONG\r\n";
	}
	else if (strncasecmp(cmd_start, "get", 3) == 0)
	{
		// GET reads string keys only; non-strings resolve as nil.
		const char *cursor = cmd_start + cmd_len;
		char key_buf[513];

		if (!parse_bulk_string(input, &cursor, key_buf, sizeof(key_buf)))
		{
			return "-ERR\r\n";
		}

		pthread_mutex_lock(&listmap_mutex);
		const char *value = listMapValueForKey(listmap, key_buf, current_time_millis());
		if (value != NULL)
		{
			pthread_mutex_unlock(&listmap_mutex);
			snprintf(buffer, buffer_size, "$%zu\r\n%s\r\n", strlen(value), value);
			return buffer;
		}
		pthread_mutex_unlock(&listmap_mutex);
		return "$-1\r\n";
	}
	else if (strncasecmp(cmd_start, "set", 3) == 0)
	{
		// SET supports optional EX/PX expiration.
		const char *cursor = cmd_start + cmd_len;
		char key_buf[513];
		char value_buf[513];
		char option_buf[16];
		char ttl_buf[32];
		int64_t expires_at_ms = 0;

		if (!parse_bulk_string(input, &cursor, key_buf, sizeof(key_buf)))
		{
			return "-ERR\r\n";
		}
		if (!parse_bulk_string(input, &cursor, value_buf, sizeof(value_buf)))
		{
			return "-ERR\r\n";
		}

		if (parse_bulk_string(input, &cursor, option_buf, sizeof(option_buf)))
		{
			if (!parse_bulk_string(input, &cursor, ttl_buf, sizeof(ttl_buf)))
			{
				return "-ERR\r\n";
			}
			if (strcasecmp(option_buf, "px") == 0)
			{
				expires_at_ms = current_time_millis() + atoll(ttl_buf);
			}
			else if (strcasecmp(option_buf, "ex") == 0)
			{
				expires_at_ms = current_time_millis() + (int64_t)atoll(ttl_buf) * 1000;
			}
			else
			{
				return "-ERR\r\n";
			}
		}

		pthread_mutex_lock(&listmap_mutex);
		if (!listMapInsert(listmap, key_buf, value_buf, expires_at_ms))
		{
			pthread_mutex_unlock(&listmap_mutex);
			return "-ERR list map is full\r\n";
		}
		pthread_mutex_unlock(&listmap_mutex);
		return "+OK\r\n";
	}
	else if (strncasecmp(cmd_start, "RPUSH", 5) == 0)
	{
		// RPUSH appends one or more items to the tail.
		const char *cursor = cmd_start + cmd_len;
		char list_key[513];
		char element_key[513];
		size_t result_count = 0;
		char *delimiters = " ";
		char *token;
		char *saveptr;

		if (!parse_bulk_string(input, &cursor, list_key, sizeof(list_key)))
		{
			return "-ERR\r\n";
		}
		if (!parse_bulk_string(input, &cursor, element_key, sizeof(element_key)))
		{
			return "-ERR\r\n";
		}

		pthread_mutex_lock(&listmap_mutex);
		do
		{
			saveptr = NULL;
			token = strtok_r(element_key, delimiters, &saveptr);
			while (token != NULL)
			{
				if (!listMapAppend(listmap, list_key, token, current_time_millis(), &result_count))
				{
					pthread_mutex_unlock(&listmap_mutex);
					return "-ERR list map is full\r\n";
				}
				token = strtok_r(NULL, delimiters, &saveptr);
			}
		} while (parse_bulk_string(input, &cursor, element_key, sizeof(element_key)));
		// Wake blocked BLPOP clients now that data may be available.
		pthread_cond_broadcast(&listmap_cond);
		pthread_mutex_unlock(&listmap_mutex);

		snprintf(buffer, buffer_size, ":%zu\r\n", result_count);
		return buffer;
	}
	else if (strncasecmp(cmd_start, "LRANGE", 6) == 0)
	{
		// LRANGE returns an inclusive sub-range with negative-index support.
		const char *cursor = cmd_start + cmd_len;
		char list_key[513];
		char start_idx_buf[32];
		char end_idx_buf[32];
		int start_idx;
		int end_idx;

		if (!parse_bulk_string(input, &cursor, list_key, sizeof(list_key)))
		{
			return "-ERR\r\n";
		}
		if (!parse_bulk_string(input, &cursor, start_idx_buf, sizeof(start_idx_buf)))
		{
			return "-ERR\r\n";
		}
		if (!parse_bulk_string(input, &cursor, end_idx_buf, sizeof(end_idx_buf)))
		{
			return "-ERR\r\n";
		}
		start_idx = atoi(start_idx_buf);
		end_idx = atoi(end_idx_buf);

		pthread_mutex_lock(&listmap_mutex);
		struct key_value *entry = listMapFindEntry(listmap, list_key);
		if (entry == NULL || entry->type != LIST_MAP_VALUE_LIST)
		{
			pthread_mutex_unlock(&listmap_mutex);
			return "*0\r\n";
		}

		size_t list_count = entry->data.list.count;
		if (start_idx < 0)
		{
			start_idx += (int)list_count;
		}
		if (end_idx < 0)
		{
			end_idx += (int)list_count;
		}
		if (start_idx < 0)
		{
			start_idx = 0;
		}
		if (end_idx >= (int)list_count)
		{
			end_idx = (int)list_count - 1;
		}
		if (start_idx > end_idx || start_idx >= (int)list_count)
		{
			pthread_mutex_unlock(&listmap_mutex);
			return "*0\r\n";
		}

		int range_count = end_idx - start_idx + 1;
		size_t response_len = 1 + snprintf(NULL, 0, "%d", range_count) + 2;
		for (int i = start_idx; i <= end_idx; ++i)
		{
			size_t item_len = strlen(entry->data.list.items[i]);
			response_len += 1 + snprintf(NULL, 0, "%zu", item_len) + 2;
			response_len += item_len + 2;
		}
		if (response_len >= buffer_size)
		{
			pthread_mutex_unlock(&listmap_mutex);
			return "-ERR\r\n";
		}

		char *ptr = buffer;
		ptr += sprintf(ptr, "*%d\r\n", range_count);
		for (int i = start_idx; i <= end_idx; ++i)
		{
			ptr += sprintf(ptr, "$%zu\r\n%s\r\n", strlen(entry->data.list.items[i]), entry->data.list.items[i]);
		}
		*ptr = '\0';
		pthread_mutex_unlock(&listmap_mutex);
		return buffer;
	}
	else if (strncasecmp(cmd_start, "LPUSH", 5) == 0)
	{
		// LPUSH prepends one or more items to the head.
		const char *cursor = cmd_start + cmd_len;
		char list_key[513];
		char element_key[513];
		size_t result_count = 0;
		char *delimiters = " ";
		char *token;
		char *saveptr;

		if (!parse_bulk_string(input, &cursor, list_key, sizeof(list_key)))
		{
			return "-ERR\r\n";
		}
		if (!parse_bulk_string(input, &cursor, element_key, sizeof(element_key)))
		{
			return "-ERR\r\n";
		}

		pthread_mutex_lock(&listmap_mutex);
		do
		{
			saveptr = NULL;
			token = strtok_r(element_key, delimiters, &saveptr);
			while (token != NULL)
			{
				if (!listMapPrepend(listmap, list_key, token, current_time_millis(), &result_count))
				{
					pthread_mutex_unlock(&listmap_mutex);
					return "-ERR list map is full\r\n";
				}
				token = strtok_r(NULL, delimiters, &saveptr);
			}
		} while (parse_bulk_string(input, &cursor, element_key, sizeof(element_key)));
		// Wake blocked BLPOP clients now that data may be available.
		pthread_cond_broadcast(&listmap_cond);
		pthread_mutex_unlock(&listmap_mutex);

		snprintf(buffer, buffer_size, ":%zu\r\n", result_count);
		return buffer;
	}
	else if (strncasecmp(cmd_start, "LLEN", 4) == 0)
	{
		// LLEN reports list cardinality, 0 for missing/non-list keys.
		const char *cursor = cmd_start + cmd_len;
		char list_key[513];

		if (!parse_bulk_string(input, &cursor, list_key, sizeof(list_key)))
		{
			return "-ERR\r\n";
		}

		pthread_mutex_lock(&listmap_mutex);
		struct key_value *entry = listMapFindEntry(listmap, list_key);
		if (entry == NULL || entry->type != LIST_MAP_VALUE_LIST)
		{
			pthread_mutex_unlock(&listmap_mutex);
			return ":0\r\n";
		}
		size_t list_count = entry->data.list.count;
		pthread_mutex_unlock(&listmap_mutex);

		snprintf(buffer, buffer_size, ":%zu\r\n", list_count);
		return buffer;
	}
	else if (strncasecmp(cmd_start, "LPOP", 4) == 0)
	{
		// LPOP supports optional count and returns popped values.
		const char *cursor = cmd_start + cmd_len;
		char list_key[513];
		char count_buf[32];
		size_t pop_count;
		bool has_count = false;
		long requested_count = 1;

		if (!parse_bulk_string(input, &cursor, list_key, sizeof(list_key)))
		{
			return "-ERR\r\n";
		}
		if (parse_bulk_string(input, &cursor, count_buf, sizeof(count_buf)))
		{
			has_count = true;
			requested_count = strtol(count_buf, NULL, 10);
			if (requested_count <= 0)
			{
				return "*0\r\n";
			}
		}

		pthread_mutex_lock(&listmap_mutex);
		struct key_value *entry = listMapFindEntry(listmap, list_key);
		if (entry == NULL || entry->type != LIST_MAP_VALUE_LIST || entry->data.list.count == 0)
		{
			pthread_mutex_unlock(&listmap_mutex);
			if (has_count)
			{
				return "*0\r\n";
			}
			return "$-1\r\n";
		}

		pop_count = has_count ? (size_t)requested_count : 1;
		if (pop_count > entry->data.list.count)
		{
			pop_count = entry->data.list.count;
		}

		char *popped_values[listMapCapacity];
		for (size_t i = 0; i < pop_count; ++i)
		{
			popped_values[i] = entry->data.list.items[i];
		}
		memmove(entry->data.list.items, entry->data.list.items + pop_count, (entry->data.list.count - pop_count) * sizeof(char *));
		entry->data.list.count -= pop_count;
		for (size_t i = entry->data.list.count; i < entry->data.list.count + pop_count; ++i)
		{
			entry->data.list.items[i] = NULL;
		}
		pthread_mutex_unlock(&listmap_mutex);

		if (!has_count)
		{
			snprintf(buffer, buffer_size, "$%zu\r\n%s\r\n", strlen(popped_values[0]), popped_values[0]);
			free(popped_values[0]);
			return buffer;
		}

		size_t used = (size_t)snprintf(buffer, buffer_size, "*%zu\r\n", pop_count);
		if (used >= buffer_size)
		{
			for (size_t i = 0; i < pop_count; ++i)
			{
				free(popped_values[i]);
			}
			return "-ERR\r\n";
		}

		for (size_t i = 0; i < pop_count; ++i)
		{
			size_t value_len = strlen(popped_values[i]);
			int wrote = snprintf(buffer + used, buffer_size - used, "$%zu\r\n%s\r\n", value_len, popped_values[i]);
			free(popped_values[i]);
			if (wrote < 0 || (size_t)wrote >= buffer_size - used)
			{
				return "-ERR\r\n";
			}
			used += (size_t)wrote;
		}
		return buffer;
	}
	else if (strncasecmp(cmd_start, "BLPOP", 5) == 0)
	{
		// BLPOP blocks until data exists or timeout expires.
		const char *cursor = cmd_start + cmd_len;
		char list_key[513];
		char timeout_buf[32];
		double timeout_sec;
		int64_t timeout_ms;
		int64_t deadline_ms = 0;
		char *popped_value = NULL;

		if (!parse_bulk_string(input, &cursor, list_key, sizeof(list_key)))
		{
			return "-ERR\r\n";
		}

		if (!parse_bulk_string(input, &cursor, timeout_buf, sizeof(timeout_buf)))
		{
			return "-ERR\r\n";
		}

		timeout_sec = strtod(timeout_buf, NULL);

		if (timeout_sec < 0)
		{
			return "-ERR\r\n";
		}

		timeout_ms = (int64_t)(timeout_sec * 1000.0);
		if (timeout_ms > 0)
		{
			deadline_ms = current_time_millis() + timeout_ms;
		}

		pthread_mutex_lock(&listmap_mutex);
		while (true)
		{
			// Recheck predicate after every wakeup (handles spurious wakeups).
			struct key_value *entry = listMapFindEntry(listmap, list_key);
			if (entry != NULL && entry->type == LIST_MAP_VALUE_LIST && entry->data.list.count > 0)
			{
				popped_value = entry->data.list.items[0];
				memmove(entry->data.list.items, entry->data.list.items + 1, (entry->data.list.count - 1) * sizeof(char *));
				entry->data.list.count--;
				entry->data.list.items[entry->data.list.count] = NULL;
				break;
			}

			if (timeout_ms == 0)
			{
				// 0 means wait forever until someone pushes data.
				pthread_cond_wait(&listmap_cond, &listmap_mutex);
				continue;
			}

			int64_t now_ms = current_time_millis();
			if (now_ms >= deadline_ms)
			{
				break;
			}

			int64_t remaining_ms = deadline_ms - now_ms;
			struct timespec ts;
			clock_gettime(CLOCK_REALTIME, &ts);
			ts.tv_sec += (time_t)(remaining_ms / 1000);
			ts.tv_nsec += (long)((remaining_ms % 1000) * 1000000);
			if (ts.tv_nsec >= 1000000000L)
			{
				ts.tv_sec += 1;
				ts.tv_nsec -= 1000000000L;
			}

			int wait_result = pthread_cond_timedwait(&listmap_cond, &listmap_mutex, &ts);
			if (wait_result == ETIMEDOUT)
			{
				break;
			}
		}
		pthread_mutex_unlock(&listmap_mutex);

		if (popped_value == NULL)
		{
			return "*-1\r\n";
		}

		snprintf(buffer, buffer_size, "*2\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n", strlen(list_key), list_key, strlen(popped_value), popped_value);
		free(popped_value);
		return buffer;
	}
	else if (strncasecmp(cmd_start, "TYPE", 4) == 0)
	{
		// TYPE reports the Redis-ish data type for a key.
		const char *cursor = cmd_start + cmd_len;
		char key_buf[513];

		if (!parse_bulk_string(input, &cursor, key_buf, sizeof(key_buf)))
		{
			return "-ERR\r\n";
		}

		pthread_mutex_lock(&listmap_mutex);
		struct key_value *entry = listMapFindEntry(listmap, key_buf);
		
		if (entry == NULL)
		{
			pthread_mutex_unlock(&listmap_mutex);
			return "+none\r\n";
		}

		const char *type_str;
		if (entry->type == LIST_MAP_VALUE_STRING)
		{
			type_str = "+string\r\n";
		}
		else if (entry->type == LIST_MAP_VALUE_STREAM)
		{
			type_str = "+stream\r\n";
		}
		else
		{
			type_str = "+list\r\n";
		}
		pthread_mutex_unlock(&listmap_mutex);
		return type_str;
	} else if (strncasecmp(cmd_start, "XADD", 4) == 0)
	{
		// Minimal XADD supporting explicit IDs and one field/value pair.
		char stream_key[513];
		char id_buf[513];
		char field_buf[513];
		char value_buf[513];
		const char *cursor = cmd_start + cmd_len;
		StreamEntry* stream = malloc(sizeof(StreamEntry));
		if (stream == NULL)
		{
			return "-ERR\r\n";
		}
		stream->id = NULL;
		stream->fields = NULL;
		stream->field_count = 0;
		stream->next = NULL;

		if (!parse_bulk_string(input, &cursor, stream_key, sizeof(stream_key)))
		{
			free_stream_entry(stream);
			return "-ERR\r\n";
		}
		if (!parse_bulk_string(input, &cursor, id_buf, sizeof(id_buf)))
		{
			free_stream_entry(stream);
			return "-ERR\r\n";
		}
		if (!parse_bulk_string(input, &cursor, field_buf, sizeof(field_buf)))
		{
			free_stream_entry(stream);
			return "-ERR\r\n";
		}
		if (!parse_bulk_string(input, &cursor, value_buf, sizeof(value_buf)))
		{
			free_stream_entry(stream);
			return "-ERR\r\n";
		}
		stream->id = strdup(id_buf);
		if (stream->id == NULL)
		{
			free_stream_entry(stream);
			return "-ERR\r\n";
		}
		stream->fields = malloc(2 * sizeof(char*));
		if (stream->fields == NULL)
		{
			free_stream_entry(stream);
			return "-ERR\r\n";
		}
		stream->fields[0] = strdup(field_buf);
		if (stream->fields[0] == NULL)
		{
			free_stream_entry(stream);
			return "-ERR\r\n";
		}
		stream->fields[1] = strdup(value_buf);
		if (stream->fields[1] == NULL)
		{
			free_stream_entry(stream);
			return "-ERR\r\n";
		}
		stream->field_count = 2;
		stream->next = NULL;

		int64_t req_ms;
		int64_t req_seq;
		if (!parse_stream_id_parts(stream->id, &req_ms, &req_seq))
		{
			free_stream_entry(stream);
			return "-ERR The ID specified in XADD is not valid\r\n";
		}
		if (req_ms == 0 && req_seq == 0)
		{
			free_stream_entry(stream);
			return "-ERR The ID specified in XADD must be greater than 0-0\r\n";
		}
		
		pthread_mutex_lock(&listmap_mutex);
		struct key_value *entry = listMapFindEntry(listmap, stream_key);
		if (entry == NULL)
		{
			if (listmap->count == listMapCapacity)
			{
				pthread_mutex_unlock(&listmap_mutex);
				free_stream_entry(stream);
				return "-ERR list map is full\r\n";
			}
			entry = &listmap->kvPairs[listmap->count];
			entry->key = strdup(stream_key);
			if (entry->key == NULL)
			{
				pthread_mutex_unlock(&listmap_mutex);
				free_stream_entry(stream);
				return "-ERR\r\n";
			}
			entry->type = LIST_MAP_VALUE_STREAM;
			entry->data.list.items = NULL;
			entry->data.list.count = 0;
			entry->data.list.capacity = 0;
			entry->expires_at_ms = 0;
			listmap->count++;
		}
		else if (entry->type != LIST_MAP_VALUE_STREAM)
		{
			pthread_mutex_unlock(&listmap_mutex);
			free_stream_entry(stream);
			return "-ERR\r\n";
		}

		// since stream entries are stored in the same list structure as lists, we can reuse the list capacity logic for streams as well. The only difference is that for streams we need to ensure that the new entry ID is greater than the last entry ID in the stream, which is not a requirement for lists.
		if (entry->data.list.count > 0)
		{
			// Ensure stream IDs are strictly increasing.
			char *last_serialized = entry->data.list.items[entry->data.list.count - 1];
			char *tab = strchr(last_serialized, '\t');
			if (tab == NULL)
			{
				pthread_mutex_unlock(&listmap_mutex);
				free_stream_entry(stream);
				return "-ERR\r\n";
			}

			size_t last_id_len = (size_t)(tab - last_serialized);
			if (last_id_len >= sizeof(id_buf))
			{
				pthread_mutex_unlock(&listmap_mutex);
				free_stream_entry(stream);
				return "-ERR\r\n";
			}

			char last_id_buf[513];
			memcpy(last_id_buf, last_serialized, last_id_len);
			last_id_buf[last_id_len] = '\0';
			bool compare_ok;
			if (compare_stream_ids(stream->id, last_id_buf, &compare_ok) <= 0)
			{
				if (!compare_ok)
				{
					pthread_mutex_unlock(&listmap_mutex);
					free_stream_entry(stream);
					return "-ERR\r\n";
				}
				pthread_mutex_unlock(&listmap_mutex);
				free_stream_entry(stream);
				return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
			}
		}

		if (entry->data.list.count == entry->data.list.capacity)
		{
			// Grow stream backing storage geometrically.
			size_t new_capacity = entry->data.list.capacity == 0 ? 4 : entry->data.list.capacity * 2;
			char **new_items = realloc(entry->data.list.items, new_capacity * sizeof(*new_items));
			if (new_items == NULL)
			{
				pthread_mutex_unlock(&listmap_mutex);
				free_stream_entry(stream);
				return "-ERR\r\n";
			}
			entry->data.list.items = new_items;
			entry->data.list.capacity = new_capacity;
		}

		size_t serialized_len = strlen(stream->id) + 1 + strlen(stream->fields[0]) + 1 + strlen(stream->fields[1]);
		char *serialized = malloc(serialized_len + 1);

		if (serialized == NULL)
		{
			pthread_mutex_unlock(&listmap_mutex);
			free_stream_entry(stream);
			return "-ERR\r\n";
		}

		// Store stream rows as a compact "id<TAB>field<TAB>value" string for now.
		snprintf(serialized, serialized_len + 1, "%s\t%s\t%s", stream->id, stream->fields[0], stream->fields[1]);
		entry->data.list.items[entry->data.list.count++] = serialized;
		pthread_mutex_unlock(&listmap_mutex);
		snprintf(buffer, buffer_size, "$%zu\r\n%s\r\n", strlen(stream->id), stream->id);
		free_stream_entry(stream);
		return buffer;
	}
	return "+PONG\r\n";
}

int restart_server(int server_fd, int reuse)
{
	// Allow immediate rebinding after restarts.
	if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0)
	{
		printf("SO_REUSEADDR failed: %s \n", strerror(errno));
		return 1;
	}
	return 0;
}

int is_client_connected(int client_fd)
{
	// accept() errors return -1; caller decides whether to continue.
	if (client_fd == -1)
	{
		printf("Accept failed: %s \n", strerror(errno));
		return 1;
	}
	printf("Client connected\n");
	return 0;
}

void *ping_response(void *arg)
{
	// One thread per client connection; processes sequential commands from socket.
	int client_fd = *(int *)arg;
	free(arg);
	const char *response;
	char request_buffer[1024];
	char response_buffer[8192];

	while (1)
	{
		ssize_t bytes_received = recv(client_fd, request_buffer, sizeof(request_buffer) - 1, 0);
		if (bytes_received < 0)
		{
			printf("Receive failed: %s \n", strerror(errno));
			break;
		}
		else if (bytes_received == 0)
		{
			printf("Client disconnected\n");
			break;
		}
		request_buffer[bytes_received] = '\0';
		response = resp_parse(request_buffer, response_buffer, sizeof(response_buffer));
		// RESP replies are plain text and can be sent directly.
		const size_t response_len = strlen(response);
		ssize_t bytes_sent = send(client_fd, response, response_len, 0);
		if (bytes_sent < 0)
		{
			printf("Send failed: %s \n", strerror(errno));
			break;
		}
		printf("Sent response to client: %s", response);
	}
	close(client_fd);
	return NULL;
}

void *handle_multiple_clients(void *arg)
{
	// Dedicated accept loop that spawns detached worker threads per client.
	server_thread_params_t *params = (server_thread_params_t *)arg;
	int server_fd = params->server_fd;
	struct sockaddr_in client_addr = params->client_addr;
	socklen_t client_addr_len = params->client_addr_len;
	free(params);

	while (1)
	{
		client_addr_len = sizeof(client_addr);
		int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_addr_len);
		if (is_client_connected(client_fd) != 0)
		{
			continue;
		}

		int *client_fd_ptr = malloc(sizeof(int));
		if (client_fd_ptr == NULL)
		{
			printf("malloc failed: %s\n", strerror(errno));
			close(client_fd);
			continue;
		}
		*client_fd_ptr = client_fd;

		pthread_t thread;
		int create_result = pthread_create(&thread, NULL, ping_response, client_fd_ptr);
		if (create_result != 0)
		{
			printf("pthread_create failed: %s\n", strerror(create_result));
			free(client_fd_ptr);
			close(client_fd);
			continue;
		}
		pthread_detach(thread);
	}

	return NULL;
}

int main()
{
	// Disable buffering so logs appear immediately in the tester output.
	setbuf(stdout, NULL);
	setbuf(stderr, NULL);
	printf("Logs from your program will appear here!\n");

	listmap = newListMap();
	if (listmap == NULL)
	{
		printf("newListMap failed\n");
		return 1;
	}

	int server_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (server_fd == -1)
	{
		printf("Socket creation failed: %s...\n", strerror(errno));
		return 1;
	}

	int reuse = 1;
	restart_server(server_fd, reuse);

	struct sockaddr_in serv_addr = {
		.sin_family = AF_INET,
		.sin_port = htons(6379),
		.sin_addr = {htonl(INADDR_ANY)},
	};

	if (bind(server_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) != 0)
	{
		printf("Bind failed: %s \n", strerror(errno));
		return 1;
	}

	int connection_backlog = 5;
	if (listen(server_fd, connection_backlog) != 0)
	{
		printf("Listen failed: %s \n", strerror(errno));
		return 1;
	}

	// Run accept loop in a background thread; main waits indefinitely.
	printf("Waiting for a client to connect...\n");

	pthread_t thread;
	server_thread_params_t *params_ptr = malloc(sizeof(server_thread_params_t));
	if (params_ptr == NULL)
	{
		printf("malloc failed: %s\n", strerror(errno));
		close(server_fd);
		return 1;
	}
	params_ptr->server_fd = server_fd;
	params_ptr->client_addr_len = sizeof(params_ptr->client_addr);
	memset(&params_ptr->client_addr, 0, sizeof(params_ptr->client_addr));

	int create_result = pthread_create(&thread, NULL, handle_multiple_clients, params_ptr);
	if (create_result != 0)
	{
		printf("pthread_create failed: %s\n", strerror(create_result));
		free(params_ptr);
		close(server_fd);
		return 1;
	}

	pthread_join(thread, NULL);
	freeListMap(listmap);
	close(server_fd);
	return 0;
}

#ifndef REDIS_H
#define REDIS_H

#include <stddef.h>

/* Redis connection handle */
typedef struct redis_connection redis_connection_t;

/* Connect to a Redis server at the given host and port.
 * Returns a connection handle on success, or NULL on failure. */
redis_connection_t *redis_connect(const char *host, int port);

/* Disconnect from the Redis server and free the connection handle. */
void redis_disconnect(redis_connection_t *conn);

#endif /* REDIS_H */

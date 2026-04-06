#include <stdlib.h>
#include "redis.h"

struct redis_connection {
    int port;
    char *host;
};

redis_connection_t *redis_connect(const char *host, int port)
{
    (void)host;
    (void)port;
    return NULL;
}

void redis_disconnect(redis_connection_t *conn)
{
    if (conn == NULL)
        return;
    free(conn->host);
    free(conn);
}

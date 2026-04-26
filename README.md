# Redis-C

A Redis application built in C language for high performance, scalability and stable connections to Redis server, using multithreading to handle client requests.

## Overview

Redis-C is a lightweight, multi-threaded Redis server implementation written in pure C. It provides a subset of Redis functionality with support for key-value pairs, lists, and streams, all accessible via the RESP (Redis Serialization Protocol).

## Key Features

- **Multi-threaded Architecture**: Handles concurrent client connections using POSIX threads
- **In-Memory Data Store**: Fast key-value storage with multiple data types
- **RESP Protocol Support**: Full support for Redis Serialization Protocol for client-server communication
- **Data Types**:
  - Strings with optional TTL (Time-To-Live)
  - Lists with LPUSH, RPUSH, LPOP, BLPOP operations
  - Streams with XADD, XRANGE, and XREAD support
- **Transaction Support**: MULTI/EXEC/DISCARD for atomic command execution
- **Blocking Operations**: BLPOP and XREAD BLOCK for waiting on data availability

## Architecture

### High-Level Design

```
┌─────────────────────────────────────────────┐
│         Client Connections (TCP)             │
│              Port 6379                       │
└────────────┬────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────┐
│       Accept Loop Thread                     │
│    (handle_multiple_clients)                 │
│  - Accepts new client connections            │
│  - Spawns worker thread per client           │
└────────────┬────────────────────────────────┘
             │
             ├─────────────────┬─────────────────┐
             ▼                 ▼                 ▼
        Worker Thread 1    Worker Thread 2   Worker Thread N
        (ping_response)    (ping_response)   (ping_response)
        ┌──────────────┐   ┌──────────────┐  ┌──────────────┐
        │ RESP Parser  │   │ RESP Parser  │  │ RESP Parser  │
        │   Command    │   │   Command    │  │   Command    │
        │  Execution   │   │  Execution   │  │  Execution   │
        └────────┬─────┘   └────────┬─────┘  └────────┬─────┘
                 │                  │                  │
                 └──────────────────┼──────────────────┘
                                    │
                    ┌───────────────┴────────────────┐
                    │  Mutex Protected Data Store    │
                    │        (ListMap)               │
                    │                                │
                    │  Global In-Memory Storage:     │
                    │  • String values               │
                    │  • List items                  │
                    │  • Stream entries              │
                    │  • TTL/Expiration tracking     │
                    └────────────────────────────────┘
```

### Core Components

#### 1. **Main Server Loop** (`src/main.c`)
- **`main()`**: Initializes the server, creates socket, binds to port 6379, and spawns accept thread
- **`handle_multiple_clients()`**: Dedicated thread that accepts incoming connections and spawns worker threads
- **`ping_response()`**: Worker thread handling per-client request/response cycle

#### 2. **RESP Protocol Parser** (`src/main.c`)
- **`resp_parse()`**: Core command dispatcher that:
  - Parses incoming RESP protocol commands
  - Routes to appropriate command handlers
  - Returns RESP-formatted responses
  - Manages transaction state (MULTI/EXEC)

#### 3. **Data Storage Layer** (`src/ListMap.c`, `include/ListMap.h`)
- **`ListMap`**: Hash table data structure storing key-value pairs with support for:
  - **Strings**: Simple text values with optional expiration
  - **Lists**: Ordered collections of strings
  - **Streams**: Time-series data with ID-based ordering
  
- **Key Functions**:
  - `newListMap()`: Create empty map
  - `listMapInsert()`: Store/update string values
  - `listMapAppend()` / `listMapPrepend()`: Manage list operations
  - `listMapFindEntry()`: Retrieve entries by key
  - `deleteKey()`: Remove keys with lazy expiration

#### 4. **Synchronization Primitives**
- **`listmap_mutex`**: POSIX mutex protecting all data store access
- **`listmap_cond`**: Condition variable for blocking operations (BLPOP, XREAD BLOCK)
  - Allows waiting threads to sleep until data becomes available
  - Broadcast wakeup on LPUSH/RPUSH/XADD operations

### Threading Model

```
Main Thread
    ↓
Creates listening socket (port 6379)
    ↓
Spawns Accept Thread
    ├─→ Listens for incoming connections
    ├─→ On new connection: creates worker thread
    └─→ Each worker thread:
        ├─→ Receives RESP command from client
        ├─→ Acquires listmap_mutex
        ├─→ Executes command on data store
        ├─→ Releases listmap_mutex
        ├─→ Sends RESP response to client
        └─→ Loops until client disconnects
```

**Thread Safety**:
- All data store operations are protected by `listmap_mutex`
- Per-thread transaction state using thread-local storage (`__thread`)
- Condition variable prevents busy-waiting on blocking commands

### Supported Redis Commands

**String Operations**:
- `GET <key>` - Retrieve value
- `SET <key> <value> [EX|PX] [ttl]` - Store with optional expiration
- `INCR <key>` - Increment integer value

**List Operations**:
- `LPUSH <key> <value...>` - Push to head
- `RPUSH <key> <value...>` - Push to tail
- `LPOP <key> [count]` - Pop from head
- `BLPOP <key> <timeout>` - Blocking pop with timeout
- `LLEN <key>` - List length
- `LRANGE <key> <start> <end>` - Get range with negative index support

**Stream Operations**:
- `XADD <key> <id> <field> <value>` - Add stream entry with ID support (*, <ms>-*, explicit)
- `XRANGE <key> <start> <end>` - Query stream range
- `XREAD [BLOCK ms] STREAMS <key...> <id...>` - Read stream entries

**Transaction Operations**:
- `MULTI` - Begin transaction
- `EXEC` - Execute all queued commands atomically
- `DISCARD` - Abort transaction

**Utility**:
- `PING` / `ECHO` - Health checks
- `TYPE <key>` - Get data type

### Data Types and Storage

#### String Storage
```c
struct key_value {
    char *key;              // Key name
    int type;               // Value type (STRING/LIST/STREAM)
    union {
        char *value;        // String value
        struct {
            char **items;   // List/Stream items
            size_t count;
            size_t capacity;
        } list;
    } data;
    int64_t expires_at_ms;  // Expiration timestamp
}
```

#### List Storage
- Dynamic array of string pointers
- Geometric growth strategy (capacity doubles on overflow)
- LPUSH/LPOP operations on head; RPUSH operations on tail

#### Stream Storage
- Rows stored as compact "id<TAB>field<TAB>value" strings
- Stream IDs in format: `<milliseconds>-<sequence>`
- Supports wildcard ID generation (* and <ms>-*)

### Command Execution Flow

```
Client Connection
    ↓
Socket recv() → RESP protocol buffer
    ↓
resp_parse() 
    ├─→ Parse command name from RESP array
    ├─→ Check transaction state
    ├─→ Route to command handler
    └─→ Acquire mutex if data modification
         ↓
    Command Handler (e.g., listMapInsert, listMapAppend)
         ├─→ Validate arguments
         ├─→ Perform operation (may trigger condition broadcast)
         └─→ Release mutex
    ↓
Format RESP response
    ↓
Socket send() → Client receives response
```

### Expiration and TTL

- TTL specified via `EX` (seconds) or `PX` (milliseconds) options in SET command
- **Lazy expiration**: Expired keys are only deleted when accessed
- During reads, `current_time_millis()` checks if key has expired
- If expired, key is deleted and operation treats it as missing

### Blocking Operations

**BLPOP / XREAD BLOCK Implementation**:
1. Worker thread acquires `listmap_mutex`
2. Checks if data exists (predicate check)
3. If data missing and timeout > 0:
   - Calculates deadline from current time
   - Calls `pthread_cond_timedwait()` with deadline
4. On LPUSH/RPUSH/XADD: `pthread_cond_broadcast()` wakes all waiting threads
5. Woken threads recheck predicate (handles spurious wakeups)
6. If deadline reached: return nil response

## Build and Run

### Requirements
- GCC compiler (C11 standard)
- POSIX-compliant operating system (Linux, macOS, etc.)
- `make` build tool

### Compilation
```bash
make clean
make
```

This generates the `redis-c` executable.

### Running the Server
```bash
./redis-c
```

Server listens on `127.0.0.1:6379` and waits for client connections.

### Testing
Use `redis-cli` or any RESP-compatible client:
```bash
redis-cli -p 6379
> PING
> SET mykey myvalue
> GET mykey
> LPUSH mylist item1 item2
> LRANGE mylist 0 -1
```

## Performance Considerations

1. **Single Mutex Design**: All operations serialize through `listmap_mutex`
   - Simplicity and correctness over maximum parallelism
   - Adequate for moderate load with typical commands

2. **Dynamic Array Growth**: Lists/streams use exponential capacity growth
   - O(1) amortized append performance
   - Reduces reallocation frequency

3. **Lazy Expiration**: No background cleanup thread
   - Reduces CPU overhead
   - Memory reclaimed on key access

4. **Thread-per-Client**: Detached worker threads handle connections
   - One thread blocks on client I/O without affecting others
   - Scales to moderate concurrent connections

## Limitations

- **Fixed Capacity**: ListMap has hardcoded maximum entry count
- **Partial Redis Compatibility**: Limited command set vs full Redis
- **No Persistence**: All data lost on shutdown
- **No Clustering**: Single-server only
- **No Authentication**: No access control

## Code Organization

```
Redis-C/
├── src/
│   ├── main.c          # Server loop, RESP parser, command handlers
│   └── ListMap.c       # In-memory data store implementation
├── include/
│   └── ListMap.h       # Data structure headers
├── Makefile            # Build configuration
└── README.md           # This file
```

## License

GNU General Public License v3.0 - See LICENSE file for details.

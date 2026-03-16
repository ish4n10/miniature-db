# miniaturedb

A from-scratch, WiredTiger-inspired storage engine and SQL database written in Go. Currently in-development.

# Usage
```cmd
PS G:\miniaturedb> go run .\cmd.go newdb.db

  opened: newdb.db
  type 'help' for commands, 'exit' to quit

mdb> CREATE TABLE newtable
...> ;
OK

mdb> INSERT INTO newtable (name, email) VALUES ('ishan', 'ishantripathi@gmail.com');
OK (1 row affected)

mdb> select * from newtable; 
+-------+-------------------------+
| key   | value                   |
+-------+-------------------------+
| ishan | ishantripathi@gmail.com |
+-------+-------------------------+
1 row(s)
```


## Storage Layer

### Disk Manager
Pages are allocated sequentially in a single file. Page 0 is a descriptor block storing the magic number, version checksum, and the catalog root page ID which is used to reopen an existing database.

### Page Format
Every page starts with a 40-byte header:
```
[0:8]   Recno        record number
[8:16]  WriteGen     write generation (for cache validation)
[16:20] MemSize      in-memory size
[20:24] Entries      cell count
[24]    Type         leaf or internal
[25]    Flags
[26]    (reserved)
[27]    Version
[28:32] NextPageID   sibling pointer (leaf pages only)
[32:40] (reserved)
```

### Buffer Pool
LRU cache of pages, inspired by WiredTiger's `WT_CACHE` / `WT_REF`. Each page slot has a state machine.


## SQL Layer

### Supported SQL
```sql
CREATE TABLE users;
DROP TABLE users;

INSERT INTO users (key, value) VALUES ('user:1', '{"name":"ishan"}');

SELECT * FROM users;
SELECT * FROM users WHERE key = 'user:1';
SELECT * FROM users WHERE key >= 'user:2';
SELECT * FROM users WHERE key > 'user:3';
SELECT * FROM users WHERE key <= 'user:4';
SELECT * FROM users WHERE key < 'user:5';

DELETE FROM users WHERE key = 'user:1';
```
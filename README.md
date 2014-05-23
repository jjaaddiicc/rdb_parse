redis-rdb-parser-java
=====================
redis rdb data format wiki:
https://github.com/sripathikrishnan/redis-rdb-tools/wiki/Redis-RDB-Dump-File-Format

rdb file parse, refer:
https://github.com/antirez/redis/blob/unstable/src/redis-check-dump.c


This is a tool for parsing redis rdb file and restoring the data to redis backup instance. 
All redis data structures(REDIS_TYPE_HASH_ZIPMAP excepted) are supported.

Entry.java -----------redis data after parsing <br>
RDBParser.java -------redis rdb file parser<br>
RDBSaver.java --------save the restored data to redis instance<br>
IRestoreRDB.java -----if you want to customize the rdb restored data, you can implement this interface to handle it.<br>


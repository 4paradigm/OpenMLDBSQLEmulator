# OpenMLDB SQL Emulator

## Build

You can build the emulator without toydb, so you can't `run`, but other cmds are working. If you want `run` with it, check [Run](#run). 

If you want to package with toydb, put toydb_run_engine in `src/main/resources`, and build it.

```bash
# pack without toydb
mvn package -DskipTests

# pack with toydb
cp toydb_run_engine src/main/resources
mvn package
```

The toydb_run_engine build way is:
```
git clone https://github.com/4paradigm/OpenMLDB.git
cd OpenMLDB
make configure
cd build
make toydb_run_engine -j<thread> # minimum build
```

## Version

We use openmldb-jdbc for validations, the version is:
|Emulator Version | OpenMLDB Version|
|--|--|
| 1.0 | 0.8.3 |

Toydb is optional, and you should update it manually, just copy the newer toydb to `/tmp`.

## Run

```bash
java -jar emulator-1.0.jar
```

If emulator jar doesn't have toydb_run_engine, you can copy toydb_run_engine to `/tmp` and run emulator.
```bash
cp toydb_run_engine /tmp
java -jar emulator-1.0.jar
```

## Example - validate SQL

```
# creations - t/addtable: create table
t t1 a int, b int64

# showtables to get all tables in emulator
st

# validate in online batch mode
val select * from t1;

# validate in online request mode
valreq select count(*) over w1 from t1 window w1 as (partition by a order by b rows between unbounded preceding and current row);
```

## Example - run SQL

You should have `toydb_run_engine`, check [Run](#run). And check [run in toydb](#run-in-toydb) for more details about running SQL case.
```
# step 1
gencase
# step 2 modify the yaml file to add table and data

# step 3 load yaml
loadcase
st

# step 4 write a new sql and validate it
valreq select count(*) over w1 from t1 window w1 as (partition by id order by std_ts rows between unbounded preceding and current row);

# step 5 dump the sql you want to run next
dumpcase select count(*) over w1 from t1 window w1 as (partition by id order by std_ts rows between unbounded preceding and current row);

# step 6 run sql in toydb
run
```

## Commands

`#` is comment.

Can't run a command in multi lines, e.g. `val select * from t1;` can't be written as
```
val select *
from
t1;
```

There are some quite useful builtin commands.
```
?help to hint
?list to get all cmds
!run-script filename reads and executes commands from given file.
```

You can run emulator script by `!run-script filename`, and the script file is just a text file with commands in it.
e.g.
```
!run-script src/test/resources/simple.emu
```

Extra:
```
!set-display-time true/false toggles displaying of command execution time. Time is shown in milliseconds and includes only your method's physical time.
!enable-logging filename and !disable-logging control logging, i.e. duplication of all Shell's input and output in a file.
```

Now we have some commands:

### creations

Note that if exists, the table will be replaced. Default db is `emudb`.

- `use <db>` use db, create if not exists
- `addtable <table_name> c1 t1,c2 t2, ...` create/replace table in current db
    - abbreviate: `t <table_name> c1 t1,c2 t2, ...`

- `adddbtable <db_name> <table_name> c1 t1,c2 t2, ...` create/replace table in specified db, if db not exists, create it
    - abbreviate: `dt <table_name> c1 t1,c2 t2, ...`
- `sql <create table sql>` create table by sql

- `showtables` / `st` list all tables

### `genddl <sql>`

If you want to create table without redundant indexes, use `genddl` to generate ddl from the query sql.

But the method `genDDL` in openmldb jdbc hasn't support multi db yet, so we can't use this method to parse sqls that have multi db.

- Example1
```
t t1 a int, b bigint
t t2 a int, b bigint
genddl select *, count(b) over w1 from t1 window w1 as (partition by a order by b rows between 1 preceding and current row)
```
output:
```
CREATE TABLE IF NOT EXISTS t1(
	a int,
	b bigint,
	index(key=(a), ttl=1, ttl_type=latest, ts=`b`)
);
CREATE TABLE IF NOT EXISTS t2(
	a int,
	b bigint
);
```
No deploy about t2, so the sql that create t2 is just a simple create table. And the sql that create t1 is a create table with index.

- Example2
```
t t1 a int, b bigint
t t2 a int, b bigint
genddl select *, count(b) over w1 from t1 window w1 as (union t2 partition by a order by b rows_range between 1d preceding and current row)
```
output:
```
CREATE TABLE IF NOT EXISTS t1(
	a int,
	b bigint,
	index(key=(a), ttl=1440m, ttl_type=absolute, ts=`b`)
);
CREATE TABLE IF NOT EXISTS t2(
	a int,
	b bigint,
	index(key=(a), ttl=1440m, ttl_type=absolute, ts=`b`)
);
```
It's a union window, so the sqls that create t1 and t2 both have index.

### validations

- `val <sql>` validate sql in batch mode, tables should be created before
- `valreq <sql>` validate sql in request mode, tables should be created before
```
t t1 a int, b int64
val select * from t1 where a == 123;
valreq select count(*) over w1 from t1 window w1 as (partition by a order by b rows between unbounded preceding and current row);
```
### run in toydb

`run <yaml_file>` can run a yaml file in toydb, and the yaml file can be generated by `gencase`. Only support one case now.

Each case should have table creations and one sql. We'll run the sql (default mode is `request`, you can set `batch`) and get the result, then compare the result with the expected result.

You can generate such a yaml file to reproduce a bug, and ask for help.

```bash
# step 1
gencase
# step 2 modify the yaml file to add table and data

# step 3 load yaml to get table catalog, then val sql in emulator, or you can skip this step (just write the sql in yaml)
loadcase
valreq <sql>
# dump will rewrite the yaml file, discard the old one, comments will be lost, be careful
dumpcase <sql>

# step 4 run sql in toydb
run
```

Notice: we don't support to add/del table or table data in emulator, you should prepare in yaml file.

## CLI framework

We use cliche to be the CLI interface. See https://code.google.com/archive/p/cliche/wikis/Manual.wiki, check the source in https://github.com/budhash/cliche.

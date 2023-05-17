# LineageX

A Column Level Lineage Graph for Postgres

Have you ever wondered what is the column level relationship among your SQL scripts and base tables? 
Don't worry, this tool is intended to help you by creating an interactive graph on a webpage to explore the 
column level lineage among them(Currently only supports Postgres, other connection types or dialects are under development).

Here is a [demo](https://zshandy.github.io/lineagex-demo/) with the [mimic-iv concepts_postgres](https://github.com/MIT-LCP/mimic-code/tree/main/mimic-iv/concepts_postgres) files([navigation instructions](#how-to-navigate-the-webpage)) and that is created with one line of code:
```python
from lineagex.lineagex import lineagex

lineagex("/path/to/SQL/", "search, path, schemas", "postgresql://username:password@server:port/database")
```
The input can be a path to a SQL file, a path to the folder containing many SQL files or simply a list of SQL strings in Python.
Optionally, you can provide less information with only the SQLs, but providing the "search_path" and database connection is highly recommended for the best result. 
```python
from lineagex.lineagex import lineagex

lineagex("/path/to/SQL/" or [a_list_of_SQL_string])
```

The output would be a output.json and a index.html file in the folder. Start a local http server and you would be able
to see the interactive graph.

## Installation

```bash
pip install lineagex
```

## Parameter and output format
When there are dependencies between the SQL files, please have the first part of the "search_path" being the schema
that the dependant table is created(default is "public"). Also, the name assumption of the table is either the file 
name if there is only 1 SQL in that file or the name extracted from "CREATE TABLE/VIEW".
#### Example:
```SQL
table1.sql - SELECT column1, column2 FROM schema1.other_table WHERE column3 IS NOT NULL;
table2.sql - SELECT column1 AS new_column1, column2 AS new_column2 from schema1.table1;
```
In that example, the call should be like this, note that "schema1" is the first element in the "search path" parameter
```python
lineagex("/path/to/SQL/", "schema1, public", "postgresql://username:password@server:port/database")
```
In the output.json file, it can be read by other programs and analyzed for other uses, the general format is as follows
(using the example from above):
```javascript
{
  schema1.other_table: {
    tables: [], 
    columns: {
      column1: [], column2: [], column3: []
    }, 
    table_name: schema1.other_table
  }, 
  schema1.table1: {
    tables: [schema1.other_table], 
    columns: {
      column1: [schema1.other_table.columns1, schema1.other_table.columns3], column2: [schema1.other_table.columns2, schema1.other_table.columns3]
    }, 
    table_name: schema1.table1
  }, 
  table2: {
    tables: [schema1.table1], 
    columns: {
      new_column1: [schema1.table1.columns1], new_column2: [schema1.table1.column2]
    }, 
    table_name: table2
  }, 
}
```

## How to Navigate the Webpage
![Alt text](/tests/example.png?raw=true "example")
- Start by clicking the star on the right(select) and input a SQL name that you want to start with.
- It should show a table on the canvas with table names and its columns, by clicking the "explore" button on the top right, it will show all the downstream and upstream tables that are related to the columns.
- Hovering over a column will highlight its downstream and upstream columns as well.
- You can navigate through the canvas by clicking "explore" on other tables.
- The buttons on the right from top to bottom are: 
  - center the lineage to the middle
  - zoom out
  - zoom in
  - select, to search the targeted table and begin the lineage tracing
  - expand all columns for all table, CAUTION: this might hinder performance if there are many tables
  - explore all lineage, this would trace all downstream and upstream tables recursively and all columns are shrunk by default for performance

## FAQ
- `"not init data"` in the webpage:
Possibly due to the content of the JSON in the index.html, please check if it is in valid JSON format, and that all keys are in string format.
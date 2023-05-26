# Basic usage
LineageX provides you with the column level lineage graph with minimum amount of code and flexible input formats.

## API
```python
lineagex.lineagex(sql: Union[List, str], target_schema: Optional[str] = "", conn_string: Optional[str] = None, search_path_schema: Optional[str] = "")
```

## Parameters
- `sql: Union[List, str]`: The input of the SQL files, it can be a path to a file, a path to a folder containing SQL files, a list of SQLs or a list of view names and/or schemas
- `target_schema: Optional[str] = ""`: The schema where the SQL files would be created, defaults to `public`, or the first schema in the `search_path_schema` if provided
- `conn_string: Optional[str] = None`: The `postgres` connection string in the format `postgresql://username:password@server:port/database`, defaults to `None`
- `search_path_schema: Optional[str] = ""`: The `SET search_path TO ...` schemas, defaults to `public` or the `target_schema` if provided

The conn_string to the database is optional, but it is highly recommended to provide the connection for the best result.
Here is a [live demo](https://zshandy.github.io/lineagex-demo/) with the [mimic-iv concepts_postgres](https://github.com/MIT-LCP/mimic-code/tree/main/mimic-iv/concepts_postgres) files([navigation instructions](https://sfu-db.github.io/lineagex/output.html))

## Examples
- This is a generic example, but there are also included examples in the package
### Example SQL:
  ```SQL
  table1.sql - SELECT column1, column2 FROM schema1.other_table WHERE column3 IS NOT NULL;
  table2.sql - SELECT column1 AS new_column1, column2 AS new_column2 from schema1.table1;
  ```
### Example function call:
  ```python
  from lineagex.lineagex import lineagex
  lineagex(sql=path/to/sql, target_schema="schema1", search_path_schema="schema1, public")
  # Other alternative ways of calling, like a list of SQL
  lineagex(sql=[list_of_sql], target_schema="schema1", search_path_schema="schema1, public") 
  # Schema and view name, this would output lineage information for all views in schema1 and schema2.view2
  lineagex(sql="schema1, schema2.view2", conn_string="postgresql://username:password@server:port/database") 
  ```

### For included examples, there are 4, the `dependency_example`, `github_example`, [mimic-iii](https://github.com/MIT-LCP/mimic-code/tree/main/mimic-iii/concepts_postgres), [mimic-iv](https://github.com/MIT-LCP/mimic-code/tree/main/mimic-iv/concepts_postgres)

  ```python
  from lineagex.example import example

  example("dependency_example")
  example("github_example") ## this is the example from above
  example("mimic-iii")
  example("mimic-iv")
  ```

For output and navigation details, please view [here](https://sfu-db.github.io/lineagex/output.html)
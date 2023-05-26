# Output and Navigation
 
## Example SQL:
  ```SQL
  table1.sql - SELECT column1, column2 FROM schema1.other_table WHERE column3 IS NOT NULL;
  table2.sql - SELECT column1 AS new_column1, column2 AS new_column2 from schema1.table1;
  ```

## Outputs
It would output the output.json and index.html. The output.json contains the lineage information and can be read by other programs for analytics, the general format is as follows:
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
As for the index.html, you can start a php server in the folder and view it in your browser, usually `php -S localhost:8000` and view at `localhost:8000`
<img src="https://raw.githubusercontent.com/sfu-db/lineagex/main/tests/example.gif"/>
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
- There is also a minimap on the bottom right to show where the tables are
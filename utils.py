import re
import os
import ast
import json
from psycopg2.extensions import connection
from typing import Tuple, List, Optional


def remove_comments(str1: Optional[str] = "") -> str:
    """
    Remove comments/excessive spaces/"create table as"/"create view as" from the sql file
    :param str1: the original sql
    :return: the parsed sql
    """
    # remove the /* */ comments
    q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", str1)
    # remove whole line -- and # comments
    lines = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]
    # remove trailing -- and # comments
    q = " ".join([re.split("--|#", line)[0] for line in lines])
    # replace all spaces around commas
    q = re.sub(r"\s*,\s*", ",", q)
    # replace all multiple spaces to one space
    str1 = re.sub("\s\s+", " ", q)
    str1 = str1.replace("\n", " ").strip()
    return str1


def find_column(
    table_name: Optional[str] = "", engine: connection = None, search_schema: Optional[str] = ""
) -> List:
    """
    Find the columns for the base table in the database
    :param search_schema: the schemas for SET search_path
    :param engine: the connection engine
    :param table_name: the base table name
    :return: the list of columns in the base table
    """
    cur = engine.cursor()
    cur.execute("""SET search_path TO {};""".format(search_schema))
    cur.execute(
        """SELECT attname AS col
        FROM   pg_attribute
        WHERE  attrelid = '{}'::regclass  -- table name optionally schema-qualified
        AND    attnum > 0
        AND    NOT attisdropped
        ORDER  BY attnum;
         ;""".format(
            table_name
        )
    )
    result = cur.fetchall()
    cur.close()
    return [s[0] for s in result]


def get_files(path: Optional[str] = "") -> List:
    """
    Extracting all files from the directory or just put the file name in a list
    :param path: path to the file/directory
    :return: all the files in the directory and its subdirectory
    """
    if os.path.isfile(path):
        sql_files = [path]
    elif os.path.isdir(path):
        sql_files = []
        for path, subdirs, files in os.walk(path):
            for name in files:
                if name.endswith(".sql") or name.endswith(".SQL"):
                    sql_files.append(os.path.join(path, name))
    else:
        sql_files = []
    return sql_files


def find_select(q: Optional[str] = "") -> str:
    """
    Find where the first SELECT starts
    :param q: the input sql
    :return: the sql with the first SELECT as the start
    """
    if q[-1] == ";":
        q = q[:-1]
    if q.upper().find("SELECT ") != -1:
        idx = q.find("SELECT ")
        if idx == 0:
            q = q
        else:
            if q[idx - 1] == "(":
                # to resolve if the SELECT is wrapped around brackets
                q = q[idx:-1]
            else:
                q = q[idx:]
    else:
        q = q
    return q


def produce_json(
    output_dict: Optional[dict] = None, engine: connection = None, search_schema: Optional[str] = ""
) -> dict:
    """
    Product the output.json and put into the html
    :param output_dict: the parsed object with column level lineage
    :param engine: db connection
    :param search_schema: search schemas for db
    :return: the output.json format with information about the base table
    """
    # Get all the table names that are not in the output_dict(mostly base tables)
    all_tables = []
    for key, val in output_dict.items():
        all_tables.extend(val['tables'])
    all_tables = list(set(all_tables) - set(output_dict.keys()))
    base_table_dict = {}
    # If no conn is provided, try to guess the base table's columns
    base_table_noconn_dict = {}
    if not engine and not search_schema:
        base_table_noconn_dict = _guess_base_table(output_dict=output_dict)
    # Iterate through the base tables, and add into the output_dict
    for t in all_tables:
        base_table_dict[t] = {}
        base_table_dict[t]["tables"] = [""]
        base_table_dict[t]["columns"] = {}
        # if db conn is provided
        if engine and search_schema:
            if t.endswith("_ANALYZED"):
                cols = find_column(table_name=t[:-9], engine=engine, search_schema=search_schema)
            else:
                cols = find_column(table_name=t, engine=engine, search_schema=search_schema)
        else:
            cols = base_table_noconn_dict.get(t, [])
        for i in cols:
            base_table_dict[t]["columns"][i] = [""]
        base_table_dict[t]["table_name"] = str(t)
    base_table_dict.update(output_dict)
    with open("output.json", "w") as outfile:
        json.dump(base_table_dict, outfile)
    _produce_html(output_json=str(base_table_dict).replace("'", '"'))
    return base_table_dict


def _guess_base_table(output_dict: Optional[dict] = None) -> dict:
    """
    Try to guess the base table columns when no db connection is given
    :param output_dict: the output dict with the lineage information
    :return: the guessed columns for the base tables
    """
    base_table_noconn_dict = {}
    for key, val in output_dict.items():
        for col_val in val["columns"].values():
            for t in col_val:
                idx = t.rfind(".")
                if t[:idx] in base_table_noconn_dict.keys():
                    if t[idx + 1:] not in base_table_noconn_dict[t[:idx]]:
                        base_table_noconn_dict[t[:idx]].append(t[idx + 1:])
                else:
                    base_table_noconn_dict[t[:idx]] = [t[idx + 1:]]
    return base_table_noconn_dict


def _produce_html(output_json: Optional[str] = "") -> None:
    """
    Produce the html file for viewing
    :param output_json: the final output.json file
    """
    # Creating the HTML file
    file_html = open("index.html", "w", encoding="utf-8")
    # Adding the input data to the HTML file
    file_html.write(
        """<!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <meta http-equiv="X-UA-Compatible" content="ie=edge">
    </head>
    <body>
      <script>
        window.inlineSource = `{}`;
      </script>
      <div id="main"></div>
    <script type="text/javascript" src="vendor.js"></script><script type="text/javascript" src="app.js"></script></body>
    </html>""".format(
            output_json
        )
    )
    # Saving the data into the HTML file
    file_html.close()


if __name__ == "__main__":
    pass

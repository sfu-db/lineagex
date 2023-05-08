import re
import os
import ast
import json
from psycopg2.extensions import connection
from typing import Tuple, List


def find_column(
    table_name: str = "", engine: connection = None, search_schema: str = ""
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


def get_files(path: str = "") -> List:
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


def find_select(q: str = "") -> str:
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
    output_dict: dict = None, engine: connection = None, search_schema: str = ""
) -> dict:
    table_to_model_dict = {}
    for key, val in output_dict.items():
        table_to_model_dict[val["table_name"]] = key

    dep_dict = {}
    for key, val in output_dict.items():
        if key not in dep_dict.keys():
            dep_dict[key] = {}
            dep_dict[key]["upstream_tables"] = val["tables"]
        else:
            dep_dict[key]["upstream_tables"] = val["tables"]
        for key_name in val["tables"]:
            # key_name = table_to_model_dict.get(i, i)
            if key_name not in dep_dict.keys():
                dep_dict[key_name] = {}
                dep_dict[key_name]["downstream_tables"] = [key]
            else:
                if "downstream_tables" not in dep_dict[key_name].keys():
                    dep_dict[key_name]["downstream_tables"] = [key]
                else:
                    dep_dict[key_name]["downstream_tables"].append(key)
    base_table_dict = {}
    for key, val in dep_dict.items():
        if "upstream_tables" not in list(val.keys()):
            val["upstream_tables"] = []
        if "downstream_tables" not in list(val.keys()):
            val["downstream_tables"] = []
        if key in list(output_dict.keys()):
            val["is_model"] = True
        else:
            base_table_dict[key] = {}
            base_table_dict[key]["tables"] = [""]
            base_table_dict[key]["columns"] = {}
            if key.endswith("_ANALYZED"):
                cols = find_column(key[:-9], engine, search_schema)
            else:
                cols = find_column(key, engine, search_schema)
            for i in cols:
                base_table_dict[key]["columns"][i] = [""]
            base_table_dict[key]["table_name"] = str(key)
            val["is_model"] = False
    base_table_dict.update(output_dict)
    with open("output.json", "w") as outfile:
        json.dump(base_table_dict, outfile)
    _produce_html(output_json=str(base_table_dict).replace("'", '"'))
    return base_table_dict


def _produce_html(output_json: str = ""):
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

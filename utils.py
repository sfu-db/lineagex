import re
import ast
import json
from psycopg2.extensions import connection
from typing import Tuple, List


def _remove_comments(str1: str = "") -> str:
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


def _preprocess_sql(org_sql: str = "", schema_list: List = None) -> str:
    """
    Process the sql, remove database name in the clause/datetime_add/datetime_sub adding quotes
    :param org_sql: the original sql
    :return: None
    """
    ret_sql = _remove_comments(str1=org_sql)
    ret_sql = ret_sql.replace("`", "")
    # remove any database names in the query
    for i in schema_list:
        ret_sql = re.sub("[^ (,]*(\.{}\.)".format(i), "{}.".format(i), ret_sql)
    ret_sql = re.sub(
        r"DATETIME_DIFF\((.+?),\s?(.+?),\s?(DAY|MINUTE|SECOND|HOUR|YEAR)\)",
        r"DATETIME_DIFF(\1, \2, '\3'::TEXT)",
        ret_sql,
    )
    ret_sql = re.sub("datetime_add", "DATETIME_ADD", ret_sql, flags=re.IGNORECASE)
    ret_sql = re.sub("datetime_sub", "DATETIME_SUB", ret_sql, flags=re.IGNORECASE)
    # DATETIME_ADD '' value
    dateime_groups = re.findall(
        r"DATETIME_ADD\(\s?(.+?),\s?INTERVAL\s?(.+?)\s?(DAY|MINUTE|SECOND|HOUR|YEAR)\)",
        ret_sql,
    )
    if dateime_groups:
        for i in dateime_groups:
            if not i[1].startswith("'") and not i[1].endswith("'"):
                ret_sql = ret_sql.replace(
                    "DATETIME_ADD({},INTERVAL {} {})".format(i[0], i[1], i[2]),
                    "DATETIME_ADD({},INTERVAL '{}' {})".format(i[0], i[1], i[2]),
                )
            else:
                continue
    # DATETIME_SUB '' value
    dateime_sub_groups = re.findall(
        r"DATETIME_SUB\(\s?(.+?),\s?INTERVAL\s?(.+?)\s?(DAY|MINUTE|SECOND|HOUR|YEAR)\)",
        ret_sql,
    )
    if dateime_sub_groups:
        for i in dateime_sub_groups:
            if not i[1].startswith("'") and not i[1].endswith("'"):
                ret_sql = ret_sql.replace(
                    "DATETIME_SUB({},INTERVAL {} {})".format(i[0], i[1], i[2]),
                    "DATETIME_SUB({},INTERVAL '{}' {})".format(i[0], i[1], i[2]),
                )
            else:
                continue
    return ret_sql


def _find_column(
    table_name: str = "", engine: connection = None, search_schema: str = ""
) -> List:
    """
    Find the columns for the base table in the database
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


def _produce_json(
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
                cols = _find_column(key[:-9], engine, search_schema)
            else:
                cols = _find_column(key, engine, search_schema)
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

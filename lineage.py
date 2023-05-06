import os
import json
import re
from psycopg2 import OperationalError
import psycopg2
from psycopg2.extensions import connection
from column_lineage import ColumnLineage
from utils import _preprocess_sql, _produce_json
from typing import List, Tuple

rem_regex = re.compile(r"[^a-zA-Z0-9_.]")


class Lineage:
    def __init__(
        self,
        url: str = "",
        username: str = "",
        password: str = "",
        sql_list: List[str] = None,
        search_schema: str = "",
    ) -> None:
        self.part_tables = None
        self.conn_string = (
            url.split("//")[0]
            + "//"
            + username
            + ":"
            + password
            + "@"
            + url.split("//")[1]
        )
        self.sql_list = sql_list
        self.search_schema = search_schema
        self.output_dict = {}
        self.analyze_list = []
        self.insertion_dict = {}
        self.deletion_dict = {}
        self.curr_name = ""
        self._run_lineage()

    def _run_lineage(self) -> None:
        """
        Start the column lineage call
        :return: the output_dict object will be the final output with each model name being key
        """
        self.conn = self._check_db_connection()
        self.part_tables, self.schema_list = self._get_part_tables()
        for sql in self.sql_list:
            sql = self._remove_comments(sql, self.sql_list.index(sql))
            if not sql.startswith("ANALYZE "):
                cur = self.conn.cursor()
                cur.execute("""SET search_path TO {};""".format(self.search_schema))
                cur.execute(
                    """EXPLAIN (VERBOSE TRUE, FORMAT JSON, COSTS FALSE) {}""".format(
                        sql
                    )
                )
                log_plan = cur.fetchall()
                cur.close()
                while True:
                    if isinstance(log_plan, list) or isinstance(log_plan, tuple):
                        log_plan = log_plan[0]
                    elif isinstance(log_plan, dict):
                        log_plan = log_plan["Plan"]
                        break
                col_lineage = ColumnLineage(
                    plan=log_plan,
                    sql=sql,
                    table_name=self.curr_name,
                    conn=self.conn,
                    part_tables=self.part_tables,
                    search_schema=self.search_schema,
                )
                self.output_dict[self.curr_name] = {}
                if not str(self.curr_name).endswith("_ANALYZED"):
                    self.output_dict[self.curr_name]["tables"] = col_lineage.table_list
                    self.output_dict[self.curr_name][
                        "columns"
                    ] = col_lineage.column_dict
                    self.output_dict[self.curr_name]["table_name"] = self.curr_name
        # To sub all the names that is after the ANALYZED query
        for name in self.analyze_list:
            for key, value in self.output_dict.copy().items():
                self.output_dict[key]["tables"] = [
                    x + "_ANALYZED" if x == name else x for x in value["tables"]
                ]
                for col_key, col_value in value["columns"].copy().items():
                    self.output_dict[key]["columns"][col_key] = [
                        x.split(".")[0]
                        + "."
                        + x.split(".")[1]
                        + "_ANALYZED."
                        + x.split(".")[2]
                        if x.split(".")[0] + "." + x.split(".")[1] == name
                        else x
                        for x in col_value
                    ]

        _produce_json(self.output_dict, self.conn, self.search_schema)
        self.conn.close()

    def _check_db_connection(self) -> connection:
        """
        Check if the conn_string is good
        :return: the sqlalchemy engine
        """
        try:
            psycopg2.connect(self.conn_string)
            print("database connected")
        except OperationalError:
            print("authentication error")
        return psycopg2.connect(self.conn_string)

    def _get_part_tables(self) -> Tuple[dict, List]:
        """
        Find out the partition table in the Postgres database
        :return: the result with the parent and partitioned table
        """
        cur = self.conn.cursor()
        cur.execute(
            """SELECT
                concat_ws('.', nmsp_parent.nspname, parent.relname) AS parent,
                concat_ws('.', nmsp_child.nspname, child.relname) AS child
            FROM pg_inherits
                JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
                JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
                JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
                JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace"""
        )
        ret = cur.fetchall()
        parent_dict = {}
        for i in ret:
            parent_dict[i[1]] = i[0]
        # get the schema list as well
        cur.execute("select nspname from pg_catalog.pg_namespace;")
        result = cur.fetchall()
        schema_list = [s[0] for s in result]
        cur.close()
        return parent_dict, schema_list

    def _remove_comments(self, input_str, idx):
        q = _preprocess_sql(input_str, self.schema_list)
        # adjust to INSERT/DELETE/SELECT/
        if q.find("INSERT INTO") != -1:
            # find the current name in the insertion dict and how many times it has been inserted
            self.curr_name = re.sub(rem_regex, "", q.split(" ")[2])
            if self.curr_name not in self.insertion_dict.keys():
                self.insertion_dict[self.curr_name] = 1
            else:
                self.insertion_dict[self.curr_name] = (
                    self.insertion_dict[self.curr_name] + 1
                )
            insert_counter = self.insertion_dict[self.curr_name]
            self.curr_name = self.curr_name + "_INSERTION_{}".format(insert_counter)
            q = self._find_select(q)
        elif q.find("DELETE FROM") != -1:
            # find the current name in the insertion dict and how many times it has been deleted
            self.curr_name = re.sub(rem_regex, "", q.split(" ")[2])
            if self.curr_name not in self.deletion_dict.keys():
                self.deletion_dict[self.curr_name] = 1
            else:
                self.deletion_dict[self.curr_name] = (
                    self.deletion_dict[self.curr_name] + 1
                )
            delete_counter = self.deletion_dict[self.curr_name]
            self.curr_name = self.curr_name + "_DELETION_{}".format(delete_counter)
            q = self._find_select(q)
        elif q.find("COPY") != -1:
            self.curr_name = q.split(" ")[1]
        elif q.find("ANALYZE") != -1:
            self.curr_name = q.split(" ")[1]
            self.curr_name = re.sub(rem_regex, "", self.curr_name)
            if self.curr_name not in self.analyze_list:
                self.analyze_list.append(self.curr_name)
            # Change the name of the table to ANALYZED
            for key, value in self.output_dict.copy().items():
                self.output_dict[key]["tables"] = [
                    x + "_ANALYZED" if x == self.curr_name else x
                    for x in value["tables"]
                ]
                for col_key, col_value in value["columns"].copy().items():
                    self.output_dict[key]["columns"][col_key] = [
                        x.split(".")[0]
                        + "."
                        + x.split(".")[1]
                        + "_ANALYZED."
                        + x.split(".")[2]
                        if x.split(".")[0] + "." + x.split(".")[1] == self.curr_name
                        else x
                        for x in col_value
                    ]
            if (
                self.curr_name + "_ANALYZED" not in self.output_dict.keys()
                and self.curr_name in self.output_dict.keys()
            ):
                self.output_dict[self.curr_name + "_ANALYZED"] = self.output_dict[
                    self.curr_name
                ]
                self.output_dict.pop(self.curr_name, None)
            self.curr_name = self.curr_name + "_ANALYZED"
        else:
            self.curr_name = str(idx)
        return q

    def _find_select(self, q):
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


if __name__ == "__main__":
    pass

import os
import re
import psycopg2
from psycopg2 import OperationalError
from psycopg2.extensions import connection
from typing import Tuple, List, Optional, Union

from lineagex.utils import produce_json
from lineagex.SqlToDict import SqlToDict
from lineagex.stack import *
from lineagex.ColumnLineage import ColumnLineage


class LineageXWithConn:
    def __init__(
        self,
        path: Optional[Union[List, str]] = None,
        search_schema: Optional[str] = "public",
        url: Optional[str] = "",
        username: Optional[str] = "",
        password: Optional[str] = "",
    ) -> None:
        self.part_tables = None
        self.df = None
        self.conn_string = (
            url.split("//")[0]
            + "//"
            + username
            + ":"
            + password
            + "@"
            + url.split("//")[1]
        )
        self.schema = search_schema.split(",")[0]
        self.search_schema = search_schema
        self.new_view_list = []
        self.s = Stack()
        self.path = path
        self.sql_files_dict = {}
        self.creation_list = []
        self.finished_list = []
        self.output_dict = {}
        self._run_table_lineage()

    def _run_table_lineage(self) -> None:
        """
        The driver function to extract the table lineage information
        :return: output an interactive html for the table lineage information
        """
        self.conn = self._check_db_connection()
        self.conn.autocommit = True
        self.part_tables, self.schema_list = self._get_part_tables()
        self.sql_files_dict = SqlToDict(self.path, self.schema_list).sql_files_dict
        for name, sql in self.sql_files_dict.items():
            try:
                if name not in self.finished_list:
                    self._explain_sql(name=name, sql=sql)
                else:
                    continue
            except Exception as e:
                print("{} is not processed because it countered {}".format(name, e))
                continue
        produce_json(output_dict=self.output_dict, engine=self.conn, search_schema=self.search_schema)
        self._delete_view()
        self.conn.close()

    def _delete_view(self) -> None:
        """
        Delete all temporary tables in the new_view_list
        :return: None
        """
        # reverse it just in case to drop dependencies first
        self.new_view_list = self.new_view_list[::-1]
        cur = self.conn.cursor()
        for i in self.new_view_list:
            cur.execute("""DROP TABLE {} CASCADE""".format(i))
            print(i + " dropped")
        cur.close()

    def _create_view(self, name: Optional[str] = "", sql: Optional[str] = "") -> None:
        """
        Create temporary tables with no data from the given sql
        :param name: name of the table
        :param sql: the sql for the table
        :param conn: the psycopg2 connection it was using
        :return: None
        """
        # connect and create view
        cur = self.conn.cursor()
        cur.execute(
            """SET search_path TO {}, {};""".format(self.search_schema, self.schema)
        )
        if sql.endswith(";"):
            cur.execute(
                """CREATE TABLE {}.{} AS {} WITH NO DATA;""".format(
                    self.schema, name, sql[:-1]
                )
            )
        else:
            cur.execute(
                """CREATE TABLE {}.{} AS {} WITH NO DATA;""".format(
                    self.schema, name, sql
                )
            )
        cur.close()
        print(self.schema + "." + name + " created")

    def _explain_sql(self, name: Optional[str] = "", sql: Optional[str] = "") -> None:
        """
        Main function for extracting the table name from the sql. It tries to explain the current file's sql by
        analyzing the logical plan. But if its dependency is missing, the current one is put onto a stack and checking
        on the dependency table, then pops the stack when the current one is done.
        :param name: name of the file
        :param sql: the sql from the file
        :return: updates file_list, sql_list, table_list, new_view_list
        """
        try:
            print(name)
            cur = self.conn.cursor()
            cur.execute("""SET search_path TO {};""".format(self.search_schema))
            cur.execute(
                """EXPLAIN (VERBOSE TRUE, FORMAT JSON, COSTS FALSE) {}""".format(sql)
            )
            log_plan = cur.fetchall()
            cur.close()
            while True:
                if isinstance(log_plan, list) or isinstance(log_plan, tuple):
                    log_plan = log_plan[0]
                elif isinstance(log_plan, dict):
                    log_plan = log_plan["Plan"]
                    break
            if name in self.creation_list:
                self._create_view(name=name, sql=sql)
                self.new_view_list.append(self.schema + "." + name)
                self.creation_list.pop(self.creation_list.index(name))
                table_name = self.schema + "." + name
            else:
                cur = self.conn.cursor()
                cur.execute("""SET search_path TO {};""".format(self.search_schema))
                cur.execute(
                    """SELECT CONCAT (schemaname,'.', tablename) from pg_tables WHERE schemaname = ANY('{{{0}}}') and tablename = '{1}'""".format(
                        self.search_schema, name
                    )
                )
                table_name = cur.fetchone()
                cur.close()
                if table_name:
                    table_name = table_name[0]
                else:
                    table_name = self.schema + "." + name
            col_lineage = ColumnLineage(
                plan=log_plan,
                sql=sql,
                table_name=table_name,
                conn=self.conn,
                part_tables=self.part_tables,
                search_schema=self.search_schema,
            )
            self.output_dict[table_name] = {
                "tables": col_lineage.table_list,
                "columns": col_lineage.column_dict,
                "table_name": table_name,
            }
            self.finished_list.append(name)
            while not self.s.isEmpty():
                if self.s.peek() in self.sql_files_dict.keys():
                    sql = self.sql_files_dict[self.s.peek()]
                    return self._explain_sql(name=self.s.pop(), sql=sql)
                else:
                    print("no sql file for the dependencies")
            return
        except psycopg2.ProgrammingError as e:
            # does not exist error code
            if e.pgcode == "42P01":
                error_msg = e.pgerror
                no_find_idx = error_msg.find("does not exist")
                relation_idx = error_msg.find("relation")
                schema_table = error_msg[relation_idx:no_find_idx]
                table_name = schema_table.split(" ")[-2].split(".")[-1].strip('\"')
                self.s.push(name)
                #print(table_name, self.sql_files_dict)
                if table_name in self.sql_files_dict.keys():
                    if self.schema + "." + table_name in self.new_view_list:
                        print(
                            "{}.{} is already created, but the created schema is different from the queried schema for {} in {}.sql".format(
                                self.schema, table_name, table_name, name
                            )
                        )
                        return
                    print(
                        name
                        + " is dependant on "
                        + table_name
                        + ", creating that first\n"
                    )
                    self.creation_list.append(table_name)
                    sql = self.sql_files_dict[table_name]
                    return self._explain_sql(name=table_name, sql=sql)
                else:
                    print(
                        name
                        + " is skipped because it is missing dependency table "
                        + table_name
                    )
                    return
            else:
                print(e)
        except Exception as e:
            print(e)

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


if __name__ == "__main__":
    pass

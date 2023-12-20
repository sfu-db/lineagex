from typing import List, Optional, Tuple, Union
import time

import psycopg2
from psycopg2 import OperationalError
from psycopg2.extensions import connection

from .ColumnLineage import ColumnLineage
from .SqlToDict import SqlToDict
from .stack import *
from .utils import find_column, produce_json


class LineageXWithConn:
    def __init__(
        self,
        sql: Optional[Union[List, str]] = None,
        target_schema: Optional[str] = "public",
        conn_string: Optional[str] = "",
        search_path_schema: Optional[str] = "public",
    ) -> None:
        self.transaction_time = 0
        self.part_tables = None
        self.df = None
        self.schema = target_schema
        self.search_schema = target_schema + "," + search_path_schema
        self.new_view_list = []
        self.s = Stack()
        self.sql = sql
        self.sql_files_dict = {}
        self.creation_list = []
        self.finished_list = []
        self.output_dict = {}
        self.conn = self._check_db_connection(conn_string)
        self.conn.autocommit = True
        self._run_table_lineage()

    def _run_table_lineage(self) -> None:
        """
        The driver function to extract the table lineage information
        :return: output an interactive html for the table lineage information
        """
        self.part_tables, self.schema_list = self._get_part_tables()
        # If the input is a list with no SELECT , assume it to be a list of views/schema
        if isinstance(self.sql, List) and not any(
            "SELECT " in s.upper() for s in self.sql
        ):
            cur = self.conn.cursor()
            cur.execute("""SET search_path TO {};""".format(self.search_schema))
            for t in self.sql:
                temp = t.split(".")
                if len(temp) == 2:
                    cur.execute(
                        "SELECT concat_ws('.',schemaname,viewname) AS view_name, definition FROM pg_catalog.pg_views WHERE schemaname = '{}' and viewname = '{}';".format(
                            temp[0], temp[1]
                        )
                    )
                    view_ret = cur.fetchall()
                    if view_ret:
                        self.sql_files_dict[view_ret[0][0]] = view_ret[0][1]
                    else:
                        print(
                            "{} is skipped because database returned no result on it".format(
                                t
                            )
                        )
                elif len(temp) == 1:
                    cur.execute(
                        "SELECT concat_ws('.',schemaname,viewname) AS view_name, definition FROM pg_catalog.pg_views WHERE schemaname = '{}';".format(
                            temp[0]
                        )
                    )
                    schema_ret = cur.fetchall()
                    if schema_ret:
                        for s in schema_ret:
                            self.sql_files_dict[s[0]] = s[1]
                    else:
                        print(
                            "{} is skipped because database returned no result on it".format(
                                t
                            )
                        )
            cur.close()
            for name, sql in self.sql_files_dict.items():
                try:
                    print(name, " processing")
                    col_lineage = ColumnLineage(
                        plan=self._get_plan(sql=sql),
                        sql=sql,
                        columns=find_column(
                            table_name=name,
                            engine=self.conn,
                            search_schema=self.search_schema,
                        ),
                        conn=self.conn,
                        part_tables=self.part_tables,
                        search_schema=self.search_schema,
                    )
                    self.output_dict[name] = {
                        "tables": col_lineage.table_list,
                        "columns": col_lineage.column_dict,
                        "table_name": name,
                    }
                except Exception as e:
                    print("{} is not processed because it countered {}".format(name, e))
                    continue
        # path or a list of SQL that at least one element contains
        else:
            self.sql_files_dict = SqlToDict(self.sql, self.schema_list).sql_files_dict
            for name, sql in self.sql_files_dict.items():
                try:
                    if name not in self.finished_list:
                        self._explain_sql(name=name, sql=sql)
                    else:
                        continue
                except Exception as e:
                    print("{} is not processed because it countered {}".format(name, e))
                    continue
        produce_json(
            output_dict=self.output_dict,
            engine=self.conn,
            search_schema=self.search_schema,
        )
        self._delete_view()
        self.conn.close()
        #print("total transaction time: ", self.transaction_time)

    def _delete_view(self) -> None:
        """
        Delete all temporary tables in the new_view_list
        :return: None
        """
        # reverse it just in case to drop dependencies first
        self.new_view_list = self.new_view_list[::-1]
        start_time = time.time()
        cur = self.conn.cursor()
        for i in self.new_view_list:
            cur.execute("""DROP TABLE {} CASCADE""".format(i))
            print(i + " dropped")
        cur.close()
        self.transaction_time += (time.time() - start_time)

    def _create_view(self, name: Optional[str] = "", sql: Optional[str] = "") -> None:
        """
        Create temporary tables with no data from the given sql
        :param name: name of the table
        :param sql: the sql for the table
        :param conn: the psycopg2 connection it was using
        :return: None
        """
        # connect and create view
        start_time = time.time()
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
        self.transaction_time += (time.time() - start_time)
        print(self.schema + "." + name + " created")

    def _get_plan(self, sql: Optional[str] = "") -> dict:
        """
        Get the plan by providing the sql
        :param sql: the sql for getting the plan
        :return: the physical plan of the sql
        """
        start_time = time.time()
        cur = self.conn.cursor()
        cur.execute("""SET search_path TO {};""".format(self.search_schema))
        cur.execute(
            """EXPLAIN (VERBOSE TRUE, FORMAT JSON, COSTS FALSE) {}""".format(sql)
        )
        log_plan = cur.fetchall()
        cur.close()
        self.transaction_time += (time.time() - start_time)
        while True:
            if isinstance(log_plan, list) or isinstance(log_plan, tuple):
                log_plan = log_plan[0]
            elif isinstance(log_plan, dict):
                log_plan = log_plan["Plan"]
                break
        return log_plan

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
            print(name + " processing")
            if name in self.creation_list:
                self._create_view(name=name, sql=sql)
                self.new_view_list.append(self.schema + "." + name)
                self.creation_list.pop(self.creation_list.index(name))
                table_name = self.schema + "." + name
            elif name.isnumeric():
                table_name = "lineagex_temp_{}".format(name)
                self._create_view(name=table_name, sql=sql)
                self.new_view_list.append(self.schema + "." + table_name)
            elif name.find("_DELETION_") != -1 or name.find("_INSERTION_") != -1:
                table_name = name.replace(".", "_")
                self._create_view(name=table_name, sql=sql)
                self.new_view_list.append(self.schema + "." + table_name)
            else:
                cur = self.conn.cursor()
                cur.execute("""SET search_path TO {};""".format(self.search_schema))
                cur.execute(
                    """SELECT CONCAT (schemaname,'.', tablename) from pg_tables WHERE schemaname = '{0}' and tablename = '{1}'""".format(
                        self.schema, name
                    )
                )
                table_result = cur.fetchone()
                cur.close()
                if not table_result:
                    self._create_view(name=name, sql=sql)
                    self.new_view_list.append(self.schema + "." + name)
                table_name = self.schema + "." + name
            cols = find_column(
                table_name=table_name,
                engine=self.conn,
                search_schema=self.search_schema,
            )
            col_lineage = ColumnLineage(
                plan=self._get_plan(sql=sql),
                sql=sql,
                columns=cols,
                conn=self.conn,
                part_tables=self.part_tables,
                search_schema=self.search_schema,
            )
            if name.isnumeric() or name.find("_DELETION_") != -1 or name.find("_INSERTION_") != -1:
                table_name = name
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
                table_name = schema_table.split(" ")[-2].split(".")[-1].strip('"')
                self.s.push(name)
                # print(table_name, self.sql_files_dict)
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

    def _check_db_connection(self, conn_string: Optional[str] = "") -> connection:
        """
        Check if the conn_string is good
        :return: the sqlalchemy engine
        """
        try:
            psycopg2.connect(conn_string)
            print("database connected")
        except OperationalError:
            print("authentication error")
        return psycopg2.connect(conn_string)

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

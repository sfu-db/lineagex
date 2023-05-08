import os
import re
import psycopg2
from psycopg2 import OperationalError
from psycopg2.extensions import connection
from stack import *
from column_lineage import ColumnLineage
from typing import Tuple, List, Any
from utils import get_files, find_select, produce_json

rem_regex = re.compile(r"[^a-zA-Z0-9_.]")


class TableLineage:
    def __init__(
        self,
        path: str = "",
        url: str = "",
        username: str = "",
        password: str = "",
        search_schema: str = "",
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
        self.insertion_dict = {}
        self.deletion_dict = {}
        self.curr_name = ""
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
        if isinstance(self.path, list):
            for idx, val in enumerate(self.path):
                self._preprocess_sql(org_sql=val, file=str(idx))
        else:
            self.sql_files = get_files(path=self.path)
            for f in self.sql_files:
                org_sql = open(f, mode="r", encoding="utf-8-sig").read()
                org_sql = self._remove_comments(org_sql)
                org_sql_split = list(filter(None, org_sql.split(";")))
                if len(org_sql_split) <= 1:
                    self._preprocess_sql(org_sql=org_sql_split[0], file=f)
                else:
                    for idx, val in enumerate(org_sql_split):
                        self._preprocess_sql(org_sql=val, file=f + "_" + str(idx))

        for name, sql in self.sql_files_dict.items():
            if name not in self.finished_list:
                self._explain_sql(name=name, sql=sql)
            else:
                continue
        produce_json(self.output_dict, self.conn, self.search_schema)
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

    def _remove_comments(self, str1: str = "") -> str:
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

    def _create_view(self, name: str = "", sql: str = "") -> None:
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

    def _preprocess_sql(self, org_sql: str = "", file: str = "") -> None:
        """
        Process the sql, remove database name in the clause/datetime_add/datetime_sub adding quotes
        :param org_sql: the original sql, file: file name for the sql
        :return: None
        """
        ret_sql = self._remove_comments(str1=org_sql)
        ret_sql = ret_sql.replace("`", "")
        # remove any database names in the query
        for i in self.schema_list:
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
        if re.search(
            "CREATE VIEW IF NOT EXISTS", ret_sql, flags=re.IGNORECASE
        ) or re.search("CREATE TABLE IF NOT EXISTS", ret_sql, flags=re.IGNORECASE):
            temp = ret_sql.split(" ")
            ret_sql = ret_sql[ret_sql.index(temp[7]) :]
            if temp[5] in self.sql_files_dict.keys():
                print("WARNING: duplicate script detected for {}".format(temp[5]))
            self.sql_files_dict[temp[5]] = ret_sql
        elif re.search("CREATE VIEW", ret_sql, flags=re.IGNORECASE) or re.search(
            "CREATE TABLE", ret_sql, flags=re.IGNORECASE
        ):
            temp = ret_sql.split(" ")
            ret_sql = ret_sql[ret_sql.index(temp[4]) :]
            if temp[2] in self.sql_files_dict.keys():
                print("WARNING: duplicate script detected for {}".format(temp[2]))
            self.sql_files_dict[temp[2]] = ret_sql
        # adjust to INSERT/DELETE/SELECT/
        elif ret_sql.find("INSERT INTO") != -1:
            # find the current name in the insertion dict and how many times it has been inserted
            self.curr_name = re.sub(rem_regex, "", ret_sql.split(" ")[2])
            if self.curr_name not in self.insertion_dict.keys():
                self.insertion_dict[self.curr_name] = 1
            else:
                self.insertion_dict[self.curr_name] = (
                    self.insertion_dict[self.curr_name] + 1
                )
            insert_counter = self.insertion_dict[self.curr_name]
            self.curr_name = self.curr_name + "_INSERTION_{}".format(insert_counter)
            self.sql_files_dict[self.curr_name] = find_select(ret_sql)
        elif ret_sql.find("DELETE FROM") != -1:
            # find the current name in the insertion dict and how many times it has been deleted
            self.curr_name = re.sub(rem_regex, "", ret_sql.split(" ")[2])
            if self.curr_name not in self.deletion_dict.keys():
                self.deletion_dict[self.curr_name] = 1
            else:
                self.deletion_dict[self.curr_name] = (
                    self.deletion_dict[self.curr_name] + 1
                )
            delete_counter = self.deletion_dict[self.curr_name]
            self.curr_name = self.curr_name + "_DELETION_{}".format(delete_counter)
            self.sql_files_dict[self.curr_name] = find_select(ret_sql)
        else:
            if os.path.isfile(file):
                name = os.path.basename(file)[:-4]
                if name in self.sql_files_dict.keys():
                    print("WARNING: duplicate script detected for {}".format(name))
                self.sql_files_dict[name] = ret_sql
            else:
                self.sql_files_dict[file] = ret_sql

    def _explain_sql(self, name: str = "", sql: str = "") -> None:
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
                        search_schema, name
                    )
                )
                table_name = cur.fetchone()
                cur.close()
                if table_name:
                    table_name = table_name[0]
                else:
                    table_name = name
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
                table_name = schema_table.split(".")[-1][:-2]
                self.s.push(name)
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

import time
from typing import List
from typing import Optional

from sqlglot import exp, expressions, parse_one

from .ColumnLineageNoConn import ColumnLineageNoConn
from .SqlToDict import SqlToDict
from .utils import produce_json


def parse_one_sql(sql: Optional[str] = "") -> expressions:
    """
    The function to try different dialects for parsing the SQL
    :param sql: the input sql
    :return: the parsed sql AST
    """
    dialects = ["postgres", "oracle", "mysql", "sqlite", ""]
    parsed_sql = None
    for dialect in dialects:
        try:
            parsed_sql = parse_one(sql, read=dialect)
        except Exception as e:
            continue

        if parsed_sql is not None:
            break
    return parsed_sql


class LineageXNoConn:
    def __init__(
        self,
        sql: Optional[str] = "",
        dialect: str = "postgres",
        target_schema: Optional[str] = "public",
        search_path_schema: Optional[str] = "public",
    ) -> None:
        self.output_dict = {}
        self.parsed = 0
        self.target_schema = target_schema
        search_path_schema = [x.strip() for x in search_path_schema.split(",")]
        search_path_schema.append(target_schema)
        s2d = SqlToDict(path=sql, schema_list=search_path_schema, dialect=dialect)
        self.sql_files_dict = s2d.sql_files_dict
        self.org_sql_files_dict = s2d.org_sql_files_dict
        self.dialect = dialect
        self.input_table_dict = {}
        self.finished_list = []
        self._find_lineage_no_conn()

    def _find_lineage_no_conn(self):
        """
        The driver function to extract the table lineage information
        :return: output an interactive html for the table lineage information
        """
        not_parsed = 0
        start_time = time.time()
        for name, sql in self.sql_files_dict.items():
            try:
                # sql_ast = parse_one(sql, read=self.dialect)
                sql_ast = parse_one_sql(sql=sql)
                all_tables = self._resolve_table(part_ast=sql_ast)
                for t in all_tables:
                    if t in self.sql_files_dict.keys() and t not in self.finished_list:
                        self._run_lineage_no_conn(name=t, sql=self.sql_files_dict[t])
                        self.finished_list.append(t)
                if name not in self.finished_list:
                    self._run_lineage_no_conn(name=name, sql=sql)
                    self.finished_list.append(name)
            except Exception as e:
                print("{} is not processed because it countered {}".format(name, e))
                not_parsed += 1
                continue
        self._guess_schema_name()
        print(
            "{} SQLs are parsed, {} SQLs are not parsed, took a total of {:.1f} seconds".format(
                self.parsed, not_parsed, time.time() - start_time
            )
        )
        produce_json(self.output_dict)

    def _run_lineage_no_conn(self, name: Optional[str] = "", sql: Optional[str] = ""):
        print(name, " processing")
        self.parsed += 1
        col_lineage = ColumnLineageNoConn(
            sql=sql, dialect=self.dialect, input_table_dict=self.input_table_dict
        )
        # if len(name.split(".")) == 1:
        #     self.output_dict[self.target_schema + "." + name] = {
        #         "tables": col_lineage.table_list,
        #         "columns": col_lineage.column_dict,
        #         "table_name": self.target_schema + "." + name,
        #     }
        # else:
        # if name in self.org_sql_files_dict.keys():
        #     sql = self.org_sql_files_dict[name]
        self.output_dict[name] = {
            "tables": col_lineage.table_list,
            "columns": col_lineage.column_dict,
            "table_name": name,
            "sql": sql,
        }
        # add to the dict with the already parsed tables
        self.input_table_dict[self.target_schema + "." + name] = list(
            col_lineage.column_dict.keys()
        )
        self.input_table_dict[name] = list(col_lineage.column_dict.keys())

    def _resolve_table(self, part_ast: expressions = None) -> List:
        """
        Find the tables in the given ast
        :param part_ast: the ast to find the table
        """
        temp_table_list = []
        # Resolve FROM
        for table_sql in part_ast.find_all(exp.From):
            for table in table_sql.find_all(exp.Table):
                temp_table_list = self._find_table(
                    table=table, temp_table_list=temp_table_list
                )
        # Resolve JOIN
        for table_sql in part_ast.find_all(exp.Join):
            for table in table_sql.find_all(exp.Table):
                temp_table_list = self._find_table(
                    table=table, temp_table_list=temp_table_list
                )
        return temp_table_list

    def _find_table(
        self, table: expressions = None, temp_table_list: Optional[List] = None
    ) -> List:
        """
        Update table alias and find all aliased used table names
        :param table: the expression with the table
        :param temp_table_list: temporary list of tables for appending the used tables
        :return:
        """
        if table.alias == "":
            temp_table_list.append(table.sql())
        else:
            temp = table.sql().split(" ")
            if temp[1] == "AS" or temp[1] == "as":
                temp_table_list.append(temp[0])
        return temp_table_list

    def _guess_schema_name(self):
        """
        Try to guess the schema names for the sql provided for a more accurate depiction
        """
        all_tables = []
        for key, val in self.output_dict.items():
            all_tables.extend(val["tables"])
        all_tables = list(set(all_tables))
        tables_dict = {}
        for t in all_tables:
            tables_dict[t.split(".")[-1]] = t
        for key, val in self.output_dict.copy().items():
            if key in tables_dict.keys():
                if tables_dict[key] != key:
                    self.output_dict[tables_dict[key]] = val
                    self.output_dict[tables_dict[key]]["table_name"] = tables_dict[key]
                    self.output_dict.pop(key)


if __name__ == "__main__":
    pass

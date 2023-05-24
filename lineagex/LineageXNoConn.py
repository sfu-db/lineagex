from typing import Optional
from sqlglot import parse_one, exp, expressions
from typing import List

from .utils import produce_json
from .SqlToDict import SqlToDict
from .ColumnLineageNoConn import ColumnLineageNoConn


class LineageXNoConn:
    def __init__(
        self, sql: Optional[str] = "", target_schema: Optional[str] = "public",  search_path_schema: Optional[str] = "public"
    ) -> None:
        self.output_dict = {}
        self.target_schema = target_schema
        search_path_schema = [x.strip() for x in search_path_schema.split(",")]
        search_path_schema.append(target_schema)
        self.sql_files_dict = SqlToDict(sql, search_path_schema).sql_files_dict
        self.input_table_dict = {}
        self.finished_list = []
        self._find_lineage_no_conn()

    def _find_lineage_no_conn(self):
        """
        The driver function to extract the table lineage information
        :return: output an interactive html for the table lineage information
        """
        for name, sql in self.sql_files_dict.items():
            try:
                sql_ast = parse_one(sql, read="postgres")
                all_tables = self._resolve_table(part_ast=sql_ast)
                for t in all_tables:
                    if t in self.sql_files_dict.keys() and t not in self.finished_list:
                        self._run_lineage_no_conn(name=t, sql=self.sql_files_dict[t])
                        self.finished_list.append(t)
                if name not in self.finished_list:
                    self._run_lineage_no_conn(name=name, sql=sql)
            except Exception as e:
                print("{} is not processed because it countered {}".format(name, e))
                continue
        self._guess_schema_name()
        produce_json(self.output_dict)

    def _run_lineage_no_conn(self, name: Optional[str] = "", sql: Optional[str] = ""):
        print(name, " processing")
        col_lineage = ColumnLineageNoConn(
            sql=sql, input_table_dict=self.input_table_dict
        )
        self.output_dict[name] = {
            "tables": col_lineage.table_list,
            "columns": col_lineage.column_dict,
            "table_name": name,
        }
        # add to the dict with the already parsed tables
        self.input_table_dict[self.target_schema + "." + name] = list(col_lineage.column_dict.keys())
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

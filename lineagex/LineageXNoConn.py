from lineagex.utils import produce_json
from lineagex.SqlToDict import SqlToDict
from lineagex.ColumnLineageNoConn import ColumnLineageNoConn
from typing import Optional


class LineageXNoConn:
    def __init__(self, path: Optional[str] = "", search_schema: Optional[str] = "public") -> None:
        self.output_dict = {}
        search_schema = [x.strip() for x in search_schema.split(",")]
        self.sql_files_dict = SqlToDict(path, search_schema).sql_files_dict
        self.input_table_dict = {}
        self._run_lineage_no_conn()

    def _run_lineage_no_conn(self):
        """
        The driver function to extract the table lineage information
        :return: output an interactive html for the table lineage information
        """
        for name, sql in self.sql_files_dict.items():
            try:
                col_lineage = ColumnLineageNoConn(
                    sql=sql, input_table_dict=self.input_table_dict
                )
                self.output_dict[name] = {
                    "tables": col_lineage.table_list,
                    "columns": col_lineage.column_dict,
                    "table_name": name,
                }
                # add to the dict with the already parsed tables
                self.input_table_dict[name] = list(col_lineage.column_dict.keys())
            except Exception as e:
                print("{} is not processed because it countered {}".format(name, e))
                continue
        self._guess_schema_name()
        produce_json(self.output_dict)

    def _guess_schema_name(self):
        """
        Try to guess the schema names for the sql provided for a more accurate depiction
        """
        all_tables = []
        for key, val in self.output_dict.items():
            all_tables.extend(val['tables'])
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

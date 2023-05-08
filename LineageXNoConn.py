from utils import produce_json
from SqlToDict import SqlToDict
from ColumnLineageNoConn import ColumnLineageNoConn


class LineageXNoConn:
    def __init__(self, path: str = "", search_schema: str = "") -> None:
        self.output_dict = {}
        search_schema = [x.strip() for x in search_schema.split(",")]
        self.sql_files_dict = SqlToDict(path, search_schema).sql_files_dict
        self._run_lineage_no_conn()

    def _run_lineage_no_conn(self):
        for name, sql in self.sql_files_dict.items():
            col_lineage = ColumnLineageNoConn(
                sql=sql, input_table_dict={}
            )
            self.output_dict[name] = {
                "tables": col_lineage.table_list,
                "columns": col_lineage.column_dict,
                "table_name": name,
            }
        produce_json(self.output_dict)


if __name__ == "__main__":
    pass

import pkgutil
from typing import List, Optional, Union

from .LineageXNoConn import LineageXNoConn
from .LineageXWithConn import LineageXWithConn


class lineagex:
    def __init__(
        self,
        sql: Optional[Union[List, str]] = None,
        target_schema: Optional[str] = "",
        conn_string: Optional[str] = None,
        search_path_schema: Optional[str] = "",
    ) -> None:
        if sql is None:
            raise ValueError(
                "the SQL input cannot be empty, please input a list of sql or path to sql"
            )
        elif not isinstance(sql, list) and not isinstance(sql, str):
            raise ValueError(
                "wrong SQL input format, please input a list of sql or path to sql"
            )
        if target_schema == "" and search_path_schema == "":
            target_schema = "public"
            search_path_schema = "public"
        elif target_schema == "" and not search_path_schema == "":
            target_schema = search_path_schema.split(",")[0]
        elif not target_schema == "" and search_path_schema == "":
            search_path_schema = target_schema
        if conn_string:
            lx = LineageXWithConn(
                sql=sql,
                target_schema=target_schema,
                conn_string=conn_string,
                search_path_schema=search_path_schema,
            )
            self._save_js_file()
            self.output_dict = lx.output_dict
        else:
            lx = LineageXNoConn(
                sql=sql, target_schema=target_schema, search_path_schema= search_path_schema
            )
            self._save_js_file()
            self.output_dict = lx.output_dict

    def _save_js_file(self):
        data = pkgutil.get_data(__name__, "app.js")
        js_file = open("app.js", "w", encoding="utf-8")
        js_file.write(data.decode("utf-8"))
        js_file.close()
        data = pkgutil.get_data(__name__, "vendor.js")
        js_file = open("vendor.js", "w", encoding="utf-8")
        js_file.write(data.decode("utf-8"))
        js_file.close()


if __name__ == "__main__":
    pass

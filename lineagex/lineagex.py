import os
import pkgutil
import webbrowser
from IPython.display import display, HTML
from typing import List, Optional, Union

from .LineageXNoConn import LineageXNoConn
from .LineageXWithConn import LineageXWithConn

IMPLEMENTED_DIALECTS_WITH_CONN = ("postgres",)


def save_js_file():
    for filename in ["app.js", "vendor.js"]:
        data = pkgutil.get_data(__name__, filename)

        with open(filename, "w", encoding="utf-8") as js_file:
            if data:
                js_file.write(data.decode("utf-8"))


def validate_sql(sql: Union[List, str]) -> None:
    if sql is None:
        raise ValueError(
            "The SQL input cannot be empty, please input a list of SQL or a path to SQL"
        )
    elif not isinstance(sql, list) and not isinstance(sql, str):
        raise ValueError(
            "Wrong SQL input format, please input a list of SQL or a path to SQL"
        )


def validate_schema(target_schema: str, search_path_schema: str) -> tuple:
    if target_schema == "" and search_path_schema == "":
        target_schema = "public"
        search_path_schema = "public"
    elif target_schema == "" and not search_path_schema == "":
        target_schema = search_path_schema.split(",")[0]
    elif not target_schema == "" and search_path_schema == "":
        search_path_schema = target_schema

    return target_schema, search_path_schema


class lineagex:
    def __init__(
        self,
        sql: Optional[Union[List, str]] = None,
        target_schema: Optional[str] = "",
        conn_string: Optional[str] = None,
        search_path_schema: Optional[str] = "",
        dialect: str = "postgres",
    ) -> None:
        validate_sql(sql)
        target_schema, search_path_schema = validate_schema(
            target_schema, search_path_schema
        )

        if conn_string:
            if dialect not in IMPLEMENTED_DIALECTS_WITH_CONN:
                raise NotImplemented(f"Not implemented dialect: {dialect}")

            lx = LineageXWithConn(
                sql=sql,
                target_schema=target_schema,
                conn_string=conn_string,
                search_path_schema=search_path_schema,
            )
            save_js_file()
            self.output_dict = lx.output_dict
        else:
            lx = LineageXNoConn(
                sql=sql,
                dialect=dialect,
                target_schema=target_schema,
                search_path_schema=search_path_schema,
            )
            save_js_file()
            self.output_dict = lx.output_dict

    def show(self):
        with open("index.html", "r", encoding="utf-8") as file_html:
            curr_html = file_html.read()
        with open("index_jupyter.html", "w", encoding="utf-8") as file_html:
            file_html.write(
                """<div style="width:100%; height:800px;">
                {}
            </div>""".format(
                    curr_html
                )
            )
        display(HTML("index_jupyter.html"))

    def show_tab(self):
        cwd = os.getcwd()
        p = os.path.join(cwd, "index.html").replace("\\", "/")
        print("opening the lineage page from {}".format(p))
        webbrowser.open_new_tab(f"file://{p}")


if __name__ == "__main__":
    pass

import os
from typing import Union, List, Optional

from .LineageXWithConn import LineageXWithConn
from .LineageXNoConn import LineageXNoConn


class lineagex:
    def __init__(
            self,
            sql: Optional[Union[List, str]] = None,
            search_schema: Optional[str] = "public",
            conn_string: Optional[str] = None,
    ) -> None:
        if sql is None:
            raise ValueError("the SQL input cannot be empty, please input a list of sql or path to sql")
        elif not isinstance(sql, list) and not isinstance(sql, str):
            raise ValueError("wrong SQL input format, please input a list of sql or path to sql")
        if conn_string:
            lx = LineageXWithConn(sql, search_schema, conn_string)
            self.output_dict = lx.output_dict
        else:
            lx = LineageXNoConn(sql, search_schema)
            self.output_dict = lx.output_dict


if __name__ == '__main__':
    pass

from LineageXWithConn import LineageXWithConn
from LineageXNoConn import LineageXNoConn
import os
from typing import Union, List, Optional


class lineagex:
    def __init__(
            self,
            sql: Optional[Union[List, str]] = None,
            search_schema: Optional[str] = "public",
            url: Optional[str] = None,
            username: Optional[str] = None,
            password: Optional[str] = None,
    ) -> None:
        if sql is None:
            raise ValueError("the SQL input cannot be empty, please input a list of sql or path to sql")
        elif not isinstance(sql, list) and not isinstance(sql, str):
            raise ValueError("wrong SQL input format, please input a list of sql or path to sql")
        if url and username and password:
            lx = LineageXWithConn(sql, search_schema, url, username, password)
            self.output_dict = lx.output_dict
        else:
            lx = LineageXNoConn(sql, search_schema)
            self.output_dict = lx.output_dict


if __name__ == '__main__':
    pass

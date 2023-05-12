import re
import os
from lineagex.utils import get_files, find_select, remove_comments
from typing import List, Optional

rem_regex = re.compile(r"[^a-zA-Z0-9_.]")


class SqlToDict:
    def __init__(self, path: Optional[str] = "", schema_list: Optional[List] = None) -> None:
        self.path = path
        self.schema_list = schema_list
        self.sql_files = []
        self.sql_files_dict = {}
        self.deletion_dict = {}
        self.insertion_dict = {}
        self.curr_name = ""
        self._sql_to_dict()
        pass

    def _sql_to_dict(self) -> None:
        """
        The driver function to make the input into the dict format, name of sql:sql
        :return:
        """
        if isinstance(self.path, list):
            for idx, val in enumerate(self.path):
                self._preprocess_sql(org_sql=val, file=str(idx))
        else:
            self.sql_files = get_files(path=self.path)
            for f in self.sql_files:
                org_sql = open(f, mode="r", encoding="utf-8-sig").read()
                org_sql = remove_comments(str1=org_sql)
                org_sql_split = list(filter(None, org_sql.split(";")))
                if len(org_sql_split) <= 1:
                    self._preprocess_sql(org_sql=org_sql_split[0], file=f)
                else:
                    for idx, val in enumerate(org_sql_split):
                        self._preprocess_sql(org_sql=val, file=f + "_" + str(idx))

    def _preprocess_sql(self, org_sql: Optional[str] = "", file: Optional[str] = "") -> None:
        """
        Process the sql, remove database name in the clause/datetime_add/datetime_sub adding quotes
        :param org_sql: the original sql, file: file name for the sql
        :return: None
        """
        ret_sql = remove_comments(str1=org_sql)
        ret_sql = ret_sql.replace("`", "")
        # remove any database names in the query
        if self.schema_list:
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
            self.sql_files_dict[self.curr_name] = find_select(q=ret_sql)
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
            self.sql_files_dict[self.curr_name] = find_select(q=ret_sql)
        else:
            if os.path.isfile(file):
                name = os.path.basename(file)[:-4]
                if name in self.sql_files_dict.keys():
                    print("WARNING: duplicate script detected for {}".format(name))
                self.sql_files_dict[name] = ret_sql
            else:
                self.sql_files_dict[file] = ret_sql


if __name__ == "__main__":
    pass

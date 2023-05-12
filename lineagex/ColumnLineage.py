import re
from sqlglot import parse_one, exp
from sqlglot.expressions import CTE
from sqlglot import expressions
from psycopg2.extensions import connection
from typing import List, Tuple, Optional

from lineagex.utils import find_column


class ColumnLineage:
    def __init__(
        self,
        plan: Optional[dict] = None,
        sql: Optional[str] = "",
        table_name: Optional[str] = "",
        conn: connection = None,
        part_tables: Optional[str] = None,
        search_schema: Optional[str] = "",
    ) -> None:
        self.split_regex = re.compile(r"[^a-zA-Z0-9._]")
        self.all_used_col = []
        self.possible_columns = []
        self.table_alias = {}
        self.table_alias_reversed = {}
        self.cte_name = ""
        self.agg_flag = False
        self.cte_dict = {}
        self.subplan_dict = {}
        self.function_call_cols = {}
        self.part_tables = part_tables
        self.column_prefix_dict = {}
        self.conn = conn
        self.search_schema = search_schema
        self.sql_ast = parse_one(sql=sql, read="postgres")
        self.cte_column = self._find_cte_col()
        self.final_output = ""
        self.final_node_type = ""
        self.subquery_final_output = ""
        self.column_dict = {}
        self.table_list = []
        self._traverse_plan(plan=plan)
        self._resolve_column_dict(cols=self._find_final_column())
        self.table_list = sorted(set(self.table_list))
        # print(self.final_output)
        # print(self.subplan_dict)
        # print(self.table_alias)
        # print(self.cte_dict)
        # print(self.all_used_col)
        # print(self.possible_columns)
        # print("columns: ", self.column_dict)
        # print("table: ", self.table_list)

    def _find_final_column(self) -> List[str]:
        """
        Find all the column names in the final projection
        :return: the list of column names in the final projection
        """
        # Pop all CTE and Subquery trees
        for with_sql in self.sql_ast.find_all(exp.With):
            with_sql.pop()
        for sub_sql in self.sql_ast.find_all(exp.Subquery):
            sub_sql.pop()
        final_column_list = []
        for projection in self.sql_ast.find(exp.Select).expressions:
            col_name = projection.alias_or_name
            # Resolve aggregations with no alias
            if isinstance(projection, exp.Count):
                col_name = "count"
            elif isinstance(projection, exp.Avg):
                col_name = "avg"
            elif isinstance(projection, exp.Max):
                col_name = "max"
            elif isinstance(projection, exp.Min):
                col_name = "min"
            # Resolve * and check if it is from a previous CTE or check database for the table
            elif (
                isinstance(projection, exp.Column) and projection.find(exp.Star)
            ) or col_name == "*":
                main_table_list = []
                for table in self.sql_ast.find_all(exp.Table):
                    table_def_split = re.split(" AS ", table.sql(), flags=re.IGNORECASE)
                    main_table_list.append(table_def_split[0])
                # if the * has a prefix
                if projection.find(exp.Identifier):
                    t_name = projection.find(exp.Identifier).text("this")
                    if table_alias_dict[t_name] in self.cte_column.keys():
                        col_name = self.cte_column[table_alias_dict[t_name]]
                    else:
                        col_name = find_column(
                            table_name=table_alias_dict[t_name],
                            engine=self.conn,
                            search_schema=self.search_schema,
                        )
                # if * has no prefix
                else:
                    for t_name in main_table_list:
                        if t_name in self.cte_column.keys():
                            final_column_list.extend(self.cte_column[t_name])
                        else:
                            final_column_list.extend(
                                find_column(
                                    table_name=t_name,
                                    engine=self.conn,
                                    search_schema=self.search_schema,
                                )
                            )
            if isinstance(col_name, list):
                final_column_list.extend(col_name)
            else:
                if col_name != "*":
                    final_column_list.append(col_name)
        return final_column_list

    def _resolve_column_dict(self, cols: Optional[List] = None) -> None:
        """
        Insert into column_dict using the column names as keys and its dependencies as child
        :param cols: the columns for the final output
        :return: None
        """
        if len(self.final_output) > len(cols):
            self.final_output = self.final_output[: len(cols)]
        elif len(self.final_output) < len(cols):
            print(
                "number of columns from the sql does not match the number in manifest"
            )
            return
        for idx, val in enumerate(self.final_output):
            # Postgres Aggregate function of count(*) that involves all the columns
            if self.final_node_type == "Aggregate" and val.strip() == "count(*)":
                all_cols = self.possible_columns
            else:
                table_col = re.split(self.split_regex, val.strip())
                all_cols = list(
                    set(set(table_col) & set(self.possible_columns)).union(
                        self.all_used_col
                    )
                )
            # Subquery in the final projection
            if val.find("SubPlan ") != -1:
                subplan_name = re.findall(re.compile(r"SubPlan [0-9]*"), val)[0]
                all_cols.extend(self.subplan_dict[subplan_name])
            self.column_dict[cols[idx]] = sorted(
                self._remove_table_alias(cols=all_cols)
            )

    def _traverse_plan(self, plan: Optional[dict] = None) -> None:
        """
        Traversing the plan using recursion and go into Plans if it is present
        :param plan: the given execution plan
        :return: None
        """
        if plan["Node Type"] in [
            "Seq Scan",
            "Parallel Seq Scan",
            "Bitmap Heap Scan",
            "Index Scan",
            "Index Only Scan",
        ]:
            self.table_alias[plan["Alias"]] = self._find_parent_table(
                table=plan["Schema"] + "." + plan["Relation Name"]
            )
            self.table_alias_reversed[
                plan["Schema"] + "." + plan["Relation Name"]
            ] = plan["Alias"]
        if "Plans" in plan.keys():
            for subplan_data in plan["Plans"]:
                self._traverse_plan(plan=subplan_data)
        temp = plan.get("Output")
        if temp is not None:
            self.final_output = temp
            self.final_node_type = plan.get("Node Type")
        # if scan from tables/views, add to possible columns
        if plan["Node Type"] in [
            "Seq Scan",
            "Parallel Seq Scan",
            "Bitmap Heap Scan",
            "Index Scan",
            "Index Only Scan",
        ]:
            # Scan and Filter and CTE in one
            if "Subplan Name" in plan.keys():
                self._add_possible_columns(plan)
                # self.possible_columns.extend(plan["Output"])
                if "Alias" in plan.keys():
                    alias = plan["Alias"]
                else:
                    alias = plan["Schema"] + "." + plan["Relation Name"]
                temp_regex = re.compile(r"({}\.[a-zA-Z0-9_]+)".format(alias))
                # print(plan['Subplan Name'], plan['Output'])
                for i in plan["Output"]:
                    self.possible_columns.extend(re.findall(temp_regex, i))
                if "Filter" in plan.keys():
                    self.possible_columns.extend(re.findall(temp_regex, plan["Filter"]))
                cte_name = plan["Subplan Name"].split(" ")[1]
                self.table_list.append(
                    self._find_parent_table(
                        table=(plan["Schema"] + "." + plan["Relation Name"])
                    )
                )
                self.table_alias[cte_name] = cte_name
                self._add_cte_dict(plan=plan)
            # Filter and scan in one plan
            elif "Filter" in plan.keys():
                self._add_possible_columns(plan)
                # self.possible_columns.extend(plan["Output"])
                self._handle_filter_in_scan(plan=plan)
                # in the filter, there is chance there is also index cond
                self._handle_index_cond(plan=plan)
            # Scan only
            else:
                # to avoid index cond
                self._add_possible_columns(plan=plan)
                self._handle_index_cond(plan=plan)
        # if scan from cte
        elif plan["Node Type"] == "CTE Scan":
            # add to possible column first and add all columns from CTE to prevent filters on top of any
            if "Alias" in plan.keys():
                all_cte_cols = [
                    plan["Alias"] + "." + s
                    for s in list(self.cte_dict[plan["CTE Name"]].keys())
                ]
            else:
                all_cte_cols = [
                    plan["CTE Name"] + "." + s
                    for s in list(self.cte_dict[plan["CTE Name"]].keys())
                ]
            self.possible_columns.extend(
                list(set(all_cte_cols).union(set(plan["Output"])))
            )
            # handle filter and scan in the same plan
            if "Filter" in plan.keys():
                self._handle_filter_in_scan(plan=plan)
            # the current scan can also be the creation of another cte
            if "Subplan Name" in plan.keys():
                if "CTE Name" in plan.keys():
                    self.possible_columns.extend(
                        [
                            plan["CTE Name"] + "." + s
                            for s in list(self.cte_dict[plan["CTE Name"]].keys())
                        ]
                    )
                self.table_alias[plan["Alias"]] = plan["CTE Name"]
                self._add_cte_dict(plan=plan)
            else:
                # if just a scan, add to alias
                self.table_alias[plan["Alias"]] = plan["CTE Name"]
        # if scan from a subquery
        elif plan["Node Type"] == "Subquery Scan":
            org_all_used_cols = self.all_used_col
            org_possible_cols = self.possible_columns
            self.all_used_col = []
            self.possible_columns = []
            if "Plans" in plan.keys():
                self.subquery_final_output = plan["Plans"][0]["Output"]
            else:
                self.subquery_final_output = plan["Output"]
            self._resolve_subquery(plan=plan)
            if "Alias" in plan.keys():
                self.cte_name = plan["Alias"]
            temp_dict = {}
            self._extract_from_cond(plan=plan)
            if self.cte_name in self.cte_column.keys():
                for idx, val in enumerate(self.cte_column[self.cte_name]):
                    cte_col = re.split(
                        self.split_regex, self.subquery_final_output[idx].strip()
                    )
                    all_cols = list(
                        set(set(cte_col) & set(self.possible_columns)).union(
                            self.all_used_col
                        )
                    )
                    # Subquery in the final projection
                    if self.subquery_final_output[idx].find("SubPlan ") != -1:
                        subplan_name = re.findall(
                            re.compile(r"SubPlan [0-9]*"),
                            self.subquery_final_output[idx],
                        )[0]
                        all_cols.extend(self.subplan_dict[subplan_name])
                    temp_dict[val] = self._remove_table_alias(cols=all_cols)
                self.cte_dict[self.cte_name] = temp_dict
            self.table_alias[plan["Alias"]] = plan["Alias"]
            self.all_used_col = org_all_used_cols
            self.possible_columns = org_possible_cols
            self.possible_columns.extend(plan["Output"])
        else:
            # creation of cte but with an Append node(usually UNION/EXCEPT/INTERSECT)
            if "Subplan Name" in plan.keys():
                # Resolve UNION/EXCEPT/INTERSECT since the Node Type will be Appended with no outputs
                if (
                    plan["Node Type"] in ["Append", "MergeAppend"]
                    and "Output" not in plan.keys()
                    and len(plan["Plans"]) != 0
                ):
                    s = plan.get("Subplan Name").split(" ")
                    if s[0] == "CTE":
                        self.cte_name = s[1]
                        temp_dict = {}
                        self._extract_from_cond(plan=plan)
                        self._resolve_union(plan=plan)
                        self.agg_flag = False
                        for val in self.cte_column[self.cte_name]:
                            temp_dict[val] = self._remove_table_alias(
                                cols=self.all_used_col
                            )
                        self.cte_dict[self.cte_name] = temp_dict
                        self.all_used_col = []
                        self.possible_columns = []
                else:
                    # if only creation of a cte, but no scan, just add it to cte_dict
                    self._add_cte_dict(plan=plan)
            # UNION/EXCEPT/INTERSECT at the last step with no creation of CTE
            elif (
                plan["Node Type"] in ["Append", "MergeAppend"]
                and "Output" not in plan.keys()
                and len(plan["Plans"]) != 0
            ):
                self._extract_from_cond(plan=plan)
                self._resolve_union(plan=plan)
                self.agg_flag = False
            # every other node aside from cte creations/scans
            else:
                self._extract_from_cond(plan=plan)

    def _handle_index_cond(self, plan: Optional[dict] = None) -> None:
        """
        When there's an index_cond in the plan, handle it and extract the necessary columns
        :param plan: the plan with the index_cond
        """
        temp = plan.get("Index Cond")
        if temp is not None:
            idx_name = plan.get("Index Name")
            if idx_name is not None:
                cur = self.conn.cursor()
                cur.execute("""SET search_path TO {};""".format(self.search_schema))
                cur.execute(
                    "SELECT schemaname, tablename, indexname, indexdef FROM pg_indexes WHERE indexname = '{}'".format(
                        idx_name
                    )
                )
                result = cur.fetchall()[0]
                cur.close()
                # the indexdef is at index 3
                indexdef = result[3]
                btree_idx = indexdef.find("USING btree")
                if btree_idx != -1:
                    close_bracket = indexdef.find(")", btree_idx)
                    if close_bracket != -1:
                        idx_cols = indexdef[btree_idx + 13 : close_bracket].split(",")
                        alias = self.table_alias_reversed[result[0] + "." + result[1]]
                        for i in idx_cols:
                            self.possible_columns.append(alias + "." + i)
            row_col = re.split(self.split_regex, temp.strip())
            self.all_used_col.extend(list(set(row_col) & set(self.possible_columns)))

    def _handle_filter_in_scan(self, plan: Optional[dict] = None) -> None:
        """
        When there is filter in the scan node, handle it and find necessary columns
        :param plan: the plan with the filter in the scan node
        """
        if "Alias" in plan.keys():
            alias = plan["Alias"]
        else:
            alias = plan["Schema"] + "." + plan["Relation Name"]
        temp_regex = re.compile(r"({}\.[a-zA-Z0-9_]+)".format(alias))
        self.possible_columns.extend(re.findall(temp_regex, plan["Filter"]))
        temp = plan.get("Filter")
        row_col = re.split(self.split_regex, temp.strip())
        self.all_used_col.extend(list(set(row_col) & set(self.possible_columns)))
        if temp.find("SubPlan ") != -1:
            subplan_name = re.findall(re.compile(r"SubPlan [0-9]*"), temp)[0]
            self.all_used_col.extend(self.subplan_dict[subplan_name])

    def _resolve_subquery(self, plan: Optional[dict] = None) -> None:
        """
        When there is a node with subquery, go in recursively and find the most inner node
        :param plan: the node with subquery
        """
        if "Plans" in plan.keys():
            for subplan_data in plan["Plans"]:
                self._resolve_subquery(plan=subplan_data)
        if "Output" in plan.keys() and plan["Node Type"] in [
            "Seq Scan",
            "Parallel Seq Scan",
            "Bitmap Heap Scan",
            "Index Scan",
            "Index Only Scan",
            "CTE Scan",
        ]:
            self.possible_columns.extend(plan["Output"])
            self._extract_from_cond(plan=plan)
        else:
            self._extract_from_cond(plan=plan)

    def _resolve_union(self, plan: Optional[dict] = None) -> None:
        """
        To resolve the UNION, all the columns involved are used since those columns need to be the same
        :param plan: the execution plan for UNION/EXCEPT/INTERSECT
        """
        if "Plans" in plan.keys():
            # Check if it is an aggregation, since it would scan all the columns
            if plan["Plans"][0]["Node Type"] == "Aggregate":
                self.agg_flag = True
            for subplan_data in plan["Plans"]:
                self._resolve_union(plan=subplan_data)
        if (
            "Output" in plan.keys()
            and plan["Node Type"]
            in [
                "Seq Scan",
                "Parallel Seq Scan",
                "Bitmap Heap Scan",
                "Index Scan",
                "Index Only Scan",
            ]
            and not self.agg_flag
        ):
            # check if it is using Append node for partitioned tables
            if plan["Schema"] + "." + plan["Relation Name"] in list(
                self.part_tables.keys()
            ):
                self.possible_columns.extend(plan["Output"])
                self._extract_from_cond(plan=plan)
            else:
                # if it is using Append node for UNION/EXCEPT/INTERSECT
                for col in plan["Output"]:
                    row_col = re.split(self.split_regex, col.strip())
                    self.all_used_col.extend(
                        list(set(row_col) & set(self.possible_columns))
                    )
        elif (
            "Output" in plan.keys()
            and plan["Node Type"] == "CTE Scan"
            and not self.agg_flag
        ):
            # if it is using Append node for UNION/EXCEPT/INTERSECT
            for col in plan["Output"]:
                row_col = re.split(self.split_regex, col.strip())
                self.all_used_col.extend(
                    list(set(row_col) & set(self.possible_columns))
                )

    def _add_cte_dict(self, plan: Optional[dict] = None) -> None:
        """
        Add to the cte dict given the CTE plan and analyze its column lineage
        :param plan: the CTE plan
        :return:
        """
        s = plan.get("Subplan Name").split(" ")
        if s[0] == "CTE":
            self.cte_name = s[1]
            temp_dict = {}
            self._extract_from_cond(plan=plan)
            for idx, val in enumerate(self.cte_column[self.cte_name]):
                cte_col = re.split(self.split_regex, plan["Output"][idx].strip())
                all_cols = list(
                    set(set(cte_col) & set(self.possible_columns)).union(
                        self.all_used_col
                    )
                )
                # Subquery in the final projection
                if plan["Output"][idx].find("SubPlan ") != -1:
                    subplan_name = re.findall(
                        re.compile(r"SubPlan [0-9]*"), plan["Output"][idx]
                    )[0]
                    all_cols.extend(self.subplan_dict[subplan_name])
                temp_dict[val] = self._remove_table_alias(cols=all_cols)
            self.cte_dict[self.cte_name] = temp_dict
            self.all_used_col = []
            self.possible_columns = []
        # temporary subplan name
        elif s[0] == "SubPlan":
            subplan_list = []
            for _, val in enumerate(plan["Output"]):
                table_col = re.split(self.split_regex, val.strip())
                all_cols = list(set(set(table_col) & set(self.possible_columns)))
                subplan_list.extend(self._remove_table_alias(cols=all_cols))
            self.subplan_dict[plan["Subplan Name"]] = subplan_list

    def _extract_from_cond(self, plan: Optional[dict] = None) -> None:
        """
        Extract column from multiple operators, some add to possible_cols and some add to all_used_cols
        :param plan: the execution plan for the operator
        :return:
        """
        # More conditions
        if plan["Node Type"] == "WindowAgg":
            self.possible_columns.extend(plan["Output"])
        # Handle index cond
        self._handle_index_cond(plan=plan)
        temp = plan.get("Hash Cond")
        if temp is not None:
            row_col = re.split(self.split_regex, temp.strip())
            self.all_used_col.extend(list(set(row_col) & set(self.possible_columns)))
        temp = plan.get("Merge Cond")
        if temp is not None:
            row_col = re.split(self.split_regex, temp.strip())
            self.all_used_col.extend(list(set(row_col) & set(self.possible_columns)))
        temp = plan.get("Recheck Cond")
        if temp is not None:
            row_col = re.split(self.split_regex, temp.strip())
            self.all_used_col.extend(list(set(row_col) & set(self.possible_columns)))
        temp = plan.get("Join Filter")
        if temp is not None:
            row_col = re.split(self.split_regex, temp.strip())
            self.all_used_col.extend(list(set(row_col) & set(self.possible_columns)))
        temp = plan.get("Filter")
        if temp is not None:
            row_col = re.split(self.split_regex, temp.strip())
            self.all_used_col.extend(list(set(row_col) & set(self.possible_columns)))
        temp = plan.get("Sort Key")
        if temp is not None:
            self.all_used_col.extend(list(set(temp) & set(self.possible_columns)))
        temp = plan.get("Group Key")
        if temp is not None:
            self.all_used_col.extend(list(set(temp) & set(self.possible_columns)))
        # Function calls
        if plan["Node Type"] == "Function Scan":
            if "Function Name" in plan.keys():
                if plan["Function Name"] == "unnest":
                    func_cols = re.split(
                        self.split_regex, plan["Function Call"].strip()
                    )
                    all_cols = list(
                        set(set(func_cols) & set(self.possible_columns)).union(
                            self.all_used_col
                        )
                    )
                    self.function_call_cols[plan["Output"][0]] = sorted(
                        self._remove_table_alias(cols=all_cols)
                    )
                    self.possible_columns.append(plan["Output"][0])

    def _add_possible_columns(self, plan: Optional[dict] = None) -> None:
        """
        Extract the columns/tables from the plan and add to possible columns and used tables, mainly used for base table
        :param plan: plan to extract columns and tables
        :return:
        """
        invalid_list = []
        for i in plan["Output"]:
            if len(i.split(".")) == 1:
                self.column_prefix_dict[i] = (
                    plan["Schema"] + "." + plan["Relation Name"] + "." + i
                )
        for i in plan["Output"]:
            # if a column name contains an invalid char(that is not a-zA-z0-9_)
            if re.search(r"[^\w.]", i):
                invalid_list.append(i)
                continue
            # if a column name starts with a digit, likely is just a number, but not a column
            elif i[0].isdigit():
                continue
            # no table prefix column outputs, append only the column name and the prefix + column
            elif len(i.split(".")) == 1:
                if "Alias" in plan.keys():
                    prefix = plan["Alias"]
                else:
                    prefix = plan["Schema"] + "." + plan["Relation Name"]
                self.possible_columns.append(prefix + "." + i)
                self.possible_columns.append(i)
            else:
                self.possible_columns.append(i)
        # if an output has invalid char, its likely it's an expression, have to extract the columns
        if invalid_list:
            all_table_cols = find_column(
                table_name=plan["Schema"] + "." + plan["Relation Name"],
                engine=self.conn,
                search_schema=self.search_schema,
            )
            for i in invalid_list:
                # split the output and match columns from all the columns of the table
                table_col = re.split(self.split_regex, i.strip())
                resolved_possible_cols = list(set(set(table_col) & set(all_table_cols)))
                for j in resolved_possible_cols:
                    self.possible_columns.append(j)
                    if len(j.split(".")) == 1:
                        self.column_prefix_dict[j] = (
                            plan["Schema"] + "." + plan["Relation Name"] + "." + j
                        )
        self.table_list.append(
            self._find_parent_table(table=plan["Schema"] + "." + plan["Relation Name"])
        )
        self.table_alias[plan["Alias"]] = self._find_parent_table(
            table=plan["Schema"] + "." + plan["Relation Name"]
        )
        self.table_alias_reversed[plan["Schema"] + "." + plan["Relation Name"]] = plan[
            "Alias"
        ]

    def _find_parent_table(self, table: Optional[str] = "") -> str:
        """
        Find the parent table from a given table name
        :param table: table name
        :return: table: the parent table name(if it is not partitioned table, it is just the input
        """
        if self.part_tables is not None:
            if table in self.part_tables.keys():
                table = self.part_tables[table]
        return table

    def _remove_table_alias(self, cols: Optional[List] = None) -> List:
        """
        Remove the alias in the name or add intended schema.table to column names
        :param cols: the list of columns that has aliases that need to be resolved
        :return: resolved aliases for the columns
        """
        current_cte_table = ""
        ret_cols = []
        temp_keys_dict = {}
        for i in cols:
            temp = i.split(".")
            # has prefix or no
            if len(temp) > 1:
                # prefix yes, is the prefix already in alias or base table name
                if temp[0] in self.table_alias.keys():
                    org_name = self.table_alias[temp[0]]
                    # prefix yes, get the non-alias cte name
                    if org_name in self.cte_dict.keys():
                        # to avoid case difference, create a temp dict for lower->original
                        # if the current cte's temp dict is already created, skip it
                        if org_name != current_cte_table:
                            cte_dict_keys = list(self.cte_dict[org_name].keys())
                            temp_keys_dict = {}
                            for k in cte_dict_keys:
                                temp_keys_dict[k.lower()] = k
                            current_cte_table = org_name
                        # find the matching column name in the cte_dict
                        if temp[1] in temp_keys_dict.keys():
                            ret_cols.extend(
                                self.cte_dict[org_name][temp_keys_dict[temp[1]]]
                            )
                    # prefix yes, get the non-alias base table name
                    else:
                        ret_cols.append(org_name + "." + temp[1])
                # prefix yes, is it from a function call temp col
                elif i in self.function_call_cols.keys():
                    ret_cols.extend(self.function_call_cols[i])
                else:
                    ret_cols.append(i)
            else:
                ret_cols.append(self.column_prefix_dict[i])
        return list(set(ret_cols))

    def _find_table(self, cte: CTE = None) -> Tuple[dict, List]:
        """
        Find aliases for the tables in the cte
        :param cte: the sql of the sql to be analyzed
        :return: the dict with table name as key and alias as child
        """
        table_alias_dict = {}
        for table in cte.find_all(exp.Table):
            table_def_split = re.split(" AS ", table.sql(), flags=re.IGNORECASE)
            if len(table_def_split) == 1:
                table_alias_dict[table_def_split[0]] = table_def_split[0]
            else:
                table_alias_dict[table_def_split[1]] = table_def_split[0]
        # Find tables that's only in the CTE but not in the Subquery
        temp_cte = cte.copy()
        for sub_ast in temp_cte.find_all(exp.Subquery):
            sub_ast.pop()
        cte_table_list = []
        for table in temp_cte.find_all(exp.Table):
            table_def_split = re.split(" AS ", table.sql(), flags=re.IGNORECASE)
            cte_table_list.append(table_def_split[0])
        return table_alias_dict, cte_table_list

    def _find_cte_col(self) -> dict:
        """
        Find the column names for each cte since it does not show in the execution plan
        :param sql: the sql that needs to be analyzed
        :return: the dict with table name as keys and list of columns as child
        """
        cte_col_dict = {}
        for cte in self.sql_ast.find_all(exp.CTE):
            cte_col_dict = self._find_cte_col_func(cte=cte, cte_col_dict=cte_col_dict)
        for cte in self.sql_ast.find_all(exp.Subquery):
            cte_col_dict = self._find_cte_col_func(cte=cte, cte_col_dict=cte_col_dict)
        return cte_col_dict

    def _find_cte_col_func(
        self, cte: expressions = None, cte_col_dict: Optional[dict] = None
    ) -> dict:
        """
        The function to find all the column names for the cte
        :param cte: the cte ast tree
        :param cte_col_dict: the dict that stores the column names for the previous cte
        :return: the dict that contains column names with the current cte
        """
        # Find each CTE
        cte_name = cte.find(exp.TableAlias).alias_or_name
        cte_col_dict[cte_name] = []
        # Iterate column for each CTE
        for projection in cte.find(exp.Select).expressions:
            col_name = projection.alias_or_name
            # Resolve aggregations with no alias
            if isinstance(projection, exp.Count):
                col_name = "count"
            elif isinstance(projection, exp.Avg):
                col_name = "avg"
            elif isinstance(projection, exp.Max):
                col_name = "max"
            elif isinstance(projection, exp.Min):
                col_name = "min"
            elif isinstance(projection, exp.Sum):
                col_name = "sum"
            # Resolve * and check if it is from a previous CTE or check database for the table
            elif (
                isinstance(projection, exp.Column) and projection.find(exp.Star)
            ) or col_name == "*":
                table_alias_dict, cte_table_list = self._find_table(cte=cte)
                # if the * has a prefix
                if projection.find(exp.Identifier):
                    t_name = projection.find(exp.Identifier).text("this")
                    if table_alias_dict[t_name] in cte_col_dict.keys():
                        col_name = cte_col_dict[table_alias_dict[t_name]]
                    else:
                        col_name = find_column(
                            table_name=table_alias_dict[t_name],
                            engine=self.conn,
                            search_schema=self.search_schema,
                        )
                # if * has no prefix
                else:
                    for t_name in cte_table_list:
                        if t_name in cte_col_dict.keys():
                            cte_col_dict[cte_name].extend(cte_col_dict[t_name])
                        else:
                            cte_col_dict[cte_name].extend(
                                find_column(
                                    table_name=t_name,
                                    engine=self.conn,
                                    search_schema=self.search_schema,
                                )
                            )
            if isinstance(col_name, list):
                cte_col_dict[cte_name].extend(col_name)
            else:
                if col_name != "*":
                    cte_col_dict[cte_name].append(col_name)
        return cte_col_dict


if __name__ == "__main__":
    pass

from sqlglot import parse_one, exp
from sqlglot import expressions
from typing import Optional, List, Tuple
import os

shared_conditions = [exp.Where, exp.EQ, exp.Group, exp.Having, exp.Order]
shared_conditions_with_table = [
    exp.Where,
    exp.EQ,
    exp.Group,
    exp.Having,
    exp.Order,
    exp.From,
    exp.Join,
]
from_join_exp = [exp.From, exp.Join]


class ColumnLineageNoConn:
    def __init__(
        self, sql: Optional[str] = "", input_table_dict: Optional[dict] = None
    ):
        self.column_dict = {}
        self.table_alias_dict = {}
        self.cte_table_dict = {}
        self.cte_dict = {}
        self.unnest_dict = {}
        self.input_table_dict = input_table_dict
        self.sql_ast = parse_one(sql, read="postgres")
        self.all_used_col = []
        self.table_list = []
        self.all_subquery_table = []
        self.sub_tables = []
        self.sub_cols = []
        self._run_cte_lineage()
        # Everything other than CTEs, and pop the CTE tree
        for with_sql in self.sql_ast.find_all(exp.With):
            with_sql.pop()
        self._sub_shared_col_conds(sql_ast=self.sql_ast)
        self._run_lineage(self.sql_ast, False)
        # print(self.cte_dict)
        # print(self.column_dict)
        # print(self.cte_table_dict)

    def _run_lineage(
        self, sql_ast: expressions = None, subquery_flag: bool = False
    ) -> None:
        """
        Run the lineage after all the cte and subquery are resolved
        :param sql_ast: the ast for the sql
        :param subquery_flag: check if it is a subquery
        """
        if not subquery_flag:
            self.all_used_col = []
            if (
                isinstance(sql_ast, exp.Union)
                or isinstance(sql_ast, exp.Except)
                or isinstance(sql_ast, exp.Intersect)
            ):
                self._handle_union(sql_ast=sql_ast)
            main_tables = self._resolve_table(part_ast=sql_ast)
            self.table_list = self._find_all_tables(temp_table_list=main_tables)
            self.table_list.extend(self.all_subquery_table)
            self._shared_col_conds(part_ast=sql_ast, used_tables=main_tables)
            self.all_used_col.extend(self.sub_cols)
            self.all_used_col = set(self.all_used_col)
            if sql_ast.find(exp.Select):
                for projection in sql_ast.find(exp.Select).expressions:
                    col_name = projection.alias_or_name
                    self.column_dict = self._resolve_proj(
                        projection=projection,
                        col_name=col_name,
                        target_dict=self.column_dict,
                        source_table=main_tables,
                    )
            self.table_list = list(set(self.table_list))
        else:
            temp_sub_cols = []
            for col in sql_ast.find_all(exp.Column):
                temp_sub_cols.extend(
                    self._find_alias_col(col_sql=col.sql(), temp_table=self.sub_tables)
                )
            self.sub_cols.extend(temp_sub_cols)
            # print(temp_sub_cols)

    def _handle_union(self, sql_ast: expressions = None) -> None:
        if (
            isinstance(sql_ast, exp.Union)
            or isinstance(sql_ast, exp.Except)
            or isinstance(sql_ast, exp.Intersect)
        ):
            self._handle_union(sql_ast=sql_ast.left)
            self._handle_union(sql_ast=sql_ast.right)
        else:
            main_tables = self._resolve_table(part_ast=sql_ast)
            self._shared_col_conds(part_ast=sql_ast, used_tables=main_tables)
            for col in sql_ast.find_all(exp.Column):
                self.all_used_col.extend(
                    self._find_alias_col(col_sql=col.sql(), temp_table=main_tables)
                )
            return

    def _sub_shared_col_conds(self, sql_ast: expressions = None) -> None:
        """
        After the cte are resolved, run the subquery ast that is with the shared conditions(WHERE, GROUP BY, etc)
        :param sql_ast: the ast without the cte
        """
        # add in more conditions, including FROM/JOIN
        for cond in shared_conditions + from_join_exp:
            for cond_sql in sql_ast.find_all(cond):
                for sub_ast in cond_sql.find_all(exp.Subquery):
                    self.sub_tables = self._resolve_table(part_ast=sub_ast)
                    self.all_subquery_table.extend(
                        self._find_all_tables(temp_table_list=self.sub_tables)
                    )
                    self._run_lineage(sub_ast, True)
                    sub_ast.pop()

    def _sub_shared_col_conds_cte(
        self, sql_ast: expressions = None
    ) -> Tuple[List, List]:
        """
        Run the subquery inside the cte first that is with the shared conditions(WHERE, GROUP BY, etc)
        :param sql_ast: the ast for the cte
        """
        all_cte_sub_table = []
        all_cte_sub_cols = []
        # add in more conditions, including FROM/JOIN
        for cond in shared_conditions + from_join_exp:
            for cond_sql in sql_ast.find_all(cond):
                for sub_ast in cond_sql.find_all(exp.Subquery):
                    temp_sub_table = self._resolve_table(part_ast=sub_ast)
                    temp_sub_cols = []
                    temp_dict = {}
                    for col in sub_ast.find_all(exp.Column):
                        if col.find(exp.Star):
                            temp_dict = self._resolve_agg_star(
                                col_name="*",
                                projection=col,
                                used_tables=temp_sub_table,
                                target_dict=temp_dict,
                            )
                            for _, value in temp_dict.items():
                                temp_sub_cols.extend(value)
                        else:
                            temp_sub_cols.extend(
                                self._find_alias_col(
                                    col_sql=col.sql(), temp_table=temp_sub_table
                                )
                            )
                    temp_sub_cols = list(set(temp_sub_cols))
                    all_cte_sub_table.extend(
                        self._find_all_tables(temp_table_list=temp_sub_table)
                    )
                    all_cte_sub_cols.extend(temp_sub_cols)
                    sub_ast.pop()
        return all_cte_sub_table, all_cte_sub_cols

    def _run_cte_lineage(self):
        """
        Run the lineage information for all the cte
        """
        for cte in self.sql_ast.find_all(exp.CTE):
            all_cte_sub_table, all_cte_sub_cols = self._sub_shared_col_conds_cte(
                sql_ast=cte
            )
            self.all_used_col = []
            temp_cte_dict = {}
            temp_cte_table = self._resolve_table(part_ast=cte)
            cte_name = cte.find(exp.TableAlias).alias_or_name
            self.cte_table_dict[cte_name] = list(
                set(
                    self._find_all_tables(temp_table_list=temp_cte_table)
                    + all_cte_sub_table
                )
            )
            # Resolving shared conditions
            if cte.find(exp.Union):
                if cte.find(exp.Union).depth == cte.depth + 1:
                    self._handle_union(sql_ast=cte.find(exp.Union))
            elif cte.find(exp.Except):
                if cte.find(exp.Except).depth == cte.depth + 1:
                    self._handle_union(sql_ast=cte.find(exp.Union))
            elif cte.find(exp.Intersect):
                if cte.find(exp.Intersect).depth == cte.depth + 1:
                    self._handle_union(sql_ast=cte.find(exp.Union))
            else:
                self._shared_col_conds(part_ast=cte, used_tables=temp_cte_table)
                self.all_used_col.extend(all_cte_sub_cols)
            self.all_used_col = set(self.all_used_col)
            # Resolving the projection
            for projection in cte.find(exp.Select).expressions:
                col_name = projection.alias_or_name
                temp_cte_dict = self._resolve_proj(
                    projection=projection,
                    col_name=col_name,
                    target_dict=temp_cte_dict,
                    source_table=temp_cte_table,
                )
            self.cte_dict[cte_name] = temp_cte_dict

    def _resolve_proj(
        self,
        projection: expressions = None,
        col_name: Optional[str] = "",
        target_dict: Optional[dict] = None,
        source_table: Optional[List] = None,
    ) -> dict:
        """
        Resolve the projection given the projection expression
        :param projection: the given projection
        :param col_name: the column name
        :param target_dict: the dict it is outputting to
        :param source_table: all the source tables that this column might originate from
        """
        # Resolve count(*) with no alias, potentially other aggregations, MIN, MAX, SUM
        if projection.find(exp.Star) and not isinstance(projection.unalias(), exp.Array) and not isinstance(projection, exp.Array):
            if isinstance(projection, exp.Count):
                col_name = "count"
                target_dict = self._resolve_agg_star(
                    col_name=col_name,
                    projection=projection,
                    used_tables=source_table,
                    target_dict=target_dict,
                )
            elif isinstance(projection, exp.Avg):
                col_name = "avg"
                target_dict = self._resolve_agg_star(
                    col_name=col_name,
                    projection=projection,
                    used_tables=source_table,
                    target_dict=target_dict,
                )
            elif isinstance(projection, exp.Max):
                col_name = "max"
                target_dict = self._resolve_agg_star(
                    col_name=col_name,
                    projection=projection,
                    used_tables=source_table,
                    target_dict=target_dict,
                )
            elif isinstance(projection, exp.Min):
                col_name = "min"
                target_dict = self._resolve_agg_star(
                    col_name=col_name,
                    projection=projection,
                    used_tables=source_table,
                    target_dict=target_dict,
                )
            elif isinstance(projection, exp.Sum):
                col_name = "sum"
                target_dict = self._resolve_agg_star(
                    col_name=col_name,
                    projection=projection,
                    used_tables=source_table,
                    target_dict=target_dict,
                )
            else:
                target_dict = self._resolve_agg_star(
                    col_name=col_name,
                    projection=projection.unalias(),
                    used_tables=source_table,
                    target_dict=target_dict,
                )
        elif isinstance(projection.unalias(), exp.Array) or isinstance(projection, exp.Array):
            temp_col = []
            proj_columns = []
            for p in projection.find_all(exp.Column):
                temp_col.append(p.sql())
            for p in temp_col:
                proj_columns.extend(self._find_alias_col(
                                col_sql=p,
                                temp_table=source_table,
                            ))
            target_dict[col_name] = sorted(
                list(set(proj_columns).union(self.all_used_col))
                )
        else:
            proj_columns = []
            # Resolve only *
            if not isinstance(projection, exp.Column) and projection.find(exp.Star):
                for t_name in source_table:
                    if t_name in self.input_table_dict.keys():
                        star_cols = self.input_table_dict[t_name]
                        # every column from there will be a column with that name
                        for per_star_col in star_cols:
                            target_dict[per_star_col] = sorted(
                                list(
                                    set(
                                        self._find_alias_col(
                                            col_sql=per_star_col,
                                            temp_table=source_table,
                                        )
                                    ).union(self.all_used_col)
                                )
                            )
                    elif t_name in self.cte_dict.keys():
                        star_cols = list(self.cte_dict[t_name].keys())
                        for per_star_col in star_cols:
                            target_dict[per_star_col] = sorted(
                                list(
                                    set(self.cte_dict[t_name][per_star_col]).union(
                                        self.all_used_col
                                    )
                                )
                            )
            # Resolve projections that have many columns, some of which could be *
            for p in projection.find_all(exp.Column):
                # Resolve * with other columns
                if isinstance(p, exp.Column) and p.find(exp.Star):
                    t_name = p.find(exp.Identifier).text("this")
                    # Resolve alias
                    if t_name in self.table_alias_dict.keys():
                        t_name = self.table_alias_dict[t_name]
                    # If from input table, get all columns from there
                    if t_name in self.input_table_dict.keys():
                        star_cols = self.input_table_dict[t_name]
                        # every column from there will be a column with that name
                        for per_star_col in star_cols:
                            target_dict[per_star_col] = sorted(
                                list(
                                    set(
                                        self._find_alias_col(
                                            col_sql=per_star_col,
                                            temp_table=source_table,
                                        )
                                    ).union(self.all_used_col)
                                )
                            )
                    # If from another CTE, get all columns from there
                    elif t_name in self.cte_dict.keys():
                        star_cols = list(self.cte_dict[t_name].keys())
                        for per_star_col in star_cols:
                            target_dict[per_star_col] = sorted(
                                list(
                                    set(self.cte_dict[t_name][per_star_col]).union(
                                        self.all_used_col
                                    )
                                )
                            )
                    # If from an unknown table, leave it with a STAR as temporary name
                    else:
                        target_dict[p.sql()] = [p.sql()] + (list(self.all_used_col))
                else:
                    # one projection can have many columns, append first
                    proj_columns.extend(
                        self._find_alias_col(col_sql=p.sql(), temp_table=source_table)
                    )
            if proj_columns:
                target_dict[col_name] = sorted(
                    list(set(proj_columns).union(self.all_used_col))
                )
            # If the column only uses literals, like MAX(1)
            if not projection.find(exp.Column):
                target_dict[col_name] = sorted(list(self.all_used_col))
        return target_dict

    def _resolve_table(self, part_ast: expressions = None) -> List:
        """
        Find the tables in the given ast
        :param part_ast: the ast to find the table
        """
        temp_table_list = []
        for cond in from_join_exp:
            # Resolve FROM and JOIN
            for table_sql in part_ast.find_all(cond):
                # Skip GenerateSeries as a Table
                if table_sql.find(exp.GenerateSeries):
                    if table_sql.find(exp.GenerateSeries).depth <= table_sql.depth + 2:
                        continue
                # Resolve Unnest for creating tables
                elif table_sql.find(exp.Unnest):
                    temp_col_name = []
                    for t in table_sql.find_all(exp.Identifier):
                        temp_col_name.append(t.text("this"))
                        dep_tables = []
                        if len(temp_col_name) == 2:
                            dep_cols = self._find_alias_col(
                                col_sql=temp_col_name[1] + "." + temp_col_name[0],
                                temp_table=[temp_col_name[1]],
                            )
                            self.all_used_col.extend(dep_cols)
                            for x in dep_cols:
                                if len(x.split(".")) == 3:
                                    idx = x.rfind(".")
                                    dep_tables.append(x[:idx])
                                elif len(x.split(".")) == 2:
                                    dep_tables.append(x)
                            dep_tables = list(set(dep_tables))
                            self.table_alias_dict[temp_col_name[0]] = dep_tables
                            self.unnest_dict[temp_col_name[0]] = dep_cols
                            if table_sql.find(exp.TableAlias):
                                self.table_alias_dict[table_sql.find(exp.TableAlias).text("this")] = dep_tables
                                self.unnest_dict[table_sql.find(exp.TableAlias).text("this")] = dep_cols
                        temp_table_list.extend(dep_tables)
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
            self.table_alias_dict[table.sql()] = table.sql()
            temp_table_list.append(table.sql())
        else:
            temp = table.sql().split(" ")
            if temp[1] == "AS" or temp[1] == "as":
                self.table_alias_dict[temp[2]] = temp[0]
                temp_table_list.append(temp[0])
        return temp_table_list

    def _find_all_tables(self, temp_table_list: Optional[List] = None) -> List:
        """
        Update the used table names, such as if a CTE, update it with the dependant tables
        :param temp_table_list: temporary list of tables for appending the used tables
        :return:
        """
        ret_table = []
        for i in temp_table_list:
            table_name = i
            if i in self.table_alias_dict.keys():
                table_name = self.table_alias_dict[i]
            if table_name in self.cte_table_dict.keys():
                ret_table.extend(self.cte_table_dict[table_name])
            else:
                ret_table.append(table_name)
        return ret_table

    def _shared_col_conds(
        self, part_ast: expressions = None, used_tables: Optional[List] = None
    ):
        """
        Extract all the columns in the shared conditions(WHERE, GROUP BY, etc)
        :param part_ast: the ast of the sql to extract
        :param used_tables: the tables that this sql uses
        """
        # COMBINE THE CONDITIONS
        for cond in shared_conditions:
            for cond_sql in part_ast.find_all(cond):
                for cond_col in cond_sql.find_all(exp.Column):
                    self.all_used_col.extend(
                        self._find_alias_col(
                            col_sql=cond_col.sql(), temp_table=used_tables
                        )
                    )

    def _find_alias_col(
        self, col_sql: Optional[str] = "", temp_table: Optional[List] = None
    ) -> List:
        """
        Find the columns and its alias and dependencies if it is from a cte
        :param col_sql: the sql to the column
        :param temp_table: the table that the sql uses
        :return:
        """
        temp = col_sql.split(".")
        # trying to deduce the table if all possible tables are eliminated
        elim_table = []
        if col_sql in self.unnest_dict.keys():
            return self.unnest_dict[col_sql]
        if len(temp) < 2:
            for t in temp_table:
                if t in self.input_table_dict.keys():
                    if col_sql in self.input_table_dict[t]:
                        return [t + "." + col_sql]
                    else:
                        elim_table.append(t)
                elif t in self.cte_dict.keys():
                    if col_sql in self.cte_dict[t].keys():
                        return self.cte_dict[t][col_sql]
                    else:
                        elim_table.append(t)
            deduced_table = set(temp_table) - set(elim_table)
            if len(deduced_table) == 1:
                return [deduced_table.pop() + "." + col_sql]
        elif len(temp) == 2:
            if temp[0] in self.table_alias_dict.keys():
                t = self.table_alias_dict[temp[0]]
            else:
                t = temp[0]
            if t in self.cte_dict.keys():
                # CTE is stored, but the column name is not, resolve case sensitivity
                if temp[1] not in self.cte_dict[t].keys():
                    temp_cte_dict = {k.lower(): v for k, v in self.cte_dict[t].items()}
                    if temp[1].lower() in temp_cte_dict.keys():
                        return temp_cte_dict[temp[1].lower()]
                else:
                    return self.cte_dict[t][temp[1]]
            else:
                return [t + "." + temp[1]]
        return [col_sql]

    def _resolve_agg_star(
        self,
        col_name: Optional[str] = "",
        projection: expressions = None,
        used_tables: Optional[List] = None,
        target_dict: Optional[dict] = None,
    ) -> dict:
        """
        Trying to resolve the * and append appropriate columns if the table is able to resolved
        :param col_name: the name of the column
        :param projection: the expression of the sql
        :param used_tables: the tables that are used
        :param target_dict: the dict it is writing to
        """
        if projection.find(exp.Star):
            # * with a table name
            if projection.find(exp.Identifier):
                t_name = projection.find(exp.Identifier).text("this")
                # Resolve alias
                if t_name in self.table_alias_dict.keys():
                    t_name = self.table_alias_dict[t_name]
                if col_name == "*":
                    if t_name in self.input_table_dict.keys():
                        for s in self.input_table_dict[t_name]:
                            target_dict[s] = sorted(
                                list(
                                    set(
                                        self._find_alias_col(
                                            col_sql=t_name + "." + s,
                                            temp_table=used_tables,
                                        )
                                    ).union(self.all_used_col)
                                )
                            )
                    elif t_name in self.cte_dict.keys():
                        for s in list(self.cte_dict[t_name].keys()):
                            target_dict[s] = sorted(
                                list(
                                    set(
                                        self._find_alias_col(
                                            col_sql=t_name + "." + s,
                                            temp_table=used_tables,
                                        )
                                    ).union(self.all_used_col)
                                )
                            )
                    else:
                        target_dict[t_name + ".*"] = sorted(self.all_used_col)
                else:
                    if t_name in self.input_table_dict.keys():
                        star_cols = []
                        for s in self.input_table_dict[t_name]:
                            star_cols.extend(
                                self._find_alias_col(col_sql=s, temp_table=used_tables)
                            )
                    elif t_name in self.cte_dict.keys():
                        star_cols = []
                        for s in list(self.cte_dict[t_name].keys()):
                            star_cols.extend(
                                self._find_alias_col(col_sql=s, temp_table=used_tables)
                            )
                    else:
                        star_cols = [t_name + ".*"]
                    target_dict[col_name] = sorted(
                        list(set(star_cols).union(self.all_used_col))
                    )
            # only star
            else:
                # only star, so to get all the columns from the used tables
                if (
                    isinstance(projection.parent, exp.Select)
                    and projection.parent.depth + 1 == projection.depth
                    and not isinstance(projection, exp.Count)
                    and not isinstance(projection, exp.Min)
                    and not isinstance(projection, exp.Max)
                    and not isinstance(projection, exp.Sum)
                    and not isinstance(projection, exp.Avg)
                ):
                    for t_name in used_tables:
                        if t_name in self.table_alias_dict.keys():
                            t_name = self.table_alias_dict[t_name]
                        if t_name in self.input_table_dict.keys():
                            for s in self.input_table_dict[t_name]:
                                target_dict[s] = sorted(
                                    list(
                                        set(
                                            self._find_alias_col(
                                                col_sql=t_name + "." + s,
                                                temp_table=used_tables,
                                            )
                                        ).union(self.all_used_col)
                                    )
                                )
                        elif t_name in self.cte_dict.keys():
                            for s in list(self.cte_dict[t_name].keys()):
                                target_dict[s] = sorted(
                                    list(
                                        set(
                                            self._find_alias_col(
                                                col_sql=t_name + "." + s,
                                                temp_table=used_tables,
                                            )
                                        ).union(self.all_used_col)
                                    )
                                )
                        else:
                            target_dict[t_name + ".*"] = sorted(self.all_used_col)
                else:
                    # only star but it is an aggregation
                    target_dict[col_name] = sorted(
                        list(self.all_used_col) + [t + ".*" for t in used_tables]
                    )
        return target_dict


if __name__ == "__main__":
    pass

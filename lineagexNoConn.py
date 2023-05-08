from sqlglot import parse_one, exp
import os


class lineagexNoConn:
    def __init__(self, sql: str = "", input_table_dict: dict = None):
        self.column_dict = {}
        self.table_alias_dict = {}
        self.cte_table_dict = {}
        self.cte_dict = {}
        self.input_table_dict = input_table_dict
        self.sql_ast = parse_one(sql)
        self.all_used_col = []
        self.table_list = []
        self.all_subquery_table = []
        self.sub_tables = []
        self.sub_cols = []
        self.run_cte_lineage()
        # Everything other than CTEs, and pop the CTE tree
        for with_sql in self.sql_ast.find_all(exp.With):
            with_sql.pop()
        self.sub_shared_col_conds(self.sql_ast)
        self.run_lineage(self.sql_ast, False)
        #print(self.cte_dict)
        print(self.column_dict)
        print(self.table_list)

    def run_lineage(self, sql_ast, subquery_flag):
        #print(sql_ast)
        if not subquery_flag:
            main_tables = self.resolve_table(sql_ast)
            self.table_list = self.find_all_tables(main_tables)
            self.table_list.extend(self.all_subquery_table)
            self.all_used_col = []
            self.shared_col_conds(sql_ast, main_tables)
            self.all_used_col.extend(self.sub_cols)
            self.all_used_col = set(self.all_used_col)
            if sql_ast.find(exp.Select):
                for projection in sql_ast.find(exp.Select).expressions:
                    col_name = projection.alias_or_name
                    self.column_dict = self.resolve_proj(projection, col_name, self.column_dict, main_tables)
        else:
            temp_sub_cols = []
            for col in sql_ast.find_all(exp.Column):
                temp_sub_cols.extend(self.find_alias_col(col.sql(), self.sub_tables))
            self.sub_cols.extend(temp_sub_cols)
            print(temp_sub_cols)

    def sub_shared_col_conds(self, sql_ast):
        # add in more conditions, including FROM/JOIN
        for where_sql in sql_ast.find_all(exp.Where):
            for sub_ast in where_sql.find_all(exp.Subquery):
                self.sub_tables = self.resolve_table(sub_ast)
                self.all_subquery_table.extend(self.find_all_tables(self.sub_tables))
                self.run_lineage(sub_ast, True)
                sub_ast.pop()

    def sub_shared_col_conds_cte(self, sql_ast):
        all_cte_sub_table = []
        all_cte_sub_cols = []
        # add in more conditions, including FROM/JOIN
        for where_sql in sql_ast.find_all(exp.Where):
            for sub_ast in where_sql.find_all(exp.Select):
                temp_sub_table = self.resolve_table(sub_ast)
                temp_sub_cols = []
                for col in sub_ast.find_all(exp.Column):
                    temp_sub_cols.extend(self.find_alias_col(col.sql(), temp_sub_table))
                all_cte_sub_table.extend(self.find_all_tables(temp_sub_table))
                all_cte_sub_cols.extend(temp_sub_cols)
                sub_ast.pop()
        return all_cte_sub_table, all_cte_sub_cols

    def run_cte_lineage(self):
        for cte in self.sql_ast.find_all(exp.CTE):
            all_cte_sub_table, all_cte_sub_cols = self.sub_shared_col_conds_cte(cte)
            self.all_used_col = []
            temp_cte_dict = {}
            temp_cte_table = self.resolve_table(cte)
            cte_name = cte.find(exp.TableAlias).alias_or_name
            self.cte_table_dict[cte_name] = list(set(self.find_all_tables(temp_cte_table) + all_cte_sub_table))
            # Resolving shared conditions
            self.shared_col_conds(cte, temp_cte_table)
            self.all_used_col.extend(all_cte_sub_cols)
            self.all_used_col = set(self.all_used_col)
            # Resolving the projection
            for projection in cte.find(exp.Select).expressions:
                col_name = projection.alias_or_name
                temp_cte_dict = self.resolve_proj(projection, col_name, temp_cte_dict, temp_cte_table)
            self.cte_dict[cte_name] = temp_cte_dict

    def resolve_proj(self, projection, col_name, target_dict, source_table):
        # Resolve count(*) with no alias, potentially other aggregations, MIN, MAX, SUM
        if isinstance(projection, exp.Count) or isinstance(projection.unalias(), exp.Count):
            if isinstance(projection, exp.Count):
                col_name = "count"
                self.resolve_agg_star(col_name, projection, source_table)
            else:
                self.resolve_agg_star(col_name, projection.unalias(), source_table)
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
                                list(set(self.find_alias_col(per_star_col, source_table)).union(
                                    self.all_used_col)))
                    elif t_name in self.cte_dict.keys():
                        star_cols = list(self.cte_dict[t_name].keys())
                        for per_star_col in star_cols:
                            target_dict[per_star_col] = sorted(
                                list(set(self.cte_dict[t_name][per_star_col]).union(self.all_used_col)))
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
                                list(set(self.find_alias_col(per_star_col, source_table)).union(
                                    self.all_used_col)))
                    # If from another CTE, get all columns from there
                    elif t_name in self.cte_dict.keys():
                        star_cols = list(self.cte_dict[t_name].keys())
                        for per_star_col in star_cols:
                            target_dict[per_star_col] = sorted(
                                list(set(self.cte_dict[t_name][per_star_col]).union(self.all_used_col)))
                    # If from an unknown table, leave it with a STAR as temporary name
                    else:
                        target_dict[p.sql()] = [p.sql()] + (list(self.all_used_col))
                else:
                    # one projection can have many columns, append first
                    proj_columns.extend(self.find_alias_col(p.sql(), source_table))
            if proj_columns:
                target_dict[col_name] = sorted(list(set(proj_columns).union(self.all_used_col)))
        return target_dict

    def resolve_table(self, part_ast):
        temp_table_list = []
        # Resolve FROM, COMBINE IT WITH JOIN/UNION/INTERSECT/EXCEPT
        for table_sql in part_ast.find_all(exp.From):
            for table in table_sql.find_all(exp.Table):
                temp_table_list = self.find_table(table, temp_table_list)
        # Resolve JOIN
        for table_sql in part_ast.find_all(exp.Join):
            for table in table_sql.find_all(exp.Table):
                temp_table_list = self.find_table(table, temp_table_list)
        return temp_table_list

    def find_table(self, table, temp_table_list):
        # Update table alias and find all aliased used table names
        if table.alias == "":
            self.table_alias_dict[table.sql()] = table.sql()
            temp_table_list.append(table.sql())
        else:
            temp = table.sql().split(" ")
            if temp[1] == "AS" or temp[1] == "as":
                self.table_alias_dict[temp[2]] = temp[0]
                temp_table_list.append(temp[0])
        return temp_table_list

    def find_all_tables(self, temp_table_list):
        # Update the used table names, such as if a CTE, update it with the dependant tables
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

    def shared_col_conds(self, part_ast, used_tables):
        # COMBINE THE CONDITIONS
        for where_sql in part_ast.find_all(exp.Where):
            for where_col in where_sql.find_all(exp.Column):
                self.all_used_col.extend(self.find_alias_col(where_col.sql(), used_tables))

        for on_sql in part_ast.find_all(exp.EQ):
            for on_col in on_sql.find_all(exp.Column):
                self.all_used_col.extend(self.find_alias_col(on_col.sql(), used_tables))

        for group_sql in part_ast.find_all(exp.Group):
            for group_col in group_sql.find_all(exp.Column):
                self.all_used_col.extend(self.find_alias_col(group_col.sql(), used_tables))

        for having_sql in part_ast.find_all(exp.Having):
            for having_col in having_sql.find_all(exp.Column):
                self.all_used_col.extend(self.find_alias_col(having_col.sql(), used_tables))

        for order_sql in part_ast.find_all(exp.Order):
            for order_col in order_sql.find_all(exp.Column):
                self.all_used_col.extend(self.find_alias_col(order_col.sql(), used_tables))

    def find_alias_col(self, col_sql, temp_table):
        temp = col_sql.split(".")
        # trying to deduce the table if all possible tables are eliminated
        elim_table = []
        if len(temp) < 2:
            for t in temp_table:
                if t in input_table_dict.keys():
                    if col_sql in input_table_dict[t]:
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
                return self.cte_dict[t][temp[1]]
            else:
                return [t + "." + temp[1]]
        return [col_sql]

    def resolve_agg_star(self, col_name, projection, used_tables):
        if projection.find(exp.Star):
            # * with a table name
            if projection.find(exp.Identifier):
                t_name = projection.find(exp.Identifier).text("this")
                # Resolve alias
                if t_name in self.table_alias_dict.keys():
                    t_name = self.table_alias_dict[t_name]
                if t_name in input_table_dict.keys():
                    star_cols = []
                    for s in input_table_dict[t_name]:
                        star_cols.extend(self.find_alias_col(s, used_tables))
                elif t_name in self.cte_dict.keys():
                    star_cols = []
                    for s in list(self.cte_dict[t_name].keys()):
                        star_cols.extend(self.find_alias_col(s, used_tables))
                else:
                    star_cols = [t_name + ".*"]
                self.column_dict[col_name] = sorted(list(set(star_cols).union(self.all_used_col)))
            # only star, like count(*)
            else:
                self.column_dict[col_name] = sorted(list(self.all_used_col))


if __name__ == "__main__":
    sql = "WITH agetbl AS ( SELECT ad.subject_id FROM mimiciii_clinical.admissions ad INNER JOIN patients p ON ad.subject_id = p.subject_id WHERE DATETIME_DIFF(ad.admittime, p.dob, 'YEAR'::TEXT) > 15 group by ad.subject_id HAVING ad.subject_id > 5 ),bun as ( SELECT width_bucket(valuenum,0,280,280) AS bucket,le.* FROM mimiciii_clinical.labevents le INNER JOIN agetbl ON le.subject_id = agetbl.subject_id WHERE itemid IN (51006) ) SELECT bucket as blood_urea_nitrogen,count(bun.*) as c FROM bun GROUP BY bucket ORDER BY bucket;"
    #sql = "DELETE FROM Customers WHERE CustomerName='Alfreds Futterkiste';"
    #input_table_dict = {"mimiciii_clinical.labevents": ['itemid', 'valuenum', 'subject_id']}
    input_table_dict = {}
    lineagexNoConn(sql, input_table_dict)

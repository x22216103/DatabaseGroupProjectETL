def createPostGres(username,password):
    return 0
def pushTransformedData(Storm,Property,Census):
    return 0

visualise_Query_Join= """SELECT item_a FROM table_a INNER JOIN table_b ON item_a = column_b;"""
visualise_Query_GroupBy = """SELECT item_a, name, date FROM table_a GROUPBY name;"""
visualise_Query_OrderBy = """SELECT item_a, name, date FROM table_a ORDERBY name;"""
visualise_Query_Where_Clause = """SELECT item_a, name, date FROM table_a WHERE name == 'Dave';"""
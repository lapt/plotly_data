import pandas as pd
from sqlalchemy import exc


def get_user_chile(connection):

    sql = "SELECT * FROM percapita_db.user_chile;"

    try:
        data = pd.read_sql_query(sql, connection)
        return data
    except exc.SQLAlchemyError, e:
        print("Error: %s" % str(e))
        return -1


def get_user_tweets_chile(connection):
    sql = "SELECT * FROM user_tweets_chile order by region asc;"
    try:
        data = pd.read_sql_query(sql, connection)
        return data
    except exc.SQLAlchemyError, e:
        print("Error: %s" % str(e))
        return -1

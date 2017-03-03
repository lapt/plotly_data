import pandas as pd
from sqlalchemy import exc


def get_percapita_regiones_date(connection, region_code):
    sql = "SELECT * FROM percapita_regiones_date " \
            "where region_code = %s and percapita_year > 2009 " \
            "order by percapita_year, percapita_month asc;"
    try:
        data = pd.read_sql_query(sql, connection, params=(region_code, ))
    except exc.SQLAlchemyError, e:
        print("Error: %s" % str(e))
        return -1
    data['comodin'] = "/"
    data['list_month_year'] = data['percapita_year'].map(str) + data['comodin'] + data["percapita_month"].map(str)
    data = data.drop("comodin", 1)
    data = data.drop("percapita_year", 1)
    data = data.drop("percapita_month", 1)
    return data


def get_percapita_regiones_date_by_year(connection, region_code, percapita_year):
    sql = "SELECT region_code, percapita_year, percapita_month, tweets_positive, tweets_negative, " \
            "(tweets_positive*100/(tweets_positive + tweets_negative)) as percentage_positive " \
            "FROM percapita_regiones_date where region_code = %s and percapita_year = %s " \
            "order by percapita_year, percapita_month asc;"


    try:
        data = pd.read_sql_query(sql, connection, params=(region_code, percapita_year))
    except exc.SQLAlchemyError, e:
        print("Error: %s" % str(e))
        return -1
    data['comodin'] = "/"
    data['list_month_year'] = data['percapita_year'].map(str) + data['comodin'] + data["percapita_month"].map(str)
    data = data.drop("comodin", 1)
    data = data.drop("percapita_year", 1)
    data = data.drop("percapita_month", 1)
    return data


def get_percapita_regiones_date_percentage(connection, region_code, percapita_year):
    sql = " SELECT * FROM percapita_regiones_date " \
          "where region_code = %s and percapita_year = %s " \
          "order by percapita_year, percapita_month asc;"


    try:
        data = pd.read_sql_query(sql, connection, params=(region_code, percapita_year))
    except exc.SQLAlchemyError, e:
        print("Error: %s" % str(e))
        return -1
    data['comodin'] = "/"
    data['list_month_year'] = data['percapita_year'].map(str) + data['comodin'] + data["percapita_month"].map(str)
    data = data.drop("comodin", 1)
    data = data.drop("percapita_year", 1)
    data = data.drop("percapita_month", 1)
    return data


def get_percapita_by_year(connection):
    sql = "select percapita_year, percapita_month, sum(tweets_positive) as tp , sum(tweets_negative) as tn " \
            " from percapita_db.percapita_regiones_date where percapita_year between 2010 and 2015 " \
            "group by percapita_year, percapita_month order by percapita_year, percapita_month asc;"


    try:
        data = pd.read_sql_query(sql, connection)
    except exc.SQLAlchemyError, e:
        print("Error: %s" % str(e))
        return -1
    data['comodin'] = "/"
    data['list_month_year'] = data['percapita_year'].map(str) + data['comodin'] + data["percapita_month"].map(str)
    data['cien'] = 100
    data['list_percentage_positive'] = (data['tp'] * data['cien']/(data['tp'] + data['tn']))
    data['list_percentage_negative'] = data['cien'] - data['list_p_positive']
    data = data.drop("comodin", 1)
    data = data.drop("percapita_year", 1)
    data = data.drop("percapita_month", 1)
    data = data.drop("cien", 1)
    return data
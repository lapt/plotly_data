import pandas as pd
from sqlalchemy import exc


def get_tweets_sentiment_avg(connection, region_code):
    sql = "SELECT * FROM tweets_sentiment_avg " \
            "where region_code = %s and avg_year between 2010 and 2015 " \
            "order by avg_year, avg_month asc;"


    try:
        data = pd.read_sql_query(sql, connection, params=(region_code,))
    except exc.SQLAlchemyError, e:
        print("Error: %s" % str(e))
        return -1
    data['comodin'] = "/"
    data['list_month_year'] = data['percapita_year'].map(str) + data['comodin'] + data["percapita_month"].map(str)
    data = data.drop("comodin", 1)
    data = data.drop("percapita_year", 1)
    data = data.drop("percapita_month", 1)
    return data


def get_tweets_sentiment_avg_by_year(connection, region_code, avg_year):
    sql = "SELECT * FROM tweets_sentiment_avg " \
            "where region_code = %s and avg_year = %s " \
            "order by avg_year, avg_month asc;"


    try:
        data = pd.read_sql_query(sql, connection, params=(region_code, avg_year))
    except exc.SQLAlchemyError, e:
        print("Error: %s" % str(e))
        return -1
    data['comodin'] = "/"
    data['list_month_year'] = data['percapita_year'].map(str) + data['comodin'] + data["percapita_month"].map(str)
    data = data.drop("comodin", 1)
    data = data.drop("percapita_year", 1)
    data = data.drop("percapita_month", 1)
    return data


def get_tweets_sentiment_avg_by_year_table(connection, region_code, avg_year):
    sql = "SELECT * FROM tweets_sentiment_avg " \
            "where region_code = %s and avg_year = %s " \
            "order by avg_year, avg_month asc;"


    try:
        data = pd.read_sql_query(sql, connection, params=(region_code, avg_year))
    except exc.SQLAlchemyError, e:
        print("Error: %s" % str(e))
        return -1
    data['comodin'] = "/"
    data['list_month_year'] = data['percapita_year'].map(str) + data['comodin'] + data["percapita_month"].map(str)
    data = data.drop("comodin", 1)
    data = data.drop("percapita_year", 1)
    data = data.drop("percapita_month", 1)
    return data


def get_tweets_sentiment_avg_total(connection):
    sql = "SELECT * FROM tweets_sentiment_avg " \
            "order by avg_year, avg_month asc;"


    try:
        data = pd.read_sql_query(sql, connection)
    except exc.SQLAlchemyError, e:
        print("Error: %s" % str(e))
        return -1
    data['comodin'] = "/"
    data['list_month_year'] = data['percapita_year'].map(str) + data['comodin'] + data["percapita_month"].map(str)
    data = data.drop("comodin", 1)
    data = data.drop("percapita_year", 1)
    data = data.drop("percapita_month", 1)
    return data



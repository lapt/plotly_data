# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import exc


def get_percapita_regiones(connection):
    sql = "SELECT * FROM percapita_regiones;"
    try:
        data = pd.read_sql_query(sql, connection)
        return data
    except exc.SQLAlchemyError, e:
        print("Error: %s" % str(e))
        return -1


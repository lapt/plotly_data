from db_mysql_connector.Connection_lpari_db import get_connection_mysql
from statistics_data.user_chile import get_user_chile
from statistics_data.percapita_regiones_date import get_percapita_regiones_date
from statistics_data.percapita_regiones_date import get_percapita_by_year

def main():
    cn = get_connection_mysql()
    df_user = get_percapita_by_year(cn)
    cn.close()
    pass

if __name__ == '__main__':
    main()
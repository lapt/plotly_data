# -*- coding: utf-8 -*-
from db_mysql_connector.Connection_lpari_db import get_connection_mysql
from statistics_data.user_chile import get_user_chile
from statistics_data.user_chile import get_user_tweets_chile
from statistics_data.percapita_regiones import get_percapita_regiones
from statistics_data.percapita_regiones_date import get_percapita_regiones_date
from statistics_data.percapita_regiones_date import get_percapita_by_year

REGIONES = {
    '00': 'Chile*',
    '03': 'Antofagasta',
    '05': 'Atacama',
    '07': 'Coquimbo',
    '14': 'Los Lagos',
    '11': 'Maule',
    '02': 'Aisén',
    '16': 'Arica y P.',
    '04': 'Araucanía',
    '17': 'Los Ríos',
    '10': 'Magallanes',
    '15': 'Tarapacá',
    '01': 'Valparaíso',
    '06': 'Bíobío',
    '08': 'O’Higgins',
    '12': 'RM Santiago'
}


def generate_csv_main(cn):
    user_chile = get_user_chile(cn)
    user_chile.to_csv("data_write/user_chile.csv", index=False, header=True, encoding='utf-8', sep=",")
    user_tweets_chile = get_user_tweets_chile(cn)
    user_tweets_chile['region'] = user_tweets_chile['region'].apply(lambda x: REGIONES[x])
    user_tweets_chile.to_csv("data_write/user_tweets_chile.csv", index=False, header=True, encoding='utf-8', sep=",")


def csv_percapita_regiones_table(cn):
    percapita_regiones = get_percapita_regiones(cn)
    percapita_regiones['region_code'] = percapita_regiones['region_code'].apply(lambda x: REGIONES[x])
    percapita_regiones.to_csv("data_write/percapita_regiones.csv", index=False, header=True, encoding='utf-8', sep=",")


def csv_percapita_regiones_date_by_year(cn):
    pass


def csv_percentage_positive_sentiment_date(cn):
    pass


def csv_sentiment_avg_regiones_date_by_year(cn):
    pass


def csv_chart_area_year(cn):
    pass


def main():
    cn = get_connection_mysql()
    csv_percapita_regiones_table(cn)

if __name__ == '__main__':
    main()
# -*- coding: utf-8 -*-
from db_mysql_connector.Connection_lpari_db import get_connection_mysql
from statistics_data.user_chile import get_user_chile
from statistics_data.user_chile import get_users_by_region
from statistics_data.user_chile import get_tweets_by_region
from statistics_data.percapita_regiones import get_percapita_regiones
from statistics_data.percapita_regiones_date import get_percapita_regiones_date
from statistics_data.percapita_regiones_date import get_percapita_by_year
import pandas as pd
import numpy as np


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

    user_by_region = get_users_by_region(cn)
    user_by_region['region'] = user_by_region['region'].apply(lambda x: REGIONES[x])
    user_by_region.to_csv("data_write/user_by_region.csv", index=False, header=True, encoding='utf-8', sep=",")

    tweet_by_region = get_tweets_by_region(cn)
    tweet_by_region['region'] = tweet_by_region['region'].apply(lambda x: REGIONES[x])
    tweet_by_region.to_csv("data_write/tweet_by_region.csv", index=False, header=True, encoding='utf-8', sep=",")


def csv_percapita_regiones_table(cn):
    percapita_regiones = get_percapita_regiones(cn)
    percapita_regiones['region_code'] = percapita_regiones['region_code'].apply(lambda x: REGIONES[x])
    percapita_regiones.to_csv("data_write/percapita_regiones.csv", index=False, header=True, encoding='utf-8', sep=",")


def csv_percapita_regiones_date_by_year(cn):
    data = get_percapita_regiones_date(cn)
    data['region_code'] = data['region_code'].apply(lambda x: REGIONES[x])

    for year in range(2015, 2016):
        df_date = data[data['percapita_year'] == year]
        series_titles = df_date['list_month_year'].unique()
        for title in series_titles:
            try:
                series_date = df_date[df_date['list_month_year'] == title]
                series_date = series_date.set_index(series_date['region_code'])
                df_year_p = pd.concat([df_year_p, series_date['percapita_positive']], axis=1)
                df_year_n = pd.concat([df_year_n, series_date['percapita_negative']], axis=1)
            except NameError:
                df_year_p = pd.DataFrame(index=series_date.index)
                df_year_n = pd.DataFrame(index=series_date.index)
                df_year_p = pd.concat([df_year_p, series_date['region_code'], series_date['percapita_positive']], axis=1)
                df_year_n = pd.concat([df_year_n, series_date['region_code'], series_date['percapita_negative']], axis=1)

        df_year_p.columns = np.concatenate((np.array(['region_code']), series_titles))
        df_year_n.columns = np.concatenate((np.array(['region_code']), series_titles))
        df_year_p.to_csv("data_write/percapita_regiones_p_%d.csv" % year, index=False, header=True, encoding='utf-8',
                         sep=",")
        df_year_n.to_csv("data_write/percapita_regiones_n_%d.csv" % year, index=False, header=True, encoding='utf-8',
                         sep=",")
        pass


def csv_percentage_positive_sentiment_date(cn):
    pass


def csv_sentiment_avg_regiones_date_by_year(cn):
    pass


def csv_chart_area_year(cn):
    pass


def main():
    cn = get_connection_mysql()
    csv_percapita_regiones_date_by_year(cn)


def prueba():
    cn = get_connection_mysql()
    generate_csv_main(cn)

if __name__ == '__main__':
    prueba()
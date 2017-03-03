from sqlalchemy import create_engine
import credentials_lpari_db as k
from sqlalchemy import exc

__author__ = 'luisangel'


def get_connection_mysql():
    # Returns a connection object whom will be given to any DB Query function.
    try:
        str_connection = "mysql://%s:%s@%s:%s/%s" % (k.DB_USER, k.DB_KEY, k.DB_HOST, k.DB_PORT, k.DB_NAME)
        engine = create_engine(str_connection, echo=False, encoding='utf-8')
        cn = engine.connect()
        return cn
    except exc.DatabaseError, e:
        print "..................................................."
        print "...Failed to create engine in database_setup.py...."
        print "..................................................."
        print str(e)


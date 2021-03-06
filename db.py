import os, sys
import mysql.connector
from mysql.connector.errors import DatabaseError

ENV_DB_HOST = os.getenv( 'DB_HOST' )
DB_USER = 'ntd'
DB_PASS = 'ntd'
DB_HOST = ENV_DB_HOST if ENV_DB_HOST else '127.0.0.1'
DB_PORT = 3306
DB_NAME = 'ntd'

class db(object):

    def __init__(self):
        try:
            self._db_connection = mysql.connector.connect( user = DB_USER, password = DB_PASS, host = DB_HOST, port = DB_PORT, database = DB_NAME )
        except DatabaseError as d:
            print( f"xx> db error: {d}" )
            sys.exit()

        self._db_cur = self._db_connection.cursor( buffered = True, dictionary = True )

    def insert(self, query, params):
        self._db_cur.execute(query, params)
        return self._db_cur.lastrowid

    def query(self, query, params):
        self._db_cur.execute(query, params)

    def fetchone( self, query, params ):
        self._db_cur.execute( query, params )
        return self._db_cur.fetchone()

    def cursor(self):
        return self._db_cur

    def commit(self):
        self._db_connection.commit()

    def __del__(self):
        if hasattr( self, '_db_connection' ):
            print( '==> db connection closing' )
            self._db_connection.close()

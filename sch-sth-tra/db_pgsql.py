import os, sys
import psycopg2
import psycopg2.extras

ENV_DB_HOST = os.getenv( 'DB_HOST' )
DB_USER = 'ntd'
DB_PASS = 'ntd'
DB_HOST = ENV_DB_HOST if ENV_DB_HOST else '127.0.0.1'
DB_PORT = 5432
DB_NAME = 'ntd'

class db(object):

    def __init__(self):

        try:

            self._db_connection = psycopg2.connect( f"host='{DB_HOST}' port={DB_PORT} dbname='{DB_NAME}' user='{DB_USER}' password='{DB_PASS}'" )

        except psycopg2.DatabaseError as d:

            print( f"xx> db error: {d}" )
            sys.exit()

        self._db_cur = self._db_connection.cursor( cursor_factory = psycopg2.extras.RealDictCursor )

    def insert( self, query, params, id_key='id' ):
        self._db_cur.execute( query, params )
        last_row = self._db_cur.fetchone()
        return last_row[ id_key ]

    def query( self, query, params ):
        self._db_cur.execute( query, params )

    def fetchone( self, query, params ):
        self._db_cur.execute( query, params )
        return self._db_cur.fetchone()

    def cursor( self ):
        return self._db_cur

    def commit( self ):
        self._db_connection.commit()

    def __del__( self ):
        if hasattr( self, '_db_connection' ):
            sys.stderr.write( '==> db connection closing\n' )
            self._db_connection.close()

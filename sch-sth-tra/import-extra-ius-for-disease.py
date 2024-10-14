import sys
import os
import re

from db import db

# SQL statements
insert_iu_sql = '''
INSERT INTO iu ( code )
VALUES ( %s )
ON CONFLICT ( code ) DO UPDATE SET code = EXCLUDED.code
RETURNING id
'''

insert_join_sql = '''
INSERT INTO iu_disease ( iu_id, disease_id )
VALUES ( %s, %s )
ON CONFLICT ( iu_id, disease_id ) DO UPDATE SET iu_id = EXCLUDED.iu_id, disease_id = EXCLUDED.disease_id
RETURNING iu_id
'''

# importer function
def import_disease_ius( disease_short_code, filename, content, DB ):

    components = filename.split( '.' )[ 0 ].split( "-" )

    print( f'-> {disease_short_code}' )
    params = ( disease_short_code, )
    disease_id = DB.fetchone( "SELECT id FROM disease WHERE short = %s", params )['id']

    inserted = 0
    ius = content.split( b"\n" )
    for iu in ius:
        iu_str = iu.decode( "utf-8" )
        if re.match( r'[A-Z]{3}\d{5}', iu_str ):
            iu_id = DB.insert( insert_iu_sql, ( iu_str, ) )
            if iu_id == 0:
                params = ( iu_str, )
                iu_id = DB.fetchone( "SELECT id FROM iu WHERE code = %s", params )['id']

            DB.insert( insert_join_sql, ( iu_id, disease_id, ), 'iu_id' )
            inserted = inserted + 1
            if inserted % 500 == 0:
                print( f"--> inserted {inserted} IUs" )

    DB.commit()

# main code block
if __name__ == '__main__':

    # check for IU data file
    if len( sys.argv ) != 3 :
        print( f"usage: {sys.argv[0]} <disease-short-code> <filename>" )
        sys.exit( 1 )

    # instantiate local db module / connection
    DB = db()

    disease_short_code = sys.argv[ 1 ]
    named_file = sys.argv[ 2 ]

    with open( named_file, 'rb' ) as file:
        import_disease_ius( disease_short_code, os.path.basename( named_file ), file.read(), DB )

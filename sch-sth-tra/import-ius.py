import tarfile
import sys
import os
import re

from db import db

# SQL statements
insert_disease_sql = '''
INSERT INTO disease ( type, species, short )
VALUES ( %s, %s, %s )
ON CONFLICT ( type, species, short ) DO UPDATE SET type = EXCLUDED.type, species = EXCLUDED.species, short = EXCLUDED.short
'''

insert_iu_sql = '''
INSERT INTO iu ( code )
VALUES ( %s )
ON CONFLICT ( code ) DO NOTHING
'''

insert_join_sql = '''
INSERT INTO iu_disease ( iu_id, disease_id )
VALUES ( %s, %s )
ON CONFLICT ( iu_id, disease_id ) DO NOTHING
'''

# importer function
def import_disease_ius( filename, content, DB ):

    components = filename.split( '.' )[ 0 ].split( "-" )

    disease_type = components[0]
    disease_species = components[1]
    disease_short = components[2]

    print( f'-> {disease_type} {disease_species} {disease_short}' )
    disease_id = DB.insert( insert_disease_sql, ( disease_type, disease_species, disease_short ) )

    if disease_id == 0:
        params = ( disease_type, disease_species )
        disease_id = DB.fetchone( "SELECT id FROM disease WHERE type = %s AND species = %s", params )['id']

    inserted = 0
    ius = content.split( b"\n" )
    for iu in ius:
        iu_str = iu.decode( "utf-8" )
        if re.match( r'[A-Z]{3}\d{5}', iu_str ):
            iu_id = DB.insert( insert_iu_sql, ( iu_str, ) )
            if iu_id == 0:
                params = ( iu_str, )
                iu_id = DB.fetchone( "SELECT id FROM iu WHERE code = %s", params )['id']

            DB.insert( insert_join_sql, ( iu_id, disease_id, ) )
            inserted = inserted + 1
            if inserted % 500 == 0:
                print( f"--> inserted {inserted} IUs" )

    DB.commit()

# main code block
if __name__ == '__main__':

    # check for IU data file
    if len( sys.argv ) != 2 :
        print( f"usage: {sys.argv[0]} <directory-containing-IU-data>" )
        sys.exit( 1 )

    # instantiate local db module / connection
    DB = db()

    named_file = sys.argv[ 1 ]

    # either extract & read from tar
    if tarfile.is_tarfile( named_file ):

        print( f"-> reading entries from tar file {named_file}" )

        # open IU data file
        tar = tarfile.open( named_file, "r:bz2" )

        # add disease & IU data for each disease file inside data file
        for member in tar.getmembers():
            import_disease_ius( member.name, tar.extractfile( member ).read(), DB )

    # or just import the specific named file
    else:
        print( f"-> reading data from file {named_file}" )
        with open( named_file, 'rb' ) as file:
            import_disease_ius( os.path.basename( named_file ), file.read(), DB )


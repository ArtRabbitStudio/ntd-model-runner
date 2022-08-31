import tarfile
import sys
import re

from db import db

# SQL statements
insert_disease_sql = '''
INSERT INTO disease ( `type`, `species`, `short` )
VALUES ( %s, %s, %s )
ON DUPLICATE KEY UPDATE type = type, species = species, short = short
'''

insert_iu_sql = '''
INSERT INTO iu ( `code` )
VALUES ( %s )
ON DUPLICATE KEY UPDATE code = code
'''

insert_join_sql = '''
INSERT IGNORE INTO iu_disease ( `iu_id`, `disease_id` )
VALUES ( %s, %s )
'''

# instantiate local db module
DB = db()

# check for IU data file
if len( sys.argv ) != 2 :
    print( f"usage: {sys.argv[0]} <tar-bz-file-containing-IU-data>" )
    sys.exit( 1 )

# open IU data file
iu_data_tar_file = sys.argv[ 1 ]
tar = tarfile.open( iu_data_tar_file, "r:bz2" )

# add disease & IU data for each disease file inside data file
for member in tar.getmembers():

    components = member.name.split( '.' )[ 0 ].split( "-" )

    disease_type = components[0]
    disease_species = components[1]
    disease_short = components[2]

    print( f'-> {disease_type} {disease_species} {disease_short}' )
    disease_id = DB.insert( insert_disease_sql, ( disease_type, disease_species, disease_short ) )

    if disease_id == 0:
        params = ( disease_type, disease_species )
        disease_id = DB.fetchone( "SELECT id FROM disease WHERE type = %s AND species = %s", params )['id']

    content = tar.extractfile(member).read()
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

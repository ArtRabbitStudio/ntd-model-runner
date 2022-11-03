import sys
import pandas as pd

from pathlib import Path

from db import db

DB = db()

# check for IU data file
if len( sys.argv ) != 2 :
    print( f"usage: {sys.argv[0]} <tar-bz-file-containing-IU-data>" )
    sys.exit( 1 )

group_data_dir = sys.argv[ 1 ]
glob_path = 'scen_grp_ref_*.csv'
files = list( Path( group_data_dir ).glob( glob_path ) )
if len( files ) == 0:
    print( f"-> no {glob_path} files found in {group_data_dir}, exiting" )
    sys.exit()

fetch_sql = '''
SELECT d.id AS disease_id, d.short, iu.id AS iu_id, iu.code
FROM disease d, iu, iu_disease iud
WHERE d.id = iud.disease_id AND iu.id = iud.iu_id
AND iu.code = %s
AND lower(d.species) = %s
'''

insert_sql = '''
INSERT INTO iu_disease_group ( iu_id, disease_id, group_id )
VALUES ( %s, %s, %s )
ON CONFLICT( iu_id, disease_id, group_id ) DO NOTHING
'''

for file in files:

    species = file.as_posix().split( '/' )[ 1 ].split( '.' )[ 0 ].split( '_' )[ 3 ]
    df = pd.read_csv( file )

    print( f'-> adding iu groups for {species}' )
    ius_added_to_groups = 0

    for rowIndex, row in df.iterrows():

        if ius_added_to_groups > 0 and ius_added_to_groups % 1000 == 0:
            print( f'-> added {ius_added_to_groups} out of {rowIndex} IUs to groups' )

        iu = df.loc[ rowIndex ][ 'IU_ID2' ]

        fetch_params = ( iu, species )
        record = DB.fetchone( fetch_sql, fetch_params )

        if( record == None ):
            print( f"no cloud data for {species}:{iu}" )
            continue
        
        # {'disease_id': 2, 'short': 'Asc', 'iu_id': 1670, 'code': 'KEN54039'}
        iu_id = record[ 'iu_id' ]
        disease_id = record[ 'disease_id' ]

        # IU_ID    IU_ID2  scen  group    median  97.5 qtl
        # 46493  TZA46493     0      1  0.089995  0.153408
        group_id = int( df.loc[ rowIndex ][ 'group' ] )

        insert_params = ( iu_id, disease_id, group_id )
        res = DB.insert( insert_sql, insert_params )
       
        ius_added_to_groups = ius_added_to_groups + 1
        
    DB.commit()
    print( f'-> added {ius_added_to_groups} total IUs to groups out of {len(df)} found in group file' )



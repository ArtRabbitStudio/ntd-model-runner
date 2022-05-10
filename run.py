import sys

# local imports
from ntd_model_runner import run
from db import db

# instantiate local db module
DB = db()

runs_sql = '''
SELECT d.type, d.species, d.short, i.code AS iu_code, d.id AS disease_id, i.id AS iu_id
FROM diseases d, ius i, ius_diseases i_d
WHERE i.id = i_d.id_ius and d.id = i_d.id_diseases
ORDER BY d.type, d.species, i.code
LIMIT %s
'''

delete_runs_sql = '''
DELETE FROM results
WHERE disease_id = %s
AND iu_id = %s
'''

# maybe get number of runs from args
limit = int( sys.argv[ 1 ] ) if len( sys.argv ) > 1 else 1

# get the runs
disease_iu_combos = DB.query( runs_sql, ( limit, ) )
print( f'-> found {DB.cursor().rowcount} disease/IU combos' )

# track current run
last_iu_combo = None

# cloud or local CSV/pickle files
useCloudStorage = True

# run the runs
for( runInfo ) in DB.cursor():

    d_id = runInfo[ 'disease_id' ]
    i_id = runInfo[ 'iu_id' ]
    new_iu_combo = [ d_id, i_id ]

    # empty out db for each run
    if new_iu_combo != last_iu_combo:
        print( f"-> clearing results for {runInfo['short']}:{runInfo['iu_code']}" )
        DB.query( delete_runs_sql, ( d_id, i_id, ) )
        DB.commit()
        
    run( runInfo = runInfo, DB = DB, useCloudStorage = useCloudStorage )
    last_iu_combo = new_iu_combo
    sys.exit()
        

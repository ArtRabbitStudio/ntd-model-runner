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
LIMIT 1
'''

delete_runs_sql = '''
DELETE FROM results
WHERE disease_id = %s
AND iu_id = %s
'''

disease_iu_combos = DB.query( runs_sql, () )
print( f'-> found {DB.cursor().rowcount} disease/IU combos' )
last_iu_combo = None

for( runInfo ) in DB.cursor():
    d_id = runInfo[ 'disease_id' ]
    i_id = runInfo[ 'iu_id' ]
    new_iu_combo = [ d_id, i_id ]
    if new_iu_combo != last_iu_combo:
        print( f"-> clearing results for {runInfo['short']}:{runInfo['iu_code']}" )
        DB.query( delete_runs_sql, ( d_id, i_id, ) )
        DB.commit()
        
    useCloudStorage = False
    run( runInfo = runInfo, DB = DB, useCloudStorage = useCloudStorage )
    last_iu_combo = new_iu_combo
    sys.exit()
        

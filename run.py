import sys
import signal

from optparse import OptionParser

# local imports
from ntd_model_runner import run
from db import db

# get CLI options
parser = OptionParser()
parser.add_option( '-i', '--iu-limit', dest='iuLimit', default=1 )
parser.add_option( '-n', '--num-sims', dest='numSims', default=1 )
parser.add_option( '-c', '--cloud-storage', action='store_true', dest='useCloudStorage', default=False )

( options, args ) = parser.parse_args()

iuLimit = int( options.iuLimit )
numSims = int( options.numSims )
useCloudStorage = options.useCloudStorage

print( f"-> running {numSims} simulations for {iuLimit} IUs, {'' if useCloudStorage else 'not '}using cloud storage" )

# instantiate local db module
DB = db()

# handle exits gracefully
def handler( signum, frame ):
    DB = None
    sys.exit()

# register ctrl-c handler
signal.signal( signal.SIGINT, handler )

# sql statements
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

# get the runs
disease_iu_combos = DB.query( runs_sql, ( iuLimit, ) )
print( f'-> found {DB.cursor().rowcount} disease/IU combos' )

# track current run
last_iu_combo = None

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
        
    run( runInfo = runInfo, numSims = numSims, DB = DB, useCloudStorage = useCloudStorage )
    last_iu_combo = new_iu_combo
    sys.exit()
        

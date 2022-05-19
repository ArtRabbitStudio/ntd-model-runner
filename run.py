import sys
import signal

from optparse import OptionParser

# local imports
from ntd_model_runner import run, DirectoryNotFoundError, MissingArgumentError
from db import db

def iu_list_callback( option, opt, value, parser ):
    setattr( parser.values, option.dest, value.split( ',' ) )

# get CLI options
parser = OptionParser()

parser.add_option( '-d', '--disease', dest='disease', default='Man' )
parser.add_option( '-i', '--iu-list', dest='iuList', type='string', action='callback', callback=iu_list_callback, default='' )
parser.add_option( '-n', '--num-sims', dest='numSims', default=1 )
parser.add_option( '-l', '--local-storage', action='store_false', dest='useCloudStorage', default=True )
parser.add_option( '-s', '--scenario', dest='scenario', type='int', default=1 )
parser.add_option( '-g', '--group-id', dest='groupId', type='int', default=None )
parser.add_option( '-u', '--uncompressed-output', dest='compress', action='store_false', default=True )

( options, args ) = parser.parse_args()

iuList = options.iuList if isinstance( options.iuList, list ) == True else [ '' ]
numSims = int( options.numSims )
disease = options.disease
useCloudStorage = options.useCloudStorage
groupId = options.groupId
scenario = options.scenario
compress = options.compress

cli_iu_list_len = len( [ x for x in iuList if x != '' ] )
cloudStorageStr = '' if useCloudStorage else 'not '
compressStr = '' if compress else 'not '
print( f"-> {numSims} simulations for {cli_iu_list_len} IUs requested, {cloudStorageStr}using cloud storage, {compressStr}compressing output files" )

# instantiate local db module
DB = db()

# handle exits gracefully
def handler( signum, frame ):
    DB = None
    sys.exit()

# register ctrl-c handler
signal.signal( signal.SIGINT, handler )

# sql statements
format_strings = ', '.join( [ '%s' ] * len( iuList ) )
runs_sql = '''
SELECT d.type, d.species, d.short, i.code AS iu_code, d.id AS disease_id, i.id AS iu_id
FROM disease d, iu i, iu_disease i_d
WHERE i.id = i_d.iu_id and d.id = i_d.disease_id
AND i.code IN ( %s )''' % format_strings

runs_sql += '''
AND d.short = %s
ORDER BY d.type, d.species, i.code
'''

# get the runs
disease_iu_params = tuple( iuList ) + tuple( [ disease ] )
disease_iu_combos = DB.query( runs_sql, disease_iu_params, )
print( f'-> found {DB.cursor().rowcount} disease/IU combos' )

# track current run
last_iu_combo = None

# run the runs
runs = []
for( runInfo ) in DB.cursor():
    runs.append( runInfo )

for runInfo in runs:
    d_id = runInfo[ 'disease_id' ]
    i_id = runInfo[ 'iu_id' ]

    try:
        print( f"-> running {numSims} simulations for {runInfo['short']}:{runInfo['iu_code']}" )
        run(
            runInfo = runInfo,
            groupId = groupId,
            scenario = scenario,
            numSims = numSims,
            DB = DB,
            useCloudStorage = useCloudStorage,
            compress = compress,
            saveIntermediateResults=False
        )

    except DirectoryNotFoundError as d:
        print( f"xx> local data directory not found: {d}" )

    except FileNotFoundError as f:
        print( f"xx> file not found: {f}" )

    except MissingArgumentError as m:
        print( f"xx> argument not provided: {m}" )

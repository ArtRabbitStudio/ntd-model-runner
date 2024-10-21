import sys, os
import signal
import base64

from optparse import OptionParser
from types import SimpleNamespace
from slugify import slugify

# local imports
from ntd_model_runner import run, DirectoryNotFoundError, MissingArgumentError
from db import db

# get CLI options passed from script runner
def get_cli_options():

    def iu_list_callback( option, opt, value, parser ):
        setattr( parser.values, option.dest, value.split( ',' ) )

    parser = OptionParser()

    parser.add_option( '-o', '--output-folder', dest='outputFolder', default=None )
    parser.add_option( '-k', '--source-bucket', dest='sourceBucket', default='ntd-disease-simulator-data' )
    parser.add_option( '-K', '--destination-bucket', dest='destinationBucket', default='ntd-endgame-result-data' )
    parser.add_option( '-d', '--disease', dest='disease', default='Man' )
    parser.add_option( '-m', '--demography-name', dest='demogName', default="Default" )
    parser.add_option( '-i', '--iu-list', dest='iuList', type='string', action='callback', callback=iu_list_callback, default='' )
    parser.add_option( '-n', '--num-sims', dest='numSims', default=1 )
    parser.add_option( '-c', '--num-procs', dest='numProcs', default=0 )
    parser.add_option( '-N', '--run-name', dest='runName', default=None )
    parser.add_option( '-e', '--person-email', dest='personEmail', default=None )
    parser.add_option( '-l', '--local-storage', action='store_false', dest='useCloudStorage', default=True )
    parser.add_option( '-s', '--scenario', dest='scenario', type='string', default='1' )
    parser.add_option( '-P', '--param-subdir', dest='paramSubdir', type='string', default='' )
    parser.add_option( '-x', '--param-file-disease-suffix', dest='paramFileDiseaseSuffix', type='string', default='' )
    parser.add_option( '-X', '--short-disease-code-suffix', dest='shortDiseaseCodeSuffix', type='string', default='' )
    parser.add_option( '-g', '--group-id', dest='groupId', type='int', default=None )
    parser.add_option( '-u', '--uncompressed-output', dest='compress', action='store_false', default=True )
    parser.add_option( '-D', '--dont-split-sch-results', dest='splitSchResults', action='store_false', default=True )
    parser.add_option( '-p', '--source-data-path', dest='sourceDataPath', default='source-data' )
    parser.add_option( '-r', '--read-pickle-file-suffix', dest='readPickleFileSuffix', type='string', default=None )
    parser.add_option( '-f', '--save-pickle-file-suffix', dest='savePickleFileSuffix', type='string', default=None )
    parser.add_option( '-b', '--burn-in-time', dest='burnInTime', type='float', default=None )
    parser.add_option( '-y', '--survey-type', dest='surveyType', type='string', default='KK2' )
    parser.add_option( '-w', '--vaccine-waning-length', dest='vaccineWaningLength', type='int', default=None )
    parser.add_option( '-T', '--secular-trend', dest='secularTrend', action='store_true', default=False )
    parser.add_option( '--model-name', dest='modelName', type='string', default=None )
    parser.add_option( '--model-path', dest='modelPath', type='string', default=None )
    parser.add_option( '--model-branch', dest='modelBranch', type='string', default=None )
    parser.add_option( '--model-commit', dest='modelCommit', type='string', default=None )

    ( options, args ) = parser.parse_args()

    return options

# check options provided via CLI
def check_options( run_options ):

    # default to using the sanitized run name for the output folder
    if run_options.outputFolder == None:
        run_options.outputFolder = slugify( run_options.runName )

    # check whether force ignore db
    if os.getenv( "DONT_WRITE_DB_RECORD" ) == "y":
        print( "not checking for flow db options" )
        return

    # check pickle options are OK
    if run_options.readPickleFileSuffix == run_options.savePickleFileSuffix and run_options.readPickleFileSuffix != None:
        print( "xx> pickle output destination must be different from pickle input destination." )
        sys.exit( 1 )

    # ensure there's a descriptive name
    if run_options.runName == None:
        print( "xx> you must supply a descriptive name for this run." )
        sys.exit( 1 )

    # ensure there's an email address for the owner of the run
    if run_options.personEmail == None:
        print( "xx> you must supply an email address for the instigator of this run." )
        sys.exit( 1 )

    # make sure all necessary model info is provided
    if None in [ run_options.modelName, run_options.modelPath, run_options.modelBranch, run_options.modelCommit ]:
        print( "xx> model info not supplied." )
        sys.exit( 1 )

# check the run info in the DB for the requested list of IUs
def get_run_info( DB, run_options ):

    # make a comma-separated list of '%s' with the same length as IU list
    format_strings = ', '.join( [ '%s' ] * len( run_options.iuList ) )

    # create the parameterised query using the IU placeholders
    runs_sql = '''
    SELECT d.type, d.species, d.short, i.code AS iu_code, d.id AS disease_id, i.id AS iu_id
    FROM disease d, iu i, iu_disease i_d
    WHERE i.id = i_d.iu_id and d.id = i_d.disease_id
    AND i.code IN ( %s )''' % format_strings

    # add in the short-disease-name parameter
    runs_sql += '''
    AND d.short = %s
    ORDER BY d.type, d.species, i.code
    '''

    # substitute in the parameters & run the query
    disease_iu_params = tuple( run_options.iuList ) + tuple( [ run_options.disease ] )
    DB.query( runs_sql, disease_iu_params )

    print( f'-> found {DB.cursor().rowcount} disease/IU combos' )

# run the runs
def carry_out_runs( DB, run_options ):

    # get the data out so as to free the cursor for writing result records
    # TODO there must be a more elegant db-client/cursor way to do this
    runs = []
    for( runInfo ) in DB.cursor():
        runs.append( runInfo )

    for run_info in runs:

    #    print( f"run( {run_info}, {run_options}" );
    #    sys.exit(0)

        run_info[ 'demogName' ] = run_options.demogName
        run_options.saveIntermediateResults = False

        try:
            displayNumProcs = run_options.numProcs if run_options.numProcs > 0 else "the default number of"
            print( f"-> running {run_options.numSims} simulations on {displayNumProcs} processes for {run_info['short']}:{run_info['iu_code']}" )
            run( SimpleNamespace( **run_info ), run_options, DB )

        except DirectoryNotFoundError as d:
            print( f"xx> local data directory not found: {d}" )

        except IOError as i:
            print( f"xx> IO error: {i}" )

        except FileNotFoundError as f:
            print( f"xx> file not found: {f}" )

        except MissingArgumentError as m:
            print( f"xx> argument not provided: {m}" )

# main code block
def run_main():

    options = get_cli_options()

    # work out what to do with the options - and put into a SimpleNamespace so as to get dot.attribute access
    run_options = SimpleNamespace( **{
        'iuList': options.iuList if isinstance( options.iuList, list ) == True else [ '' ],
        'numSims': int( options.numSims ),
        'numProcs': int( options.numProcs ),
        'runName': base64.b64decode( options.runName ).decode( 'UTF-8' ),
        'personEmail': options.personEmail,
        'disease': options.disease,
        'demogName': options.demogName,
        'useCloudStorage': options.useCloudStorage,
        'outputFolder': options.outputFolder,
        'sourceBucket': options.sourceBucket,
        'destinationBucket': options.destinationBucket,
        'groupId': options.groupId if options.groupId != 0 else None,
        'scenario': options.scenario,
        'paramSubdir': options.paramSubdir,
        'paramFileDiseaseSuffix': options.paramFileDiseaseSuffix,
        'shortDiseaseCodeSuffix': options.shortDiseaseCodeSuffix,
        'compress': options.compress,
        'splitSchResults': options.splitSchResults,
        'sourceDataPath': options.sourceDataPath,
        'readPickleFileSuffix': options.readPickleFileSuffix,
        'savePickleFileSuffix': options.savePickleFileSuffix,
        'burnInTime': options.burnInTime,
        'surveyType': options.surveyType,
        'vaccineWaningLength': options.vaccineWaningLength,
        'secularTrend': options.secularTrend,
        'modelName': options.modelName,
        'modelPath': options.modelPath,
        'modelBranch': options.modelBranch,
        'modelCommit': options.modelCommit,
    } )

    check_options( run_options )

    # display run info
    cli_iu_list_len = len( [ x for x in run_options.iuList if x != '' ] )
    cloudStorageStr = '' if run_options.useCloudStorage else 'not '
    compressStr = '' if run_options.compress else 'not '
    splitStr = '' if run_options.splitSchResults else 'not '

    print( f"-> {run_options.numSims} simulations for {cli_iu_list_len} IUs requested, {cloudStorageStr}using cloud storage, {compressStr}compressing output files, {splitStr}splitting SCH results" )

    if run_options.outputFolder != '':
        print (f"-> saving result to output folder {run_options.outputFolder}" )

    # instantiate local db module
    DB = db()

    # handle exits gracefully
    def handler( signum, frame ):
        DB = None
        sys.exit()

    # register ctrl-c handler
    signal.signal( signal.SIGINT, handler )

    # check the work is valid
    get_run_info( DB, run_options )

    # do the work
    carry_out_runs( DB, run_options )

if __name__ == '__main__':
    run_main()

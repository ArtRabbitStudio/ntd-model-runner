'''
####################################################
# commands used to create import files
####################################################
gsutil -m ls -l gs://ntd-endgame-result-data/ntd/**/*.bz2 > ntd-endgame-result-data-listings-long.txt
sed -i '' -e 's/^ *//g' -e 's/  /\//g' ntd-endgame-result-data-listings-long.txt
for s in $( awk -F / '{print $7}' < ntd-endgame-result-data-listings-long.txt | sort | uniq ) ; do echo "$s " ; ag --no-numbers $s ntd-endgame-result-data-listings-long.txt > endgame-run-long-results-$s.txt ; done
for s in endgame-run-long-results-*.txt ; do for d in $(sort < $s | awk -F / '{print $8}' | sort | uniq) ; do newname=${s/\.txt/-$d.txt} ; echo $s $d $newname ; ag --no-numbers $d $s > $newname ; done ; done
GITHUB_API_TOKEN=<xxxxx> python import-runs-lf-sch-sth-tra.py egrlf/endgame-run-long-results-202209b-sch-mansoni.txt

####################################################
# examples of data in import files
####################################################
787170/2022-07-25T10:08:48Z/gs://ntd-endgame-result-data/ntd/202207a/sch-mansoni/scenario_1_1/group_001/NAM34260/ihme-NAM34260-mansoni-group_001-scenario_1_1-group_001-200_simulations.csv.bz2
355930/2022-09-06T10:49:09Z/gs://ntd-endgame-result-data/ntd/202209a/trachoma/scenario_1/BDI/BDI06385/ihme-BDI06385-trachoma-scenario_1-200_simulations.csv.bz2
459826/2022-08-23T21:12:23Z/gs://ntd-endgame-result-data/ntd/202208b/lf/scenario_1/AGO/AGO02049/ihme-AGO02049-lf-scenario_1-200.csv.bz2

####################################################
# examples of data parsed from import files
####################################################
SCH/STH result: {
    'destination_bucket': 'ntd-endgame-result-data',
    'description': '202209b',
    'disease': 'sch-mansoni',
    'scenario': '0_1',
    'group': '001',
    'iu': 'NAM34260',
    'file': 'gs://ntd-endgame-result-data/ntd/202209b/sch-mansoni/scenario_0_1/group_001/NAM34260/ihme-NAM34260-mansoni-group_001-scenario_0_1-group_001-200_simulations.csv.bz2',
    'institution': 'ihme',
    'simulations': '200'
}

TRA result: {
    'destination_bucket': 'ntd-endgame-result-data',
    'description': '202209a',
    'disease': 'trachoma',
    'scenario': '1',
    'country': 'BDI',
    'iu': 'BDI06385',
    'file': 'gs://ntd-endgame-result-data/ntd/202209a/trachoma/scenario_1/BDI/BDI06385/ihme-BDI06385-trachoma-scenario_1-200_simulations.csv.bz2',
    'institution': 'ihme',
    'simulations': '200'}
'''
import os, sys, json

from types import SimpleNamespace
from datetime import datetime
from dateutil.parser import isoparse

from db import db
from run import check_options, get_run_info
from model_info import get_model_info

model_names_by_disease = {
    'sth-hookworm': 'sch_simulation',
    'sth-whipworm': 'sch_simulation',
    'sth-roundworm': 'sch_simulation',
    'sch-mansoni': 'sch_simulation',
    'trachoma': 'trachoma',
    'lf': 'lf',
}

person_emails_by_disease = {
    'sth-hookworm': 'matthew.graham@ndm.ox.ac.uk',
    'sth-whipworm': 'matthew.graham@ndm.ox.ac.uk',
    'sth-roundworm': 'matthew.graham@ndm.ox.ac.uk',
    'sch-mansoni': 'matthew.graham@ndm.ox.ac.uk',
    'trachoma': 'S.E.F.Spencer@warwick.ac.uk',
    'lf': 'matthew.graham@ndm.ox.ac.uk',
}

disease_short_codes_by_disease = {
    'sth-hookworm': 'Hook',
    'sth-whipworm': 'Tri',
    'sth-roundworm': 'Asc',
    'sch-mansoni': 'Man',
    'trachoma': 'Tra',
    'lf': 'LF',
}

disease_species_by_disease = {
    'sth-hookworm': 'Hookworm',
    'sth-whipworm': 'Trichuris',
    'sth-roundworm': 'Ascaris',
    'sch-mansoni': 'Mansoni',
    'trachoma': 'Trachoma',
    'lf': 'LF',
}

disease_types_by_disease = {
    'sth-hookworm': 'sth',
    'sth-whipworm': 'sth',
    'sth-roundworm': 'sth',
    'sch-mansoni': 'sch',
    'trachoma': 'tra',
    'lf': 'lf',
}

diseases = [ 'sch-mansoni', 'sth-hookworm', 'sth-roundworm', 'sth-whipworm', 'trachoma', 'lf' ]

# ['355930', '2022-09-06T10:49:09Z', 'gs:', '', 'ntd-endgame-result-data', 'ntd', '202206', 'sth-hookworm', 'scenario_3', 'group_170', 'BDI06375', 'ipm-BDI06375-hookworm-group_170-scenario_3-group_170-200_simulations.csv.bz2\n']

file_disease_index = 7

file_mapping = {
    'lf': {
        'size': 0,
        'ended': 1,
        'destination_bucket': 4,
        'description': 6,
        'disease': 7,
        'scenario': 8,
        'iu': 9,
        'file': 11
    },
    'trachoma': {
        'size': 0,
        'ended': 1,
        'destination_bucket': 4,
        'description': 6,
        'disease': 7,
        'scenario': 8,
        'country': 9,
        'iu': 10,
        'file': 11
    },
    'default': {
        'size': 0,
        'ended': 1,
        'destination_bucket': 4,
        'description': 6,
        'disease': 7,
        'scenario': 8,
        'group': 9,
        'iu': 10,
        'file': 11
    }
}

# ['ipm', 'RWA38118', 'hookworm', 'group_165', 'scenario_3', 'group_165', '200_simulations']
line_mapping = {
    'default':  { 'institution': 0, 'simulations': 6 },
    'lf':       { 'institution': 0, 'simulations': 4 },
    'trachoma': { 'institution': 0, 'simulations': 4 }
}

# fill out e.g. sth-mansoni, sth-roundworm with 'default' settings
for d in diseases:
    for m in [ file_mapping, line_mapping ]:
        if not d in m:
            m[ d ] = m[ 'default' ]


# main function
def import_file( import_file_path ):

    disease = '-'.join( import_file_path.split( '/' ).pop().split('.')[ 0 ].split( '-' )[5:] )
    
    if not disease in diseases:
        sys.stderr.write( f"xx> unknown disease {disease}\n" )
        sys.exit( 1 )

    # only fetch the model info from github once
    model_name = model_names_by_disease[ disease ]

    try:
        model_info = SimpleNamespace( **get_model_info( model_name, False ) ) # don't return json/b64-encoded version
    except KeyError as k:
        sys.stderr.write( f"xx> unknown model name {model_name}\n" )
        sys.exit( 1 )

    # only create one local db connection
    DB = db()

    file = open( import_file_path, 'r' )

    while True:

        line = file.readline()

        if not line:
            break

        result = parse_line( line )

        save_result_to_db( SimpleNamespace( **result ), model_name, model_info, DB )

# get the data
def parse_line( line ):

    result = {}

    # read in the line and chop off the newline
    entries = line.split('\n')[0].split( '/' )

    # work out the disease type from the 'file_disease_index' field
    disease_name = entries[ file_disease_index ] 

    # get the data mappings for this disease type
    disease_entry_mapping = file_mapping[ disease_name ]
    disease_filename_mapping = line_mapping[ entries[ file_disease_index ] ]

    # read in all the data from the whole line for the disease type
    for key in disease_entry_mapping:

        # reconstruct the filename
        if key == 'file':
            result[ key ] = '/'.join(entries[2:])

        # parse out the value from scenario_X or group_Y
        elif ( key == 'scenario' or key == 'group' ):
            value = entries[ disease_entry_mapping[ key ] ]
            result[ key ] = '_'.join( value.split( '_' )[1:] )

        # parse in the iso 8601 date
        elif ( key == 'ended' ):
            result[ key ] = isoparse( entries[ disease_entry_mapping[ key ] ] )

        # just read in any other value
        else:
            result[ key ] = entries[ disease_entry_mapping[ key ] ].split('_')[0]

    # read in the base filename from the gs URI & split into components on dashes
    file_info = entries[ disease_entry_mapping['file'] ].split('.')[0].split('-')
    
    # read in the data from the base filename for this disease type
    for key in disease_filename_mapping:
        result[ key ] = file_info[ disease_filename_mapping[ key ] ].split('_')[0]

    return result

'''
####################################################
# params passed to db.write_db_result_record() by
# ntd_model_runner when writing new results
####################################################
run_info:
  type='sch'
  species='Mansoni'
  short='Man'
  iu_code='ETH18692'
  disease_id=1
  iu_id=1426
  demogName='KenyaKDHS'

run_options:
  iuList=['ETH18692']
  numSims=1
  runName='descriptive name for "this run"'
  personEmail='igor@artrabbit.com'
  disease='Man'
  demogName='KenyaKDHS'
  useCloudStorage=False
  outputFolder='whatever2' // result files go in gs://<destinationBucket>/ntd/<outputFolder>
  sourceBucket='input_bucket'
  destinationBucket='output_bucket'
  groupId=59
  scenario='1_2'
  compress=True
  sourceDataPath='source-data'
  readPickleFileSuffix='202209b_burn_in'
  savePickleFileSuffix=None
  burnInTime=43
  saveIntermediateResults=False
  coverageFileName='SCH_params/mansoni_coverage_scenario_1_2.xlsx'
  paramFileName='SCH_params/mansoni_scenario_1_2.txt'
  modelName='sch_simulation'
  modelPath='/ntd-modelling-consortium/ntd-model-sch'
  modelBranch='Endgame_v2'
  modelCommit='5610d55814dac3fea76bc01436e9ca6041cb669d'

file_name:
  ./data/output/ntd/whatever2/sch-mansoni/scenario_1_2/group_059/ETH18692/
      ipm-ETH18692-mansoni-group_059-scenario_1_2-group_059-1_simulations.csv.bz2

compression: bz2

####################################################
# values required by db.write_db_result_record() 
####################################################
run_options
    scenario, coverageFileName, paramFileName, runName, disease, numSims, personEmail, sourceBucket
    sourceDataPath, destinationBucket, outputFolder,, readPickleFileSuffix, savePickleFileSuffix, burnInTime,
    modelName, modelPath, modelBranch, modelCommit, groupId

run_info
    started, ended,, iu_code, disease_id, demogName

scenario_id, institution, file_name, run_id
'''

def save_result_to_db( result, model_name, model_info, DB ):

    person_email = person_emails_by_disease[ result.disease ]

    disease_short_code = disease_short_codes_by_disease[ result.disease ]
    disease_species = disease_species_by_disease[ result.disease ]
    disease_type = disease_types_by_disease[ result.disease ]

    compression = True

    run_options = SimpleNamespace( **{

        'iuList': [ result.iu ],
        'numSims': int( result.simulations ),
        'runName': result.description,
        'personEmail': person_email,
        'disease': disease_short_code,
        'demogName': 'KenyaKDHS',
        'compress': compression,
        'groupId': int( result.group ) if hasattr( result, 'group' ) else None,
        
        'scenario': result.scenario,
        'coverageFileName': get_coverage_filename( disease_type, disease_species, result.scenario ),
        'paramFileName': get_param_filename( disease_type, disease_species, result.scenario ),

        'sourceBucket': 'ntd-disease-simulator-data',
        'sourceDataPath': 'source-data',
        'destinationBucket': result.destination_bucket,
        'outputFolder': result.description,

        'readPickleFileSuffix': None,
        'savePickleFileSuffix': None,
        'burnInTime': None,
        'saveIntermediateResults': False,

        'modelName': model_name,
        'modelPath': model_info.path,
        'modelBranch': model_info.branch,
        'modelCommit': model_info.commit,

    } )

    check_options( run_options )

    get_run_info( DB, run_options )

    runs = []
    for( runInfo ) in DB.cursor():
        runs.append( SimpleNamespace( **runInfo ) )

    for run_info in runs:
        run_info.demogName = run_options.demogName
        run_info.started = run_info.ended = result.ended
        DB.write_db_result_record( run_info, run_options, result.institution, result.file, compression )

def get_coverage_filename( disease_type, disease_species, scenario ):
    if disease_type == 'tra':
        return f'coverage/scen{scenario}.xlsx'

    return f'{disease_type.upper()}_params/{disease_species.lower()}_coverage_scenario_{scenario}.xlsx'

def get_param_filename( disease_type, disease_species, scenario ):
    if disease_type == 'tra':
        return f'coverage/scen{scenario}.csv'

    elif disease_type == 'lf':
        sys.stderr.write( f"xx> unimplemented disease type {disease_type}\n" )
        sys.exit( 1 )

    else:
        return f'{disease_type.upper()}_params/{disease_species.lower()}_scenario_{scenario}.txt'

if __name__ == "__main__":
    import_file( sys.argv[ 1 ] )

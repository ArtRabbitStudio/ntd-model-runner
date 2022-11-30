import os, sys, json

from types import SimpleNamespace
from datetime import datetime

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

# ['gs:', '', 'ntd-endgame-result-data', 'ntd', '202206', 'sth-hookworm', 'scenario_3', 'group_170', 'BDI06375', 'ipm-BDI06375-hookworm-group_170-scenario_3-group_170-200_simulations.csv.bz2\n']

file_disease_index = 5

file_mapping = {
    'lf': {
        'destination_bucket': 2,
        'description': 4,
        'disease': 5,
        'scenario': 6,
        'iu': 7,
        'file': 9
    },
    'trachoma': {
        'destination_bucket': 2,
        'description': 4,
        'disease': 5,
        'scenario': 6,
        'country': 7,
        'iu': 8,
        'file': 9
    },
    'default': {
        'destination_bucket': 2,
        'description': 4,
        'disease': 5,
        'scenario': 6,
        'group': 7,
        'iu': 8,
        'file': 9
    }
}

# ['ipm', 'RWA38118', 'hookworm', 'group_165', 'scenario_3', 'group_165', '200_simulations']
line_mapping = {
    'default': {
        'institution': 0,
        'simulations': 6
    },
    'lf': {
        'institution': 0,
        'simulations': 4
    },
    'trachoma': {
        'institution': 0,
        'simulations': 4
    }
}


##########################
# run_info:
#   type='sch'
#   species='Mansoni'
#   short='Man'
#   iu_code='ETH18692'
#   disease_id=1
#   iu_id=1426
#   demogName='KenyaKDHS'
#
# run_options:
#   iuList=['ETH18692']
#   numSims=1
#   runName='descriptive name for "this run"'
#   personEmail='igor@artrabbit.com'
#   disease='Man'
#   demogName='KenyaKDHS'
#   useCloudStorage=False
#   outputFolder='whatever2' // result files go in gs://<destinationBucket>/ntd/<outputFolder>
#   sourceBucket='input_bucket'
#   destinationBucket='output_bucket'
#   groupId=59
#   scenario='1_2'
#   compress=True
#   sourceDataPath='source-data'
#   readPickleFileSuffix='202209b_burn_in'
#   savePickleFileSuffix=None
#   burnInTime=43
#   saveIntermediateResults=False
#   coverageFileName='SCH_params/mansoni_coverage_scenario_1_2.xlsx'
#   paramFileName='SCH_params/mansoni_scenario_1_2.txt'
#   modelName='sch_simulation'
#   modelPath='/ntd-modelling-consortium/ntd-model-sch'
#   modelBranch='Endgame_v2'
#   modelCommit='5610d55814dac3fea76bc01436e9ca6041cb669d'
#
# file_name:
#   ./data/output/ntd/whatever2/sch-mansoni/scenario_1_2/group_059/ETH18692/
#       ipm-ETH18692-mansoni-group_059-scenario_1_2-group_059-1_simulations.csv.bz2
#
# compression: bz2

'''
run_options.scenario
run_options.coverageFileName
run_options.paramFileName
run_options.runName
run_options.disease
run_options.numSims
run_options.personEmail,
run_options.sourceBucket
run_options.sourceDataPath
run_options.destinationBucket
run_options.outputFolder,
run_options.readPickleFileSuffix
run_options.savePickleFileSuffix
run_options.burnInTime,
run_options.modelName
run_options.modelPath
run_options.modelBranch
run_options.modelCommit
run_options.groupId

run_info.started
run_info.ended,
run_info.iu_code
run_info.disease_id
run_info.demogName

scenario_id
institution
file_name
run_id
'''

'''
SCH result: {
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
def save_result_to_db( result, DB ):

    model_name = model_names_by_disease[ result.disease ]

    try:
        model_info = SimpleNamespace( **get_model_info( model_name, False ) ) # don't return json/b64-encoded version
    except KeyError as k:
        sys.stderr.write( f"xx> unknown model name {model_name}\n" )
        sys.exit( 1 )

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
        run_info.started = datetime.now()
        run_info.ended = datetime.now()
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

def parse_ntd_gs_uri( line ):
    result = {}

    entries = line.split('\n')[0].split( '/' )
    disease_name = entries[ file_disease_index ] 

    disease_entry_mapping = file_mapping[ disease_name ]
    disease_filename_mapping = line_mapping[ entries[ file_disease_index ] ]

    for key in disease_entry_mapping:
        if key == 'file':
            result[ key ] = '/'.join(entries)
        elif ( key == 'scenario' or key == 'group' ):
            value = entries[ disease_entry_mapping[ key ] ]
            result[ key ] = '_'.join( value.split( '_' )[1:] )
        else:
            result[ key ] = entries[ disease_entry_mapping[ key ] ].split('_')[0]

    file_info = entries[ disease_entry_mapping['file'] ].split('.')[0].split('-')
    
    for key in disease_filename_mapping:
        result[ key ] = file_info[ disease_filename_mapping[ key ] ].split('_')[0]

    return result

if __name__ == "__main__":

    for d in diseases:
        for m in [ file_mapping, line_mapping ]:
            if not d in m:
                m[ d ] = m[ 'default' ]

    file = open( sys.argv[1], 'r' )
    while True:

        line = file.readline()

        if not line:
            break

        result = parse_ntd_gs_uri( line )

        DB = db()
        save_result_to_db( SimpleNamespace( **result ), DB )

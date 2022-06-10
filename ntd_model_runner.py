import pickle
import time
import multiprocessing
import math
import os, sys
import pkg_resources

import pandas as pd
import numpy as np

import gcs
import db

from pathlib import Path
from joblib import Parallel, delayed

from sch_simulation.helsim_RUN_KK import *
from sch_simulation.helsim_FUNC_KK import *

class MissingArgumentError( ValueError ):
    pass

class DirectoryNotFoundError( ValueError ):
    pass

# run the model with the right params and then transform results for IHME/IPM
def run( runInfo, groupId, scenario, numSims, DB, useCloudStorage, compress=False, saveIntermediateResults=False ):

    if groupId is None:
        raise MissingArgumentError( 'groupId' )

    # get run info
    iu = runInfo[ "iu_code" ]
    region = iu[:3]
    disease = runInfo[ 'type' ]
    species = runInfo[ "species" ]
    short = runInfo[ "short" ]
    demogName = runInfo[ 'demogName' ]

    # construct cloud path for this disease/species
    GcsSpecies = {
        'Ascaris': 'roundworm',
        'Trichuris': 'whipworm',
        'Hookworm': 'hookworm',
        'Mansoni': 'mansoni'
    }[ species ]

    DISEASE_CLOUD_ROOT = f'diseases/{runInfo[ "type" ]}-{GcsSpecies.lower()}'
    DISEASE_CLOUD_SRC_PATH = f'{DISEASE_CLOUD_ROOT}/source-data'
    DISEASE_CLOUD_DST_PATH = f'ntd/202206/{runInfo[ "type" ]}-{GcsSpecies.lower()}/scenario_{scenario}/group_{groupId:03}'

    # get model package's data dir for finding scenario files
    MODEL_DATA_DIR = pkg_resources.resource_filename( "sch_simulation", "data" )

    # make sure local data directory is present
    LOCAL_INPUT_DATA_DIR = './data/input'

    if useCloudStorage == False:
        if not os.path.isdir( LOCAL_INPUT_DATA_DIR ):
            raise DirectoryNotFoundError( LOCAL_INPUT_DATA_DIR )

    # construct GCS/local output directory paths
    LOCAL_OUTPUT_DATA_DIR = './data/output'
    output_data_root = f"{DISEASE_CLOUD_DST_PATH}/{iu}"
    GcsOutputDataPath = f'gs://ntd-endgame-result-data/{output_data_root}'
    LocalOutputDataPath = f'{LOCAL_OUTPUT_DATA_DIR}/{output_data_root}'
    output_data_path = GcsOutputDataPath if useCloudStorage else LocalOutputDataPath

    # make sure local output directory is present
    if useCloudStorage == False:
        if not os.path.isdir( output_data_path ):
            Path( output_data_path ).mkdir( parents = True, exist_ok = True )

    # locate pickle file for IU
    InSimFilePath = f'{LOCAL_INPUT_DATA_DIR}/{short}_{iu}.p'
    GcsInSimFilePath = f'{DISEASE_CLOUD_SRC_PATH}/{region}/{iu}/{short}_{iu}.p'

    # locate RK input file for IU
    RkFilePath = f'{LOCAL_INPUT_DATA_DIR}/Input_Rk_{short}_{iu}.csv'
    GcsRkFilePath = f'gs://ntd-disease-simulator-data/{DISEASE_CLOUD_SRC_PATH}/{region}/{iu}/Input_Rk_{short}_{iu}.csv'

    # locate & check coverage file for selected disease/scenario
    coverageFileName = f'{disease.upper()}_params/{species.lower()}_coverage_scenario_{scenario}.xlsx'
    coverageFilePath = f'{MODEL_DATA_DIR}/{coverageFileName}'

    if not os.path.exists( coverageFilePath ):
        print( f"xx> couldn't find {disease.lower()}-{species.lower()} coverage file for scenario {scenario}" )
        sys.exit( 1 )

    print( f"-> using coverage file {coverageFileName}" )

    coverageTextFileStorageName = f'/tmp/{short}_{iu}_MDA_vacc.txt'

    # locate & check parameter file for selected disease/scenario
    paramFileName = f'{disease.upper()}_params/{species.lower()}_scenario_{scenario}.txt'
    paramFilePath = f'{MODEL_DATA_DIR}/{paramFileName}'

    if not os.path.exists( paramFilePath ):
        print( f"xx> couldn't find {disease.lower()}-{species.lower()} parameter file for scenario {scenario}" )
        sys.exit( 1 )

    print( f"-> using parameter file {coverageFileName}" )

    # run the model
    results = run_model(
        InSimFilePath = GcsInSimFilePath if useCloudStorage else InSimFilePath,
        RkFilePath = GcsRkFilePath if useCloudStorage else RkFilePath,
        coverageFileName = coverageFileName,
        coverageTextFileStorageName = coverageTextFileStorageName,
        demogName = demogName,
        paramFileName = paramFileName,
        numSims = numSims,
        cloudModule = gcs if useCloudStorage else None
    )

    # output the raw model result data for comparison?
    if saveIntermediateResults == True:
        for i in range(len(results)):
            df = results[i]

            intermediate_results_dir = f"{output_data_path}/intermediate-results"
            if ( useCloudStorage == False ) and ( not os.path.isdir( intermediate_results_dir ) ):
                print( f"-> making local intermediate results directory {intermediate_results_dir}" )
                Path( intermediate_results_dir ).mkdir( parents = True, exist_ok = True )

            df.to_csv( f'{intermediate_results_dir}/{iu}_results_{i:03}.csv', index=False )

    # get a transformer generator function for the IHME/IPM transforms
    transformer = sim_result_transform_generator( results, iu, runInfo['species'], scenario, numSims )

    # decide whether to put the group ID in the filename
    groupId_string = f'-group_{groupId:03}' if groupId is not None else ''

    # add a '.bz2' suffix if compressing the IHME/IPM files
    compressSuffix = ".bz2" if compress == True else ""
    compression = None if compress == False else "bz2"

    # run IHME transforms
    ihme_df = next( transformer )
    ihme_file_name = f"{output_data_path}/ihme-{iu}-{runInfo['species'].lower()}{groupId_string}-scenario_{scenario}-group_{groupId:03}-{numSims}_simulations.csv{compressSuffix}"
    ihme_df.to_csv( ihme_file_name, index=False, compression=compression )

    # run IPM transforms
    ipm_df = next( transformer )
    ipm_file_name = f"{output_data_path}/ipm-{iu}-{runInfo['species'].lower()}{groupId_string}-scenario_{scenario}-group_{groupId:03}-{numSims}_simulations.csv{compressSuffix}"
    ipm_df.to_csv( ipm_file_name, index=False, compression=compression )

    os.remove( coverageTextFileStorageName )

    return

'''
function to load in a pickle file and associated parameters file and then
run forward in time 23 years and give back results
'''
def run_model( InSimFilePath=None, RkFilePath=None, coverageFileName='Coverage_template.xlsx', coverageTextFileStorageName=None,
                demogName='Default', paramFileName='sch_example.txt', numSims=None, cloudModule=None ):

    # number of simulations to run
    if numSims is None:
        raise MissingArgumentError( 'numSims' )

    # path to pickle file
    if InSimFilePath is None:
        raise MissingArgumentError( "InSimFilePath" )

    # load pickle file
    print( f'-> reading in pickle data from {InSimFilePath}' )
    pickleData = pickle.loads( cloudModule.get_blob( InSimFilePath ) ) if cloudModule != None else pickle.load( open( InSimFilePath, 'rb' ) )

    # path to 200 parameters file
    if RkFilePath is None:
        raise MissingArgumentError( 'RkFilePath' )

    # read in parameters
    print( f'-> reading in parameters from {RkFilePath}' )
    simparams = pd.read_csv(RkFilePath)
    simparams.columns = [s.replace(' ', '') for s in simparams.columns]

    # define the lists of random seeds, R0 and k
    seed = simparams.iloc[:, 0].tolist()
    R0 = simparams.iloc[:, 1].tolist()
    k = simparams.iloc[:, 2].tolist()

    # coverageFileName: path to coverage file supplied or defaulted in argument
    # paramFileName: standard parameter file path (in sch_simulation/data folder) supplied or default arg

    # file name to store munged coverage information in
    if coverageTextFileStorageName is None:
        raise MissingArgumentError( 'coverageTextFileStorageName' )

    # parse coverage file
    cov = parse_coverage_input(coverageFileName, coverageTextFileStorageName)

    # initialize the parameters
    print( f'-> loading parameters for demography {demogName}' )
    params = loadParameters(paramFileName, demogName)

    # add coverage data to parameters file
    params = readCoverageFile(coverageTextFileStorageName, params)

    # count number of processors
    num_cores = multiprocessing.cpu_count()
    print( f'-> running {numSims} simulations on {num_cores} cores' )

    # randomly pick indices for number of simulations
    indices = np.random.choice(a=range(200), size = numSims, replace=False)

    start_time = time.time()

    # run simulations in parallel
    results = Parallel(n_jobs=num_cores)(
            delayed(multiple_simulations)(params, pickleData, simparams, indices, i) for i in range(numSims))
        
    end_time = time.time()

    print( f'-> finished {numSims} simulations on {num_cores} cores in {(end_time - start_time):.3f}s' )

    return results

def sim_result_transform_generator( results, iu, species, scenario, numSims ):

    # espen_loc,year_id,age_start,age_end,sex_id,intensity,scenario,species,sequelae,measure
    keys = {
        "espen_loc":[],
        "year_id":[],
        "age_start":[],
        "age_end":[],
        "intensity":[],
        "scenario":[],
        "species":[],
        "measure":[]
    }

    for i in range( 0, numSims ):
        keys[ f'draw_{i}' ] = []

    # we're going to take the draw_1 from each run result for an IU:

    #   Time,age_start,age_end,intensity,species,measure,draw_1
    #   0.0,0,1,light,Mansoni,prevalence,0.0

    # and add it as draw_x at the end of a row in this output file:

    #   espen_loc,year_id,age_start,age_end,intensity,scenario,species,measure,draw_0,draw_1,draw_3,draw_4,draw_x,draw_x,,
    #   BFA05335,2030,4,4,light,1,mansoni,prevalence,0.1,0.08,0.09,0.11,,

    ################################################################################
    # IHME data file
    ################################################################################

    print( f'-> starting IHME transform for {numSims} simulations' )
    a = time.time()
    values = transform_results( results, iu, 'ihme', species, scenario, numSims, keys )
    b = time.time()
    print( f'-> finished IHME transform for {numSims} simulations in {(b-a):.3f}s' )
    yield pd.DataFrame( values, columns = keys )

    ################################################################################
    # IPM costs file
    ################################################################################

    print( f'-> starting IPM transform for {numSims} simulations' )
    a = time.time()
    values = transform_results( results, iu, 'ipm', species, scenario, numSims, keys )
    b = time.time()
    print( f'-> finished IPM transform for {numSims} simulations in {(b-a):.3f}s' )
    yield pd.DataFrame( values, columns = keys )

    return

def transform_results( results, iu, type, species, scenario, numSims, keys ):

    # previously used row 7584 for end of IPM data, now just run to the end of the results
    num_result_rows = results[ 0 ].shape[ 0 ]

    # first 7440 rows = standard ESPEN results + population data
    # rows 7441-end = IPM cost data
    startrow = { 'ihme': 0, 'ipm': 7440 }[ type ]
    endrow = { 'ihme': 7440, 'ipm': num_result_rows }[ type ]

    # array to put the transformed data into
    values = []

    # spin through the relevant rows
    for rowIndex, row in results[ 0 ][ startrow:endrow ].iterrows():

        # get an index into target array, i.e. where we're putting the new row
        # - jump ahead by 7440 for the ipm data
        arrIndex = ( rowIndex - 7440 ) if type == 'ipm' else rowIndex

        if arrIndex > 0 and arrIndex % 1000 == 0:
            print( f"-> transformed {arrIndex} rows" )

        # add in the first fields, up to the first draw
        scenario_fields = [ iu, math.trunc( row.Time + 2018 ), row.age_start, row.age_end, row.intensity, scenario, species, row.measure, row.draw_1 ]
        values.insert( arrIndex, scenario_fields )

        # add draw_1 from the current line in each file as draw_X in the ihme file
        for simNo in range( 1, numSims ):
            # put the 'draw_1' value from row 'rowIndex' of the source data into row 'arrIndex' of the target
            values[ arrIndex ].append( results[ simNo ].loc[ rowIndex ].draw_1 )

    return values



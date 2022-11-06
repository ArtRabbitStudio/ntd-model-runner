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

from run_trachoma import run_trachoma_model

class MissingArgumentError( ValueError ):
    pass

class DirectoryNotFoundError( ValueError ):
    pass


# run the model with the right params and then transform results for IHME/IPM
def run( runInfo, run_options, DB ):

    # get run info
    iu = runInfo[ "iu_code" ]
    region = iu[:3]
    disease = runInfo[ 'type' ]
    species = runInfo[ "species" ]
    short = runInfo[ "short" ]
    demogName = runInfo[ 'demogName' ]

    # get/check run options
    if not hasattr( run_options, 'scenario' ) or run_options.scenario is None:
        raise MissingArgumentError( 'run_options.scenario' )

    if not hasattr( run_options, 'groupId' ) or run_options.groupId is None:
        # trachoma doesn't have groups
        if species != "Trachoma":
            raise MissingArgumentError( 'run_options.groupId' )

    if not hasattr( run_options, 'numSims' ) or run_options.numSims is None:
        raise MissingArgumentError( 'run_options.numSims' )

    # get local vars out of dictionary
    compress = run_options.compress if hasattr( run_options, 'compress' ) else False
    readPickleFileSuffix = run_options.readPickleFileSuffix if hasattr( run_options, 'readPickleFileSuffix' ) else None
    savePickleFileSuffix = run_options.savePickleFileSuffix if hasattr( run_options, 'savePickleFileSuffix' ) else None
    saveIntermediateResults = run_options.saveIntermediateResults if hasattr( run_options, 'saveIntermediateResults' ) else False
    burnInTime = run_options.burnInTime if hasattr( run_options, 'burnInTime' ) else None
    outputFolder = run_options.outputFolder if hasattr( run_options, 'outputFolder' ) else '202206'
    sourceDataPath = run_options.sourceDataPath if hasattr( run_options, 'sourceDataPath' ) else 'source-data'

    # construct cloud path for this disease/species
    GcsSpecies = {
        'Ascaris': 'roundworm',
        'Trichuris': 'whipworm',
        'Hookworm': 'hookworm',
        'Mansoni': 'mansoni',
        'Trachoma': 'trachoma',
    }[ species ]

    GcsPrefix = {
        'Ascaris': f"{runInfo['type']}-",
        'Trichuris': f"{runInfo['type']}-",
        'Hookworm': f"{runInfo['type']}-",
        'Mansoni': f"{runInfo['type']}-",
        'Trachoma': '',
    }[ species ]

    DISEASE_CLOUD_ROOT = f'diseases/{GcsPrefix}{GcsSpecies.lower()}'
    DISEASE_CLOUD_SRC_PATH = f'{DISEASE_CLOUD_ROOT}/{sourceDataPath}'

    # only include the group if it's been specified
    if run_options.groupId is None:
        DISEASE_CLOUD_DST_PATH = f'ntd/{outputFolder}/{GcsPrefix}{GcsSpecies.lower()}/scenario_{run_options.scenario}/{iu[0:3]}'
    else:
        DISEASE_CLOUD_DST_PATH = f'ntd/{outputFolder}/{GcsPrefix}{GcsSpecies.lower()}/scenario_{run_options.scenario}/group_{run_options.groupId:03}'

    # get model package's data dir for finding scenario files
    MODEL_DATA_DIR = pkg_resources.resource_filename( "sch_simulation", "data" )

    # make sure local data directory is present
    LOCAL_INPUT_DATA_DIR = './data/input'

    if run_options.useCloudStorage == False:
        if not os.path.isdir( LOCAL_INPUT_DATA_DIR ):
            raise DirectoryNotFoundError( LOCAL_INPUT_DATA_DIR )

    # construct GCS/local output directory paths
    LOCAL_OUTPUT_DATA_DIR = './data/output'
    output_data_root = f"{DISEASE_CLOUD_DST_PATH}/{iu}"
    GcsOutputDataPath = f'gs://ntd-endgame-result-data/{output_data_root}'
    LocalOutputDataPath = f'{LOCAL_OUTPUT_DATA_DIR}/{output_data_root}'
    output_data_path = GcsOutputDataPath if run_options.useCloudStorage else LocalOutputDataPath

    # make sure local output directory is present
    if run_options.useCloudStorage == False:
        if not os.path.isdir( output_data_path ):
            Path( output_data_path ).mkdir( parents = True, exist_ok = True )

    # short-circuit out to trachoma?
    if species == 'Trachoma':

        # locate pickle file for IU
        GcsInSimFilePath = f'{DISEASE_CLOUD_SRC_PATH}/{region}/{iu}/OutputVals_{iu}.p'
        InSimFilePath = GcsInSimFilePath if run_options.useCloudStorage else f'{LOCAL_INPUT_DATA_DIR}/OutputVals_{iu}.p'

        # locate Beta input file for IU
        GcsBetaFilePath = f'gs://ntd-disease-simulator-data/{DISEASE_CLOUD_SRC_PATH}/{region}/{iu}/InputBet_{iu}.csv'
        BetaFilePath = GcsBetaFilePath if run_options.useCloudStorage else f'{LOCAL_INPUT_DATA_DIR}/InputBet_{iu}.csv'

        # specify compression settings
        compressSuffix = ".bz2" if compress == True else ""
        compression = None if compress == False else "bz2"

        # specify file output locations
        ihme_file_name = f"{output_data_path}/ihme-{iu}-{runInfo['species'].lower()}-scenario_{run_options.scenario}-{run_options.numSims}_simulations.csv{compressSuffix}"
        ipm_file_name = f"{output_data_path}/ipm-{iu}-{runInfo['species'].lower()}-scenario_{run_options.scenario}-{run_options.numSims}_simulations.csv{compressSuffix}"

        cloudModule = gcs if run_options.useCloudStorage else None
        return run_trachoma_model( iu, run_options.scenario, run_options.numSims, BetaFilePath, InSimFilePath, cloudModule, ihme_file_name, ipm_file_name, compressSuffix, compression )

    # locate pickle file for IU
    pickleReadSuffix = f"_{readPickleFileSuffix}" if readPickleFileSuffix != None else ""
    InSimFilePath = f'{LOCAL_INPUT_DATA_DIR}/{short}_{iu}{pickleReadSuffix}.p'
    GcsInSimFilePath = f'{DISEASE_CLOUD_SRC_PATH}/{region}/{iu}/{short}_{iu}{pickleReadSuffix}.p'

    # specify output pickle file location for IU?
    if savePickleFileSuffix != None:
        OutSimFilePath = f'{LOCAL_INPUT_DATA_DIR}/{short}_{iu}_{savePickleFileSuffix}.p'
        GcsOutSimFilePath = f'{DISEASE_CLOUD_SRC_PATH}/{region}/{iu}/{short}_{iu}_{savePickleFileSuffix}.p'
        print( f"-> will write output pickle file to {GcsOutSimFilePath if run_options.useCloudStorage else OutSimFilePath}" )
    else:
        print( "-> not writing output pickle file" )

    # locate RK input file for IU
    RkFilePath = f'{LOCAL_INPUT_DATA_DIR}/Input_Rk_{short}_{iu}.csv'
    GcsRkFilePath = f'gs://ntd-disease-simulator-data/{DISEASE_CLOUD_SRC_PATH}/{region}/{iu}/Input_Rk_{short}_{iu}.csv'

    # locate & check coverage file for selected disease/scenario
    coverageFileName = f'{disease.upper()}_params/{species.lower()}_coverage_scenario_{run_options.scenario}.xlsx'
    coverageFilePath = f'{MODEL_DATA_DIR}/{coverageFileName}'

    if not os.path.exists( coverageFilePath ):
        print( f"xx> couldn't find {disease.lower()}-{species.lower()} coverage file for scenario {run_options.scenario} : {coverageFilePath}" )
        sys.exit( 1 )

    print( f"-> using coverage file {coverageFileName}" )

    coverageTextFileStorageName = f'/tmp/{short}_{iu}_MDA_vacc.txt'

    # locate & check parameter file for selected disease/scenario
    paramFileName = f'{disease.upper()}_params/{species.lower()}_scenario_{run_options.scenario}.txt'
    paramFilePath = f'{MODEL_DATA_DIR}/{paramFileName}'

    if not os.path.exists( paramFilePath ):
        print( f"xx> couldn't find {disease.lower()}-{species.lower()} parameter file for scenario {run_options.scenario} : {paramFilePath}" )
        sys.exit( 1 )

    print( f"-> using parameter file {paramFileName}" )

    # run the model
    results, simData = run_model(
        InSimFilePath = GcsInSimFilePath if run_options.useCloudStorage else InSimFilePath,
        RkFilePath = GcsRkFilePath if run_options.useCloudStorage else RkFilePath,
        coverageFileName = coverageFileName,
        coverageTextFileStorageName = coverageTextFileStorageName,
        demogName = demogName,
        paramFileName = paramFileName,
        numSims = run_options.numSims,
        cloudModule = gcs if run_options.useCloudStorage else None,
        runningBurnIn = ( savePickleFileSuffix != None and burnInTime != None ),
        burnInTime = burnInTime
    )

    # store the results in a pickle file for use in later runs?
    if savePickleFileSuffix != None:

        # to make the pickle files compatible with older models these should be straight dicts
        # simDataAsDicts = [ dataclasses.asdict( d ) for d in simData ]
        print( f"-> writing output pickle file to {GcsOutSimFilePath if run_options.useCloudStorage else OutSimFilePath}" )

        if run_options.useCloudStorage:
            gcs.write_string_to_file( pickle.dumps( simData, protocol=pickle.HIGHEST_PROTOCOL ), GcsOutSimFilePath )

        else:
            pickle.dump( simData, open( OutSimFilePath, 'wb' ), protocol=pickle.HIGHEST_PROTOCOL )

    # output the raw model result data for comparison?
    if saveIntermediateResults == True:
        for i in range(len(results)):
            df = results[i]

            intermediate_results_dir = f"{output_data_path}/intermediate-results"
            if ( run_options.useCloudStorage == False ) and ( not os.path.isdir( intermediate_results_dir ) ):
                print( f"-> making local intermediate results directory {intermediate_results_dir}" )
                Path( intermediate_results_dir ).mkdir( parents = True, exist_ok = True )

            df.to_csv( f'{intermediate_results_dir}/{iu}_results_{i:03}.csv', index=False )


    # don't waste time saving output if running burn-in
    if ( savePickleFileSuffix != None ):
        print( f"-> running burn-in, not saving output" )
        return

    # get a transformer generator function for the IHME/IPM transforms
    transformer = sim_result_transform_generator( results, iu, runInfo['species'], run_options.scenario, run_options.numSims )

    # decide whether to put the group ID in the filename
    groupId_string = f'-group_{run_options.groupId:03}' if run_options.groupId is not None else ''

    # add a '.bz2' suffix if compressing the IHME/IPM files
    compressSuffix = ".bz2" if compress == True else ""
    compression = None if compress == False else "bz2"

    # run IHME transforms
    ihme_df = next( transformer )
    ihme_file_name = f"{output_data_path}/ihme-{iu}-{runInfo['species'].lower()}{groupId_string}-scenario_{run_options.scenario}-group_{run_options.groupId:03}-{run_options.numSims}_simulations.csv{compressSuffix}"
    ihme_df.to_csv( ihme_file_name, index=False, compression=compression )

    # run IPM transforms
    ipm_df = next( transformer )
    ipm_file_name = f"{output_data_path}/ipm-{iu}-{runInfo['species'].lower()}{groupId_string}-scenario_{run_options.scenario}-group_{run_options.groupId:03}-{run_options.numSims}_simulations.csv{compressSuffix}"
    ipm_df.to_csv( ipm_file_name, index=False, compression=compression )

    os.remove( coverageTextFileStorageName )

    print( f"-> IHME file: {ihme_file_name}" )
    print( f"-> IPM file:  {ipm_file_name}" )

    return

'''
function to load in a pickle file and associated parameters file and then
run forward in time 23 years and give back results
'''
def run_model(
    InSimFilePath=None, RkFilePath=None,
    coverageFileName='Coverage_template.xlsx', coverageTextFileStorageName=None,
    demogName='Default', paramFileName='sch_example.txt',
    numSims=None, cloudModule=None, runningBurnIn=False, burnInTime=None
):

    # number of simulations to run
    if numSims is None:
        raise MissingArgumentError( 'numSims' )

    # path to pickle file
    if InSimFilePath is None:
        raise MissingArgumentError( "InSimFilePath" )

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

    # pick parameters and saved populations in order
    indices = range( numSims )

    start_time = time.time()

    # run simulations in parallel starting from zero
    if runningBurnIn == True:

        print( f'-> running burn-in, not reading pickle data (from {InSimFilePath})' )
        res = Parallel(n_jobs=num_cores)(
            delayed(BurnInSimulations)(params,simparams, i) for i in range(numSims)
        )

    # run simulations in parallel starting from specified pickled state
    else:

        # load pickle file
        print( f'-> reading in pickle data from {InSimFilePath}' )
        pickleData = pickle.loads( cloudModule.get_blob( InSimFilePath ) ) if cloudModule != None else pickle.load( open( InSimFilePath, 'rb' ) )

        res = Parallel(n_jobs=num_cores)(
            delayed(multiple_simulations_after_burnin)(params, pickleData, simparams, indices, i, burnInTime) for i in range(numSims)
        )

    results = [ item[ 0 ] for item in res ]
    simData = [ item[ 1 ] for item in res ]

    end_time = time.time()

    print( f'-> finished {numSims} simulations on {num_cores} cores in {(end_time - start_time):.3f}s' )

    return results, simData

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

    if species == 'Mansoni':
        # first 9280 rows = standard ESPEN results + population data
        # rows 9281-end = IPM cost data
        last_ihme_row = 9280

    else:
        # first 7440 rows = standard ESPEN results + population data
        # rows 7441-end = IPM cost data
        last_ihme_row = 7440

    startrow = { 'ihme': 0, 'ipm': last_ihme_row }[ type ]
    endrow = { 'ihme': last_ihme_row, 'ipm': num_result_rows }[ type ]

    # array to put the transformed data into
    values = []

    # spin through the relevant rows
    for rowIndex, row in results[ 0 ][ startrow:endrow ].iterrows():

        # get an index into target array, i.e. where we're putting the new row
        # - jump ahead by last_ihme_row for the ipm data
        arrIndex = ( rowIndex - last_ihme_row ) if type == 'ipm' else rowIndex

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



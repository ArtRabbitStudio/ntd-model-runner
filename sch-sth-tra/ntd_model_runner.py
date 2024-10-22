import pickle
import time
import multiprocessing
import math
import os, sys
import pkg_resources

import pandas as pd
import numpy as np

import db
from gcs import gcs

from datetime import datetime
from types import SimpleNamespace
from pathlib import Path
from joblib import Parallel, delayed

from sch_simulation.helsim_RUN_KK import *
from sch_simulation.helsim_FUNC_KK import *

from run_trachoma import run_trachoma_model

# constants
INSTITUTION_TYPE_IHME = 'ihme'
INSTITUTION_TYPE_IPM = 'ipm'
INSTITUTION_TYPE_ALL = 'all'

# custom exceptions
class MissingArgumentError( ValueError ):
    pass

class DirectoryNotFoundError( ValueError ):
    pass

# run the model with the right params and then transform results for IHME/IPM
def run( run_info: SimpleNamespace, run_options: SimpleNamespace, DB ):

    # get run info
    iu = run_info.iu_code
    region = iu[:3]
    disease = run_info.type
    species = run_info.species
    short = run_info.short
    demogName = run_info.demogName

    # get/check run options
    if not hasattr( run_options, 'scenario' ) or run_options.scenario is None:
        raise MissingArgumentError( 'run_options.scenario' )

    if not hasattr( run_options, 'groupId' ) or run_options.groupId is None:
        # trachoma doesn't have groups
        if species not in [ 'Trachoma', 'Epioncho' ]:
            raise MissingArgumentError( 'run_options.groupId' )

    if not hasattr( run_options, 'numSims' ) or run_options.numSims is None:
        raise MissingArgumentError( 'run_options.numSims' )

    # get local vars out of dictionary, using defaults if not supplied
    compress = run_options.compress if hasattr( run_options, 'compress' ) else False
    splitSchResults = run_options.splitSchResults if hasattr( run_options, 'splitSchResults' ) else False
    readPickleFileSuffix = run_options.readPickleFileSuffix if hasattr( run_options, 'readPickleFileSuffix' ) else None
    savePickleFileSuffix = run_options.savePickleFileSuffix if hasattr( run_options, 'savePickleFileSuffix' ) else None
    saveIntermediateResults = run_options.saveIntermediateResults if hasattr( run_options, 'saveIntermediateResults' ) else False
    burnInTime = run_options.burnInTime if hasattr( run_options, 'burnInTime' ) else None
    outputFolder = run_options.outputFolder if hasattr( run_options, 'outputFolder' ) else '202206'
    sourceBucket = run_options.sourceBucket if hasattr( run_options, 'sourceBucket' ) else 'ntd-disease-simulator-data'
    destinationBucket = run_options.destinationBucket if hasattr( run_options, 'destinationBucket' ) else 'ntd-endgame-result-data'
    sourceDataPath = run_options.sourceDataPath if hasattr( run_options, 'sourceDataPath' ) else 'source-data'
    surveyType = run_options.surveyType if hasattr( run_options, 'surveyType' ) else 'KK2'
    secularTrend = run_options.secularTrend if hasattr( run_options, 'secularTrend' ) else False
    vaccineWaningLength = run_options.vaccineWaningLength if hasattr( run_options, 'vaccineWaningLength' ) else None
    paramSubdir = run_options.paramSubdir if hasattr( run_options, 'paramSubdir' ) else None
    paramFileDiseaseSuffix = run_options.paramFileDiseaseSuffix if hasattr( run_options, 'paramFileDiseaseSuffix' ) else ''
    shortDiseaseCodeSuffix = run_options.shortDiseaseCodeSuffix if hasattr( run_options, 'shortDiseaseCodeSuffix' ) else ''

    # construct cloud path for this disease/species
    GcsSpecies = {
        'Ascaris': 'roundworm',
        'Trichuris': 'whipworm',
        'Hookworm': 'hookworm',
        'Mansoni': 'mansoni',
        'Haematobium': 'haematobium',
        'Trachoma': 'trachoma',
        'Epioncho': 'epioncho',
    }[ species ]

    GcsPrefix = {
        'Ascaris': f"{run_info.type}-",
        'Trichuris': f"{run_info.type}-",
        'Hookworm': f"{run_info.type}-",
        'Mansoni': f"{run_info.type}-",
        'Haematobium': f"{run_info.type}-",
        'Trachoma': '',
        'Epioncho': '',
    }[ species ]

    DISEASE_CLOUD_ROOT = f'diseases/{GcsPrefix}{GcsSpecies.lower()}'
    DISEASE_CLOUD_SRC_PATH = f'{DISEASE_CLOUD_ROOT}/{sourceDataPath}'

    # TODO check if survey type used for other diseases & check they run without
    surveyTypeDirSuffix = f"/survey_type_{surveyType}" if species == 'Mansoni' else ''

    # note secular or non-secular trend in Trachoma paths
    if species == "Trachoma":
        isSecularTrend = "non_" if secularTrend == False else ""
        secularTrendPath = f"{isSecularTrend}secular_trend/"
        vaccineWaningLengthPath = "" if vaccineWaningLength == None else f"waning_length_{vaccineWaningLength}/"
    else:
        secularTrendPath = ""
        vaccineWaningLengthPath = ""

    # only include the group if it's been specified (which it only is in SCH, so secularTrendPath/vaccineWaningLengthPath only needed if it's None)
    if run_options.groupId is None:
        DISEASE_CLOUD_DST_PATH = (
            f'ntd/{outputFolder}/{GcsPrefix}{GcsSpecies.lower()}{paramFileDiseaseSuffix.replace("_","-")}/scenario_{run_options.scenario}{surveyTypeDirSuffix}/'
            f'{secularTrendPath}{vaccineWaningLengthPath}{iu[0:3]}'
        )
    else:
        DISEASE_CLOUD_DST_PATH = f'ntd/{outputFolder}/{GcsPrefix}{GcsSpecies.lower()}{paramFileDiseaseSuffix.replace("_","-")}/scenario_{run_options.scenario}{surveyTypeDirSuffix}/group_{run_options.groupId:03}'

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
    GcsOutputDataPath = f'gs://{destinationBucket}/{output_data_root}'
    LocalOutputDataPath = f'{LOCAL_OUTPUT_DATA_DIR}/{output_data_root}'
    output_data_path = GcsOutputDataPath if run_options.useCloudStorage else LocalOutputDataPath

    # make sure local output directory is present
    if run_options.useCloudStorage == False:
        if not os.path.isdir( output_data_path ):
            Path( output_data_path ).mkdir( parents = True, exist_ok = True )

    GCS = gcs( sourceBucket )

    # short-circuit out to trachoma?
    if species == 'Trachoma':

        # locate pickle file for IU
        GcsInSimFilePath = f'{DISEASE_CLOUD_SRC_PATH}/{region}/{iu}/OutputVals_{iu}.p'
        InSimFilePath = GcsInSimFilePath if run_options.useCloudStorage else f'{LOCAL_INPUT_DATA_DIR}/OutputVals_{iu}.p'

        # locate Beta input file for IU
        GcsBetaFilePath = f'gs://{sourceBucket}/{DISEASE_CLOUD_SRC_PATH}/{region}/{iu}/InputBet_{iu}.csv'
        BetaFilePath = GcsBetaFilePath if run_options.useCloudStorage else f'{LOCAL_INPUT_DATA_DIR}/InputBet_{iu}.csv'

        # specify compression settings
        compressSuffix = ".bz2" if compress == True else ""
        compression = None if compress == False else "bz2"

        # note non-standard vaccine waning length in filename
        vwlIndicator = "" if vaccineWaningLength == None else f"-waning_length_{vaccineWaningLength}"

        # specify file output locations
        ihme_file_name = (
            f"{output_data_path}/ihme-{iu}-{run_info.species.lower()}-scenario_{run_options.scenario}-"
            f"{run_options.numSims}_simulations-{isSecularTrend}secular_trend{vwlIndicator}.csv{compressSuffix}"
        )

        ipm_file_name = (
            f"{output_data_path}/ipm-{iu}-{run_info.species.lower()}-scenario_{run_options.scenario}-"
            f"{run_options.numSims}_simulations-{isSecularTrend}secular_trend{vwlIndicator}.csv{compressSuffix}"
        )

        cloudModule = GCS if run_options.useCloudStorage else None
        return run_trachoma_model(
            iu, run_options.scenario, run_options.numSims,
            vaccineWaningLength, secularTrend,
            BetaFilePath, InSimFilePath, cloudModule, ihme_file_name, ipm_file_name, compressSuffix, compression
        )

    if species == 'Epioncho':
        return run_epioncho_model( iu, run_options.scenario, run_options.numSims, cloudModule, ihme_file_name, ipm_file_name, compressSuffix, compression )

    # locate pickle file for IU
    pickleReadSuffix = f"_{readPickleFileSuffix}" if readPickleFileSuffix != None else ""
    InSimFilePath = f'{LOCAL_INPUT_DATA_DIR}/{short}{shortDiseaseCodeSuffix}_{iu}{pickleReadSuffix}.p'
    GcsInSimFilePath = f'{DISEASE_CLOUD_SRC_PATH}/{region}/{iu}/{short}{shortDiseaseCodeSuffix}_{iu}{pickleReadSuffix}.p'

    # specify output pickle file location for IU?
    if savePickleFileSuffix != None:
        OutSimFilePath = f'{LOCAL_INPUT_DATA_DIR}/{short}{shortDiseaseCodeSuffix}_{iu}_{savePickleFileSuffix}.p'
        GcsOutSimFilePath = f'{DISEASE_CLOUD_SRC_PATH}/{region}/{iu}/{short}{shortDiseaseCodeSuffix}_{iu}_{savePickleFileSuffix}.p'
        print( f"-> will write output pickle file to {GcsOutSimFilePath if run_options.useCloudStorage else OutSimFilePath}" )
    else:
        print( "-> not writing output pickle file" )

    # locate RK input file for IU
    RkFilePath = f'{LOCAL_INPUT_DATA_DIR}/Input_Rk_{short}{shortDiseaseCodeSuffix}_{iu}.csv'
    GcsRkFilePath = f'gs://{sourceBucket}/{DISEASE_CLOUD_SRC_PATH}/{region}/{iu}/Input_Rk_{short}{shortDiseaseCodeSuffix}_{iu}.csv'

    # locate & check coverage file for selected disease/scenario
    paramFileSubdirectory = f"/{paramSubdir}" if paramSubdir else ""
    run_options.coverageFileName = f'{disease.upper()}_params{paramFileSubdirectory}/{species.lower()}_coverage_scenario_{run_options.scenario}.xlsx'
    coverageFilePath = f'{MODEL_DATA_DIR}/{run_options.coverageFileName}'

    if not os.path.exists( coverageFilePath ):
        print( f"xx> couldn't find {disease.lower()}-{species.lower()} coverage file for scenario {run_options.scenario} : {coverageFilePath}" )
        sys.exit( 1 )

    print( f"-> using coverage file {run_options.coverageFileName}" )

    coverageTextFileStorageName = f'/tmp/{os.getpid()}_{short}_{iu}_MDA_vacc.txt'

    # locate & check parameter file for selected disease/scenario
    paramFileDirectory = f'{disease.upper()}_params{paramFileSubdirectory}'

    #paramFileName = 'mansoni_params.txt' if species.lower() == 'mansoni' else f'{species.lower()}_scenario_{run_options.scenario}.txt'
    ## fixed to 'all diseases use specific parameter files' for Rwanda 202307 - TODO FIXME change back for Endgame
    ## fixed to explicit x_params_scenario_y.txt 20241010 - TODO FIXME for SCH mansoni(/haematobium)
    paramFileName = f'{species.lower()}{paramFileDiseaseSuffix}_params_scenario_{run_options.scenario}.txt'
    run_options.paramFileName = f'{paramFileDirectory}/{paramFileName}'
    paramFilePath = f'{MODEL_DATA_DIR}/{run_options.paramFileName}'

    if not os.path.exists( paramFilePath ):
        print( f"xx> couldn't find {disease.lower()}-{species.lower()} parameter file for scenario {run_options.scenario} : {paramFilePath}" )
        sys.exit( 1 )

    print( f"-> using parameter file {run_options.paramFileName}" )

    # run the model
    run_info.started = datetime.now()
    results, simData, ntdmcData = run_model(
        InSimFilePath = GcsInSimFilePath if run_options.useCloudStorage else InSimFilePath,
        RkFilePath = GcsRkFilePath if run_options.useCloudStorage else RkFilePath,
        coverageFileName = run_options.coverageFileName,
        coverageTextFileStorageName = coverageTextFileStorageName,
        demogName = demogName,
        paramFileName = run_options.paramFileName,
        numSims = run_options.numSims,
        numProcs = run_options.numProcs,
        surveyType = surveyType,
        cloudModule = GCS if run_options.useCloudStorage else None,
        runningBurnIn = ( savePickleFileSuffix != None and burnInTime != None ),
        burnInTime = burnInTime
    )
    run_info.ended = datetime.now()

    # store the results in a pickle file for use in later runs?
    if savePickleFileSuffix != None:

        # to make the pickle files compatible with older models these should be straight dicts
        # simDataAsDicts = [ dataclasses.asdict( d ) for d in simData ]
        print( f"-> writing output pickle file to {GcsOutSimFilePath if run_options.useCloudStorage else OutSimFilePath}" )

        if run_options.useCloudStorage:
            GCS.write_string_to_file( pickle.dumps( simData, protocol=pickle.HIGHEST_PROTOCOL ), GcsOutSimFilePath )

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

    # decide whether to put the group ID in the filename
    groupId_string = f'-group_{run_options.groupId:03}' if run_options.groupId is not None else ''

    # add a '.bz2' suffix if compressing the IHME/IPM files
    compressSuffix = ".bz2" if compress == True else ""
    compression = None if compress == False else "bz2"

    # create common cloud filename components
    surveyTypeFileSuffix = f"-survey_type_{surveyType.lower().replace('-','_')}" if species == 'Mansoni' else ''
    file_name_ending = f"{iu}-{run_info.species.lower()}{paramFileDiseaseSuffix}{groupId_string}-scenario_{run_options.scenario}{surveyTypeFileSuffix}-group_{run_options.groupId:03}-{run_options.numSims}_simulations.csv{compressSuffix}"

    # get a transformer generator function for the IHME/IPM transforms
    transformer = sim_result_transform_generator( results, iu, run_info.species, run_options.scenario, run_options.numSims, surveyTypeFileSuffix )

    # not splitting results for IHME, save results straight into CSV
    all_results_file_name = f"{output_data_path}/all_results-{file_name_ending}"
    all_df = sim_result_transform_all( results, iu, run_info.species, run_options.scenario, run_options.numSims, surveyTypeFileSuffix )
    all_df.to_csv( all_results_file_name, index=False, compression=compression )
    print( f"-> Result file: {all_results_file_name}" )
    # TODO write db result record?

    # write out NTDMC file. TODO write this instead of IPM in split-results segment?
    ntdmc_file_name = f"{output_data_path}/ntdmc-{file_name_ending}"
    ntdmcData.to_csv( ntdmc_file_name, index=False, compression=compression )
    print( f"-> NTDMC file:  {ntdmc_file_name}" )

    os.remove( coverageTextFileStorageName )
    return

#    # if not splitting results for IHME, save results straight into CSV
#    if splitSchResults == False:
#        all_results_file_name = f"{output_data_path}/all_results-{file_name_ending}"
#        all_df = sim_result_transform_all( results, iu, run_info.species, run_options.scenario, run_options.numSims, surveyTypeFileSuffix )
#        all_df.to_csv( all_results_file_name, index=False, compression=compression )
#        print( f"-> Result file: {all_results_file_name}" )
#        # TODO write db result record?
#
#        # write out NTDMC file. TODO write this instead of IPM in split-results segment?
#        ntdmc_file_name = f"{output_data_path}/ntdmc-{file_name_ending}"
#        ntdmcData.to_csv( ntdmc_file_name, index=False, compression=compression )
#        print( f"-> NTDMC file:  {ntdmc_file_name}" )
#
#        os.remove( coverageTextFileStorageName )
#        return
#
#    # run IHME transforms
#    ihme_df = next( transformer )
#    ihme_file_name = f"{output_data_path}/ihme-{file_name_ending}"
#    ihme_df.to_csv( ihme_file_name, index=False, compression=compression )
#
#    # store metadata in flow db
#    if run_options.useCloudStorage:
#        DB.write_db_result_record( run_info, run_options, INSTITUTION_TYPE_IHME, ihme_file_name, compression )
#
#    # run IPM transforms
#    ipm_df = next( transformer )
#    ipm_file_name = f"{output_data_path}/ipm-{file_name_ending}"
#    ipm_df.to_csv( ipm_file_name, index=False, compression=compression )
#
#    # store metadata in flow db
#    if run_options.useCloudStorage:
#        DB.write_db_result_record( run_info, run_options, INSTITUTION_TYPE_IPM, ipm_file_name, compression )
#
#    os.remove( coverageTextFileStorageName )
#
#    print( f"-> IHME file: {ihme_file_name}" )
#    print( f"-> IPM file:  {ipm_file_name}" )
#
#    return

'''
function to load in a pickle file and associated parameters file and then
run forward in time 23 years and give back results
'''
def run_model(
    InSimFilePath=None, RkFilePath=None,
    coverageFileName='Coverage_template.xlsx', coverageTextFileStorageName=None,
    demogName='Default', surveyType='KK2', paramFileName='sch_example.txt',
    numSims=None, numProcs=0, cloudModule=None, runningBurnIn=False, burnInTime=None
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

    # add vector control data to parameters
    params = parse_vector_control_input(coverageFileName, params)

    # count number of processors
    num_cores = numProcs if numProcs > 0 else multiprocessing.cpu_count()
    print( f'-> running {numSims} simulations on {num_cores} cores' )

    # pick parameters and saved populations in order
    indices = range( numSims )

    start_time = time.time()

    # run simulations in parallel starting from zero
    if runningBurnIn == True:

        print( f'-> running burn-in, not reading pickle data (from {InSimFilePath})' )
        res = Parallel(n_jobs=num_cores)(
            delayed( BurnInSimulations )( params, simparams, i, surveyType ) for i in range( numSims )
        )

    # run simulations in parallel starting from specified pickled state
    else:

        # load pickle file
        print( f'-> reading in pickle data from {InSimFilePath}' )
        pickleData = pickle.loads( cloudModule.get_blob( InSimFilePath ) ) if cloudModule != None else pickle.load( open( InSimFilePath, 'rb' ) )

        # calculate burnt-in time from pickle data
        burnInTime = 0 if burnInTime == None else burnInTime
        for i in range(len(pickleData)):
            burnInTime = max( burnInTime,round( max( pickleData[i].demography.birthDate * 10 ) ) / 10 )

        res = Parallel(n_jobs=num_cores, backend="multiprocessing")(
            delayed( multiple_simulations_after_burnin )( params, pickleData, simparams, indices, i, burnInTime, surveyType ) for i in range( numSims )
        )

    results = [ item[ 0 ] for item in res ]
    simData = [ item[ 1 ] for item in res ]

    # generate ntdmc data. doing it here because it needs access to 'params' and raw 'res'
    startYear = 2026
    ntdmcData = constructNTDMCResultsAcrossAllSims( params, res, surveyType.upper(), startYear, resultsIndex = 2 )

    end_time = time.time()

    print( f'-> finished {numSims} simulations on {num_cores} cores in {(end_time - start_time):.3f}s' )

    return results, simData, ntdmcData

def sim_result_transform_all( results, iu, species, scenario, numSims, surveyTypeFileSuffix ):

    # create key dict for IHME/IPM format dataframes
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

    # add a column for each result draw
    for i in range( 0, numSims ):
        keys[ f'draw_{i}' ] = []

    print( f'-> starting ALL transform for {numSims} simulations with pandas' )
    a = time.time()
    values = transform_results_with_pandas( results, iu, INSTITUTION_TYPE_ALL, species, scenario, numSims, keys )
    b = time.time()
    print( f'-> finished ALL transform for {numSims} simulations with pandas in {(b-a):.3f}s' )
    return values

def sim_result_transform_generator( results, iu, species, scenario, numSims, surveyTypeFileSuffix ):

    # create key dict for IHME/IPM format dataframes
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

    # add a column for each result draw
    for i in range( 0, numSims ):
        keys[ f'draw_{i}' ] = []

    ################################################################################
    # we're going to take the draw_1 from each run result for an IU:
    #
    #   Time,age_start,age_end,intensity,species,measure,draw_1
    #   0.0,0,1,light,Mansoni,prevalence,0.0
    #
    # and add it as draw_x at the end of a row in this output file:
    #
    #   espen_loc,year_id,age_start,age_end,intensity,scenario,species,measure,draw_0,draw_1,draw_3,draw_4,draw_y,draw_z,...
    #   BFA05335,2030,4,4,light,1,mansoni,prevalence,0.1,0.08,0.09,0.11,...
    ################################################################################

    ################################################################################
    # IHME data file
    ################################################################################

    print( f'-> starting IHME transform for {numSims} simulations with pandas' )
    a = time.time()
    values = transform_results_with_pandas( results, iu, INSTITUTION_TYPE_IHME, species, scenario, numSims, keys )
    b = time.time()
    print( f'-> finished IHME transform for {numSims} simulations with pandas in {(b-a):.3f}s' )
    yield values

    ################################################################################
    # IPM costs file
    ################################################################################

    print( f'-> starting IPM transform for {numSims} simulations with pandas' )
    a = time.time()
    values = transform_results_with_pandas( results, iu, INSTITUTION_TYPE_IPM, species, scenario, numSims, keys )
    b = time.time()
    print( f'-> finished IPM transform for {numSims} simulations in {(b-a):.3f}s' )
    yield values

    return

def transform_results_with_pandas( results, iu, type, species, scenario, numSims, keys ):

    # strip off newlines from model output
    for i in range( 0, len( results ) ):
        results[ i ][ 'species' ] = results[ i ][ 'species' ].replace( '\n', '', regex=True )

    # work out which lines from file to use
    # previously used row 7584 for end of IPM data, now just run to the end of the results
    num_result_rows = results[ 0 ].shape[ 0 ]

    if species == 'Mansoni':
        # first 9280 rows = standard ESPEN results + population data
        # rows 9281-end = IPM cost data
        #last_ihme_row = 9280
        # set to 8400 for Rwanda
        last_ihme_row = 8400

    else:
        # first 7440 rows = standard ESPEN results + population data
        # rows 7441-end = IPM cost data
        #last_ihme_row = 7440
        # set to 8400 for Rwanda
        last_ihme_row = 8400

    startrow = { INSTITUTION_TYPE_IHME: 0, INSTITUTION_TYPE_IPM: last_ihme_row, INSTITUTION_TYPE_ALL: 0 }[ type ]
    endrow = { INSTITUTION_TYPE_IHME: last_ihme_row, INSTITUTION_TYPE_IPM: num_result_rows, INSTITUTION_TYPE_ALL: num_result_rows }[ type ]

    # create a DF
    output = pd.DataFrame.from_dict( keys )

    # copy over the dynamic rows to fill out the DataFrame rows
    for key in [ 'age_start', 'age_end', 'intensity', 'measure' ]:
        output[key] = results[ 0] [ key ][ startrow:endrow ]

    # work out the year for the row
    output[ 'year_id' ] = ( results[ 0 ][ 'Time' ] + 2018 ).astype( int )
    ## use 2020.5 for Rwanda TODO FIXME revert for Endgame
    #output[ 'year_id' ] = ( results[ 0 ][ 'Time' ] + 2020.5 ).astype( int )

    # copy the static rows into the full list
    output[ 'espen_loc' ] = iu
    output[ 'scenario' ] = scenario
    output[ 'species' ] = species

    # add on the draw for this result
    for i in range( 0, len( results ) ):
        output[ f'draw_{i}' ] = results[ i ][ 'draw_1' ][ startrow:endrow ]

    # reset the line indexes to start from 0
    return output.reset_index().drop( 'index', axis = 1 )

################################################################################


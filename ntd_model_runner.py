import pickle
import time
import multiprocessing
import math
import os

import pandas

import gcs
import db

from joblib import Parallel, delayed

from sch_simulation.helsim_RUN_KK import *
from sch_simulation.helsim_FUNC_KK import *

class MissingArgumentError( ValueError ):
    pass

class DirectoryNotFoundError( ValueError ):
    pass

def sim_result_transform_generator( results, iu, species, scenario, numSims ):

    # espen_loc,year_id,age_start,age_end,sex_id,intensity,scenario,species,sequelae,measure
    ihme_keys = {
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
        ihme_keys[ f'draw_{i}' ] = []

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

    ihme_values = []

    # first 7440 rows = standard ESPEN results + population data
    for rowIndex, row in results[0][0:7440].iterrows():

        if rowIndex > 0 and rowIndex % 1000 == 0:
            print( f"-> {rowIndex}" )

        # add the first fields up to the first draw
        scenario_fields = [ iu, math.trunc( row.Time + 2018 ), row.age_start, row.age_end, row.intensity, scenario, species, row.measure, row.draw_1 ]
        ihme_values.insert( rowIndex, scenario_fields )

        # add draw_1 from the current line in each file as draw_X in the ihme file
        for simNo in range(1,numSims):
            ihme_values[ rowIndex ].append( results[ simNo ].loc[ rowIndex ].draw_1 )

    b = time.time()
    print( f'-> finished IHME transform for {numSims} simulations in {(b-a):.3f}s' )

    yield pd.DataFrame( ihme_values, columns = ihme_keys )

    ################################################################################
    # IPM costs file
    ################################################################################

    print( f'-> starting IPM transform for {numSims} simulations' )
    a = time.time()

    ipm_values = []

    # 7440 -> end = cost data
    for rowIndex, row in results[0][7440:].iterrows():

        arrIndex = rowIndex - 7440

        if arrIndex > 0 and arrIndex % 1000 == 0:
            print( f"-> {arrIndex}" )

        # add the first fields up to the first draw
        ipm_values.insert( arrIndex, [ iu, math.trunc( row.Time + 2018 ), row.age_start, row.age_end, row.intensity, scenario, species, row.measure, row.draw_1 ] )

        # add draw_1 from the current line in each file as draw_X in the ipm file
        for simNo in range(1,numSims):
            ipm_values[ arrIndex ].append( results[ simNo ].loc[ arrIndex ].draw_1 )

    b = time.time()
    print( f'-> finished IPM transform for {numSims} simulations in {(b-a):.3f}s' )

    yield pd.DataFrame( ipm_values, columns = ihme_keys )

    return

def run( runInfo, scenario, numSims, DB, useCloudStorage, saveResults=False ):

    DISEASE_CLOUD_PATH = f'diseases/{runInfo[ "type" ]}-{runInfo[ "species" ].lower()}/source-data'

    # make sure local data directory is present
    if useCloudStorage == False:
        if not os.path.isdir( "./local-data" ):
            raise DirectoryNotFoundError( "./local-data" )

    iu = runInfo[ "iu_code" ]
    region = iu[:3]
    short = runInfo[ "short" ]
    species = runInfo[ "species" ]

    InSimFilePath = f'./local-data/{short}_{iu}.p'
    GcsInSimFilePath = f'{DISEASE_CLOUD_PATH}/{region}/{iu}/{short}_{iu}.p'

    RkFilePath = f'./local-data/Input_Rk_{short}_{iu}.csv'
    GcsRkFilePath = f'gs://ntd-disease-simulator-data/{DISEASE_CLOUD_PATH}/{region}/{iu}/Input_Rk_{short}_{iu}.csv'

    coverageTextFileStorageName = f'/tmp/{short}_{iu}_MDA_vacc.txt'

    results = run_model(
        InSimFilePath = GcsInSimFilePath if useCloudStorage else InSimFilePath,
        RkFilePath = GcsRkFilePath if useCloudStorage else RkFilePath,
        coverageTextFileStorageName = coverageTextFileStorageName,
        numSims = numSims,
        cloudModule = gcs if useCloudStorage else None
    )

    scenario = 'SCEN_01'
    transformer = sim_result_transform_generator( results, iu, runInfo['species'], scenario, numSims )

    ihme_df = next( transformer )
    ihme_file_name = f"ihme-{iu}-{runInfo['species']}-{scenario}-{numSims}.csv"
    ihme_df.to_csv( ihme_file_name, index=False )

    ipm_df = next( transformer )
    ipm_file_name = f"ipm-{iu}-{runInfo['species']}-{scenario}-{numSims}.csv"
    ipm_df.to_csv( ipm_file_name, index=False )

    if saveResults == True:
        for i in range(len(results)):
            df = results[i]
            df.to_csv( f'{iu}_results_{i:03}.csv', index=False )

    return

def run_model( InSimFilePath=None, RkFilePath=None, coverageFileName='Coverage_template.xlsx', coverageTextFileStorageName=None,
                demogName='Default', paramFileName='sch_example.txt', numSims=None, cloudModule=None ):
    '''
    File to load in a pickle file and associated parameters file and then
    run forward in time 23 years and give back results
    '''

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
            delayed(multiple_simulations)(params, pickleData, simparams, i) for i in range(numSims))
        
    end_time = time.time()

    print( f'-> finished {numSims} simulations on {num_cores} cores in {(end_time - start_time):.3f}s' )

    return results

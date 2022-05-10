from sch_simulation.helsim_RUN_KK import *
from sch_simulation.helsim_FUNC_KK import *

from joblib import Parallel, delayed

import gcs
import db

import pickle
import time
import multiprocessing

insert_result_sql = '''
INSERT INTO results( Time, age_start, age_end, intensity, measure, draw_1, disease_id, iu_id, sim_no )
VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s )
'''

class MissingArgumentError( ValueError ):
    pass

def run( runInfo, numSims, DB, useCloudStorage ):

    DISEASE_CLOUD_PATH = f'diseases/{runInfo["type"]}-{runInfo["species"].lower()}/source-data'

    iu = runInfo["iu_code"]
    region = iu[:3]
    short = runInfo["short"]

    InSimFilePath = f'/tmp/{short}_{iu}.p'
    GcsInSimFilePath = f'{DISEASE_CLOUD_PATH}/{region}/{iu}/{short}_{iu}.p'

    RkFilePath = f'/tmp/Input_Rk_{short}_{iu}.csv'
    GcsRkFilePath = f'gs://ntd-disease-simulator-data/{DISEASE_CLOUD_PATH}/{region}/{iu}/Input_Rk_{short}_{iu}.csv'

    coverageTextFileStorageName = f'/tmp/{short}_{iu}_MDA_vacc.txt'

    results = run_model(
        InSimFilePath = GcsInSimFilePath if useCloudStorage else InSimFilePath,
        RkFilePath = GcsRkFilePath if useCloudStorage else RkFilePath,
        coverageTextFileStorageName = coverageTextFileStorageName,
        numSims = numSims,
        cloudModule = gcs if useCloudStorage else None
    )

    db_start_time = time.time()

    for sim_no in range( len( results ) ):

        df = results[ sim_no ]
        df.drop( columns = ['species'], inplace = True )
        df[ 'disease_id' ] = runInfo[ 'disease_id' ]
        df[ 'iu_id' ] = runInfo[ 'iu_id' ]
        df[ 'sim_no' ] = sim_no
        # print( df )
        df_list = df.values

        data = []
        for sim_row in df.values:
            data.append( tuple( sim_row ) )

        print( f"-> inserting {len(data)} results for sim_no {sim_no} ({runInfo['short']}:{runInfo['iu_code']}) into db" )

        start_time = time.time()
        DB.cursor().executemany( insert_result_sql, data )
        DB.commit()
        end_time = time.time()
        total_time = end_time - start_time
        print( f"  -> {total_time:.3f}s" )

    db_end_time = time.time()
    total_db_time = db_end_time - db_start_time
    print( f"-> total time writing to db {total_db_time:.3f}s" )


def run_model( InSimFilePath=None, RkFilePath=None, coverageFileName='Coverage_template.xlsx', coverageTextFileStorageName=None,
                demogName='Default', paramFileName='sch_example.txt', resultOutputPath=None, numSims=None, cloudModule=None ):
    '''
    File to load in a pickle file and associated parameters file and then
    run forward in time 23 years and give back results
    Can store results as a set of csv's if saveResults = True
    '''

    # flag for saving results or not
    saveResults = ( resultOutputPath != None )

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

#    if saveResults:
#        for i in range(len(results)):
#            df = results[i]
#            df.to_csv(f'tmp/Man_AGO02049_results_{i:03}.csv', index=False)

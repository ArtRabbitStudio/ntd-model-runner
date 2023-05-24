from trachoma.trachoma_functions import *
import multiprocessing
import sys
import time
from joblib import Parallel, delayed
num_cores = multiprocessing.cpu_count()
import pickle

def run_trachoma_model( iu, scenario, numSims, vaccineWaningLength, secularTrend, BetaFilePath, InSimFilePath, cloudModule, ihme_file_name, ipm_file_name, compressSuffix, compression ):

    #############################################################################################################################
    #############################################################################################################################

    # initialize parameters, sim_params, and demography

    params = {
        'N': 2500,
        'av_I_duration' : 2,
        'av_ID_duration':200/7,
        'inf_red':0.45,
        'min_ID':11, # Parameters relating to duration of infection period, including ID period
        'av_D_duration':300/7,
        'min_D':1, # Parameters relating to duration of disease period
        'v_1':1,
        'v_2':2.6,
        'phi':1.4,
        'epsilon':0.5,# Parameters relating to lambda function- calculating force of infection
        # Parameters relating to MDA
        'MDA_Cov':0.8,
        'MDA_Eff': 0.85, # Efficacy of treatment
        'rho':0.3,
        'nweeks_year':52,
        'babiesMaxAge':0.5, # Note this is years, need to check it converts to weeks later
        'youngChildMaxAge':9,# Note this is years, need to check it converts to weeks later
        'olderChildMaxAge':15, # Note this is years, need to check it converts to weeks later
        'b1':1,# this relates to bacterial load function
        'ep2':0.114,
        'n_inf_sev':38,
        'TestSensitivity': 0.96,
        'TestSpecificity': 0.98,
        'SecularTrendIndicator': 0 if secularTrend == False else 1,
        'SecularTrendYearlyBetaDecrease': 0.07,
        'vacc_prob_block_transmission':  0.8,
        'vacc_reduce_bacterial_load': 0.5,
        'vacc_reduce_duration': 0.5,
        'vacc_waning_length': 52 * 5 if vaccineWaningLength == None else vaccineWaningLength
    }

    sim_params = {
        'timesim':52*23,
        'burnin': 26,
        'N_MDA':5,
        'nsim':10
    }

    demog = {
        'tau': 0.0004807692,
        'max_age': 3120,
        'mean_age': 1040
    }

    previous_rounds = 0

    Start_date = date( 2019, 1, 1 )
    End_date = date( 2030, 12, 31 )

    #############################################################################################################################
    #############################################################################################################################

    # load pickle file
    pickleData = pickle.loads( cloudModule.get_blob( InSimFilePath ) ) if cloudModule != None else pickle.load( open( InSimFilePath, 'rb' ) )

    # load beta values file
    print( f"-> reading beta values file from {BetaFilePath}" )
    allBetas = pd.read_csv( BetaFilePath )

    #############################################################################################################################
    #############################################################################################################################
    # make sure the N parameter is the same as the number of people in the pickle file
    a = pickleData[1]
    params['N'] = len(a['IndI'])
    #############################################################################################################################
    #############################################################################################################################
    # which years to make endgame output specify and convert these to simulation time
    outputYear = range(2019, 2041)
    outputTimes = getOutputTimes(outputYear)
    outputTimes = get_Intervention_times(outputTimes, Start_date, sim_params['burnin'])

    #############################################################################################################################
    #############################################################################################################################

    # generate MDA data from coverage file
    coverageFileName = 'scen' + scenario + '.csv'
    MDAData = readPlatformData(coverageFileName, "MDA")
    MDA_dates = getInterventionDates(MDAData)
    MDA_times = get_Intervention_times(MDA_dates, Start_date, sim_params['burnin'])
    sim_params['N_MDA'] = len(MDA_times)

    VaccData = readPlatformData(coverageFileName, "Vaccine")
    Vaccine_dates = getInterventionDates(VaccData)
    vacc_times = get_Intervention_times(Vaccine_dates, Start_date, sim_params['burnin'])
    sim_params['N_Vaccines'] = len(vacc_times)

    #############################################################################################################################
    #############################################################################################################################

    print( f'-> Running {numSims} simulations on {num_cores} cores' )
    start = time.time()

    #############################################################################################################################
    #############################################################################################################################
    # run as many simulations as specified
    results = Parallel(n_jobs=num_cores)(
             delayed(run_single_simulation)(pickleData = pickleData[i],
                                            params = params,
                                            timesim = sim_params['timesim'],
                                            burnin = sim_params['burnin'],
                                            demog=demog,
                                            beta = allBetas.beta[i],
                                            MDA_times = MDA_times,
                                            MDAData=MDAData,
                                            vacc_times = vacc_times,
                                            VaccData = VaccData,
                                            outputTimes= outputTimes,
                                            index = i) for i in range(numSims))

    print( time.time() - start )

    #############################################################################################################################
    #############################################################################################################################
    # collate and output IHME data

    outsIHME = getResultsIHME(results, demog, params, outputYear)
    outsIHME.to_csv( ihme_file_name, index=False, compression=compression )

    #############################################################################################################################
    #############################################################################################################################
    # collate and output IPM data
    MDAAgeRanges = getInterventionAgeRanges(coverageFileName, "MDA")
    VaccAgeRanges = getInterventionAgeRanges(coverageFileName, "Vaccine")
    outsIPM = getResultsIPM(results, demog, params, outputYear, MDAAgeRanges, VaccAgeRanges)
    outsIPM.to_csv( ipm_file_name, index=False, compression=compression )

    print( f"-> IHME file: {ihme_file_name}" )
    print( f"-> IPM file:  {ipm_file_name}" )

    return

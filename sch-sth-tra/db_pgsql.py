import os, sys
import psycopg2
import psycopg2.extras

ENV_DB_HOST = os.getenv( 'DB_HOST' )
DB_USER = 'ntd'
DB_PASS = 'ntd'
DB_HOST = ENV_DB_HOST if ENV_DB_HOST else '127.0.0.1'
DB_PORT = 5432
DB_NAME = 'ntd'

class db(object):

    def __init__(self):

        try:

            self._db_connection = psycopg2.connect( f"host='{DB_HOST}' port={DB_PORT} dbname='{DB_NAME}' user='{DB_USER}' password='{DB_PASS}'" )

        except psycopg2.DatabaseError as d:

            print( f"xx> db error: {d}" )
            sys.exit()

        self._db_cur = self._db_connection.cursor( cursor_factory = psycopg2.extras.RealDictCursor )

    def insert( self, query, params, id_key='id' ):
        self._db_cur.execute( query, params )
        last_row = self._db_cur.fetchone()
        return last_row[ id_key ]

    def query( self, query, params ):
        self._db_cur.execute( query, params )

    def fetchone( self, query, params ):
        self._db_cur.execute( query, params )
        return self._db_cur.fetchone()

    def cursor( self ):
        return self._db_cur

    def commit( self ):
        self._db_connection.commit()

    def __del__( self ):
        if hasattr( self, '_db_connection' ):
            sys.stderr.write( '==> db connection closing\n' )
            self._db_connection.close()

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
    ##########################
    #   CREATE TABLE public.run (
    #       id SERIAL NOT NULL PRIMARY KEY,
    #       started TIMESTAMP,
    #       ended TIMESTAMP,
    #       num_sims INTEGER,
    #       disease_id INTEGER, -- FK to 'disease'
    #       description CHARACTER VARYING,
    #       person_email CHARACTER VARYING,
    #       source_bucket CHARACTER VARYING,
    #       destination_bucket CHARACTER VARYING,
    #       output_folder CHARACTER VARYING,
    #       burn_in_years INTEGER,
    #       read_pickle_file_suffix CHARACTER VARYING,
    #       save_pickle_file_suffix CHARACTER VARYING,
    #       UNIQUE( description, disease_id )
    #   );
    #
    #   CREATE TABLE public.scenario (
    #       id SERIAL NOT NULL PRIMARY KEY,
    #       name CHARACTER VARYING,
    #       coverage_filename CHARACTER VARYING,
    #       parameters_filename CHARACTER VARYING,
    #       description CHARACTER VARYING,
    #       UNIQUE( name, coverage_filename, parameters_filename )
    #   );
    #
    #   CREATE TABLE public.result (
    #       id SERIAL NOT NULL PRIMARY KEY,
    #       started TIMESTAMP,
    #       ended TIMESTAMP,
    #       run_id INTEGER NOT NULL, -- FK to 'run'
    #       iu_id INTEGER NOT NULL, -- FK to 'iu'
    #       scenario_id INTEGER NOT NULL, -- FK to 'scenario'
    #       result_type institution,
    #       filename CHARACTER VARYING,
    #       demography_name demography,
    #       group_id INTEGER -- just an int
    #   );
    ##########################
    def write_db_result_record( self, run_info, run_options, institution, file_name, compression ):
        print( f'-> writing db {institution} result record' )

        # create or re-use existing 'scenario' record
        sql = '''
        INSERT INTO scenario ( name, disease_id, coverage_filename, parameters_filename )
        VALUES ( %s, %s, %s, %s )
        ON CONFLICT ( name, coverage_filename, parameters_filename )
        DO UPDATE
        SET name = EXCLUDED.name, coverage_filename = EXCLUDED.coverage_filename, parameters_filename = EXCLUDED.parameters_filename
        RETURNING id
        '''

        params = (
            run_options.scenario, run_info.disease_id,
            run_options.coverageFileName, run_options.paramFileName
        )
        scenario_id = self.insert( sql, params )

        # create or re-use existing 'run' record
        sql = '''
        INSERT INTO run (
            description, disease_id, started, num_sims, demography_name, person_email,
            source_bucket, source_data_path, destination_bucket, output_folder,
            read_pickle_file_suffix, save_pickle_file_suffix, burn_in_years,
            model_name, model_path, model_branch, model_commit
        )
        VALUES (
            %s, ( SELECT id FROM disease WHERE short = %s ), NOW(), %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s
        )
        ON CONFLICT ( description, disease_id )
        DO UPDATE
        SET description = EXCLUDED.description, disease_id = EXCLUDED.disease_id
        RETURNING id
        '''

        params = (
            run_options.runName, run_options.disease, run_options.numSims, run_info.demogName, run_options.personEmail,
            run_options.sourceBucket, run_options.sourceDataPath, run_options.destinationBucket, run_options.outputFolder,
            run_options.readPickleFileSuffix, run_options.savePickleFileSuffix, run_options.burnInTime,
            run_options.modelName, run_options.modelPath, run_options.modelBranch, run_options.modelCommit
        )
        run_id = self.insert( sql, params )

        # join run to scenario
        sql = '''
        INSERT INTO run_scenario ( run_id, scenario_id )
        VALUES ( %s, %s )
        ON CONFLICT( run_id, scenario_id )
        DO UPDATE
        SET run_id = EXCLUDED.run_id, scenario_id = EXCLUDED.scenario_id
        RETURNING run_id
        '''

        params = ( run_id, scenario_id )
        join_id = self.insert( sql, params, 'run_id' )

        # create new or re-use existing 'result' record
        sql = '''
        INSERT INTO result ( run_id, started, ended, iu_id, scenario_id, group_id )
        VALUES ( %s, %s, %s, ( SELECT id FROM iu WHERE code = %s ), %s, COALESCE( %s, 0 ) )
        ON CONFLICT( run_id, iu_id, scenario_id, group_id )
        DO UPDATE
        SET run_id = EXCLUDED.run_id, iu_id = EXCLUDED.iu_id, scenario_id = EXCLUDED.scenario_id, group_id = EXCLUDED.group_id
        RETURNING id
        '''

        params = (
            run_id, run_info.started, run_info.ended,
            run_info.iu_code, scenario_id, run_options.groupId
        )

        result_id = self.insert( sql, params )

        # create new 'result_file' record
        sql = '''
        INSERT INTO result_file( result_id, result_type, filename )
        VALUES( %s, %s, %s )
        ON CONFLICT( result_id, result_type )
        DO UPDATE
        SET result_id = EXCLUDED.result_id, result_type = EXCLUDED.result_type
        RETURNING id
        '''

        params = (
            result_id, institution, file_name
        )
        result_file_id = self.insert( sql, params )

        self.commit()

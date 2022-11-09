import sys
import base64

from db import db

def find_run( disease, base64_run_name ):
    DB = db()
    # TODO get output_folder from existing run
    # select convert_from( decode( encode( 'hello', 'base64' ), 'base64' ), 'utf-8' );
    sql = '''
        SELECT DISTINCT run.id, TO_CHAR( run.started, 'YYYY-Mon-DD/HH24:MI:SS' ) AS started, count(*) AS result_count
        FROM run, result
        WHERE run.id = result.run_id
        AND disease_id = ( SELECT id FROM disease WHERE short = %s )
        AND description = %s
        GROUP BY run.id
    '''
    params = ( disease, base64.b64decode( base64_run_name ).decode( 'UTF-8' ) )
    run_result = DB.fetchone( sql, params )
    if run_result is not None and len( run_result ) == 3:
        print( '='.join( [ str( run_result[ 'id' ] ), str( run_result[ 'result_count' ] ), str( run_result[ 'started' ] ) ]) )

if __name__ == "__main__":

    if len( sys.argv ) != 3 :
        sys.stderr.write( f"usage: {sys.argv[ 0 ]} <short-disease-code> <run-name>\n" )
        sys.exit( 1 )
    else:
        find_run( sys.argv[ 1 ], sys.argv[ 2 ] )

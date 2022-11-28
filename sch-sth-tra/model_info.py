import json
import os
import sys
import requests
import distutils.core
from functools import reduce
from urllib.parse import urlparse

from b64 import b64encode

def get_model_info( model_name ):
    repo_uri = repo_uri_for_model_name( model_name )
    path, branch = repo_path_and_branch_for_uri( repo_uri )
    commit = current_commit_for_path_and_branch( path, branch )
    json_info = json.dumps( {
        'path': path,
        'branch': branch,
        'commit': commit
    } )
    return b64encode( json_info )

def repo_uri_for_model_name( model_name ):
    setup = distutils.core.run_setup( "setup.py" )
    l = list( map( lambda x: x.split(), setup.install_requires ) )
    libs = [ x for x in l if x[ 0 ].endswith( '@' ) ]
    d = reduce( key0_to_val, libs, {} )
    return d[ model_name ]

# [ [ 'a@', 'b' ], [ 'c@', 'd' ] ] -> { 'a': 'b', 'c': 'd' }
def key0_to_val( a, b ):
    key0 = b[ 0 ].replace( '@', '' )
    a[ key0 ] = b[ 1 ]
    return a

def repo_path_and_branch_for_uri( repo_uri ):
    [ repo_path, branch ] = urlparse( repo_uri ).path.split('@')
    path = repo_path.split( '.' )[ 0 ]
    return path, branch

def current_commit_for_path_and_branch( path, branch ):
    # curl -s https://api.github.com/repos/NTD-Modelling-Consortium/ntd-model-sch/commits/Endgame_v2 | jq -r .sha
    api_url_root = "https://api.github.com/repos"
    api_url = f"{api_url_root}{path}/commits/{branch}"

    headers = {}
    api_token = os.getenv( 'GITHUB_API_TOKEN' )
    if api_token != None:
        headers[ 'Authorization' ] = f'Bearer {api_token}'

    r = requests.get( api_url, headers = headers )
    commit_sha = r.json()[ 'sha' ]
    return commit_sha

if __name__ == "__main__":

    if len( sys.argv ) != 2 :
        sys.stderr.write( f"usage: {sys.argv[ 0 ]} <model-name>\n" )
        sys.exit( 1 )

    model_name = sys.argv[ 1 ]

    try:
        print( get_model_info( model_name ) )
    except KeyError as k:
        sys.stderr.write( f"xx> unknown model name '{model_name}' {k}\n" )
        sys.exit( 1 )
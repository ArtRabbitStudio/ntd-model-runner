import json
import os
import sys
import requests
import distutils.core
from functools import reduce
from urllib.parse import urlparse

from b64 import b64encode

def get_model_info( model_name, encoded = True ):
    repo_uri = repo_uri_for_model_name( model_name )
    path, branch = repo_path_and_branch_for_uri( repo_uri )
    commit = current_commit_for_path_and_branch( path, branch )
    info = {
        'path': path,
        'branch': branch,
        'commit': commit
    }

    if encoded == True:
        json_info = json.dumps( info )
        return b64encode( json_info )

    return info

def repo_uri_for_model_name( model_name ):
    if model_name == 'lf':
        return 'git+https://github.com/NTD-Modelling-Consortium/LF.git@main'

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

    headers = { 'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15" }
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
        sys.stderr.write( f"xx> model_info found unknown model name '{model_name}': {k}\n" )
        sys.exit( 1 )

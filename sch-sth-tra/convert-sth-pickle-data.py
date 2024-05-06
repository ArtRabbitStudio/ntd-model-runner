import sys
import pickle
from dotted_dict import DottedDict
from gcs import gcs

data_bucket = "ntd-disease-simulator-data"
GCS = gcs( data_bucket )

iu = sys.argv[ 1 ]
iu_path = f"{iu[:3]}/{iu}/Hook_{iu}.p"

source_path = "diseases/sth-hookworm/source-data-uploaded-20220712"
pickle_data = pickle.loads( GCS.get_blob( f"{source_path}/{iu_path}" ) )

row_list = []
for i in range( len( pickle_data ) ):
    row = DottedDict( **( pickle_data[ i ] ) )
    row_list.append( row )

print( f"=> converted {len( row_list )} entries" )

output_path = "diseases/sth-hookworm/source-data-converted-20240505"
GCS.write_string_to_file( pickle.dumps( row_list, protocol=pickle.HIGHEST_PROTOCOL ), f"{output_path}/{iu_path}" )
print( "=> done." )

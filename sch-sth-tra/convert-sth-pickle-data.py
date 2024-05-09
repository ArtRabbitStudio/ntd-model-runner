import sys
import pickle
from dotted_dict import DottedDict
from gcs import gcs

data_bucket = "ntd-disease-simulator-data"
GCS = gcs( data_bucket )

lowercase_to_short_uppercase_disease_names = {
    'hookworm': 'Hook',
    'whipworm': 'Tri',
    'roundworm': 'Asc'
}

disease = sys.argv[ 1 ]
short_disease = lowercase_to_short_uppercase_disease_names[ disease ]
iu = sys.argv[ 2 ]
iu_path = f"{iu[:3]}/{iu}/{short_disease}_{iu}.p"
source_data_dir = sys.argv[ 3 ]
converted_data_dir= sys.argv[ 4 ]

source_path = f"diseases/sth-{disease}/{source_data_dir}"
pickle_data = pickle.loads( GCS.get_blob( f"{source_path}/{iu_path}" ) )

row_list = []
for i in range( len( pickle_data ) ):
    row = DottedDict( **( pickle_data[ i ] ) )
    row_list.append( row )

print( f"=> converted {len( row_list )} entries" )

output_path = f"diseases/sth-{disease}/{converted_data_dir}"
GCS.write_string_to_file( pickle.dumps( row_list, protocol=pickle.HIGHEST_PROTOCOL ), f"{output_path}/{iu_path}" )
print( "=> done." )

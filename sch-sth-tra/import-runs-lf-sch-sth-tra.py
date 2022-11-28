import os, sys, json

diseases = [ 'sch-mansoni', 'sth-hookworm', 'sth-roundworm', 'sth-whipworm', 'trachoma', 'lf' ]

# ['gs:', '', 'ntd-endgame-result-data', 'ntd', '202206', 'sth-hookworm', 'scenario_3', 'group_170', 'BDI06375', 'ipm-BDI06375-hookworm-group_170-scenario_3-group_170-200_simulations.csv.bz2\n']

file_disease_index = 5

file_mapping = {
    'lf': {
        'destination_bucket': 2,
        'description': 4,
        'disease': 5,
        'scenario': 6,
        'iu': 7,
        'file': 8
    },
    'trachoma': {
        'destination_bucket': 2,
        'description': 4,
        'disease': 5,
        'scenario': 6,
        'country': 7,
        'iu': 8,
        'file': 9
    },
    'default': {
        'destination_bucket': 2,
        'description': 4,
        'disease': 5,
        'scenario': 6,
        'group': 7,
        'iu': 8,
        'file': 9
    }
}

# ['ipm', 'RWA38118', 'hookworm', 'group_165', 'scenario_3', 'group_165', '200_simulations']

line_mapping = {
    'default': {
        'institution': 0,
        'simulations': 6
    },
    'lf': {
        'institution': 0,
        'simulations': 4
    },
    'trachoma': {
        'institution': 0,
        'simulations': 4
    }
}

for d in diseases:
    for m in [ file_mapping, line_mapping ]:
        if not d in m:
            m[ d ] = m[ 'default' ]

file = open( sys.argv[1], 'r' )
while True:

    line = file.readline()

    if not line:
        break

    result = {}

    entries = line.split('\n')[0].split( '/' )
    disease_name = entries[ file_disease_index ] 

    disease_entry_mapping = file_mapping[ disease_name ]
    disease_filename_mapping = line_mapping[ entries[ file_disease_index ] ]

    for key in disease_entry_mapping:
        if key != 'file':
            result[ key ] = entries[ disease_entry_mapping[ key ] ].split('_')[0]
        else:
            result[ key ] = '/'.join(entries)

    file_info = entries[ disease_entry_mapping['file'] ].split('.')[0].split('-')

    for key in disease_filename_mapping:
        result[ key ] = file_info[ disease_filename_mapping[ key ] ].split('_')[0]

    print( json.dumps(result) )

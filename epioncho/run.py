'''
taken from https://github.com/dreamingspires/EPIONCHO-IBM/blob/master/examples/final_run_state_and_scenario.py
'''

import os
import sys
import h5py
import time

from epioncho_ibm.endgame_simulation import EndgameSimulation
from epioncho_ibm.state.params import EpionchoEndgameModel
from epioncho_ibm.tools import Data, add_state_to_run_data, write_data_to_csv

def run_simulations( IU, hdf5_file, scenario_file, output_file, n_sims, inclusive, prevalence_OAE ):

	group_names = [f"draw_{i}" for i in range(n_sims)]

	new_file = h5py.File(hdf5_file, "r")
	sims = []
	output_data: list[Data] = []
	init_time = time.perf_counter()
	for group_name in group_names:
		start_time = time.perf_counter()
		restored_grp = new_file[group_name]
		assert isinstance(restored_grp, h5py.Group)
		sim = EndgameSimulation.restore(restored_grp)
		new_endgame_model = EpionchoEndgameModel.parse_file(scenario_file)

		current_params = sim.simulation.get_current_params()
		sim.reset_endgame(new_endgame_model)
		new_params = sim.simulation.get_current_params()

		# Save out attributes to keep
		new_params.blackfly.bite_rate_per_person_per_year = (
			current_params.blackfly.bite_rate_per_person_per_year
		)
		new_params.gamma_distribution = current_params.gamma_distribution
		new_params.seed = current_params.seed

		sim.simulation.reset_current_params(new_params)

		run_data: Data = {}
		for state in sim.iter_run( end_time = 2040, sampling_interval = 1, inclusive = inclusive ):
			add_state_to_run_data(
				state,
				run_data=run_data,
				number=True,
				n_treatments=False,
				achieved_coverage=False,
				with_age_groups=True,
				prevalence=True,
				mean_worm_burden=False,
				prevalence_OAE=prevalence_OAE,
				intensity=True,
				with_sequela=True
			)
		output_data.append(run_data)
		end_time = time.perf_counter()
		run_time = end_time - start_time
		print( f"{os.getpid()} {IU} | [ python: {group_name}: {run_time:.4f} secs ]" )

	total_time = time.perf_counter() - init_time
	print( f"{os.getpid()} {IU} | [ python: total time: {total_time:.4f} secs ]" )

	write_data_to_csv( output_data, output_file )

"""
expects to be called e.g.:

python run.py \
	OutputVals_AGO02053.hdf5 \
	/Users/igor/Work/ntd/ntd-model-runner/epioncho/scenarios/scenario3c.json \
	/Users/igor/whatever/ihme-AGO02053-scenario_3c-10.csv \
	10 \
	true \
	true

python run.py \
	OutputVals_AGO02049.hdf5 \
	/Users/igor/Work/ntd/ntd-model-runner/epioncho/scenarios/scenario1.json \
	/Users/igor/whatever/ihme-AGO02049-scenario_1-200.csv \
	200 \
	true \
	true

- .hdf5 files fetched from GCS by caller
- scenario file included in runner but potentially specified elsewhere in caller
- absolute output file path specified by caller
- number of simulations specified by caller
- whether to call model with 'inclusive' to include all years
- whether to ask model to provide prevalence_OAE

"""

if __name__ == '__main__':

	hdf5_file = sys.argv[ 1 ]
	IU = hdf5_file[:-5].split('_')[-1] # OutputVals_AGO02053.hdf5 => AGO02053
	scenario_file = sys.argv[ 2 ]
	output_file = sys.argv[ 3 ]
	n_sims = sys.argv[ 4 ]
	inclusive = sys.argv[ 5 ].lower() == 'true' if len( sys.argv ) >= 6 else False
	prevalence_OAE = sys.argv[ 6 ].lower() == 'true' if len( sys.argv ) >= 7 else False

	run_simulations( IU, hdf5_file, scenario_file, output_file, int(n_sims), inclusive, prevalence_OAE )

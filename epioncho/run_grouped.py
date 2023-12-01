'''
taken from https://github.com/dreamingspires/EPIONCHO-IBM/blob/master/examples/final_run_state_and_scenario.py
age-grouping from https://github.com/adiramani/P_EPIONCHO-IBM/blob/master/examples/test_prob_elim.py
'''

import os
import sys
import h5py
import time

import numpy as np
import pandas as pd

from collections import defaultdict

from epioncho_ibm.endgame_simulation import EndgameSimulation
from epioncho_ibm.state.params import EpionchoEndgameModel
from epioncho_ibm.tools import Data, add_state_to_run_data, write_data_to_csv

def run_simulations( IU, hdf5_file, scenario_file, output_file_root, n_sims, inclusive, prevalence_OAE ):

	ov = hdf5_file.split('/')[-1]
	sf = scenario_file.split('/')[-1]
	ofr = output_file_root.split('/')[-1]

	print( f"{os.getpid()} {IU} | [ python: running {n_sims} simulations for IU {IU} on {ov} {sf} outputting to {ofr} ]" )

	new_file = h5py.File(hdf5_file, "r")
	sims = []

	simulation_stop = 2050

	# read in scenario file
	new_endgame_model = EpionchoEndgameModel.parse_file(scenario_file)

	last_program = new_endgame_model.programs[ -1 ]
	mda_start = last_program.first_year
	mda_stop = last_program.last_year
	interval = int( last_program.interventions.treatment_interval )

	# data stores
	age_grouped_output_data: list[Data] = []
	all_age_output_data: list[Data] = []

	init_time = time.perf_counter()

	# run sims draw_0 .. draw_199 (e.g.)
	group_names = [f"draw_{i}" for i in range(n_sims)]

	for group_name in group_names:

		start_time = time.perf_counter()
		restored_grp = new_file[group_name]
		assert isinstance(restored_grp, h5py.Group)
		sim = EndgameSimulation.restore(restored_grp)

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

		age_grouped_run_data: Data = {}
		all_age_run_data: Data = {}

#		for state in sim.iter_run(
#			end_time = simulation_stop,
#			sampling_years=[ i for i in range( mda_start, simulation_stop ) ],
#			inclusive = inclusive
#		):

		for state in sim.iter_run(
			end_time = simulation_stop,
			sampling_interval = 1,
			inclusive = inclusive
		):

			add_state_to_run_data(
				state,
				run_data=age_grouped_run_data,
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

			add_state_to_run_data(
				state,
				run_data=all_age_run_data,
				number=True,
				n_treatments=False,
				achieved_coverage=False,
				with_age_groups=False,
				prevalence=True,
				mean_worm_burden=False,
				prevalence_OAE=prevalence_OAE,
				intensity=True,
				with_sequela=True
			)

		age_grouped_output_data.append( age_grouped_run_data )
		all_age_output_data.append( all_age_run_data )

		end_time = time.perf_counter()
		run_time = end_time - start_time
		print( f"{os.getpid()} {IU} | [ python: {group_name}: {run_time:.4f} secs ]" )

	total_time = time.perf_counter() - init_time
	print( f"{os.getpid()} {IU} | [ python: total time: {total_time:.4f} secs ]" )

	mda_file_suffix = f"mda_stop_{mda_stop}"

	write_data_to_csv( age_grouped_output_data, f"{output_file_root}-{mda_file_suffix}-age_grouped_raw_data.csv" )
	write_data_to_csv( all_age_output_data, f"{output_file_root}-{mda_file_suffix}-raw_all_age_data.csv" )

	calculate_probability_elimination(
		all_age_output_data,
		IU,
		scenario_file[:-5].split('/')[-1],
		mda_start,
		mda_stop,
		interval,
		f"{output_file_root}-{mda_file_suffix}-all_age_data.csv"
	)


def calculate_probability_elimination(
	data: list[Data],
	iuName: str,
	scenario: str,
	mda_start_year: int | None,
	mda_stop_year: int,
	mda_interval: int,
	csv_file: str,
) -> None:

#	print( f"[ python: calculating probability elimination: {data} {iuName} {scenario} {mda_start_year} {mda_stop_year} {mda_interval} {csv_file} ]" )
	# Arranging data into an easy to manipulate format (taken from tools.py)
	data_combined_runs: dict[
		tuple[float, float, float, str], list[float | int]
	] = defaultdict(list)
	for run in data:
		for k, v in run.items():
			data_combined_runs[k].append(v)

	rows = sorted(
		(k + tuple(v) for k, v in data_combined_runs.items()),
		key=lambda r: (r[0], r[3], r[1]),
	)

	tmp = np.array(rows)
	# Data manipulation
	if mda_start_year:
		yrs_after_mda_start_mask = tmp[:, 0].astype(float) >= mda_start_year
		tmp = tmp[yrs_after_mda_start_mask, :]

	# Calculating probability of elimination using mf_prev
	mf_prev_mask = tmp[:, 3] == "prevalence"
	mf_prev_vals = tmp[mf_prev_mask, 4:].astype(float)

	# Calculating the year where each run has < 1% prev
	mf_under_1_mask = mf_prev_vals <= 0.01
	under_1_prev_indices = np.argmax(mf_under_1_mask, axis=0)
	yearOfUnder1Prev = np.array(
		[
			float(tmp[mf_prev_mask][under_1_prev_indices[i], 0])
			if any(mf_under_1_mask[:, i])
			else np.nan
			for i in range(mf_under_1_mask.shape[1])
		]
	)

	# Probability of elimination for a given year = the average number of runs that reach 0 mf prev
	prob_elim = np.mean(mf_prev_vals == 0, axis=1)
	num_prob_elim = np.sum(mf_prev_mask)
	none_array = np.full(num_prob_elim, None)
	# combining results into a matrix format for output
	prob_elim_output = np.column_stack(
		(
			tmp[mf_prev_mask, :3],
			np.full(num_prob_elim, "prob_elim"),
			prob_elim,
			none_array,
			none_array,
			none_array,
		)
	)

	# Probability of getting < 1% mfp for a given year
	prob_under_1_mfp = np.mean(mf_under_1_mask, axis=1)
	under_1_prev_90_index = np.argmax(prob_under_1_mfp >= 0.90, axis=0)
	over_90_prob_elim_index = np.argmax(prob_elim >= 0.90, axis=0)

	prob_under_1_mfp_output = np.column_stack(
		(
			tmp[mf_prev_mask, :3],
			np.full(num_prob_elim, "prob_under_1_mfp"),
			prob_under_1_mfp,
			none_array,
			none_array,
			none_array,
		)
	)

	# Find the year where 90% of the runs have <1% mfp or have reached elimination completely
	yearOf90ProbElim = (
		tmp[mf_prev_mask, 0][under_1_prev_90_index]
		if np.any(prob_under_1_mfp >= 0.90)
		else ""
	)
	yearOf90Under1Prev = (
		tmp[mf_prev_mask, 0][over_90_prob_elim_index]
		if np.any(prob_elim >= 0.90)
		else ""
	)

	# Summarizing all other prevalence outputs (filtering to only mfp)
	other_prevs = tmp[mf_prev_mask, 4:].astype(float)
	other_prevs_output = np.column_stack(
		(
			tmp[mf_prev_mask, :4],
			np.mean(other_prevs, axis=1),
			np.percentile(other_prevs, 2.5, axis=1),
			np.percentile(other_prevs, 97.5, axis=1),
			np.median(other_prevs, axis=1),
		)
	)
	output = np.row_stack(
		(
			other_prevs_output,
			# probability of elim for each year
			prob_elim_output,
			# probability of 1% mf_prev for each year
			prob_under_1_mfp_output,
			# year of >=90% elim
			np.array(
				[
					"",
					np.nan,
					np.nan,
					"years_to_90_under_1_mfp",
					yearOf90ProbElim,
					None,
					None,
					None,
				]
			),
			# year of >=90% under 1% prev
			np.array(
				[
					"",
					np.nan,
					np.nan,
					"years_to_90_prob_elim",
					yearOf90Under1Prev,
					None,
					None,
					None,
				]
			),
			# avg year of <=1% mf prev
			np.array(
				[
					"",
					np.nan,
					np.nan,
					"years_to_1_mfp",
					np.nanmean(yearOfUnder1Prev),
					np.percentile(yearOfUnder1Prev, 2.5),
					np.percentile(yearOfUnder1Prev, 97.5),
					np.median(yearOfUnder1Prev),
				]
			),
			# all years to <1% mfp
			np.array(
				[
					"",
					np.nan,
					np.nan,
					"years_to_1_mfp_all_runs",
					",".join(yearOfUnder1Prev.astype(str)),
					None,
					None,
					None,
				]
			),
		)
	)

	descriptor_output = np.column_stack(
		(
			np.full(output.shape[0], iuName),
			np.full(output.shape[0], scenario),
			np.full(output.shape[0], mda_start_year),
			np.full(output.shape[0], mda_stop_year),
			np.full(output.shape[0], mda_interval),
			output,
		)
	)

	pd.DataFrame(
		descriptor_output,
		columns=[
			"iu_name",
			"scenario",
			"mda_start_year",
			"mda_stop_year",
			"mda_interval",
			"year_id",
			"age_start",
			"age_end",
			"measure",
			"mean",
			"lower_bound",
			"upper_bound",
			"median",
		],
	).to_csv(csv_file)

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
- absolute output file path root specified by caller
- number of simulations specified by caller
- whether to call model with 'inclusive' to include all years
- whether to ask model to provide prevalence_OAE

"""

if __name__ == '__main__':

	hdf5_file = sys.argv[ 1 ]
	IU = hdf5_file[:-5].split('_')[-1] # OutputVals_AGO02053.hdf5 => AGO02053
	scenario_file = sys.argv[ 2 ]
	output_file_root = sys.argv[ 3 ][:-4] # remove the ".csv"
	n_sims = sys.argv[ 4 ]
	inclusive = sys.argv[ 5 ].lower() == 'true' if len( sys.argv ) >= 6 else False
	prevalence_OAE = sys.argv[ 6 ].lower() == 'true' if len( sys.argv ) >= 7 else False

	run_simulations( IU, hdf5_file, scenario_file, output_file_root, int(n_sims), inclusive, prevalence_OAE )

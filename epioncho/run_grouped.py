import os
import csv
import sys
import glob
import h5py
import time

import numpy as np
import pandas as pd

from collections import defaultdict

from epioncho_ibm.endgame_simulation import EndgameSimulation
from epioncho_ibm.state.params import EpionchoEndgameModel
from epioncho_ibm.tools import (
	Data,
	add_state_to_run_data,
	write_data_to_csv,
	convert_data_to_pandas,
)
from endgame_postprocessing.model_wrappers import constants as modelwrapper_constants
from endgame_postprocessing.post_processing import measures, single_file_post_processing
from endgame_postprocessing.post_processing.aggregation import (
	aggregate_post_processed_files,
	iu_lvl_aggregate,
	country_lvl_aggregate,
	africa_lvl_aggregate,
)


def run_simulations(
	IU,
	hdf5_file,
	scenario_file,
	output_file_root,
	n_sims,
	inclusive,
	prevalence_OAE,
	sampling_interval,
):

	ov = hdf5_file.split("/")[-1]
	sf = scenario_file.split("/")[-1]
	ofr = output_file_root.split("/")[-1]

	print(
		f"{os.getpid()} {IU} | [ python: running {n_sims} simulations for IU {IU} on {ov} {sf} outputting to {ofr} ]"
	)

	new_file = h5py.File(hdf5_file, "r")
	sims = []

	simulation_stop = 2042

	# read in scenario file
	new_endgame_model = EpionchoEndgameModel.parse_file(scenario_file)

	if len(new_endgame_model.programs) > 0:

		last_program = new_endgame_model.programs[-1]
		mda_start = last_program.first_year
		mda_stop = last_program.last_year
		interval = float(last_program.interventions.treatment_interval)

	# zero-intervention scenarios
	else:

		mda_start = 2026
		mda_stop = 2040
		interval = 1.0

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

		new_endgame_model.parameters.initial.blackfly.bite_rate_per_person_per_year = (
			current_params.blackfly.bite_rate_per_person_per_year
		)
		new_endgame_model.parameters.initial.gamma_distribution = (
			current_params.gamma_distribution
		)
		new_endgame_model.parameters.initial.seed = current_params.seed

		# sim.simulation.state.current_time = 2026
		sim.reset_endgame(new_endgame_model)

		age_grouped_run_data: Data = {}
		all_age_run_data: Data = {}

		for state in sim.iter_run(
			end_time=simulation_stop,
			sampling_interval=sampling_interval,
			inclusive=inclusive,
			# make_time_backwards_compatible = True
		):

			add_state_to_run_data(
				state,
				run_data=age_grouped_run_data,
				number=True,
				n_treatments=True,
				achieved_coverage=True,
				with_age_groups=True,
				prevalence=True,
				mean_worm_burden=True,
				prevalence_OAE=True,
				intensity=True,
				with_sequela=True,
				with_pnc=True,
				saving_multiple_states=True,
			)

			add_state_to_run_data(
				state,
				run_data=all_age_run_data,
				number=True,
				n_treatments=True,
				achieved_coverage=True,
				with_age_groups=False,
				prevalence=True,
				mean_worm_burden=True,
				prevalence_OAE=True,
				intensity=True,
				with_sequela=True,
				with_pnc=True,
				saving_multiple_states=False,
			)

		age_grouped_output_data.append(age_grouped_run_data)
		all_age_output_data.append(all_age_run_data)

		end_time = time.perf_counter()
		run_time = end_time - start_time
		print(f"{os.getpid()} {IU} | [ python: {group_name}: {run_time:.4f} secs ]")

	total_time = time.perf_counter() - init_time
	print(f"{os.getpid()} {IU} | [ python: total time: {total_time:.4f} secs ]")

	mda_file_suffix = f"mda_stop_{mda_stop}"
	sampling_interval_suffix = f"sampling_interval_{sampling_interval}"

	write_data_to_csv(
		age_grouped_output_data,
		f"{output_file_root}-{mda_file_suffix}-{sampling_interval_suffix}-age_grouped_raw_data.csv",
	)
	write_data_to_csv(
		all_age_output_data,
		f"{output_file_root}-{mda_file_suffix}-{sampling_interval_suffix}-raw_all_age_data.csv",
	)
	all_age_output_data_pandas = convert_data_to_pandas(all_age_output_data)
	single_file_post_processing.process_single_file(
		raw_model_outputs=all_age_output_data_pandas,
		scenario=scenario_file[:-5].split("/")[-1],
		iuName=IU,
		prevalence_marker_name="prevalence",
		num_draws=n_sims,
		post_processing_start_time=2025,
		measure_summary_map={
			measure: measures.measure_summary_float
			for measure in modelwrapper_constants.ONCHO_MEASURES
		},
	).to_csv(
		f"{output_file_root}-all_age_data.csv"
	)


# Combines all data files in a folder and aggregates as necessary.
def combineAndFilter(
	path_to_output_files=".", specific_files="*-all_age_data.csv", output_file_root="."
):

	print(
		f"[ python: combineAndFilter path: {path_to_output_files} ]"
	)

	combined_iu_df = iu_lvl_aggregate(
		aggregate_post_processed_files(
			path_to_files=path_to_output_files,
			specific_files=specific_files
		)
	)
	combined_iu_df.to_csv(f"{output_file_root}_combined-oncho-iu-lvl-agg.csv")
	country_lvl_data = country_lvl_aggregate(
		iu_lvl_data=combined_iu_df,
		general_summary_measure_names=modelwrapper_constants.COUNTRY_SUMMARY_COLUMNS,
		general_groupby_cols=modelwrapper_constants.COUNTRY_SUMMARY_GROUP_COLUMNS,
		threshold_summary_measure_names=modelwrapper_constants.COUNTRY_THRESHOLD_SUMMARY_COLUMNS,
		threshold_groupby_cols=modelwrapper_constants.COUNTRY_THRESHOLD_SUMMARY_GROUP_COLUMNS,
		threshold_cols_rename=modelwrapper_constants.COUNTRY_THRESHOLD_RENAME_MAP,
	)
	country_lvl_data.to_csv(
		f"{output_file_root}_combined-oncho-country-lvl-agg.csv"
	)
	africa_lvl_aggregate(
		country_lvl_data=country_lvl_data,
		measures_to_summarize=modelwrapper_constants.AFRICA_SUMMARY_MEASURES,
		columns_to_group_by=modelwrapper_constants.AFRICA_LVL_GROUP_COLUMNS,
	).to_csv(f"{output_file_root}_combined-oncho-africa-lvl-agg.csv")



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

if __name__ == "__main__":

	hdf5_file = sys.argv[1]
	IU = hdf5_file[:-5].split("_")[-1]  # OutputVals_AGO02053.hdf5 => AGO02053
	scenario_file = sys.argv[2]
	output_file_root = sys.argv[3][:-4]  # remove the ".csv"
	n_sims = sys.argv[4]
	inclusive = sys.argv[5].lower() == "true" if len(sys.argv) >= 6 else False
	prevalence_OAE = sys.argv[6].lower() == "true" if len(sys.argv) >= 7 else False
	sampling_interval = float(sys.argv[7]) if len(sys.argv) >= 8 else False

	run_simulations(
		IU,
		hdf5_file,
		scenario_file,
		output_file_root,
		int(n_sims),
		inclusive,
		prevalence_OAE,
		sampling_interval,
	)

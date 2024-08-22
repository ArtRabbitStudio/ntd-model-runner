import sys
from endgame_postprocessing.model_wrappers import constants as modelwrapper_constants
from endgame_postprocessing.post_processing.aggregation import (
    aggregate_post_processed_files,
    iu_lvl_aggregate,
    country_lvl_aggregate,
    africa_lvl_aggregate,
)

# Combines all data files in a folder and aggregates as necessary.
def combineAndFilter(
    path_to_output_files=".", specific_files="*-all_age_data.csv", output_file_root="."
):

    print(
        f"python: combineAndFilter path: {path_to_output_files} ]"
    )

    combined_iu_df = iu_lvl_aggregate(
        aggregate_post_processed_files(
            path_to_files=path_to_output_files,
            specific_files=specific_files
        )
    )
    combined_iu_df.to_csv(f"{output_file_root}combined-oncho-iu-lvl-agg.csv")
    country_lvl_data = country_lvl_aggregate(
        iu_lvl_data=combined_iu_df,
        general_summary_measure_names=modelwrapper_constants.COUNTRY_SUMMARY_COLUMNS,
        general_groupby_cols=modelwrapper_constants.COUNTRY_SUMMARY_GROUP_COLUMNS,
        threshold_summary_measure_names=modelwrapper_constants.COUNTRY_THRESHOLD_SUMMARY_COLUMNS,
        threshold_groupby_cols=modelwrapper_constants.COUNTRY_THRESHOLD_SUMMARY_GROUP_COLUMNS,
        threshold_cols_rename=modelwrapper_constants.COUNTRY_THRESHOLD_RENAME_MAP,
    )
    country_lvl_data.to_csv(
        f"{output_file_root}combined-oncho-country-lvl-agg.csv"
    )
    africa_lvl_aggregate(
        country_lvl_data=country_lvl_data,
        measures_to_summarize=modelwrapper_constants.AFRICA_SUMMARY_MEASURES,
        columns_to_group_by=modelwrapper_constants.AFRICA_LVL_GROUP_COLUMNS,
    ).to_csv(f"{output_file_root}combined-oncho-africa-lvl-agg.csv")


if __name__ == "__main__":
    combineAndFilter(path_to_output_files=sys.argv[1], output_file_root=sys.argv[2])

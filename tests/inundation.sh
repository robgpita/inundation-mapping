./projects/foss_fim/lib/inundation.py -r data/foss_fim/test3/outputs/rem_clipped_zeroed_masked.tif -c data/foss_fim/test3/outputs/gw_catchments_reaches_clipped_addedAttributes.tif -f data/foss_fim/validation/forecast_120903_100yr.csv -s data/foss_fim/test3/outputs/src.json -w data/foss_fim/test3/outputs/crosswalk_table.csv -i data/foss_fim/validation/inundation_results/inundation_120903_100yr_v1.tif -p data/foss_fim/validation/inundation_results/inundation_120903_100yr_v1.gpkg -d data/foss_fim/validation/inundation_results/depths_120903_100yr_v1.tif -g data/foss_fim/validation/inundation_results/stages_120903_100yr_v1.tif

./projects/inundationValidation/vectorValidation.py -p data/validation/inundation_results/inundation_120903_100yr_v1.gpkg -v data/validation/validation_fim_120903_100yr_simplified.gpkg -a data/validation/analysisExtents_120903.gpkg -e data/validation/water_bodies_120903.gpkg -t 12500 -l 0 -m data/validation/scoring_results/contingency_map_120903_100yr_v1.gpkg -s data/validation/scoring_results/contingency_stats_120903_100yr_v1.txt

chgrp -R fim data/validation/

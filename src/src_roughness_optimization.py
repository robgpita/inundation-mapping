import argparse
import geopandas as gpd
from geopandas.tools import sjoin
import os
import rasterio
import pandas as pd
import numpy as np
import sys
import json
import datetime as dt
from collections import deque
import multiprocessing
from multiprocessing import Pool

from utils.shared_variables import DOWNSTREAM_THRESHOLD, ROUGHNESS_MIN_THRESH, ROUGHNESS_MAX_THRESH


def update_rating_curve(fim_directory, water_edge_median_df, htable_path, huc, branch_id, catchments_poly_path, debug_outputs_option, source_tag, merge_prev_adj=False, down_dist_thresh=DOWNSTREAM_THRESHOLD):
    '''
    This script ingests a dataframe containing observed data (HAND elevation and flow) and then calculates new SRC roughness values via Manning's equation. The new roughness values are averaged for each HydroID and then progated downstream and a new discharge value is calculated where applicable.

    Processing Steps:
    - Read in the hydroTable.csv and check whether it has previously been updated (rename default columns if needed)
    - Loop through the user provided point data --> stage/flow dataframe row by row and copy the corresponding htable values for the matching stage->HAND lookup
    - Calculate new HydroID roughness values for input obs data using Manning's equation
    - Create dataframe to check for erroneous Manning's n values (values set in tools_shared_variables.py: >0.6 or <0.001 --> see input args)
    - Create magnitude and ahps column by subsetting the "layer" attribute
    - Create df grouped by hydroid with ahps_lid and huc number and then pivot the magnitude column to display n value for each magnitude at each hydroid
    - Create df with the most recent collection time entry and submitter attribs
    - Cacluate median ManningN to handle cases with multiple hydroid entries and create a df with the median hydroid_ManningN value per feature_id
    - Rename the original hydrotable variables to allow new calculations to use the primary var name 
    - Check for large variabilty in the calculated Manning's N values (for cases with mutliple entries for a singel hydroid)
    - Create attributes to traverse the flow network between HydroIDs
    - Calculate group_ManningN (mean calb n for consective hydroids) and apply values downsteam to non-calb hydroids (constrained to first Xkm of hydroids - set downstream diststance var as input arg)
    - Create the adjust_ManningN column by combining the hydroid_ManningN with the featid_ManningN (use feature_id value if the hydroid is in a feature_id that contains valid hydroid_ManningN value(s))
    - Merge in previous SRC adjustments (where available) for hydroIDs that do not have a new adjusted roughness value
    - Update the catchments polygon .gpkg with joined attribute - "src_calibrated"
    - Merge the final ManningN dataframe to the original hydroTable
    - Create the ManningN column by combining the hydroid_ManningN with the default_ManningN (use modified where available)
    - Calculate new discharge_cms with new adjusted ManningN
    - Export a new hydroTable.csv and overwrite the previous version and output new src json (overwrite previous)

    Inputs:
    - fim_directory:        fim directory containing individual HUC output dirs
    - water_edge_median_df: dataframe containing observation data (attributes: "hydroid", "flow", "submitter", "coll_time", "flow_unit", "layer", "HAND")
    - htable_path:          path to the current HUC hydroTable.csv
    - huc:                  string variable for the HUC id # (huc8 or huc6)
    - branch_id:            string variable for the branch id
    - catchments_poly_path: path to the current HUC catchments polygon layer .gpkg 
    - debug_outputs_option: optional input argument to output additional intermediate data files (csv files with SRC calculations)
    - source_tag:           input text tag used to specify the type/source of the input obs data used for the SRC adjustments (e.g. usgs_rating or point_obs)
    - merge_prev_adj:       boolean argument to specify when to merge previous SRC adjustments vs. overwrite (default=False)
    - down_dist_thresh:     optional input argument to override the env variable that controls the downstream distance new roughness values are applied downstream of locations with valid obs data

    Ouputs:
    - output_catchments:    same input "catchments_poly_path" .gpkg with appened attributes for SRC adjustments fields
    - df_htable:            same input "htable_path" --> updated hydroTable.csv with new/modified attributes
    - output_src_json:      src.json file with new SRC discharge values

    '''
    #print("Processing huc --> " + str(huc))
    log_text = "\nProcessing huc --> " + str(huc) + '  branch id: ' + str(branch_id) + '\n'
    log_text += "DOWNSTREAM_THRESHOLD: " + str(down_dist_thresh) + 'km\n'
    log_text += "Merge Previous Adj Values: " + str(merge_prev_adj) + '\n'
    df_nvalues = water_edge_median_df.copy()
    df_nvalues = df_nvalues[ (df_nvalues.hydroid.notnull()) & (df_nvalues.hydroid > 0) ] # remove null entries that do not have a valid hydroid

    ## Read in the hydroTable.csv and check wether it has previously been updated (rename default columns if needed)
    df_htable = pd.read_csv(htable_path, dtype={'HUC': object, 'last_updated':object, 'submitter':object, 'obs_source':object})
    df_prev_adj = pd.DataFrame() # initialize empty df for populating/checking later
    if 'default_discharge_cms' not in df_htable.columns: # need this column to exist before continuing
        df_htable['adjust_src_on'] = False
        df_htable['last_updated'] = pd.NA
        df_htable['submitter'] = pd.NA
        df_htable['adjust_ManningN'] = pd.NA
        df_htable['obs_source'] = pd.NA
        df_htable['default_discharge_cms'] = pd.NA
        df_htable['default_ManningN'] = pd.NA
    if df_htable['default_discharge_cms'].isnull().values.any(): # check if there are not valid values in the column (True = no previous calibration outputs)
        df_htable['default_discharge_cms'] = df_htable['discharge_cms'].values
        df_htable['default_ManningN'] = df_htable['ManningN'].values

    if merge_prev_adj and not df_htable['adjust_ManningN'].isnull().all(): # check if the merge_prev_adj setting is True and there are valid 'adjust_ManningN' values from previous calibration outputs
        # Create a subset of hydrotable with previous adjusted SRC attributes
        df_prev_adj_htable = df_htable.copy()[['HydroID','submitter','last_updated','adjust_ManningN','obs_source']] 
        df_prev_adj_htable.rename(columns={'submitter':'submitter_prev','last_updated':'last_updated_prev','adjust_ManningN':'adjust_ManningN_prev','obs_source':'obs_source_prev'}, inplace=True)
        df_prev_adj_htable = df_prev_adj_htable.groupby(["HydroID"]).first()
        # Only keep previous USGS rating curve adjustments (previous spatial obs adjustments are not retained)
        df_prev_adj = df_prev_adj_htable[df_prev_adj_htable['obs_source_prev'].str.contains("usgs_rating", na=False)] 
        log_text += 'HUC: ' + str(huc) + '  Branch: ' + str(branch_id) + ': found previous hydroTable calibration attributes --> retaining previous calb attributes for blending...\n'
    # Delete previous adj columns to prevent duplicate variable issues (if src_roughness_optimization.py was previously applied)
    df_htable.drop(['ManningN','discharge_cms','submitter','last_updated','adjust_ManningN','adjust_src_on','obs_source'], axis=1, inplace=True) 
    df_htable.rename(columns={'default_discharge_cms':'discharge_cms','default_ManningN':'ManningN'}, inplace=True)

    ## loop through the user provided point data --> stage/flow dataframe row by row
    for index, row in df_nvalues.iterrows():
        if row.hydroid not in df_htable['HydroID'].values:
            print('ERROR: HydroID for calb point was not found in the hydrotable (check hydrotable) for HUC: ' + str(huc) + '  branch id: ' + str(branch_id) + ' hydroid: ' + str(row.hydroid))
            log_text += 'ERROR: HydroID for calb point was not found in the hydrotable (check hydrotable) for HUC: ' + str(huc) + '  branch id: ' + str(branch_id) + ' hydroid: ' + str(row.hydroid) + '\n'
        else:
            df_htable_hydroid = df_htable[df_htable.HydroID == row.hydroid] # filter htable for entries with matching hydroid
            if df_htable_hydroid.empty:
                print('ERROR: df_htable_hydroid is empty but expected data: ' + str(huc) + '  branch id: ' + str(branch_id) + ' hydroid: ' + str(row.hydroid))
                log_text += 'ERROR: df_htable_hydroid is empty but expected data: ' + str(huc) + '  branch id: ' + str(branch_id) + ' hydroid: ' + str(row.hydroid) + '\n'
                
            find_src_stage = df_htable_hydroid.loc[df_htable_hydroid['stage'].sub(row.hand).abs().idxmin()] # find closest matching stage to the user provided HAND value
            ## copy the corresponding htable values for the matching stage->HAND lookup
            df_nvalues.loc[index,'feature_id'] = find_src_stage.feature_id
            df_nvalues.loc[index,'LakeID'] = find_src_stage.LakeID
            df_nvalues.loc[index,'NextDownID'] = find_src_stage.NextDownID
            df_nvalues.loc[index,'LENGTHKM'] = find_src_stage.LENGTHKM
            df_nvalues.loc[index,'src_stage'] = find_src_stage.stage
            df_nvalues.loc[index,'ManningN'] = find_src_stage.ManningN
            df_nvalues.loc[index,'SLOPE'] = find_src_stage.SLOPE
            df_nvalues.loc[index,'HydraulicRadius_m'] = find_src_stage['HydraulicRadius (m)']
            df_nvalues.loc[index,'WetArea_m2'] = find_src_stage['WetArea (m2)']
            df_nvalues.loc[index,'discharge_cms'] = find_src_stage.discharge_cms

    ## mask src values that crosswalk to the SRC zero point (src_stage ~ 0 or discharge <= 0)
    df_nvalues[['HydraulicRadius_m','WetArea_m2']] = df_nvalues[['HydraulicRadius_m','WetArea_m2']].mask((df_nvalues['src_stage'] <= 0.1) | (df_nvalues['discharge_cms'] <= 0.0), np.nan)

    ## Calculate roughness using Manning's equation
    df_nvalues.rename(columns={'ManningN':'default_ManningN','hydroid':'HydroID'}, inplace=True) # rename the previous ManningN column
    df_nvalues['hydroid_ManningN'] = df_nvalues['WetArea_m2']* \
    pow(df_nvalues['HydraulicRadius_m'],2.0/3)* \
    pow(df_nvalues['SLOPE'],0.5)/df_nvalues['flow']

    ## Create dataframe to check for erroneous Manning's n values (values set in tools_shared_variables.py --> >0.6 or <0.001)
    df_nvalues['Mann_flag'] = np.where((df_nvalues['hydroid_ManningN'] >= ROUGHNESS_MAX_THRESH) | (df_nvalues['hydroid_ManningN'] <= ROUGHNESS_MIN_THRESH) | (df_nvalues['hydroid_ManningN'].isnull()),'Fail','Pass')
    df_mann_flag = df_nvalues[(df_nvalues['Mann_flag'] == 'Fail')][['HydroID','hydroid_ManningN']]
    if not df_mann_flag.empty:
        log_text += '!!! Flaged Mannings Roughness values below !!!' +'\n'
        log_text += df_mann_flag.to_string() + '\n'

    ## Create magnitude and ahps column by subsetting the "layer" attribute
    df_nvalues['magnitude'] = df_nvalues['layer'].str.split("_").str[5]
    df_nvalues['ahps_lid'] = df_nvalues['layer'].str.split("_").str[1]
    df_nvalues['huc'] = str(huc)
    df_nvalues.drop(['layer'], axis=1, inplace=True)

    ## Create df grouped by hydroid with ahps_lid and huc number
    df_huc_lid = df_nvalues.groupby(["HydroID"]).first()[['ahps_lid','huc']]
    df_huc_lid.columns = pd.MultiIndex.from_product([['info'], df_huc_lid.columns])

    ## pivot the magnitude column to display n value for each magnitude at each hydroid
    df_nvalues_mag = df_nvalues.pivot_table(index='HydroID', columns='magnitude', values=['hydroid_ManningN'], aggfunc='mean') # if there are multiple entries per hydroid and magnitude - aggregate using mean
    
    ## Optional: Export csv with the newly calculated Manning's N values
    if debug_outputs_option:
        output_calc_n_csv = os.path.join(fim_directory, 'calc_src_n_vals_' + branch_id + '.csv')
        df_nvalues.to_csv(output_calc_n_csv,index=False)

    ## filter the modified Manning's n dataframe for values out side allowable range
    df_nvalues = df_nvalues[df_nvalues['Mann_flag'] == 'Pass']

    ## Check that there are valid entries in the calculate roughness df after filtering
    if not df_nvalues.empty:
        ## Create df with the most recent collection time entry and submitter attribs
        df_updated = df_nvalues[['HydroID','coll_time','submitter','ahps_lid']] # subset the dataframe
        df_updated = df_updated.sort_values('coll_time').drop_duplicates(['HydroID'],keep='last') # sort by collection time and then drop duplicate HydroIDs (keep most recent coll_time per HydroID)
        df_updated.rename(columns={'coll_time':'last_updated'}, inplace=True)

        ## cacluate median ManningN to handle cases with multiple hydroid entries
        df_mann_hydroid = df_nvalues.groupby(["HydroID"])[['hydroid_ManningN']].median()

        ## Create a df with the median hydroid_ManningN value per feature_id
        #df_mann_featid = df_nvalues.groupby(["feature_id"])[['hydroid_ManningN']].mean()
        #df_mann_featid.rename(columns={'hydroid_ManningN':'featid_ManningN'}, inplace=True)

        ## Rename the original hydrotable variables to allow new calculations to use the primary var name
        df_htable.rename(columns={'ManningN':'default_ManningN','discharge_cms':'default_discharge_cms'}, inplace=True)

        ## Check for large variabilty in the calculated Manning's N values (for cases with mutliple entries for a singel hydroid)
        df_nrange = df_nvalues.groupby('HydroID').agg({'hydroid_ManningN': ['median', 'min', 'max', 'std', 'count']})
        df_nrange['hydroid_ManningN','range'] = df_nrange['hydroid_ManningN','max'] - df_nrange['hydroid_ManningN','min']
        df_nrange = df_nrange.join(df_nvalues_mag, how='outer') # join the df_nvalues_mag containing hydroid_manningn values per flood magnitude category
        df_nrange = df_nrange.merge(df_huc_lid, how='outer', on='HydroID') # join the df_huc_lid df to add attributes for lid and huc#
        log_text += 'Statistics for Modified Roughness Calcs -->' +'\n'
        log_text += df_nrange.to_string() + '\n'
        log_text += '----------------------------------------\n'

        ## Optional: Output csv with SRC calc stats
        if debug_outputs_option:
            output_stats_n_csv = os.path.join(fim_directory, 'stats_src_n_vals_' + branch_id + '.csv')
            df_nrange.to_csv(output_stats_n_csv,index=True)

        ## subset the original hydrotable dataframe and subset to one row per HydroID
        df_nmerge = df_htable[['HydroID','feature_id','NextDownID','LENGTHKM','LakeID','order_']].drop_duplicates(['HydroID'],keep='first') 

        ## Need to check that there are non-lake hydroids in the branch hydrotable (prevents downstream error)
        df_htable_check_lakes = df_nmerge.loc[df_nmerge['LakeID'] == -999]
        if not df_htable_check_lakes.empty:

            ## Create attributes to traverse the flow network between HydroIDs
            df_nmerge = branch_network(df_nmerge)

            ## Merge the newly caluclated ManningN dataframes
            df_nmerge = df_nmerge.merge(df_mann_hydroid, how='left', on='HydroID')
            df_nmerge = df_nmerge.merge(df_updated, how='left', on='HydroID')

            ## Calculate group_ManningN (mean calb n for consective hydroids) and apply values downsteam to non-calb hydroids (constrained to first Xkm of hydroids - set downstream diststance var as input arg)
            df_nmerge = group_manningn_calc(df_nmerge, down_dist_thresh)

            ## Create a df with the median hydroid_ManningN value per feature_id
            df_mann_featid = df_nmerge.groupby(["feature_id"])[['hydroid_ManningN']].mean()
            df_mann_featid.rename(columns={'hydroid_ManningN':'featid_ManningN'}, inplace=True)
            df_mann_featid_attrib = df_nmerge.groupby('feature_id').first() # create a seperate df with attributes to apply to other hydroids that share a featureid
            df_mann_featid_attrib = df_mann_featid_attrib[df_mann_featid_attrib['submitter'].notna()][['last_updated','submitter']]
            df_nmerge = df_nmerge.merge(df_mann_featid, how='left', on='feature_id').set_index('feature_id')
            df_nmerge = df_nmerge.combine_first(df_mann_featid_attrib).reset_index()
            
            if not df_nmerge['hydroid_ManningN'].isnull().all():
                ## Temp testing filter to only use the hydroid manning n values (drop the featureid and group ManningN variables)
                #df_nmerge = df_nmerge.assign(featid_ManningN=np.nan)
                #df_nmerge = df_nmerge.assign(group_ManningN=np.nan)

                ## Create the adjust_ManningN column by combining the hydroid_ManningN with the featid_ManningN (use feature_id value if the hydroid is in a feature_id that contains valid hydroid_ManningN value(s))
                conditions  = [ (df_nmerge['hydroid_ManningN'].isnull()) & (df_nmerge['featid_ManningN'].notnull()), (df_nmerge['hydroid_ManningN'].isnull()) & (df_nmerge['featid_ManningN'].isnull()) & (df_nmerge['group_ManningN'].notnull()) ]
                choices     = [ df_nmerge['featid_ManningN'], df_nmerge['group_ManningN'] ]
                df_nmerge['adjust_ManningN'] = np.select(conditions, choices, default=df_nmerge['hydroid_ManningN'])
                df_nmerge['obs_source'] = np.where(df_nmerge['adjust_ManningN'].notnull(), source_tag, pd.NA)
                df_nmerge.drop(['feature_id','NextDownID','LENGTHKM','LakeID','order_'], axis=1, inplace=True) # drop these columns to avoid duplicates where merging with the full hydroTable df

                ## Merge in previous SRC adjustments (where available) for hydroIDs that do not have a new adjusted roughness value
                if not df_prev_adj.empty:
                    df_nmerge = pd.merge(df_nmerge,df_prev_adj, on='HydroID', how='outer')
                    df_nmerge['submitter'] = np.where((df_nmerge['adjust_ManningN'].isnull() & df_nmerge['adjust_ManningN_prev'].notnull()),df_nmerge['submitter_prev'],df_nmerge['submitter'])
                    df_nmerge['last_updated'] = np.where((df_nmerge['adjust_ManningN'].isnull() & df_nmerge['adjust_ManningN_prev'].notnull()),df_nmerge['last_updated_prev'],df_nmerge['last_updated'])
                    df_nmerge['obs_source'] = np.where((df_nmerge['adjust_ManningN'].isnull() & df_nmerge['adjust_ManningN_prev'].notnull()),df_nmerge['obs_source_prev'],df_nmerge['obs_source'])
                    df_nmerge['adjust_ManningN'] = np.where((df_nmerge['adjust_ManningN'].isnull() & df_nmerge['adjust_ManningN_prev'].notnull()),df_nmerge['adjust_ManningN_prev'],df_nmerge['adjust_ManningN'])
                    df_nmerge.drop(['submitter_prev','last_updated_prev','adjust_ManningN_prev','obs_source_prev'], axis=1, inplace=True)
                
                ## Update the catchments polygon .gpkg with joined attribute - "src_calibrated"
                if os.path.isfile(catchments_poly_path):
                    input_catchments = gpd.read_file(catchments_poly_path)
                    ## Create new "src_calibrated" column for viz query
                    if 'src_calibrated' in input_catchments.columns: # check if this attribute already exists and drop if needed
                        input_catchments.drop(['src_calibrated'], axis=1, inplace=True)
                    df_nmerge['src_calibrated'] = np.where(df_nmerge['adjust_ManningN'].notnull(), 'True', 'False')
                    output_catchments = input_catchments.merge(df_nmerge[['HydroID','src_calibrated']], how='left', on='HydroID')
                    output_catchments['src_calibrated'].fillna('False', inplace=True)
                    output_catchments.to_file(catchments_poly_path,driver="GPKG",index=False) # overwrite the previous layer
                    df_nmerge.drop(['src_calibrated'], axis=1, inplace=True)
                ## Optional ouputs: 1) merge_n_csv csv with all of the calculated n values and 2) a catchments .gpkg with new joined attributes
                if debug_outputs_option:
                    output_merge_n_csv = os.path.join(fim_directory, 'merge_src_n_vals_' + branch_id + '.csv')
                    df_nmerge.to_csv(output_merge_n_csv,index=False)
                    ## output new catchments polygon layer with several new attributes appended
                    if os.path.isfile(catchments_poly_path):
                        input_catchments = gpd.read_file(catchments_poly_path)
                        output_catchments_fileName = os.path.join(os.path.split(catchments_poly_path)[0],"gw_catchments_src_adjust_" + str(branch_id) + ".gpkg")
                        output_catchments = input_catchments.merge(df_nmerge, how='left', on='HydroID')
                        output_catchments.to_file(output_catchments_fileName,driver="GPKG",index=False)

                ## Merge the final ManningN dataframe to the original hydroTable
                df_nmerge.drop(['ahps_lid','start_catch','route_count','branch_id','hydroid_ManningN','featid_ManningN','group_ManningN',], axis=1, inplace=True) # drop these columns to avoid duplicates where merging with the full hydroTable df
                df_htable = df_htable.merge(df_nmerge, how='left', on='HydroID')
                df_htable['adjust_src_on'] = np.where(df_htable['adjust_ManningN'].notnull(), 'True', 'False') # create true/false column to clearly identify where new roughness values are applied

                ## Create the ManningN column by combining the hydroid_ManningN with the default_ManningN (use modified where available)
                df_htable['ManningN'] = np.where(df_htable['adjust_ManningN'].isnull(),df_htable['default_ManningN'],df_htable['adjust_ManningN'])

                ## Calculate new discharge_cms with new adjusted ManningN
                df_htable['discharge_cms'] = df_htable['WetArea (m2)']* \
                pow(df_htable['HydraulicRadius (m)'],2.0/3)* \
                pow(df_htable['SLOPE'],0.5)/df_htable['ManningN']

                ## Replace discharge_cms with 0 or -999 if present in the original discharge (carried over from thalweg notch workaround in SRC post-processing)
                df_htable['discharge_cms'].mask(df_htable['default_discharge_cms']==0.0,0.0,inplace=True)
                df_htable['discharge_cms'].mask(df_htable['default_discharge_cms']==-999,-999,inplace=True)

                ## Export a new hydroTable.csv and overwrite the previous version
                out_htable = os.path.join(fim_directory, 'hydroTable_' + branch_id + '.csv')
                df_htable.to_csv(out_htable,index=False)

            else:
                print('ALERT!! HUC: ' + str(huc) + '  branch id: ' + str(branch_id) + ' --> no valid hydroid roughness calculations after removing lakeid catchments from consideration')
                log_text += 'ALERT!! HUC: ' + str(huc) + '  branch id: ' + str(branch_id) + ' --> no valid hydroid roughness calculations after removing lakeid catchments from consideration\n'
        else:
                print('WARNING!! HUC: ' + str(huc) + '  branch id: ' + str(branch_id) + ' --> hydrotable is empty after removing lake catchments (will ignore branch)')
                log_text += 'ALERT!! HUC: ' + str(huc) + '  branch id: ' + str(branch_id) + ' --> hydrotable is empty after removing lake catchments (will ignore branch)\n'
    else:
        print('ALERT!! HUC: ' + str(huc) + '  branch id: ' + str(branch_id) + ' --> no valid roughness calculations - please check point data and src calculations to evaluate')
        log_text += 'ALERT!! HUC: ' + str(huc) + '  branch id: ' + str(branch_id) + ' --> no valid roughness calculations - please check point data and src calculations to evaluate\n'

    log_text += 'Completed: ' + str(huc) + ' --> branch: ' + str(branch_id) + '\n'
    log_text += '#########################################################\n'
    print("Completed huc: " + str(huc) + ' --> branch: ' + str(branch_id))
    return(log_text)

def branch_network(df_input_htable):
    df_input_htable = df_input_htable.astype({'NextDownID': 'int64'}) # ensure attribute has consistent format as int
    df_input_htable = df_input_htable.loc[df_input_htable['LakeID'] == -999] # remove all hydroids associated with lake/water body (these often have disjoined artifacts in the network)
    df_input_htable["start_catch"] = ~df_input_htable['HydroID'].isin(df_input_htable['NextDownID']) # define start catchments as hydroids that are not found in the "NextDownID" attribute for all other hydroids
            
    df_input_htable.set_index('HydroID',inplace=True,drop=False) # set index to the hydroid
    branch_heads = deque(df_input_htable[df_input_htable['start_catch'] == True]['HydroID'].tolist()) # create deque of hydroids to define start points in the while loop
    visited = set() # create set to keep track of all hydroids that have been accounted for
    branch_count = 0 # start branch id 
    while branch_heads:
        hid = branch_heads.popleft() # pull off left most hydroid from deque of start hydroids
        Q = deque(df_input_htable[df_input_htable['HydroID'] == hid]['HydroID'].tolist()) # create a new deque that will be used to populate all relevant downstream hydroids
        vert_count = 0; branch_count += 1
        while Q:
            q = Q.popleft()
            if q not in visited:
                df_input_htable.loc[df_input_htable.HydroID==q,'route_count'] = vert_count # assign var with flow order ranking
                df_input_htable.loc[df_input_htable.HydroID==q,'branch_id'] = branch_count # assign var with current branch id
                vert_count += 1
                visited.add(q)
                nextid = df_input_htable.loc[q,'NextDownID'] # find the id for the next downstream hydroid
                order = df_input_htable.loc[q,'order_'] # find the streamorder for the current hydroid
            
                if nextid not in visited and nextid in df_input_htable.HydroID:
                    check_confluence = (df_input_htable.NextDownID == nextid).sum() > 1 # check if the NextDownID is referenced by more than one hydroid (>1 means this is a confluence)
                    nextorder = df_input_htable.loc[nextid,'order_'] # find the streamorder for the next downstream hydroid
                    if nextorder > order and check_confluence == True: # check if the nextdownid streamorder is greater than the current hydroid order and the nextdownid is a confluence (more than 1 upstream hydroid draining to it)
                        branch_heads.append(nextid) # found a terminal point in the network (append to branch_heads for second pass)
                        continue # if above conditions are True than stop traversing downstream and move on to next starting hydroid
                    Q.append(nextid)
    df_input_htable.reset_index(drop=True, inplace=True) # reset index (previously using hydroid as index)
    df_input_htable.sort_values(['branch_id','route_count'], inplace=True) # sort the dataframe by branch_id and then by route_count (need this ordered to ensure upstream to downstream ranking for each branch)
    return(df_input_htable)

def group_manningn_calc(df_nmerge, down_dist_thresh):
    ## Calculate group_ManningN (mean calb n for consective hydroids) and apply values downsteam to non-calb hydroids (constrained to first Xkm of hydroids - set downstream diststance var as input arg
    #df_nmerge.sort_values(by=['NextDownID'], inplace=True)
    dist_accum = 0; hyid_count = 0; hyid_accum_count = 0; 
    run_accum_mann = 0; group_ManningN = 0; branch_start = 1                                        # initialize counter and accumulation variables
    lid_count = 0; prev_lid = 'x'
    for index, row in df_nmerge.iterrows():                                                         # loop through the df (parse by hydroid)
        if int(df_nmerge.loc[index,'branch_id']) != branch_start:                                   # check if start of new branch
            dist_accum = 0; hyid_count = 0; hyid_accum_count = 0;                                   # initialize counter vars
            run_accum_mann = 0; group_ManningN = 0                                                  # initialize counter vars
            branch_start = int(df_nmerge.loc[index,'branch_id'])                                    # reassign the branch_start var to evaluate on next iteration
            # use the code below to withold downstream hydroid_ManningN values (use this for downstream evaluation tests)
            '''
            lid_count = 0                                                                         
        if not pd.isna(df_nmerge.loc[index,'ahps_lid']):
            if df_nmerge.loc[index,'ahps_lid'] == prev_lid:
                lid_count += 1
                if lid_count > 3: # only keep the first 3 HydroID n values (everything else set to null for downstream application)
                    df_nmerge.loc[index,'hydroid_ManningN'] = np.nan
                    df_nmerge.loc[index,'featid_ManningN'] = np.nan
            else:
                lid_count = 1
            prev_lid = df_nmerge.loc[index,'ahps_lid']
            '''
        if np.isnan(df_nmerge.loc[index,'hydroid_ManningN']):                                       # check if the hydroid_ManningN value is nan (indicates a non-calibrated hydroid)
            df_nmerge.loc[index,'accum_dist'] = row['LENGTHKM'] + dist_accum                        # calculate accumulated river distance
            dist_accum += row['LENGTHKM']                                                           # add hydroid length to the dist_accum var
            hyid_count = 0                                                                          # reset the hydroid counter to 0
            df_nmerge.loc[index,'hyid_accum_count'] = hyid_accum_count                              # output the hydroid accum counter
            if dist_accum < down_dist_thresh:                                                   # check if the accum distance is less than Xkm downstream from valid hydroid_ManningN group value
                if hyid_accum_count > 1:                                                            # only apply the group_ManningN if there are 2 or more valid hydorids that contributed to the upstream group_ManningN
                    df_nmerge.loc[index,'group_ManningN'] = group_ManningN                          # output the group_ManningN var
            else:
                run_avg_mann = 0                                                                    # reset the running average manningn variable (greater than 10km downstream)
        else:                                                                                       # performs the following for hydroids that have a valid hydroid_ManningN value
            dist_accum = 0; hyid_count += 1                                                         # initialize vars
            df_nmerge.loc[index,'accum_dist'] = 0                                                   # output the accum_dist value (set to 0)
            if hyid_count == 1:                                                                     # checks if this the first in a series of valid hydroid_ManningN values
                run_accum_mann = 0; hyid_accum_count = 0                                            # initialize counter and running accumulated manningN value
            group_ManningN = (row['hydroid_ManningN'] + run_accum_mann)/float(hyid_count)           # calculate the group_ManningN (NOTE: this will continue to change as more hydroid values are accumulated in the "group" moving downstream)
            df_nmerge.loc[index,'group_ManningN'] = group_ManningN                                  # output the group_ManningN var 
            df_nmerge.loc[index,'hyid_count'] = hyid_count                                          # output the hyid_count var 
            run_accum_mann += row['hydroid_ManningN']                                               # add current hydroid manningn value to the running accum mann var
            hyid_accum_count += 1                                                                   # increase the # of hydroid accum counter
            df_nmerge.loc[index,'hyid_accum_count'] = hyid_accum_count                              # output the hyid_accum_count var

    ## Delete unnecessary intermediate outputs
    if 'hyid_count' in df_nmerge.columns:
        df_nmerge.drop(['hyid_count','accum_dist','hyid_accum_count'], axis=1, inplace=True) # drop hydroid counter if it exists
    #df_nmerge.drop(['accum_dist','hyid_accum_count'], axis=1, inplace=True) # drop accum vars from group calc
    return(df_nmerge)
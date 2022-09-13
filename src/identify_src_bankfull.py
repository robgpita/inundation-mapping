#!/usr/bin/env python3

import os
import sys
import pandas as pd
import argparse
import matplotlib.pyplot as plt
import seaborn as sns
from functools import reduce
from multiprocessing import Pool
from os.path import isfile, join, dirname, isdir
import shutil
import warnings
from pathlib import Path
import datetime as dt
import re
sns.set_theme(style="whitegrid")
warnings.simplefilter(action='ignore', category=FutureWarning)

"""
    Identify the SRC bankfull stage values using the NWM 1.5yr flows

    Parameters
    ----------
    fim_dir : str
        Directory containing FIM output folders.
    bankfull_flow_dir : str
        Directory containing "bankfull" flows files (e.g. NWM 1.5yr recurr).
    number_of_jobs : str
        Number of jobs.
    plots : str
        Flag to create SRC plots for all hydroids (True/False)
"""

def src_bankfull_lookup(args):

    src_full_filename           = args[0]
    src_modify_filename         = args[1]
    df_bflows                   = args[2]
    huc                         = args[3]
    branch_id                   = args[4]
    src_plot_option             = args[5]
    huc_output_dir              = args[6]

    ## Read the src_full_crosswalked.csv
    print('Calculating bankfull: ' + str(huc) + '  branch id: ' + str(branch_id))
    log_text = 'Calculating: ' + str(huc) + '  branch id: ' + str(branch_id) + '\n'
    df_src = pd.read_csv(src_full_filename,dtype={'HydroID': int,'feature_id': int})

    ## NWM recurr rename discharge var
    df_bflows = df_bflows.rename(columns={'discharge':'bankfull_flow'})

    ## Combine the nwm 1.5yr flows into the SRC via feature_id
    df_src = df_src.merge(df_bflows,how='left',on='feature_id')

    ## Check if there are any missing data, negative or zero flow values in the bankfull_flow
    check_null = df_src['bankfull_flow'].isnull().sum()
    if check_null > 0:
        log_text += 'Missing feature_id in crosswalk for huc: ' + str(huc) + '  branch id: ' + str(branch_id) + ' --> these featureids will be ignored in bankfull calcs (~' + str(check_null/84) +  ' features) \n'
        ## Fill missing/nan nwm bankfull_flow values with -999 to handle later
        df_src['bankfull_flow'] = df_src['bankfull_flow'].fillna(-999)
    negative_flows = len(df_src.loc[(df_src.bankfull_flow <= 0) & (df_src.bankfull_flow != -999)])
    if negative_flows > 0:
        log_text += 'HUC: ' + str(huc) + '  branch id: ' + str(branch_id) + ' --> Negative or zero flow values found (likely lakeid loc)\n'

    ## Define the channel geometry variable names to use from the src
    hradius_var = 'HydraulicRadius (m)'
    volume_var = 'Volume (m3)'
    surface_area_var = 'SurfaceArea (m2)'

    ## Locate the closest SRC discharge value to the NWM 1.5yr flow
    df_src['Q_bfull_find'] = (df_src['bankfull_flow'] - df_src['Discharge (m3s-1)']).abs()

    ## Check for any missing/null entries in the input SRC
    if df_src['Q_bfull_find'].isnull().values.any(): # there may be null values for lake or coastal flow lines (need to set a value to do groupby idxmin below)
        log_text += 'HUC: ' + str(huc) + '  branch id: ' + str(branch_id) + ' --> Null values found in "Q_bfull_find" calc. These will be filled with 999999 () \n'
        ## Fill missing/nan nwm 'Discharge (m3s-1)' values with 999999 to handle later
        df_src['Q_bfull_find'] = df_src['Q_bfull_find'].fillna(999999)
    if df_src['HydroID'].isnull().values.any():
        log_text += 'HUC: ' + str(huc) + '  branch id: ' + str(branch_id) + ' --> Null values found in "HydroID"... \n'

    df_bankfull_calc = df_src[['Stage','HydroID',volume_var,hradius_var,surface_area_var,'Q_bfull_find']] # create new subset df to perform the Q_1_5 lookup
    df_bankfull_calc = df_bankfull_calc[df_bankfull_calc['Stage'] > 0.0] # Ensure bankfull stage is greater than stage=0
    df_bankfull_calc.reset_index(drop=True, inplace=True)
    df_bankfull_calc = df_bankfull_calc.loc[df_bankfull_calc.groupby('HydroID')['Q_bfull_find'].idxmin()].reset_index(drop=True) # find the index of the Q_bfull_find (closest matching flow)
    df_bankfull_calc = df_bankfull_calc.rename(columns={'Stage':'Stage_bankfull',volume_var:'Volume_bankfull',hradius_var:'HRadius_bankfull',surface_area_var:'SurfArea_bankfull'}) # rename volume to use later for channel portion calc
    df_src = df_src.merge(df_bankfull_calc[['Stage_bankfull','HydroID','Volume_bankfull','HRadius_bankfull','SurfArea_bankfull']],how='left',on='HydroID')
    df_src.drop(['Q_bfull_find'], axis=1, inplace=True)

    ## Calculate the channel portion of bankfull Volume
    df_src['chann_volume_ratio'] = 1.0 # At stage=0 set channel_ratio to 1.0 (avoid div by 0)
    df_src['chann_volume_ratio'].where(df_src['Stage'] == 0, df_src['Volume_bankfull'] / (df_src[volume_var]),inplace=True)
    #df_src['chann_volume_ratio'] = df_src['chann_volume_ratio'].clip_upper(1.0)
    df_src['chann_volume_ratio'].where(df_src['chann_volume_ratio'] <= 1.0, 1.0, inplace=True) # set > 1.0 ratio values to 1.0 (these are within the channel)
    df_src['chann_volume_ratio'].where(df_src['bankfull_flow'] > 0.0, 0.0, inplace=True) # if the bankfull_flow value <= 0 then set channel ratio to 0 (will use global overbank manning n)
    #df_src.drop(['Volume_bankfull'], axis=1, inplace=True)

    ## Calculate the channel portion of bankfull Hydraulic Radius
    df_src['chann_hradius_ratio'] = 1.0 # At stage=0 set channel_ratio to 1.0 (avoid div by 0)
    df_src['chann_hradius_ratio'].where(df_src['Stage'] == 0, df_src['HRadius_bankfull'] / (df_src[hradius_var]),inplace=True)
    #df_src['chann_hradius_ratio'] = df_src['HRadius_bankfull'] / (df_src[hradius_var]+.0001) # old adding 0.01 to avoid dividing by 0 at stage=0
    df_src['chann_hradius_ratio'].where(df_src['chann_hradius_ratio'] <= 1.0, 1.0, inplace=True) # set > 1.0 ratio values to 1.0 (these are within the channel)
    df_src['chann_hradius_ratio'].where(df_src['bankfull_flow'] > 0.0, 0.0, inplace=True) # if the bankfull_flow value <= 0 then set channel ratio to 0 (will use global overbank manning n)
    #df_src.drop(['HRadius_bankfull'], axis=1, inplace=True)

    ## Calculate the channel portion of bankfull Surface Area
    df_src['chann_surfarea_ratio'] = 1.0 # At stage=0 set channel_ratio to 1.0 (avoid div by 0)
    df_src['chann_surfarea_ratio'].where(df_src['Stage'] == 0, df_src['SurfArea_bankfull'] / (df_src[surface_area_var]),inplace=True)
    df_src['chann_surfarea_ratio'].where(df_src['chann_surfarea_ratio'] <= 1.0, 1.0, inplace=True) # set > 1.0 ratio values to 1.0 (these are within the channel)
    df_src['chann_surfarea_ratio'].where(df_src['bankfull_flow'] > 0.0, 0.0, inplace=True) # if the bankfull_flow value <= 0 then set channel ratio to 0 (will use global overbank manning n)
    #df_src.drop(['HRadius_bankfull'], axis=1, inplace=True)

    ## mask bankfull variables when the 1.5yr flow value is <= 0
    df_src['Stage_bankfull'].mask(df_src['bankfull_flow'] <= 0.0,inplace=True)

    ## Create a new column to identify channel/floodplain via the bankfull stage value
    df_src.loc[df_src['Stage'] <= df_src['Stage_bankfull'], 'channel_fplain_1_5'] = 'channel'
    df_src.loc[df_src['Stage'] > df_src['Stage_bankfull'], 'channel_fplain_1_5'] = 'floodplain'
    df_src['channel_fplain_1_5'] = df_src['channel_fplain_1_5'].fillna('channel')

    ## Output new SRC with bankfull column
    df_src.to_csv(src_modify_filename,index=False)
    log_text += 'Completed: ' + str(huc)

    ## plot rating curves (optional arg)
    if src_plot_option:
        if isdir(huc_output_dir) == False:
            os.mkdir(huc_output_dir)
        generate_src_plot(df_src, huc_output_dir)

    return(log_text)

def generate_src_plot(df_src, plt_out_dir):

    ## create list of unique hydroids
    hydroids = df_src.HydroID.unique().tolist()
    #hydroids = [17820017]

    for hydroid in hydroids:
        print("Creating SRC plot: " + str(hydroid))
        plot_df = df_src.loc[df_src['HydroID'] == hydroid]

        fig, axes = plt.subplots(1,2,figsize=(12, 6))
        fig.suptitle(str(hydroid))
        axes[0].set_title('Rating Curve w/ Bankfull')
        axes[1].set_title('Channel Volume vs. HRadius Ratio')
        sns.despine(fig, left=True, bottom=True)
        sns.scatterplot(x='Discharge (m3s-1)', y='Stage', data=plot_df, ax=axes[0])
        sns.lineplot(x='Discharge (m3s-1)', y='Stage_bankfull', data=plot_df, color='green', ax=axes[0])
        axes[0].fill_between(plot_df['Discharge (m3s-1)'], plot_df['Stage_bankfull'],alpha=0.5)
        axes[0].text(plot_df['Discharge (m3s-1)'].median(), plot_df['Stage_bankfull'].median(), "Bankfull Proxy Stage: " + str(plot_df['Stage_bankfull'].median()))
        sns.scatterplot(x='chann_volume_ratio', y='Stage', data=plot_df, ax=axes[1], label="chann_volume_ratio", s=38)
        sns.scatterplot(x='chann_hradius_ratio', y='Stage', data=plot_df, ax=axes[1], label="chann_hradius_ratio", s=12)
        sns.scatterplot(x='chann_surfarea_ratio', y='Stage', data=plot_df, ax=axes[1], label="chann_surfarea_ratio", s=12)
        axes[1].legend()
        plt.savefig(plt_out_dir + os.sep + str(hydroid) + '_bankfull.png',dpi=100, bbox_inches='tight')
        plt.close()

def multi_process(src_bankfull_lookup, procs_list, log_file):
    ## Initiate multiprocessing
    print(f"Identifying bankfull stage for {len(procs_list)} hucs using {number_of_jobs} jobs")
    with Pool(processes=number_of_jobs) as pool:
        map_output = pool.map(src_bankfull_lookup, procs_list)
    log_file.writelines(["%s\n" % item  for item in map_output])

def run_prep(fim_dir,bankfull_flow_filepath,number_of_jobs,src_plot_option):
    procs_list = []

    ## Print message to user and initiate run clock
    print('Writing progress to log file here: ' + str(join(fim_dir,'bankfull_detect.log')))
    print('This may take a few minutes...')
    ## Create a time var to log run time
    begin_time = dt.datetime.now()

    ## Check that the input fim_dir exists
    assert os.path.isdir(fim_dir), 'ERROR: could not find the input fim_dir location: ' + str(fim_dir)
    ## Check that the bankfull flow filepath exists and read to dataframe
    assert os.path.isfile(bankfull_flow_filepath), 'ERROR: Can not find the input bankfull flow file: ' + str(bankfull_flow_filepath)
    
    df_bflows = pd.read_csv(bankfull_flow_filepath,dtype={'feature_id': int})
    huc_list  = os.listdir(fim_dir)
    huc_pass_list = []
    for huc in huc_list:
        #if huc != 'logs' and huc[-3:] != 'log' and huc[-4:] != '.csv':
        if re.match('\d{8}', huc):    
            huc_branches_dir = os.path.join(fim_dir, huc,'branches')
            for branch_id in os.listdir(huc_branches_dir):
                branch_dir = os.path.join(huc_branches_dir,branch_id)
                src_orig_full_filename = join(branch_dir,'src_full_crosswalked_' + branch_id + '.csv')
                src_modify_filename = join(branch_dir,'src_full_crosswalked_bankfull_' + branch_id + '.csv')
                huc_output_dir = join(branch_dir,'src_plots')
                ## check if BARC modified src_full_crosswalked_BARC.csv exists otherwise use the orginial src_full_crosswalked.csv
                if isfile(src_orig_full_filename):
                    huc_pass_list.append(str(huc) + " --> src_full_crosswalked.csv")
                    procs_list.append([src_orig_full_filename, src_modify_filename, df_bflows, huc, branch_id, src_plot_option, huc_output_dir])
                else:
                    print('HUC: ' + str(huc) + '  branch id: ' + str(branch_id) + 'WARNING --> can not find the SRC crosswalked csv file in the fim output dir: ' + str(branch_dir) + ' - skipping this branch!!!\n')
                    log_file.write('HUC: ' + str(huc) + '  branch id: ' + str(branch_id) + 'WARNING --> can not find the SRC crosswalked csv file in the fim output dir: ' + str(branch_dir) + ' - skipping this branch!!!\n')

    ## initiate log file
    log_file = open(join(fim_dir,'logs','log_bankfull_indentify.log'),"w")
    log_file.write('START TIME: ' + str(begin_time) + '\n')
    log_file.writelines(["%s\n" % item  for item in huc_pass_list])
    log_file.write('#########################################################\n\n')

    ## Pass huc procs_list to multiprocessing function
    multi_process(src_bankfull_lookup, procs_list, log_file)

    ## Record run time and close log file
    end_time = dt.datetime.now()
    log_file.write('END TIME: ' + str(end_time) + '\n')
    tot_run_time = end_time - begin_time
    log_file.write('TOTAL RUN TIME: ' + str(tot_run_time))
    log_file.close()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Identify bankfull stage for each hydroid synthetic rating curve')
    parser.add_argument('-fim_dir','--fim-dir', help='FIM output dir', required=True,type=str)
    parser.add_argument('-flows','--bankfull-flow-input',help='NWM recurrence flows dir (flow units in CMS!!!)',required=True,type=str)
    parser.add_argument('-j','--number-of-jobs',help='number of workers',required=False,default=1,type=int)
    parser.add_argument('-plots','--src-plot-option',help='OPTIONAL flag: use this flag to create src plots for all hydroids (helpful for evaluating). WARNING - long runtime',default=False,required=False, action='store_true')

    args = vars(parser.parse_args())

    fim_dir = args['fim_dir']
    bankfull_flow_filepath = args['bankfull_flow_input']
    number_of_jobs = args['number_of_jobs']
    src_plot_option = args['src_plot_option']
    
    ## Prepare/check inputs, create log file, and spin up the proc list
    run_prep(fim_dir,bankfull_flow_filepath,number_of_jobs,src_plot_option)
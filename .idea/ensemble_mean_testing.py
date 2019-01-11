import os
import subprocess
import datetime
import glob
import urllib2
import gzip
import shutil
from bs4 import BeautifulSoup
import shlex
import sys

# Calculates the annual average number of days above a specified threshold (including no_analog) for total days chunks calculated in no_analog_days_analysis or threshold_days_analysis methods
# threshold options are: no_analog, above_100, or above_105
def calculate_annual_average_days(models, scenarios, time_periods, thresholds):
    for model in models:
        path_to_days_results_files = '/Volumes/hot_drive/heat_data/{0}/days_results/' .format(model)
        #path_to_days_results_files = '/Users/kristinadahl/Desktop/heat_data/test_abatz/'
        for scenario in scenarios:
            for time_period in time_periods:
                if time_period == 'historical':
                    start_year = '1971'
                    end_year = '2000'
                if time_period == 'mid_century':
                    start_year = '2036'
                    end_year = '2065'
                if time_period == 'late_century':
                    start_year = '2070'
                    end_year = '2099'
                for threshold in thresholds:
                    files_to_analyze = glob.glob1(path_to_days_results_files,
                                                  'total_{0}_days_macav2metdata_{1}_r1i1p1_{2}_hi_*.nc'.format(threshold, model, scenario)) # need to edit because of the above_100 threshold construct
                    print 'Analyzing these conditions: {0} {1} {2} {3}' .format(model, scenario, time_period, threshold)
                    print 'Full file list to analyze: '
                    print files_to_analyze
                    ncea_arguments = ['ncea']
                    for file in files_to_analyze:
                        file_start_year = file[-12:-8]
                        file_end_year = file[-7:-3]
                        print 'Start year: ' + file_start_year
                        if int(file_start_year) >= int(start_year):
                            if int(file_start_year) < int(end_year) + 30:
                                if int(file_start_year) < int(end_year):
                                    print 'Analyzing this file: ' + file
                                    infile = path_to_days_results_files + file
                                    annual_average_outfile = path_to_days_results_files + 'annual_average_{0}_days_macav2metdata_{1}_r1i1p1_{2}_hi_{3}_{4}.nc' .format(threshold, model, scenario, file_start_year, file_end_year)
                                    time_period_average_outfile = path_to_days_results_files + '{0}_average_{1}_days_macav2metdata_{2}_r1i1p1_{3}_hi_{4}_{5}.nc' .format(time_period, threshold, model, scenario, start_year, end_year)
                                    # get number of years over which to average from file start and end years
                                    number_of_years = int(file_end_year) - int(file_start_year) + 1
                                    number_of_years = str(number_of_years)
                                    subprocess.Popen(["bash","/Users/kristinadahl/PycharmProjects/heat2/calculate_annual_average_days.sh", threshold, number_of_years, infile, annual_average_outfile, time_period_average_outfile]).wait()
                                    print 'Calculated annual average number of days at or above {0} and updated attributes' .format(threshold)
                                ncea_arguments.append(annual_average_outfile)
                    ncea_arguments.append('-O')
                    ncea_arguments.append(time_period_average_outfile)
                    print ncea_arguments
                    # calculate 30 year mean annual number of days above threshold
                    subprocess.call(ncea_arguments)


# thresholds here are a little different. options are "no_analog", "above_100", or "above_105
def calculate_ensemble_mean(models, scenarios, time_periods, thresholds):
    for scenario in scenarios:
        for time_period in time_periods:
            if time_period == 'historical':
                start_year = '1971'
                end_year = '2000'
            if time_period == 'mid_century':
                start_year = '2036'
                end_year = '2065'
            if time_period == 'late_century':
                start_year = '2070'
                end_year = '2099'
            for threshold in thresholds:
                print 'Calculating ensemble mean for {0}, {1}, {2}' .format(scenario, time_period, threshold)
                ncea_arguments = ['ncea']
                ensemble_mean_outfile = '/Volumes/hot_drive/heat_data/ensemble_means/{0}_ensemble_mean_average_{1}_hi_days_{2}_{3}_{4}.nc' .format(time_period, threshold, scenario, start_year, end_year)
                for model in models:
                    path_to_days_results_files = '/Volumes/hot_drive/heat_data/{0}/days_results/'.format(model)
                    file_to_include_in_ensemble = path_to_days_results_files + '{0}_average_{1}_days_macav2metdata_{2}_r1i1p1_{3}_hi_{4}_{5}.nc' .format(time_period, threshold, model, scenario, start_year, end_year)
                    ncea_arguments.append(file_to_include_in_ensemble)
                ncea_arguments.append('-O')
                ncea_arguments.append(ensemble_mean_outfile)
                if len(ncea_arguments) == 21:
                    subprocess.call(ncea_arguments)
                    subprocess.call(['ncatted','-a', 'description,annual_average_{0}_days,o,c,Ensemble mean annual average number of days at or above threshold' .format(threshold), ensemble_mean_outfile])
                    subprocess.call(['ncatted', '-a','long_name,annual_average_{0}_days,o,c,Ensemble mean annual average number of days at or above threshold'.format(threshold), ensemble_mean_outfile])
                    print 'calculated ensemble mean for {0} {1} {2}' .format(threshold, scenario, time_period)
                else:
                    print 'Something is missing!'


# threshold options are 'no_analog', 'above_100', or 'above_105'
def calculate_days_anomalies(scenarios, time_periods, thresholds):
    path_to_ensemble_mean_files = '/Volumes/hot_drive/heat_data/ensemble_means/'
    for scenario in scenarios:
        for time_period in time_periods:
            if time_period == 'mid_century':
                start_year = '2036'
                end_year = '2065'
            if time_period == 'late_century':
                start_year = '2070'
                end_year = '2099'
            for threshold in thresholds:
                print 'Calculating anomaly for {0} {1} {2}' .format(scenario, time_period, threshold)
                historical_file = path_to_ensemble_mean_files + 'historical_ensemble_mean_average_{0}_hi_days_historical_1971_2000.nc' .format(threshold)
                future_file = path_to_ensemble_mean_files + '{0}_ensemble_mean_average_{1}_hi_days_{2}_{3}_{4}.nc' .format(time_period, threshold, scenario, start_year, end_year)
                output_file = path_to_ensemble_mean_files + '{0}_anomaly_ensemble_mean_average_{1}_hi_days_{2}_{3}_{4}_minus_1971_2000.nc' .format(time_period, threshold, scenario, start_year, end_year)
                subprocess.call(['ncdiff',future_file, historical_file, output_file])

def compress_data(models, size_threshold):
    for model in models:
        path_to_data_files = '/Volumes/hot_drive/heat_data/{0}/days_results_testing/' .format(model)
        full_file_list = glob.glob1(path_to_data_files, 'above_100_flag_macav2metdata_bcc-csm1-1_r1i1p1_rcp45_hi_2056_2065.nc')
        for file in full_file_list:
            file_with_path = path_to_data_files + file
            if os.stat(file_with_path).st_size > size_threshold:
                subprocess.call(['ncks', '-4', '-L', '5', '-O', file_with_path, path_to_data_files + 'test_ncks_L5.nc'])
                print 'New size is: ' + str(os.stat(file_with_path).st_size)


full_models_list = ['bcc-csm1-1','bcc-csm1-1-m','BNU-ESM','CanESM2','CNRM-CM5','CSIRO-Mk3-6-0','GFDL-ESM2M','GFDL-ESM2G','HadGEM2-ES365','HadGEM2-CC365','inmcm4','IPSL-CM5A-LR','IPSL-CM5A-MR','IPSL-CM5B-LR','MIROC5','MIROC-ESM','MIROC-ESM-CHEM','MRI-CGCM3']

#hyperslab_files_to_get_desired_time_blocks(full_models_list, ['historical'], ['historical_1970s','historical_2000s'])

#calculate_ensemble_mean(full_models_list, ['historical'],['historical'],['no_analog'])

#calculate_days_anomalies(['rcp45','rcp85'],['mid_century','late_century'],['no_analog'])

compress_data(['bcc-csm1-1'], 1000000000)
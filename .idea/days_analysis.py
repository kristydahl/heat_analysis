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

# Download data from the Northwest Knowledge Network
def download_files_from_nkn(models):
    response = urllib2.urlopen('https://climate.northwestknowledge.net/ACSL/HEAT/')
    soup = BeautifulSoup(response,"html.parser")
    list_of_files_to_download = []
    for a in soup.findAll("a",href=True):
        print a.text
        list_of_files_to_download.append(a.text)

    for model in models:
        print 'Starting download for ' + model
        print(datetime.datetime.now().time())
        for item in list_of_files_to_download:
            if model + '_' in item:
                print 'Downloading ' + item
                file_to_download = urllib2.urlopen('https://climate.northwestknowledge.net/ACSL/HEAT/%s' %item)
                file_to_write = file_to_download.read()

                with open('/Volumes/hot_drive/heat_data/%s/' %model + item,'wb') as f:
                    f.write(file_to_write)
                f = '/Volumes/hot_drive/heat_data/%s/' %model + item

                print os.path.basename(f)

                f_out = os.path.basename(f)[:-3]

                file_to_unzip = gzip.GzipFile(f,'rb')
                s = file_to_unzip.read()
                file_to_unzip.close()

                unzipped_file = file('/Volumes/hot_drive/heat_data/' + model + '/' + f_out, 'wb')
                unzipped_file.write(s)
                unzipped_file.close()
        print 'Downloaded and unzipped files for ' + model
        print(datetime.datetime.now().time())

# This method hyperslabs specific files in order to have the right years available for creating 30-year means
# This is for a historical period of 1971-2000 and a late century period of 2070-2099
def hyperslab_files_to_get_desired_time_blocks(models, scenarios, time_periods):
    # specify the start and end index for the different time periods.
    for scenario in scenarios:
        for time_period in time_periods:
            if time_period == 'historical_1970s':
                original_start_year = '1970'
                new_start_year = '1971'
                original_end_year = '1979'
                new_end_year = '1979'
                start_index = '214'
                end_index = '2139'
            if time_period == 'historical_2000s':
                original_start_year = '2000'
                new_start_year = '2000'
                original_end_year = '2005'
                new_end_year = '2000'
                start_index = '0'
                end_index = '213'
            if time_period == 'late_century':
                original_start_year = '2066'
                new_start_year = '2070'
                end_year = '2075'
                start_index = '857'
                end_index = '2139'
            for model in models:
                path_to_hi_data_files = '/Volumes/hot_drive/heat_data/{0}/'.format(model)
                files_to_hyperslab = glob.glob1(path_to_hi_data_files,
                                                'macav2metdata*{0}_r1i1p1_{1}_hi_{2}_{3}.nc'.format(model, scenario,
                                                                                                    original_start_year,
                                                                                                    original_end_year))
                for file_to_hyperslab in files_to_hyperslab:
                    input_file = path_to_hi_data_files + file_to_hyperslab
                    output_file = path_to_hi_data_files + 'macav2metdata_{0}_r1i1p1_{1}_hi_{2}_{3}.nc'.format(model,
                                                                                                              scenario,
                                                                                                              new_start_year,
                                                                                                              new_end_year)
                    subprocess.call(
                        ['ncks', '-d', 'time,{0},{1}'.format(start_index, end_index), '-O', input_file, output_file])

# This method calculates the number of days above specific heat index thresholds OR the number of no analog days.
# The method calls shell sripts containing NCO commands in order to do the actual calculations
# scenario is historical, rcp45, or rcp85; time_period is historical, mid-century, or late-century
# threshold options are "no_analog", or a HI value passed in as a string
def days_above_threshold_analysis(models,scenarios,time_periods, thresholds):
    for model in models:
        print model
        path_to_hi_data_files = '/Volumes/hot_drive/heat_data/{0}/' .format(model)
        #path_to_hi_data_files = '/Users/kristinadahl/Desktop/heat_data/test_abatz/{0}/'.format(model)
        path_to_days_results_files = '/Volumes/hot_drive/heat_data/{0}/days_results/' .format(model)
        for scenario in scenarios:
            print scenario

            # specify time periods of interest (doing this for expediency, but could change to do all years, of course)
            for time_period in time_periods:
                print time_period
                if time_period == 'historical':
                    start_year = '1971'
                    end_year = '2000'
                if time_period == 'mid_century':
                    start_year = '2036'
                    end_year = '2065'
                if time_period == 'late_century':
                    start_year = '2070'
                    end_year = '2099'

                files_to_analyze = glob.glob1(path_to_hi_data_files, 'macav2metdata_{0}_r1i1p1_{1}_hi_*.nc' .format(model, scenario))
                print 'Full file list to analyze: '
                print files_to_analyze
                for file in files_to_analyze:
                    file_start_year = file.split('_')[5]
                    file_end_year = file.split('_')[6][0:4]
                    print file_start_year
                    if int(file_start_year) >= int(start_year):
                        if int(file_start_year) < int(end_year)+30:
                            if int(file_end_year) <= int(end_year):
                                print 'Analyzing ' + file
                                infile = path_to_hi_data_files + file
                                print 'infile is: ' + infile

                                for threshold in thresholds:
                                    print 'Doing calculations for threshold of ' + threshold
                                    if threshold == 'no_analog':
                                        outfile_with_flag = path_to_days_results_files + 'no_analog_flag_' + file
                                        print 'outfile_with_flag is: ' + outfile_with_flag
                                        print(datetime.datetime.now().time())
                                        print 'starting process for no_analog days'
                                        outfile_number_of_days = path_to_days_results_files + 'total_no_analog_days_' + file
                                        print outfile_number_of_days
                                        # call nco_no_analog_days.sh and pass in parameters in list
                                        subprocess.Popen(["bash","/Users/kristinadahl/PycharmProjects/heat2/nco_no_analog_days.sh", infile, outfile_with_flag, outfile_number_of_days]).wait()
                                        print(datetime.datetime.now().time())
                                    else:
                                        outfile_with_flag = path_to_days_results_files + 'above_{0}_flag_' .format(threshold) + file
                                        outfile_number_of_days = path_to_days_results_files + 'total_above_{0}_days_'.format(
                                            threshold) + file
                                        print 'outfile_with_flag is: ' + outfile_with_flag
                                        print 'starting process for threshold analysis'
                                        print(datetime.datetime.now().time())
                                        # call nco_days_above_threshold.sh and pass in parameters in list
                                        subprocess.Popen(
                                            ["bash", "/Users/kristinadahl/PycharmProjects/heat2/nco_days_above_threshold.sh",
                                             str(threshold), infile, outfile_with_flag, outfile_number_of_days]).wait()
                                        print(datetime.datetime.now().time())

# Calculates the annual average number of days above a specified threshold (including no_analog) for total days chunks calculated in days_above_threshold_analysis method
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
                                    # call calculate_annual_average_days.sh and pass in parameters in list
                                    subprocess.Popen(["bash","/Users/kristinadahl/PycharmProjects/heat2/calculate_annual_average_days.sh", threshold, number_of_years, infile, annual_average_outfile, time_period_average_outfile]).wait()
                                    print 'Calculated annual average number of days at or above {0} and updated attributes' .format(threshold)
                                ncea_arguments.append(annual_average_outfile)
                    ncea_arguments.append('-O')
                    ncea_arguments.append(time_period_average_outfile)
                    print ncea_arguments
                    # calculate 30 year mean annual number of days above threshold
                    subprocess.call(ncea_arguments)

# Calculates the ensemble mean number of no analog days or days above specified thresholds
# Threshold options are "no_analog", "above_100", or "above_105
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
                if len(ncea_arguments) == 21: # this is a check to ensure that all 18 models are included in the ncea call along with 'ncea', '-O', and the name of the output file
                    subprocess.call(ncea_arguments)
                    subprocess.call(['ncatted','-a', 'description,annual_average_{0}_days,o,c,Ensemble mean annual average number of days at or above threshold' .format(threshold), ensemble_mean_outfile])
                    subprocess.call(['ncatted', '-a','long_name,annual_average_{0}_days,o,c,Ensemble mean annual average number of days at or above threshold'.format(threshold), ensemble_mean_outfile])
                    print 'calculated ensemble mean for {0} {1} {2}' .format(threshold, scenario, time_period)
                else:
                    print 'Something is missing!'

# This method calculates the change in the number of no analog days or the number of days above specified thresholds
# Threshold options are 'no_analog', 'above_100', or 'above_105'
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
                # use ncdiff to calculate anomaly
                subprocess.call(['ncdiff',future_file, historical_file, output_file])

# The above methods do not use data compression and therefore create very large files. But they do it quickly, which is nice.
# This method can be used to loop through model folders and compress files over a specified size threshold, such as 1 GB
def compress_data(models, size_threshold):
    for model in models:
        print 'Compressing days results for ' + model
        print(datetime.datetime.now().time())
        path_to_data_files = '/Volumes/hot_drive/heat_data/{0}/days_results/' .format(model)
        full_file_list = glob.glob1(path_to_data_files, '*.nc')
        print str(len(full_file_list)) + 'files to compress'
        for file in full_file_list:
            file_with_path = path_to_data_files + file
            if os.stat(file_with_path).st_size > size_threshold:
                subprocess.call(['ncks', '-4', '-L', '5', '-O', file_with_path, file_with_path])
                print 'Compressed ' + file
        print(datetime.datetime.now().time())



# below are commands for running the above methods
full_models_list = ['bcc-csm1-1','bcc-csm1-1-m','BNU-ESM','CanESM2','CNRM-CM5','CSIRO-Mk3-6-0','GFDL-ESM2M','GFDL-ESM2G','HadGEM2-ES365','HadGEM2-CC365','inmcm4','IPSL-CM5A-LR','IPSL-CM5A-MR','IPSL-CM5B-LR','MIROC5','MIROC-ESM','MIROC-ESM-CHEM','MRI-CGCM3']

#models_list = ['bcc-csm1-1-m','BNU-ESM','CanESM2','CNRM-CM5','CSIRO-Mk3-6-0','GFDL-ESM2M','GFDL-ESM2G','HadGEM2-ES365','HadGEM2-CC365','inmcm4']
#models_list = ['bcc-csm1-1-m','BNU-ESM','CanESM2','CNRM-CM5','CSIRO-Mk3-6-0','GFDL-ESM2M','GFDL-ESM2G','HadGEM2-ES365','HadGEM2-CC365','inmcm4','IPSL-CM5A-LR','IPSL-CM5A-MR','IPSL-CM5B-LR','MIROC5','MIROC-ESM','MIROC-ESM-CHEM','MRI-CGCM3']

#download_files_from_nkn(models_list)
#hyperslab_files_to_get_desired_time_blocks(full_models_list,['historical','rcp45','rcp85'],['historical','late_century'])
#days_above_threshold_analysis(full_models_list,['historical'],['historical'],['no_analog'])
#calculate_annual_average_days(full_models_list,['historical'],['historical'],['above_100','above_105'])
#days_above_threshold_analysis(models_list,['mid_century'],['rcp45'],['100'])
#calculate_ensemble_mean(full_models_list, ['historical'],['historical'],['above_100','above_105'])
#days_above_threshold_analysis(models_list_2,['historical'],['historical'],['100','105'])
#calculate_days_anomalies(['rcp45','rcp85'],['mid_century','late_century'],['above_100','above_105'])
#compress_data(models_list, 1000000000)


import os
import subprocess
import datetime
import glob
import urllib2
import gzip
import shutil
from bs4 import BeautifulSoup
import shlex

# run this method first to dowload data from the Northwest Knowledge Network
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
def hyperslab_files_to_get_desired_time_blocks(models, scenarios, time_periods):
    # specify the start and end index for the different time periods. the different start_index values reflect that the historical hyperslab represents 4 years of data (1976-1979) while the late_century
    # hyperslab represents six years of data (2070-2075)
    for scenario in scenarios:
        for time_period in time_periods:
            if time_period == 'historical':
                original_start_year = '1970'
                new_start_year = '1976'
                end_year = '1979'
                start_index = '1284'
                end_index = '2139'
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
                                                                                                    end_year))
                for file_to_hyperslab in files_to_hyperslab:
                    input_file = path_to_hi_data_files + file_to_hyperslab
                    output_file = path_to_hi_data_files + 'macav2metdata_{0}_r1i1p1_{1}_hi_{2}_{3}.nc'.format(model,
                                                                                                              scenario,
                                                                                                              new_start_year,
                                                                                                              end_year)
                    subprocess.call(
                        ['ncks', '-d', 'time,{0},{1}'.format(start_index, end_index), '-O', input_file, output_file])
# think about directory structure
# parameters to pass in: threshold, model, time period, start year, end year, scenario? look at file names to figure out

# need to update this to make it consistent with the no_analog analysis below, i.e. using scenarios, etc. as passed in parameters
def threshold_days_analysis(models, hi_thresholds):
    path_to_raw_data_files = '/Users/kristinadahl/Desktop/heat_data/test_historical/'
    #get list of file names
    files_to_analyze = glob.glob1(path_to_raw_data_files, '1995_macav2metdata*%s*_daily.nc' %(model))
    print files_to_analyze
    number_of_inputs = len(files_to_analyze)
    outputs = []
    for file in files_to_analyze:
        infile = path_to_raw_data_files + file
        print 'infile is: ' + infile
        outfile_with_flag = os.path.splitext(infile)[0] + '_with_flags_120718.nc'
        print 'outfile_with_flag is: ' + outfile_with_flag
        for hi_threshold in hi_thresholds:
            print 'starting process for hi_threshold = %s' %(hi_threshold)
            outfile_number_of_days = os.path.splitext(infile)[0] + '_days_above_%s_120718.nc' %(hi_threshold)
            print 'outfile_number_of_days is: ' + outfile_number_of_days
            print(datetime.datetime.now().time())
            subprocess.Popen(["bash","/Users/kristinadahl/PycharmProjects/heat2/nco_days_above_threshold.sh", str(hi_threshold), infile, outfile_with_flag, outfile_number_of_days]).wait() # add str(hi_threshold) as parameter to get passed to script
            print(datetime.datetime.now().time())
            outputs.append(infile)

# scenario is historical, rcp45, or rcp85; time_period is historical, mid-century, or late-century
def no_analog_days_analysis(models,scenarios,time_periods):
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
                    start_year = '1976'
                    end_year = '2005'
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
                    print file_start_year
                    if int(file_start_year) >= int(start_year):
                        if int(file_start_year) < int(end_year):
                            print 'Analyzing ' + file
                            infile = path_to_hi_data_files + file
                            print 'infile is: ' + infile
                            outfile_with_flag = path_to_days_results_files + 'no_analog_flag_' + file
                            print 'outfile_with_flag is: ' + outfile_with_flag
                            print(datetime.datetime.now().time())
                            print 'starting process for no_analog days'
                            outfile_number_of_days = path_to_days_results_files + 'total_no_analog_days_' + file
                            print outfile_number_of_days
                            subprocess.Popen(["bash","/Users/kristinadahl/PycharmProjects/heat2/nco_no_analog_days.sh", infile, outfile_with_flag, outfile_number_of_days]).wait()
                            print(datetime.datetime.now().time())

# Calculates the annual average number of days above a specified threshold (including no_analog) for total days chunks calculated in no_analog_days_analysis or threshold_days_analysis methods
# threshold options are: no_analog, 100, or 105
def calculate_annual_average_days(models, scenarios, time_periods, threshold):
    for model in models:
        path_to_days_results_files = '/Volumes/hot_drive/heat_data/{0}/days_results/' .format(model)
        #path_to_days_results_files = '/Users/kristinadahl/Desktop/heat_data/test_abatz/'
        for scenario in scenarios:
            for time_period in time_periods:
                if time_period == 'historical':
                    start_year = '1976'
                    end_year = '2005'
                if time_period == 'mid_century':
                    start_year = '2036'
                    end_year = '2065'
                if time_period == 'late_century':
                    start_year = '2070'
                    end_year = '2099'
                files_to_analyze = glob.glob1(path_to_days_results_files,
                                              'total_{0}_days_macav2metdata_{1}_r1i1p1_{2}_hi_*.nc'.format(threshold, model, scenario))
                print 'Full file list to analyze: '
                print files_to_analyze
                ncea_arguments = ['ncea']
                for file in files_to_analyze:
                    file_start_year = file[-12:-8]
                    file_end_year = file[-7:-3]
                    print file_start_year
                    if int(file_start_year) >= int(start_year):
                        if int(file_start_year) < int(end_year):
                            print 'Analyzing ' + file
                            infile = path_to_days_results_files + file
                            annual_average_outfile = path_to_days_results_files + 'annual_average_{0}_days_macav2metdata_{1}_r1i1p1_{2}_hi_{3}_{4}.nc' .format(threshold, model, scenario, file_start_year, file_end_year)
                            time_period_average_outfile = path_to_days_results_files + '{0}_average_{1}_days_macav2metdata_{2}_r1i1p1_{3}_hi_{4}_{5}.nc' .format(time_period, threshold, model, scenario, start_year, end_year)
                            # get number of years over which to average from file start and end years
                            number_of_years = int(file_end_year) - int(file_start_year) + 1
                            print number_of_years
                            number_of_years = str(number_of_years)
                            subprocess.Popen(["bash","/Users/kristinadahl/PycharmProjects/heat2/calculate_annual_average_days.sh", threshold, number_of_years, infile, annual_average_outfile, time_period_average_outfile]).wait()
                            print 'Calculated annual average number of days at or above {0} and updated attributes' .format(threshold)
                        ncea_arguments.append(annual_average_outfile)
                ncea_arguments.append('-O')
                ncea_arguments.append(time_period_average_outfile)
                print ncea_arguments
                # calculate 30 year mean annual number of days above threshold
                subprocess.call(ncea_arguments)

                #subprocess.call(['ncea','/Volumes/hot_drive/heat_data/bcc-csm1-1/days_results/annual_average_no_analog_days_macav2metdata_bcc-csm1-1_r1i1p1_rcp85_hi_2070_2075.nc', '/Volumes/hot_drive/heat_data/bcc-csm1-1/days_results/annual_average_no_analog_days_macav2metdata_bcc-csm1-1_r1i1p1_rcp85_hi_2076_2085.nc', time_period_average_outfile])






full_models_list = ['bcc-csm1-1','bcc-csm1-1-m','BNU-ESM','CanESM2','CNRM-CM5','CSIRO-Mk3-6-0','GFDL-ESM2M','GFDL-ESM2G','HadGEM2-ES365','HadGEM2-CC365','inmcm4','IPSL-CM5A-LR','IPSL-CM5A-MR','IPSL-CM5B-LR','MIROC5','MIROC-ESM','MIROC-ESM-CHEM','MRI-CGCM3']

#models_list = ['bcc-csm1-1-m','BNU-ESM','CanESM2','CNRM-CM5','CSIRO-Mk3-6-0','GFDL-ESM2M','GFDL-ESM2G','HadGEM2-ES365','HadGEM2-CC365','inmcm4','IPSL-CM5A-LR','IPSL-CM5A-MR','IPSL-CM5B-LR','MIROC5','MIROC-ESM','MIROC-ESM-CHEM','MRI-CGCM3']

#download_files_from_nkn(models_list)


#hyperslab_files_to_get_desired_time_blocks(full_models_list,['historical','rcp45','rcp85'],['historical','late_century'])
#no_analog_days_analysis(['bcc-csm1-1'],['historical','rcp45','rcp85'],['historical','mid_century','late_century'])
#calculate_annual_average_days(['bcc-csm1-1'],['rcp85'],['late_century'],'no_analog')
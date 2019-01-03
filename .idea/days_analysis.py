import os
import subprocess
import datetime
import glob
import urllib2
import gzip
import shutil
from bs4 import BeautifulSoup

def download_files_from_nwk(models):
    response = urllib2.urlopen('https://climate.northwestknowledge.net/ACSL/HEAT/')
    soup = BeautifulSoup(response,"html.parser")
    list_of_files_to_download = []
    for a in soup.findAll("a",href=True):
        print a.text
        list_of_files_to_download.append(a.text)

    for model in models:
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

# think about directory structure
# parameters to pass in: threshold, model, time period, start year, end year, scenario? look at file names to figure out

# need to update this to make it consistent with the no_analog analysis below, i.e. using scenarios, etc. as passed in parameters
def historical_days_analysis(models, hi_thresholds):
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
                files_to_hyperslab = glob.glob1(path_to_hi_data_files, 'macav2metdata*{0}_r1i1p1_{1}_hi_{2}_{3}.nc' .format(model, scenario, original_start_year, end_year))
                for file_to_hyperslab in files_to_hyperslab:
                    input_file = path_to_hi_data_files + file_to_hyperslab
                    output_file = path_to_hi_data_files + 'macav2metdata_{0}_r1i1p1_{1}_hi_{2}_{3}.nc' .format(model, scenario, new_start_year, end_year)
                    subprocess.call(['ncks', '-d', 'time,{0},{1}' .format(start_index, end_index), input_file, output_file])


# scenario is historical, rcp45, or rcp85; time_period is historical, mid-century, or late-century
def no_analog_days_analysis(models,scenarios,time_periods):
    for model in models:
        print model
        #path_to_hi_data_files = '/Volumes/hot_drive/heat_data/{model}' .format(model)
        path_to_hi_data_files = '/Users/kristinadahl/Desktop/heat_data/test_abatz/{0}/'.format(model)
        path_to_days_results_files = '/Volumes/hot_drive/heat_data/{0}/days_results/' .format(model)
        for scenario in scenarios:
            print scenario

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

                files_to_analyze = glob.glob1(path_to_hi_data_files, 'macav2metdata*{0}_*_{1}_hi_*.nc' .format(model, scenario))
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


#historical_days_analysis('bcc-csm1-1',['105'])

# not sure this is necessary
def calculate_model_average(models, year_range, month_or_season,hi_thresholds):
    path_to_hi_data_files = 'Users/kristinadahl/Desktop/heat_data/XXX/'
    if year_range == 'historical':
        start_year = 'XXX'
        end_year = 'XXX'
    elif year_range == 'mid_century':
        start_year = 'XXX'
        end_year = 'XXX'
    elif year_range == 'late_century':
        start_year = 'XXX'
        end_year = 'XXX'

    for model in models:
        for hi_threshold in hi_thresholds:
            for year in years: # this will change depending on how hi files are structured (individual years vs.
                files_to_average = path_to_hi_data_files + 'use string substition for model and year range to get files'
#models_list = ['bcc-csm1-1','bcc-csm1-1-m','BNU-ESM','CanESM2','CNRM-CM5','CSIRO-Mk3-6-0','GFDL-ESM2M','GFDL-ESM2G','HadGEM2-ES365','HadGEM2-CC365','inmcm4','IPSL-CM5A-LR','IPSL-CM5A-MR','IPSL-CM5B-LR','MIROC5','MIROC-ESM','MIROC-ESM-CHEM','MRI-CGCM3']

#download_files_from_nwk(['bcc-csm1-1'])

#no_analog_days_analysis(['bcc-csm1-1-m'],['rcp85'],['mid_century'])
hyperslab_files_to_get_desired_time_blocks(['bcc-csm1-1'],['historical'],['historical'])
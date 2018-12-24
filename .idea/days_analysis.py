import os
import subprocess
import datetime
import glob

# think about directory structure
# parameters to pass in: threshold, model, time period, start year, end year, scenario? look at file names to figure out

def historical_days_analysis(model, hi_thresholds):
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
            subprocess.Popen(["bash","/Users/kristinadahl/Desktop/heat_data/nco_days_above_threshold.sh", str(hi_threshold), infile, outfile_with_flag, outfile_number_of_days]).wait() # add str(hi_threshold) as parameter to get passed to script
            print(datetime.datetime.now().time())
            outputs.append(infile)

        # if number_of_inputs != len(outputs):
        #     print 'Trouble in River City!'


#historical_days_analysis('bcc-csm1-1',['105'])

# not sure this is necessary
def calculate_model_average(models, year_range, month_or_season,hi_thresholds):
    path_to_hi_data_files = 'Users/kristinadahl/Desktip/heat_data/XXX/'
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
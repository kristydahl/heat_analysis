# this function is called from the python historical_days_analysis method in days_analysis.py $1 is threshold temperature, $2 is the input file, $3 is the output file with flag, $4 is the output file showing number of days above threshold. This script presumes that one year of data is getting read in at a time. Depending on how JA delivers the data, it may make sense to hyperslab his output to the years of interest, use this script to calculate days above threshold over that period of years, and average using an end_year - start_year + 1 sort of calculation, as in: (total_days_above_XXX) / (end_year - start_year + 1) to get the average number of days above the threshold ove the period of interest
 
function days_above_threshold(){
	#chunking="--cnk_csh=5000000000 --cnk_plc=nco --cnk_dmn=time,366 --cnk_dmn=lat,585 --cnk_dmn=lon,1386"

	echo 'starting nco calculations'
	
	echo $1
	# create binary flag variable and populate where appropriate; will need to alter slightly to get sum of FH days, as they will be flagged with multiple values 1-4 (I think)
	ncap2 -s "flag_above_$1=(hi>$1)||(hi>=2&&hi<=4)" -O $2 $3
	ncatted -a description,flag_above_$1,o,c,'Maximum daily heat index falls above specified threshold' $3
	ncatted -a standard_name,flag_above_$1,o,c,'Day above heat threshold' $3
	ncatted -a long_name,flag_above_$1,o,c,'Day above heat index threshold' $3
	ncatted -a units,flag_above_$1,o,c,'days' $3
	echo 'created file with flags'
	
	# calculate total days above threshold
	ncap2 -s "total_above_$1_days=flag_above_$1.total(\$time)" -O $3 $3 
	ncatted -a description,total_above_$1_days,o,c,'Total days above specified heat index threshold' $3
	ncatted -a standard_name,total_above_$1_days,o,c,'Days above threshold' $3
	ncatted -a long_name,total_above_$1_days,o,c,'Days above threshold' $3
	ncatted -a units,total_above_$1_days,o,c,'days' $3
	echo 'calculated total days'
	
	# write total days above threshold to new file
	ncks -v total_above_$1_days -O $3 $4
	echo 'copied total days to new file'
}

days_above_threshold "$1" "$2" "$3" "$4"
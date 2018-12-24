# this function is called from the python historical_days_analysis method in days_analysis.py $1 is threshold temperature, $2 is the input file, $3 is the output file
 
function days_above_threshold(){
	chunking="--cnk_csh=5000000000 --cnk_plc=nco --cnk_dmn=time,366 --cnk_dmn=lat,585 --cnk_dmn=lon,1386"

	echo 'starting nco calculations'
	
	echo $1
	# create binary flag variable and populate where appropriate
	ncap2 -s "flag_$1=(hi>$1)" $chunking -O $2 $3
	echo 'created file with flags'
	
	# calculate total days above threshold
	ncap2 -s "total_days_above_$1=flag_$1.total(\$time)" $chunking -O $3 $3 
	echo 'calculated total days'
	
	# write total days above threshold to new file
	ncks -v total_days_above_$1 -O $3 $4 # working with hard coding; need to pass in var
	echo 'copied total days to new file'
}

days_above_threshold "$1" "$2" "$3" "$4"
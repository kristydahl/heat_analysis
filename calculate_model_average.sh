# this function calculates the average number of days above threshold XXX for all of the years in a specified time period for a specified model. If/else statements allow for the calculation either over a specific month (e.g. July) or over the entire warm season (annual)

function model_average(){

	echo 'starting nco calculations'
	
	# create binary flag variable and populate where appropriate

	
	# calculate total days above threshold
	ncap2 -s "total_days_above_$1=flag_$1.total(\$time)" $chunking -O $3 $3 
	echo 'calculated total days'
	
	# write total days above threshold to new file
	ncks -v total_days_above_$1 -O $3 $4 # working with hard coding; need to pass in var
	echo 'copied total days to new file'
}

model_average # parameters will include: model, time period (years), time period (month vs. warm season ave) threshold, 
#days_above_threshold "$1" "$2" "$3" "$4"
function no_analog_days(){

	echo 'starting nco calculations'
	
	# create binary flag variable and populate where hi values are 2, 3,or 4 (these are the no-analog values)
	ncap2 -s "flag_no_analog=(hi>=2&&hi<=4)" -O $1 $2
	ncatted -a description,flag_no_analog,o,c,'Values outside of Steadman (1979) heat index formulation flagged with value of 1' $2
	ncatted -a standard_name,flag_no_analog,o,c,'No analog day' $2
	ncatted -a long_name,flag_no_analog,o,c,'No analog day' $2
	ncatted -a units,flag_no_analog,o,c,'days' $2
	echo 'created file with flags'
	
	# calculate total days above threshold
	ncap2 -s "total_no_analog_days=flag_no_analog.total(\$time)" -O $2 $2
	ncatted -a description,total_no_analog_days,o,c,'Total days outside Steadman (1979) heat index formulation' $2
	ncatted -a standard_name,total_no_analog_days,o,c,'No analog days' $2
	ncatted -a long_name,total_no_analog_days,o,c,'No analog days' $2
	ncatted -a units,total_no_analog_days,o,c,'days' $2
	echo 'calculated total days'
	
	# write total days above threshold to new file
	ncks -v total_no_analog_days -O $2 $3
	echo 'copied total days to new file'
}	
	
no_analog_days "$1" "$2" "$3"
# this function is called from the python historical_days_analysis method in days_analysis.py $1 is threshold temperature, $2 is the input file, $3 is the output file
 
function days_above_threshold(){

	echo 'starting cdo timsum'
	cdo -f nc -timsum -gec,$1 $2 $3
	echo 'cdo timsum done'
}

days_above_threshold "$1" "$2" "$3"
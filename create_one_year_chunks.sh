# This function hyperslabs historical netcdf files, which generally contain 5 years worth of data, into 1 year chunks.

# inputs: $1 is year, $2 is input file, $3 is output file
function hyperslab_by_year(){
	echo 'starting hyperslabbing'
	#ncks -d time,$1-04-01,$1-10-31 $2 $3
	ncks -4 -L 5 -d time,$1-04-01,$1-10-31 $2 $3
	echo 'hyperslabbing done for one year'
}

hyperslab_by_year "$1" "$2" "$3"
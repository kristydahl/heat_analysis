# This function hyperslabs historical netcdf files, which generally contain 5 years worth of data, into 1 year chunks. The one year chunks then get processed by cdo timsum

function hyperslab_by_year(){
	echo 'starting hyperslabbing'
	ncks -d time,$1-01-01,$1-12-31 $2 $3
	echo 'hyperslabbing done for one year'
}

hyperslab_by_year "$1" "$2" "$3"
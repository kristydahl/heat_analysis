# this function calculates the average number of days above threshold XXX for all of the years in a specified time period for a specified model. If/else statements allow for the calculation either over a specific month (e.g. July) or over the entire warm season (annual)

function annual_average_days(){
	ncap2 -s "annual_average_$1_days=(total_$1_days)/$2" -O $3 $4

}

annual_average_days "$1" "$2" "$3" "$4" # parameters will include: model, time period (years), time period (month vs. warm season ave) threshold, 
#days_above_threshold "$1" "$2" "$3" "$4"
create external table yelp_taxi_stats(
	grid_id int, 
	pickups int, 
	dropoffs int,
	tip float,
	number_of_restaurants int,
	total_review_cnts int,
	sum_of_ratings float
) row format delimited fields terminated by ','
  location '/user/stn223/output';
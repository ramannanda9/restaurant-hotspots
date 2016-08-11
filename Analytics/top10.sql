select 
40.7 + 0.002*cast(grid_id*1.0/120 as int) as lat, 
-74.02 + 0.001*cast(grid_id%120 as int) as lon, 
cast(pickups/365.0 as double) as pickups, cast(dropoffs/365.0 as double) as dropoffs, cast(tip/365.0 as double) as tip, 
cast(total_review_cnts*1.0/number_of_restaurants as double) as avg_review, 
cast(sum_of_ratings*1.0/number_of_restaurants as double) as avg_ratings 
from yelp_taxi_stats 
order by pickups desc, dropoffs desc, tip desc
limit 10;
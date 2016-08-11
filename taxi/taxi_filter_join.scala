import org.apache.spark.sql.SQLContext

// hadoop fs -put yelp_data_clean.csv taxi/.
// 

val taxi = sc.textFile("taxi/yellowjune.csv")

var header = taxi.first()

// header
// res45: String = 0 VendorID,
				// 1	tpep_pickup_datetime,
				// 2	tpep_dropoff_datetime,
				// 3	passenger_count,
				// 4	trip_distance,
				// 5	pickup_longitude,
				// 6	pickup_latitude,
				// 7	RateCodeID,
				// 8	store_and_fwd_flag,
				// 9	dropoff_longitude,
				// 10	dropoff_latitude,
				// 11	payment_type,
				// 12	fare_amount,
				// 13	extra,
				// 14	mta_tax,
				// 15	tip_amount,
				// 16	tolls_amount,
				// 17	improvement_surcharge,
				// 18	total_amount


// Removing header
val taxi_wh = taxi.filter(row => row != header)

// Calculating Number of Pickups for each grid
val taxi_night_pick = taxi_wh.map(_.split(",")).filter(row => row(1).substring(11,13).toInt <= 3 || row(1).substring(11,13).toInt >= 22).filter(r=> r(5).toDouble >= -74.02 && r(5).toDouble <= -73.9).filter(r => r(6).toDouble >= 40.7 && r(6).toDouble <= 40.85 )

val taxi_grid_pick= taxi_night_pick.map(r => ( ((r(6).toDouble - 40.7 )/0.002).toInt *120 + ((r(5).toDouble + 74.02 )/0.001).toInt , 1)).reduceByKey(_+_)

// taxi_grid_pick.take(5).foreach(println)
// (1110,7204)                                                                                                                                                              
// (6915,175)
// (3630,1789)
// (1260,267)
// (8265,1)

// Calculating Number of Dropoffs for each grid
val taxi_night_drop = taxi_wh.map(_.split(",")).filter(row => row(1).substring(11,13).toInt <= 3 || row(1).substring(11,13).toInt >= 22).filter(r=> r(9).toDouble >= -74.02 && r(9).toDouble <= -73.9).filter(r => r(10).toDouble >= 40.7 && r(10).toDouble <= 40.85 )

val taxi_grid_drop= taxi_night_pick.map(r => ( ((r(10).toDouble - 40.7 )/0.002).toInt *120 + ((r(9).toDouble + 74.02 )/0.001).toInt , 1)).reduceByKey(_+_)

// 	
// (1110,3251)                                                                                                                                                              
// (3525,1090)
// (9690,73)
// (1410,13)
// (7530,4)


// Calculating the total fare amount of Dropoffs for each grid
val taxi_night_fare = taxi_wh.map(_.split(",")).filter(row => row(1).substring(11,13).toInt <= 3 || row(1).substring(11,13).toInt >= 22).filter(r=> r(9).toDouble >= -74.02 && r(9).toDouble <= -73.9).filter(r => r(10).toDouble >= 40.7 && r(10).toDouble <= 40.85 ).filter(r(12).toFloat > 0.0 )

val taxi_grid_fare= taxi_night_pick.map(r => ( ((r(10).toDouble - 40.7 )/0.002).toInt *120 + ((r(9).toDouble + 74.02 )/0.001).toInt , r(12).asFloat )).reduceByKey(_+_)

// taxi_grid_fare.take(5).foreach(println)
// (1110,34968.3)                                                                                                                                                           
// (3525,9503.609)
// (9690,1893.5)
// (1410,491.5)
// (14055,61.5)


// JOIN => gird id, drop offs, pickups, fareamount
val taxi_grid_stats = taxi_grid_drop.join(taxi_grid_pick).join(taxi_grid_fare)


// YELP
val yelpdata = sc.textFile("taxi/yelp_data_clean.csv")

// Header
var headerYelp = yelpdata.first()
// headerYelp: String = review,rating,lat,long

// Removing header
var yelp_noh = yelpdata.filter(row => row != headerYelp)

val yelp_grid_manhattan = yelp_noh.map(_.split(",")).filter(r=> r(3).toDouble >= -74.02 && r(3).toDouble <= -73.9).filter(r => r(2).toDouble >= 40.7 && r(2).toDouble <= 40.85 ).map(r => ( ((r(2).toDouble - 40.7 )/0.002).toInt *120 + ((r(3).toDouble + 74.02 )/0.001).toInt , (1, r(0).toInt, r(1).toFloat) ))

// yelp_grid_manhattan.take(10).foreach(println)
// (981,(1,216,3.5))
// (669,(1,82,4.0))
// (781,(1,64,4.0))
// (1816,(1,1025,4.0))
// (2548,(1,50,3.0))
// (1819,(1,129,4.0))
// (2681,(1,536,3.5))
// (1710,(1,1124,3.5))
// (1929,(1,42,3.5))
// (2550,(1,191,4.0))

//Yelp Stats for each grid : Number of Places, Total Review Cnt, Sum of Ratings
val yelp_grid_stats = yelp_grid_manhattan.reduceByKey((p1, p2) => (p1._1 + p2._1, p1._2 + p2._2, p1._3 + p2._3 ))
// yelp_grid_stats: org.apache.spark.rdd.RDD[(Int, (Int, Int, Float))] = ShuffledRDD[99] at reduceByKey at <console>:37

yelp_grid_stats.take(10).foreach(println)
// (778,(3,939,11.5))
// (1110,(9,1827,34.5))
// (3764,(11,3953,38.5))
// (4718,(3,498,11.0))
// (3272,(1,103,3.0))
// (4374,(1,359,4.0))
// (6308,(1,411,3.5))
// (1330,(3,567,12.5))
// (1350,(10,2777,39.5))
// (5956,(1,9,4.0))

//JOIN Taxi + Yelp Stats

val taxi_yelp_grid = taxi_grid_stats.join(yelp_grid_stats)

taxi_yelp_grid.saveAsTextFile("taxi/june_stats.txt")

// taxi_yelp_grid.take(10).foreach(println)
// (1110,((3251,7204),(9,1827,34.5)))
// (6915,((410,175),(1,170,4.0)))
// (3630,((772,1789),(13,5548,49.5)))
// (2670,((1280,1661),(4,1185,16.5)))
// (795,((215,54),(3,160,12.5)))
// (5325,((1271,518),(3,2046,12.0)))
// (1740,((42,9),(2,307,7.0)))
// (4755,((854,423),(1,5,3.5)))
// (1350,((2500,1999),(10,2777,39.5)))
// (4965,((1090,1753),(1,128,2.5)))

// (grid_id, ((dropoff_cnt, pickup_cnts), (number of places, total review cnts, sum of ratings)



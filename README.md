# restaurant-taxi-hotspots
In this project we tried to ascertain the relationship between yelp ratings, taxi pickups and dropoffs with the tip_amount.
The spatial join is performed with yelp data, nyc_liquor_dataset and taxi dataset for yellow cabs.

The main table after the join is yelp_taxi_stats and the script to join the dataset is located under taxi folder.
Its structure and schema is mentioned below.

+---------------------+-----------
|grid_id              |    int
|pickups              |    int
|dropoffs             |    int
|tip                  |    float
|number_of_restaurants|    int
|total_review_cnts    |    int
|sum_of_ratings       |    float
----------------------+-----------

We then ran regressions against the average tip_amount by using features such as pickups, average tip, average rating, average review_counts.

First let's see the summary statistics of the data.

+-------+------------------+------------------+-------------------+------------------+-------------------+
|summary|           pickups|          dropoffs|                tip| total_review_cnts|     sum_of_ratings|
+-------+------------------+------------------+-------------------+------------------+-------------------+
|  count|              1294|              1294|               1294|              1294|               1294|
|   mean|13271.180061823803|18587.781298299844| 0.7939889641348215|215.88824594000207| 3.7131400489882664|
| stddev| 8348.114237681642|              null|0.46379318419400173| 255.6623636458724|0.47847556938542746|
|    min|                38|                19|0.08149456589488731|               1.0|                1.0|
|    max|            111093|            191337| 3.2357967210852583|            3459.0|                5.0|
+-------+------------------+------------------+-------------------+------------------+-------------------+

And the result of regressions

* R-Squared= 0.3553494656386954
* Explained Variance=0.371849060866309
* MAE= 0.28527250832848805

Feature weights
+-----------------------+---------------------------+
|pickups                | 9.109559958325188E-6      |
|dropoffs               | -1.555492212643803E-5     |
|average_review_cnts    | -2.3107553115783453E-4    |
|average_of_ratings     | 0.09289409463571735       |
+-----------------------+---------------------------+                                 

As we can see that average_of_ratings has a positive coefficient and is the highest weight in determining the tip amount.

* To run the code just copy paste ``HotspotAnalytics.scala`` and then invoke the method ``HotspotAnalytics.runRegressions()``
* You need to ensure to run the table scripts and that the data is there in the hive table.
* Visualizations showing the top 10 hotspots is top10.png and the data used to produce that is also there in the top directory.
* We used hue for Visualizations.

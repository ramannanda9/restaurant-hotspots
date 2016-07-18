create external table nyc_taxi_data(
  VendorID int, tpep_pickup_datetime string, tpep_dropoff_datetime string, passenger_count int,trip_distance double,pickup_longitude double,pickup_latitude double,RateCodeID int,store_and_fwd_flag string ,dropoff_longitude double,dropoff_latitude double,payment_type int,fare_amount double,extra double,mta_tax double,tip_amount double,tolls_amount double,improvement_surcharge double,total_amount double)
  row format delimited fields terminated by ','
  location '/user/cloudera/restaurant_hotspots/nyc_taxi_data';
--in impala
compute stats nyc_taxi_data;


create external table nyc_liquor_licenses(
LICENSE_S_NO bigint, LICENSE_TYPE_NAME string, LICENSE_CLASS_CODE int, LICENSE_TYPE_CODE string, ZONE_OFFICE_NAME  string, ZONE_OFFICE_NUMBER int, COUNTY_NAME string, PREMISIS_NAME string, DBA string, ADDRESS_LINE_1 string, ADDRESS_LINE_2 string,   CITY string, STATE string, ZIP int, LICENSE_CERTIFICATE_NO string,  LICENSE_ISSUE_DATE string, LICENSE_EFF_DATE string,
LICENSE_EXP_DATE string, LATITUDE double, LONGITUDE double, LOCATION_STRING string, address string
)
row format delimited fields terminated by ',' escaped by '"'
location '/user/cloudera/restaurant_hotspots/liquor_dataset';

--in impala
  compute stats nyc_liquor_licenses;

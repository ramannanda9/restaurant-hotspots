#!/bin/bash

arr1=("01" "02" "03" "04" "05" "06" "07" "08" "09" "10" "11" "12")
arr2=("green" "yellow")

for i in ${arr1[@]}; do
	for j in ${arr2[@]}; do  
		tail -n +2 "$j"_tripdata_2015-"$i".csv > "$j"_2015-"$i".csv
	done
done
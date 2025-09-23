#!/bin/bash

hadoop fs -rm -r -f /Output/clean_trips || true
hadoop fs -rm -r -f /Output/enriched_trips || true
hadoop fs -rm -r -f /Output/company_stats || true
hadoop fs -rm -r -f /Output/fare_bands_by_company || true

pig -x mapreduce a2.pig

hadoop fs -cat /Output/clean_trips/part* > t1_output.txt
hadoop fs -cat /Output/enriched_trips/part* > t2_output.txt
hadoop fs -cat /Output/company_stats/part* > t3_output.txt
hadoop fs -cat /Output/fare_bands_by_company/part* > t4_output.txt

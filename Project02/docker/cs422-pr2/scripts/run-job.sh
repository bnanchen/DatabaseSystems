#!/bin/bash

$SPARK_HOME/bin/spark-submit --class Main --master spark://master:7077 /root/jars/samples/cs422-test_2.11-0.1.0.jar /stream_input 5 5 precise /sample-results #/samples /sample-results

#./../spark−2.2.1−bin−hadoop2.7/bin/spark−submit −−class streaming.Main ./jars/cs422−project2_2.11−0.1.0.jar

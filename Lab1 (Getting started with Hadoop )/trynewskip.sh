#!/bin/bash
hdfs dfs -rm /user/Nikita/skip.txt
hdfs dfs -put skip.txt /user/Nikita/skip.txt
hdfs dfs -rm -r /user/Nikita/output-alice-skip
hadoop jar wordcount2.jar WordCount2 alice.txt output-alice-skip -skip skip.txt -D wordcount.case.sensitive=false

#!/bin/bash
javac JaccardMap.java -cp $(hadoop classpath) -Xlint:deprecation -Xlint:unchecked
jar cf jm.jar JaccardMap*.class
hadoop jar jm.jar JaccardMap /input/s1.txt /input/s2.txt /output
hadoop fs -cat /output/part-r-00000 | sort

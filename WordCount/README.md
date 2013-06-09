# WordCount
============

The Hello World of Map Reduce is a simple word counting job. To run this job you must first have hadoop installed on your machine. Installation instructions can be found herehttp://hadoop.apache.org/docs/stable/single_node_setup.html

Instructions to build and run the word count MR job

1. cd to the WordCount directory
2. load the data we'll be working with into HDFS by running the command 'hadoop fs -put ./data/mobydick /data/mobydick'
3. next build the map reduce jar with maven by executing the command 'mvn clean install' from the WordCount directory
4. finally start the map reduce job with the jar that was created in the target directory by entering the command 'hadoop jar target/WordCount-0.0.1-SNAPSHOT-jar-with-dependencies.jar /data/mobydick /results/mobydick"
5. You can view the results in hdfs with the command 'hadoop fs -cat /results/mobydick/part-r-00000'

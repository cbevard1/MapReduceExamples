#StudentAnalyticCompositeKey
=============================

In this example we are working with data in the format of a comma delimited file. The data is basic information about students such as age, grade, gender and their ranking of looks, sports, money, and grades based on importance. This map reduce job we're going to build a composite key in the format of school:grade:gender:most_important_factor similar to the format of a key you might see in a distributed database such as HBase or Accumulo.

Sample output:
- Brentwood Elementary:5:girl :Looks  10
- Brentwood Elementary:5:girl :Sports	3
- Brentwood Middle:5:boy :Looks	1
- Brentwood Middle:6:boy :Grades	9

Instructions to build and run this MR job

1. cd to the StudentAnalyticCompositeKey directory
2. load the data we'll be working with into HDFS by running the command 'hadoop fs -put ./data/popular /data/popular'
3. next build the map reduce jar with maven by executing the command 'mvn clean install' from the StudentAnalyticCompositeKey directory
4. finally start the map reduce job with the jar that was created in the target directory by entering the command 'hadoop jar target/composite-key-0.0.1-SNAPSHOT-jar-with-dependencies.jar /data/popular /results/compositeKey"
6. You can view the results in hdfs with the command 'hadoop fs -cat /results/compositeKey/part-r-00000'

#StudentAnalyticGroupByGender
=============================

In this example we are working with data in the format of a comma delimited file. The data is basic information about students such as age, grade, gender and their ranking of looks, sports, money, and grades based on importance. This map reduce job we're going to do a simple query to get the number of students in a specific school who ranked looks as being most important to them and grouping the results by gender.

Instructions to build and run this MR job

1. cd to the StudentAnalyticGroupByGender directory
2. load the data we'll be working with into HDFS by running the command 'hadoop fs -put ./data/popular /data/popular'
3. next build the map reduce jar with maven by executing the command 'mvn clean install' from the StudentAnalyticGroupByGender directory
4. finally start the map reduce job with the jar that was created in the target directory by entering the command 'hadoop jar target/StudentAnalyticGroupByGender-0.0.1-SNAPSHOT-jar-with-dependencies.jar /data/popular /results/popular"
6. You can view the results in hdfs with the command 'hadoop fs -cat /results/popular/part-r-00000'

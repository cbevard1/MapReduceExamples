#MovieCountGroupByDirector
==========================

This map reduce job works with unstructured data in the form of an xml file. The data we'll be working with is an xml dump of my XBMC video library, which contains movies and TV Shows. In this project we'll be doing a simple query to get only the movie directors, the number of movies they've directed and also the movie titles.

Example MR output: 
Director        Count   Titles
Steven Brill      2	Heavyweights, Drillbit Taylor
Steven Lisberger	1	TRON
Steven Spielberg	9	Catch Me If You Can, Saving Private Ryan, Indiana Jones and the Last Crusade, Indiana Jones and the Temple of Doom, Jurassic Park, The Lost World: Jurassic Park, Minority Report, Indiana Jones and the Kingdom of the Crystal Skull, Raiders of the Lost Ark

Instructions on how to build and run the project:
1. cd to the MovieCountGroupByDirector directory
2. load the data we'll be working with into HDFS by running the command 'hadoop fs -put ./data/videodb.xml /data/videodb.xml'
3. next build the map reduce jar with maven by executing the command 'mvn clean install' from the MovieCountGroupByDirector directory
4. finally start the map reduce job with the jar that was created in the target directory by entering the command 'hadoop jar target/MovieCountGroupByDirector-0.0.1-SNAPSHOT-jar-with-dependencies.jar /data/videodb.xml /results/movies"
You can view the results in hdfs with the command 'hadoop fs -cat /results/movies/part-r-00000'

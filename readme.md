run maven build

/Applications/Spark/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --class "gratinalfi.twitter.PrintTwitterStream" --master local[4]  target/twitter.stream-0.0.1-SNAPSHOT-jar-with-dependencies.jar 



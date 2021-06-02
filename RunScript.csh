javac -cp "lib/*" -d test_classes src/FlightDelay.java
jar cvf FlightDelay.jar -C test_classes/ .
spark-submit --class "FlightDelay.FlightDelay" --master local FlightDelay.jar /user/currieferg/FlightDelayData /user/currieferg/FlightDelayResult

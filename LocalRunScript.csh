javac -cp "lib/*" -d test_classes src/OnlineNewsPopularity.java
jar cvf ONP.jar -C test_classes/ .
spark-submit --class "ONP.OnlineNewsPopularity" --master local ONP.jar /user/currieferg/OnlineNewsPopResInp /user/currieferg/OnlineNewsPopRes

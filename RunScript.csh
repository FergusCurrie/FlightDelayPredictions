javac -cp "lib/*" -d test_classes src/OnlineNewsPopularity.java
jar cvf ONP.jar -C test_classes/ .
spark-submit --class "ONP.OnlineNewsPopularity" --master yarn --deploy-mode cluster ONP.jar /user/currieferg/OnlineNewsPopResInp /user/currieferg/OnlineNewsPopRes

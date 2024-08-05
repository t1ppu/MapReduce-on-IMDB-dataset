### MapReduce program for IMDb dataset
## Steps to execute:
1. Execute the java program
javac -classpath $(hadoop classpath) -d ./ imdb_mr.java

2. Generate a jar file
jar cf imdb_mr.jar -C ./ .

3. Upload the input files to HDFS
hadoop fs -mkdir /temp/input
hadoop fs -copyFromLocal /input/imdb00-title-actors.csv
hadoop fs -copyFromLocal /input/title.basics.tsv
hadoop fs -copyFromLocal /input/title.crew.tsv

4. Run the mapreduce job
hadoop jar imdb_mr.jar imdb_mr /temp/input/title.basics.tsv /temp/input/imdb00-title-actors.csv /temp/input/title.crew.tsv /temp/output

5. Copy the output file to local filesystem.
hadoop fs -get /temp/output/part-r-00000 ./
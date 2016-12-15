## Collaborative Filtering with Alternating Least Squares

There should be a "target" folder. If there is not, you must build the project (see below). There will be two .jar files in the target directory. One of those jar files will end in "jar-with-dependencies.jar". This is the jar you must use to run the following examples, not the other one.

Below the "Building" section are files with Main methods that you can execute on your own computer.

You must have Java installed to run classes from the jar. If you get `OutOfMemory` errors, add the `-Xmx10G` flag to the java commands below.

For any questions, email ssingal05@gmail.com.

### Building

In order to build the project, you must have Apache Maven installed. First, you must build the project:

```
mvn clean
mvn package
```

This will create a "target" folder. There will be two .jar files in the target directory. One of those jar files will end in "jar-with-dependencies.jar". This is the jar you must use to run the following examples, not the other one.

### Get Recommendations

`src/main/scala/com/sidd/alsfilter/MovieLensCF.scala`

Get suggestions for movies based on your own personal ratings from a given ratings file. Refer to data/movies.dat for movies you can get ratings for. Each line in the configuration file should represent a rating delimited by `::`. Refer to data/childrenratingsquery.dat for an example.

Run the file with the following command:

```
java -cp [jar] com.sidd.alsfilter.MovieLensCF <movies file> <ratings file> <query file> <num recommendations> <num processors>
```

As an example:

```
java -cp target/alsfiltering-1.0-SNAPSHOT-jar-with-dependencies.jar com.sidd.alsfilter.MovieLensCF data/movies.dat data/ratings.dat data/childrenratingsquery.dat 20 8
```

Mandatory Command line arguments:
* movies file: The location of the MovieLens movies.dat file
* ratings file: The location of the MovieLens ratings.dat file
* query file: The location of a user input vector of movie preferences
* num recommendations: The number of recommendations to output at the end
* num processors: The number of processors to parallelize this program with

### Get Execution Times

`src/main/scala/com/sidd/alsfilter/MovieLensCFTimingAnalysis.scala`

Get execution times for all combinations of given (comma separated) processors, data fractions, and k's. This will output to an external file

A sample output file is in `out/timing.csv`

Run the file with the following command:

```
java -cp [jar] com.sidd.alsfilter.MovieLensCFTimingAnalysis <ratings file> <output file> <processor attempts> <data fractions> <ks>
```

As an example:

```
java -cp target/alsfiltering-1.0-SNAPSHOT-jar-with-dependencies.jar com.sidd.alsfilter.MovieLensCFTimingAnalysis data/ratings.dat out/timing2.csv 1,2,4,8,16 .2,.4,.6,.8,1.0 5,10,20
```

Command line arguments:
* ratings file: The location of the MovieLens ratings.dat file
* output file: The location to output a CSV of execution times based on parameters
* processor numbers: Comma separated value of different processor values to get an analysis for
* data fractions: Comma separated value of different fractions of workloads to get an analysis for. Cuts the number of users to analyze.
* ks: Comma separated value of different k values to get an analysis for. k is the rank of the decomposed matrices

### Perform Validation

`src/main/scala/com/sidd/alsfilter/MovieLensCFTimingValidation.scala`

Perform validation of the algorithm by creating a training set and test set of users and removing some movie recommendations from the test set of users to see if they will get rerecommended with a high score. This will output to an external file.

A sample output file is in `out/validation.csv`

Run the file with the following command:

```
java -cp [jar] com.sidd.alsfilter.MovieLensCFValidation <ratings file> <output file> <training set fraction>
```

As an example:

```
java -cp target/alsfiltering-1.0-SNAPSHOT-jar-with-dependencies.jar com.sidd.alsfilter.MovieLensCFValidation  data/ratings.dat out/validation2.csv 0.90 20 1 10
```

Command line arguments:
* ratings file: The location of the MovieLens ratings.dat file
* output file: The location to output a CSV of movie recommendation ranks and their occurences
* training set fraction: what fraction of users belong to the training set
* k: rank of ALS matrices (20 is a safe number)
* lambda: regularization factor (1 is a safe number)
* iter: number of iterations to run ALS for
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]), 1))

results1 = movies.collect()

for result in results1:
    print(result)

movieCounts = movies.reduceByKey(lambda x, y: x + y)


results2 = movieCounts.collect()

for result in results2:
    print(result)

flipped = movieCounts.map( lambda xy: (xy[1],xy[0]) )

#flipped = movieCounts.map( lambda (x,y): (y,x ))

sortedMovies = flipped.sortByKey()

results = sortedMovies.collect()

for result in results:
    print(result)

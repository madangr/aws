from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("file:///SparkCourse/1800.csv")
parsedLines = lines.map(parseLine)
parsedlines2 =parsedLines.collect();

for result in parsedlines2:
    print(result[0] + "\t{:.2f}F".format(result[2])+ "\t"+result[1])


minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])

minstationTemps = minTemps.map(lambda x: (x[0], x[2]))
resultsA = minstationTemps.collect();

for result in resultsA:
    print(result[0] + "\t{:.2f}F".format(result[1]))

maxstationTemps = maxTemps.map(lambda x: (x[0], x[2]))
resultsB = maxstationTemps.collect();


for result in resultsB:
    print(result[0] + "\t{:.2f}F".format(result[1]))


minTemps = minstationTemps.reduceByKey(lambda x, y: min(x,y))

maxTemps = maxstationTemps.reduceByKey(lambda x, y: max(x,y))
results = minTemps.collect();


for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
    
results2 = maxTemps.collect();


for result in results2:
    print(result[0] + "\t{:.2f}F".format(result[1]))
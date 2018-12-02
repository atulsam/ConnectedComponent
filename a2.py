from pyspark import SparkConf, SparkContext
import sys
import time

def graphEdge(data):
    d = data.split(" ") 
    v = [int(x) for x in d]
    return [(v[0], v[1])]

def largeStar(v):
    m = v[0]
    for i in v[1]:
        if m > i:
            m = i
    list = []
    for i in v[1]:
        if i > v[0]:
            list.append((i,m))
    return list

def smallStar(v):
    m = v[0]
    for i in v[1]:
        if m > i:
            m = i
    list = []
    if(v[0] != m):
	list.append((v[0],m))
    for i in v[1]:
        if i < v[0] and i!=m:
            list.append((i,m))
    return list

if __name__ == "__main__":
    start = time.time()
    conf = SparkConf().setAppName("RDDcreate")
    sc = SparkContext(conf=conf)
    rdd = sc.textFile("graph1.txt").flatMap(graphEdge)
    #V = rdd.reduceByKey(lambda a, b: a + b)
    #n = V.count()
    #print(n)
    end = time.time()
    print("Init Time:",end - start)
    while True:
	start1 = time.time()
	#LargeStar Operation
	rdd = rdd.flatMap(lambda v:[(v[0],[v[1]]), (v[1],[v[0]])])
	rdd = rdd.reduceByKey(lambda a, b: a + b)
	rdd = rdd.flatMap(largeStar).distinct()
	lrdd = rdd
	n1 = lrdd.count()
	end1 = time.time()
	print("LargeStar Count:",n1)
	print("LargeStar Time:",end1 - start1)
	
	#SmallStar Operation
	start2 = time.time()
	rdd = rdd.flatMap(lambda v:[(v[0],[v[1]]), (v[1],[v[0]])])
	rdd = rdd.reduceByKey(lambda a, b: a + b)
	rdd = rdd.flatMap(smallStar).distinct()
	srdd = rdd
	n2 = srdd.count()
	end2 = time.time()
	print("SmallStar Count:",n2)
	print("SmallStar Time:",end2 - start2)

	#Comparing to RDDs by taking intersection and matching count of intersection with large and small star count
	if(n1 == n2):
	    start3 = time.time()
	    cdata = srdd.intersection(lrdd)
	    cdataCount = cdata.count()
	    end3 = time.time()
	    print("Comparision Time:",end3 - start3)
	    if(cdataCount == n1):
	        break
	print("#***************************************************#")

    #Saving output to the 'out' Folder
    start4 = time.time()
    rdd.coalesce(1).saveAsTextFile("out")
    end4 = time.time()
    print("Out Time:",end4 - start4)
    print("Total Time:",end4 - start)
    sc.stop()

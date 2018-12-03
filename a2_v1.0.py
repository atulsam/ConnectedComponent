from pyspark import SparkConf, SparkContext
import sys
import time

def graphEdge(data):
    d = data.split(" ") 
    v = [int(x) for x in d]
    #Testing for self Loop
    if(v[0] == v[1]):
	return []
    else:
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
    #print("vertex:",n)

    while True:
	rdd_old = rdd

	#LargeStar Operation
	rdd = rdd.flatMap(lambda v:[(v[0],v[1]), (v[1],v[0])]).distinct()
	rdd = rdd = rdd.flatMap(lambda v:[(v[0],[v[1]])])
	rdd = rdd.reduceByKey(lambda a, b: a + b)
	rdd = rdd.flatMap(largeStar).distinct()
	
	#SmallStar Operation
	rdd = rdd.flatMap(lambda v:[(v[0],v[1]), (v[1],v[0])]).distinct()
	rdd = rdd = rdd.flatMap(lambda v:[(v[0],[v[1]])])
	rdd = rdd.reduceByKey(lambda a, b: a + b)
	rdd = rdd.flatMap(smallStar).distinct()

	#testing for Convergence
	p = -1
	p = rdd_old.subtract(rdd).count()
	print("Compare Count:",p)
	if(p == 0):
	    break
	
    #Saving output to the 'output' Folder
    rdd.coalesce(1).saveAsTextFile("output")
    end = time.time()
    print("Total Time:",end - start)

    #Printing Number of connected components with label and number of vertices connected to it
    '''rdd = rdd.flatMap(lambda v: [(v[1],v[0])])
    vc = rdd.countByKey().items()
    print("Label\tcount")
    for l in vc:
	print(str(l[0])+"\t"+str(l[1]))'''
    sc.stop()

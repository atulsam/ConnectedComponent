import random
if __name__ == "__main__":
	for x in range(10000):
		lis1 = random.sample(xrange(0,1000), 2)
		s = str(lis1[0])+" "+str(lis1[1])
		print(s)

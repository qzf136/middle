
import time

def Five(iterator):
	for x in iterator:
		fivelist = []
		for i in range(0,5):
			fivelist.append(x)
		fivelist.sort()
		res = []
		res.append(fivelist[len(fivelist)//2])
	return res


def middlesOfPart(datalist):
	rdd = sc.parallelize(datalist, 3)
	middles = rdd.mapPartitions(Five).collect()
	return middles


def swap(lst, i, j):
	temp = lst[i]
	lst[i] = lst[j]
	lst[j] = temp


def partition(lst, x):
	a = lst.index(x)
	swap(lst, 0, a)
	n = len(lst)
	i = 0
	j = len(lst)-1
	while (True):
		while (lst[i] <= x and i < n):
			i += 1
		while (lst[j] > x and j >= 0):
			j -= 1
		if (i >= j):
			break
		swap(lst, i, j)
	swap(lst, 0, j)
	return j


def select(lst, k):
	if (len(lst) < 50):
		lst.sort()
		return lst[k]
	middles = middlesOfPart(lst)
	x = select(middles, len(middles)//2)
	index = partition(lst, x)
	if (k < index):
		return select(lst[0:index], k)
	else:
		return select(lst[index:len(lst)], k-index)


datardd = sc.textFile("hdfs://master:9000/data.txt")
datastrlist = datardd.collect()[0].split(',')
data = [int(x) for x in datastrlist]
t1 = time.time()
a = select(data, len(data)//2)
t2 = time.time()
print("我的函数")
print("中位数 " + str(a))
print("时间: " + str(t2-t1))

v1 = time.time()
v = np.median(data)
v2 = time.time()
print("numpy API")
print("中位数: " + str(v))
print("时间: " + str(v2-v1))
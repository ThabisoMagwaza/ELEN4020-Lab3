from mrjob.job import MRJob
import re
import os
import sys
import time


class onesStepMatrixMult(MRJob):
	
	def mapper(self,_,value):
		if len(value.split()) == 2:
			return
			
		row,col,val = value.split()
		matrixName = os.environ['map_input_file']
		
		if matrixName == "outA1.list":
			for c in range(0,k):
				yield (int(row),c),('M',int(col),int(val))
		else:
			for r in range(0,i):
				# j,k,val = i,j,val
				yield (r,int(col)),('N',int(row),int(val))
	
	def reducer(self,key,values):
		listM = [0] * j
		listN = [0] * j
		listTupple = []
		
		for value in values:
			if value[0] == 'M':
				listM[value[1]] = value[2]
			else:
				listN[value[1]] = value[2]
		
		for it in range(0,j):
			listTupple.append(listM[it]*listN[it])
		yield key , sum(listTupple)
		#print("reducer done")

if __name__ == '__main__':
	
	matrix1 = open('outA1.list','r')
	matrix2 = open('outB1.list','r')
	
	i,j_1 = matrix1.readline().split()
	j_2,k = matrix2.readline().split()
	
	i = int(i)
	k = int(k)
	j = int(j_1)

	
	matrix1.close()
	matrix2.close()
	start_time = time.time()
	onesStepMatrixMult.run()
	print("--- %s seconds ---" % (time.time() - start_time))
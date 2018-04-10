from mrjob.job import MRJob
from mrjob.step import MRStep
import os
import re
import sys
import time


class onestepMatrixMult(MRJob):
	def steps(self):
		return [MRStep(mapper = self.mapper,reducer = self.first_reducer),MRStep(reducer = self.second_reducer)]
	
	def mapper(self,_,value):
	
		if(len(value.split())) < 3:
			return
			
		i,j,val = value.split()
		matrixName = os.environ['map_input_file']
		
		#if matrixName == "Matrix1.txt":
		#if matrixName == "outA1.list":
		if matrixName == "outA2.list":
		#if matrixName == "outA3.list":
			yield j, ('M',i,val)
		else:
			# j,k,val = i,j,val
			# yield(j,('N',k,val)
			yield i,('N',j,val)
		
	
	def first_reducer(self,key,values):
		listM = [];
		listN = [];

		for value in values:
			if value[0] == 'M':
				listM.append((value[1],value[2]))
			else:
				listN.append((value[1],value[2]))
		
		for elem1 in listM:
			for elem2 in listN:
				yield (int(elem1[0]),int(elem2[0])),(int(elem1[1])*int(elem2[1]))
			
		#print("reducer1 done")
	
	def second_reducer(self,key,values):
		yield key,sum(values)
	#print("reducer done")
		
if __name__ == '__main__':
	start_time = time.time()
	onestepMatrixMult.run()
	print("--- %s seconds ---" % (time.time() - start_time))
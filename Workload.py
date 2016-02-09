#!/usr/bin/python
from Job import Job
import sys as sys

"""
This class handles the functionality that parses ths swf file into some job_list and has functionality that returns each entry in the job_list as
some job with the required attributes as longs. 
"""

class Workload(object):

	def __init__(self,filename):
		object.__init__(self)
		self.filename = filename
		self.job_list = [] 


    #This function parses and creates a list of strings from the swf file
	def job_list_creator(self):
		try:
			job_list_file = open(self.filename, 'r')
		except IOError, e:
			print 'No such file or directory:'
			sys.exit()

		for (i,line) in enumerate(job_list_file.readlines()):
			#split removes multiple whitespaces, join removes leading and trailing whitespace and joins on ' '
			line = ' '.join(line.split())
			if not line.startswith(';'):
				self.job_list.append(line)	
		self.job_list.append('-1 100000000000 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 1 -1 -1 -1')
		job_list_file.close()


	#This function gets one entry from the job_list and returns the required bits of information as longs inside a Job object
	def get_job(self, id):
		if id > len(self.job_list) :
			print 'Requested workload job is out of range of workload list'
			sys.exit()
		else:
			arr = self.job_list[id].split(" ")
			#job id, submit time, requested time, actual time, num_procs, runqueuestart, runqueueend, sys_procs_avail_at_run_queue_start, is special job
			is_dag_job = False
			job = Job(long(arr[0]), long(arr[1]), long(arr[8]), long(arr[3]) , long(arr[4]), 0, 0, 0, is_dag_job)
			return job
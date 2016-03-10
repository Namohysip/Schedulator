#!/usr/bin/python
from time import gmtime, strftime
from Job import Job

class CSV_Creator(object):
	def __init__(self, completed_jobs):
		object.__init__(self)
		self.completed_jobs = completed_jobs
		self.file_name = strftime("%Y-%m-%d %H:%M:%S", gmtime()) + ".csv"


	def results_to_file(self):
		target = open(self.file_name, 'w')
		target.write("")
		for i in self.completed_jobs:
			target.write(str(i.job.job_id))
			target.write("\n")

		target.close()




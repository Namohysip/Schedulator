#!/usr/bin/python
from Job_Queue import Job_Queue
import copy

try:
    import Queue as Q  # ver. < 3.0
except ImportError:
    import queue as Q

#########################################
#########################################

#####     TODO:
#####     update total jobs in system 1/25/16

#########################################
#########################################

class BatchScheduler(object):

	#initialize a batch scheduler with the amount of processors some system has
	def __init__(self, number_of_procs_in_system, graph):
		object.__init__(self)
		self.number_of_procs_in_system = number_of_procs_in_system
		self.number_of_procs_available = self.number_of_procs_in_system
		self.current_time = 0
		self.total_jobs_in_system = []
		self.waiting_queue = []
		self.running_queue = Q.PriorityQueue()
		self.duration_reached = True
		self.graph = graph
		self.DAG = graph.graph
		self.completed_dag_jobs = []
		self.is_head_job_submitted = True
		self.dag_jobs_in_system = []


	def submit_new_job(self, job_to_submit):
		#print "job ", job_to_submit.job_id, "was submitted at time ", self.current_time
		self.total_jobs_in_system.append(job_to_submit)
		self.waiting_queue.append(job_to_submit)
		#####     calculates the job run queue start time and the number of prcesses available in system at that time
		job_to_submit.run_queue_start, job_to_submit.sys_procs_avail_at_run_queue_start = self.calculate_job_runqueue_start_and_procs_avail_at_start(job_to_submit)
		self.current_time = job_to_submit.submit_time
		#####     Puts job in run queue if resources are available
		self.put_job_in_run_queue_if_can()
		#####     attempts to backfill the only the submitted job
		self.backfill_submitted_job()
		
		

	def FCFS(self):
		while not len(self.waiting_queue) == 0 and self.number_of_procs_available >= self.waiting_queue[0].number_of_procs:
			self.number_of_procs_available = self.number_of_procs_available - self.waiting_queue[0].number_of_procs
			endtime_priority = self.waiting_queue[0].actual_time
			self.waiting_queue[0].run_queue_start = self.current_time
			self.running_queue.put(Job_Queue(endtime_priority, self.waiting_queue.pop(0)))
			self.running_queue_priority_adjust()


	def time_advance(self, duration):
		self.current_time = self.current_time + duration


	def running_queue_peek(self):
		if not self.running_queue.empty():
			temp = self.running_queue.get()
			self.running_queue.put(Job_Queue(temp.priority, temp.job))
			return temp
		else:
			return none


	#####     The priority of the run queue is based off of the actual run time of a job
	#####     This function adjusts the priority of each job in the run queue    
	#####     Function is called each time a job begins to run 
	def running_queue_priority_adjust(self):
		temp = Q.PriorityQueue()
		while not self.running_queue.empty():
			temp.put(self.running_queue.get())
		while not temp.empty():
			entry = temp.get()
			endtime_priority = entry.job.run_queue_start + entry.job.actual_time - self.current_time
			self.running_queue.put(Job_Queue(endtime_priority, entry.job))


	#####     Places job in run queue if resources are available essentially FCFS
	def put_job_in_run_queue_if_can	(self):
		while not len(self.waiting_queue) == 0 and self.number_of_procs_available >= self.waiting_queue[0].number_of_procs:
			self.waiting_queue[0].run_queue_start = self.current_time
			self.waiting_queue[0].run_queue_end = self.waiting_queue[0].run_queue_start + self.waiting_queue[0].requested_time
			self.waiting_queue[0].sys_procs_avail_at_run_queue_start = self.number_of_procs_available
			self.number_of_procs_available = self.number_of_procs_available - self.waiting_queue[0].number_of_procs
			endtime_priority = self.waiting_queue[0].actual_time
			#print 'job', self.waiting_queue[0].job_id, 'began running at time', self.current_time
			#print '     run que start = ', self.waiting_queue[0].run_queue_start 
			#print '     sys procs avail at start = ', self.waiting_queue[0].sys_procs_avail_at_run_queue_start
			self.running_queue.put(Job_Queue(endtime_priority, self.waiting_queue.pop(0)))
			self.running_queue_priority_adjust()


	#####     Traverses the waiting list and attempts to backfill all waiting jobs
	#####     function is called when a job ends
	def backfill(self):
		i = 0
		while i < len(self.waiting_queue):
			#print 'job', self.waiting_queue[i].job_id, "is considered for backfilling at time", self.current_time
			if not len(self.waiting_queue) == 0 and self.can_conservative_backfill(self.waiting_queue[i]):
				#job_to_backfill_endtime, job_to_backfill_num_procs, self.waiting_queue[i].job_id
				# place job in running queue
				self.waiting_queue[i].run_queue_start = self.current_time
				self.waiting_queue[i].sys_procs_avail_at_run_queue_start = self.number_of_procs_available
				self.number_of_procs_available = self.number_of_procs_available - self.waiting_queue[i].number_of_procs
				endtime_priority = self.waiting_queue[i].actual_time
				#print 'backfilled job', self.waiting_queue[i].job_id, 'at time', self.current_time
				#print '     run que start = ', self.waiting_queue[i].run_queue_start 
				#print '     sys procs avail at start = ', self.waiting_queue[i].sys_procs_avail_at_run_queue_start
				self.running_queue.put(Job_Queue(endtime_priority, self.waiting_queue.pop(i)))
				i = i -1
				self.running_queue_priority_adjust()
			i = i +1

	#####     attempts to backfill the last job of the waiting queue - which is the most recent submitted job
	def backfill_submitted_job(self):
		if not len(self.waiting_queue) == 0 and self.can_conservative_backfill(self.waiting_queue[len(self.waiting_queue)-1]):
			self.waiting_queue[len(self.waiting_queue)-1].run_queue_start = self.current_time
			self.waiting_queue[len(self.waiting_queue)-1].sys_procs_avail_at_run_queue_start = self.number_of_procs_available
			self.number_of_procs_available = self.number_of_procs_available - self.waiting_queue[len(self.waiting_queue)-1].number_of_procs
			endtime_priority = self.waiting_queue[len(self.waiting_queue)-1].actual_time
			#print 'backfilled job', self.waiting_queue[len(self.waiting_queue)-1].job_id, 'at time', self.current_time
			#print '     run que start = ', self.waiting_queue[len(self.waiting_queue)-1].run_queue_start 
			#print '     sys procs avail at start = ', self.waiting_queue[len(self.waiting_queue)-1].sys_procs_avail_at_run_queue_start
			self.running_queue.put(Job_Queue(endtime_priority, self.waiting_queue.pop(len(self.waiting_queue)-1)))
			self.running_queue_priority_adjust()


	#####     Compare one waiting job against all waiting jobs 
	#####     Checks all jobs in waiting queue that will start before job ends
	#####     If resources available for all jobs with job to backfill running, return true   
	def can_conservative_backfill(self, job):
		job_to_backfill_endtime = self.current_time + job.requested_time
		should_backfill = True

		if job.number_of_procs > self.number_of_procs_available:
			should_backfill = False
		else:
			i = 0
			while  i < len(self.waiting_queue) and job_to_backfill_endtime > self.waiting_queue[i].run_queue_start:
				if self.waiting_queue[i].sys_procs_avail_at_run_queue_start - job.number_of_procs < self.waiting_queue[i].number_of_procs:
					should_backfill = False
				i = i +1

			#####     update procs available at runqueue start for waiting jobs
			if should_backfill == True:
				i = 0
				while job_to_backfill_endtime > self.waiting_queue[i].run_queue_start:
					self.waiting_queue[i].sys_procs_avail_at_run_queue_start = self.waiting_queue[i].sys_procs_avail_at_run_queue_start - job.number_of_procs
					i = i+1
		return should_backfill


		
	#####     calculates the run queue start time and the available processes at that time for a given job			
	def calculate_job_runqueue_start_and_procs_avail_at_start(self, job):
		job_run_queue_start_time = -1
		procs_avail_at_run_queue_start_time = -1
		procs_count = self.number_of_procs_available
		found_values = False

		#####     If running queue is empty, simply return values.  The values are then set when the job is placed
		#####     on the running queue
		if self.running_queue.empty() or self.number_of_procs_available >= job.number_of_procs:
			return job_run_queue_start_time, procs_avail_at_run_queue_start_time
		elif len(self.waiting_queue) == 1 and not self.running_queue.empty():
			#####     This code executes when there is only 1 job in the waiting queue
			#####     Necessary to traverse running queue to see when given job will start
			temp_queue = Q.PriorityQueue()
			while not self.running_queue.empty():
				temp_queue.put(self.running_queue.get())
			while not temp_queue.empty():
				entry = temp_queue.get()
				procs_count = procs_count + entry.job.number_of_procs
				if procs_count >= job.number_of_procs and found_values == False:
					procs_avail_at_run_queue_start_time = procs_count
					job_run_queue_start_time = entry.job.run_queue_end
					found_values = True
				self.running_queue.put(Job_Queue(entry.priority, entry.job))
			#print 'job ',job.job_id, 'will start running at', job_run_queue_start_time
			return job_run_queue_start_time, procs_avail_at_run_queue_start_time
		else:
			#####     Set run_queue_start for job to last job in waiting list end time
			#####     It is set from the job before it in the waiting list
			#####     Set num-procs_avail_at_start for given job = to job before it in wait list
			i = 0
			job_run_queue_start_time = self.waiting_queue[len(self.waiting_queue)-2].run_queue_start + self.waiting_queue[len(self.waiting_queue)-2].requested_time
			procs_avail_at_run_queue_start_time = self.waiting_queue[len(self.waiting_queue)-2].sys_procs_avail_at_run_queue_start
			return job_run_queue_start_time, procs_avail_at_run_queue_start_time


	def print_running_queue(self):
		while not self.running_queue.empty():
			queue_entry = self.running_queue.get()
			if queue_entry.job.job_id < 0:
				print 'running job_id: ', queue_entry.job.job_id, 'actual time', queue_entry.job.actual_time
				print 'submit time ', queue_entry.job.submit_time
				print 'run_queue_start', queue_entry.job.run_queue_start
				print 'number_of_procs', queue_entry.job.number_of_procs

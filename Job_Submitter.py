#!/usr/bin/python
import sys as sys
from Workload import Workload
from BatchScheduler import BatchScheduler
from Job import Job
from Job_Queue import Job_Queue
from DAG_Generator import DAG_Generator

#Workload
if len(sys.argv) == 2:
	workload = Workload(sys.argv[1])
else:
	print "Please specify a workload file"
	sys.exit()

print 'file = ', workload.filename
workload.job_list_creator()



#DAG
graph = DAG_Generator(64)


#Batch Scheduler and jobs 
completed_jobs = []
jobs = []
system_procs = 512#6
batchscheduler = BatchScheduler(system_procs, graph)



##########     SIMULATION     ####################

#loop to traverse workload jobs
begin = 0
end = 800#5
for i in range(begin, end):


	job_to_submit = workload.get_job(i)
	# print 'Submit time = ', job_to_submit.submit_time
	# #print 'run queue start = ', job_to_submit.run_queue_start
	# print 'actual time = ', job_to_submit.actual_time
	# #print 'run queue end =', job_to_submit.run_queue_end

	if job_to_submit.number_of_procs <= system_procs:
		batchscheduler.submit_new_job(job_to_submit)

	###########     get the time for the next workload job submission     ##########
	time_of_next_job_submission = workload.get_job(i+1).submit_time
	#print 'time of next job submission', time_of_next_job_submission

	
	##########     Get the end time of the top job in the running queue     ##########
	if not batchscheduler.running_queue.empty():
		top_job = batchscheduler.running_queue_peek()
		top_job_end_time = top_job.job.run_queue_start + top_job.job.actual_time 


	##########     Get the completed jobs before the time of the next job submission     ##########
	while not batchscheduler.running_queue.empty() and top_job_end_time <= time_of_next_job_submission:
		completed_job_from_queue = batchscheduler.running_queue.get()

		##########     Update the top job and end time ##########
		if not batchscheduler.running_queue.empty():
			top_job = batchscheduler.running_queue_peek()
			top_job_end_time = top_job.job.run_queue_start + top_job.job.actual_time 


		##########     Update variables     ##########
		batchscheduler.current_time = completed_job_from_queue.job.run_queue_start + completed_job_from_queue.job.actual_time
		completed_job_from_queue.job.run_queue_end = batchscheduler.current_time
		completed_jobs.append(completed_job_from_queue)
		batchscheduler.number_of_procs_available = batchscheduler.number_of_procs_available + completed_job_from_queue.job.number_of_procs
		#print "num procs avail ", batchscheduler.number_of_procs_available, 'at time', batchscheduler.current_time
		# for z in completed_jobs:
		# 	print 'contents of completed queue'
		# 	print 'job_id == ', z.job.job_id

		##########     submit first dag job     ##########
		if batchscheduler.current_time >= 100000 and batchscheduler.is_head_job_submitted == True:
			x = batchscheduler.graph.get_head_node()
			x.submit_time = batchscheduler.current_time
			batchscheduler.submit_new_job(x)
			batchscheduler.dag_jobs_in_system.append(x)
			batchscheduler.is_head_job_submitted = False

		###submit completed dag jobs to a list
		count = 0
		if completed_job_from_queue.job.is_dag_job:
			batchscheduler.completed_dag_jobs.append(completed_job_from_queue.job)

			##########     Logic to submit the dag jobs dependent on completed dag job     ##########
			successors_list = batchscheduler.DAG.successors(completed_job_from_queue.job)
			for x in successors_list:
				predecessors_list = batchscheduler.DAG.predecessors(x)
				count = 0
				for y in predecessors_list:
					if y in batchscheduler.completed_dag_jobs:
						#print y.job_id
						count = count + 1
				if count == batchscheduler.DAG.in_degree(x):
					x.submit_time = batchscheduler.current_time
					batchscheduler.submit_new_job(x)
					batchscheduler.dag_jobs_in_system.append(x)


		batchscheduler.put_job_in_run_queue_if_can()
		batchscheduler.backfill() 


	##########     Moves the time forward the appropriate amount     ##########
	time_pad = workload.get_job(i+1).submit_time - batchscheduler.current_time
	batchscheduler.time_advance(time_pad)





head_job = batchscheduler.graph.get_head_node()
tail_job = batchscheduler.graph.get_tail_node()

head_tail_list = []
###### print the jobs ###########
print 'completed_jobs', len(completed_jobs)
for i in completed_jobs:
	print 'job_id = ', i.job.job_id
	print 'completed time =', i.job.run_queue_end
	#if i.job.job_id < 0:
	if i.job.job_id == head_job.job_id:
		head_tail_list.append(i.job)
		print 'completed job id:', i.job.job_id
		print 'head job submit time', i.job.submit_time
		print
	if i.job.job_id == tail_job.job_id:
		head_tail_list.append(i.job)
		print 'completed job id:', i.job.job_id
		print 'tail job end time', i.job.run_queue_end

if len(head_tail_list) == 2:
	print 'total time for dag job = ', (head_tail_list[1].run_queue_end - head_tail_list[0].submit_time)


#batchscheduler.print_running_queue()
print 'program terminates at: ', batchscheduler.current_time

for x in batchscheduler.waiting_queue:
	if x.job_id < 0:
		print 'wait queue job', x.job_id

batchscheduler.print_running_queue()
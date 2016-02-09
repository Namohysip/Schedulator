#!/usr/bin/python
from Job import Job
import networkx as nx
from random import randint
import math

class DAG_Generator(object):

	id = -1000

	def __init__(self, shape_count):
		object.__init__(self)
		self.graph = nx.DiGraph()
		self.shape_list = []
		self.shape_count = shape_count
		#create the DAG
		self.graph = self.dag_creator()



	def parallel_shape_creator(self):
		job0 = Job(DAG_Generator.id, 1, 1, 1, 1, 0, 0, 0, True)
		DAG_Generator.id += 1
		job1 = Job(DAG_Generator.id, 1, 1, 1, 1, 0, 0, 0, True)
		DAG_Generator.id += 1
		job2 = Job(DAG_Generator.id, 1, 1, 1, 1, 0, 0, 0, True)
		DAG_Generator.id += 1
		job3 = Job(DAG_Generator.id, 1, 1, 1, 1, 0, 0, 0, True)
		DAG_Generator.id += 1
		self.graph.add_path([job0, job1, job3])	
		self.graph.add_path([job0, job2, job3])
		shape_a_points = [job0, job3]
		return shape_a_points

	def series_shape_creator(self):
		job0 = Job(DAG_Generator.id, 1, 1, 1, 1, 0, 0, 0, True)
		DAG_Generator.id += 1
		job1 = Job(DAG_Generator.id, 1, 1, 1, 1, 0, 0, 0, True)
		DAG_Generator.id += 1
		job2 = Job(DAG_Generator.id, 1, 1, 1, 1, 0, 0, 0, True)
		DAG_Generator.id += 1
		self.graph.add_path([job0, job1, job2])
		shape_b_points = [job0, job2]
		return shape_b_points
	
	def parallel_merge(self, a_points, b_points):
		shape_a_points = a_points 
		shape_b_points = b_points 

		#add time to nodes
		shape_a_points[0].submit_time += shape_b_points[0].submit_time
		shape_a_points[0].requested_time += shape_b_points[0].requested_time
		shape_a_points[0].actual_time += shape_b_points[0].actual_time
		shape_a_points[0].number_of_procs += shape_b_points[0].number_of_procs
		
		shape_a_points[1].submit_time += shape_b_points[1].submit_time
		shape_a_points[1].requested_time += shape_b_points[1].requested_time
		shape_a_points[1].actual_time += shape_b_points[1].actual_time
		shape_a_points[1].number_of_procs += shape_b_points[1].number_of_procs
	
		for x in self.graph.successors(shape_b_points[0]):
			self.graph.add_edge(shape_a_points[0], x)
			self.graph.remove_edge(shape_b_points[0], x)

		self.graph.remove_node(shape_b_points[0])

		for x in self.graph.predecessors(shape_b_points[1]):
			self.graph.add_edge(x, shape_a_points[1])
			self.graph.remove_edge(x, shape_b_points[1])

		self.graph.remove_node(shape_b_points[1])
		return shape_a_points
				
	def series_merge(self, a_points, b_points):
		shape_a_points = a_points
		shape_b_points = b_points

		#add time to nodes
		shape_a_points[1].submit_time += shape_b_points[0].submit_time
		shape_a_points[1].requested_time += shape_b_points[0].requested_time
		shape_a_points[1].actual_time += shape_b_points[0].actual_time
		shape_a_points[1].number_of_procs += shape_b_points[0].number_of_procs

		for x in self.graph.successors(shape_b_points[0]):
			self.graph.add_edge(shape_a_points[1], x)

		self.graph.remove_node(shape_b_points[0])
		shape_a_points[1] = shape_b_points[1]
		return shape_a_points

	def shape_list_creator(self):
		#create the leaf shapes
		for x in range(0, self.shape_count):
			random = randint(0,1)
			if random == 0:
				self.shape_list.append(self.parallel_shape_creator())
			else:
				self.shape_list.append(self.series_shape_creator())

	def dag_creator(self):
		#merge all leaf shapes into a DAG
		self.shape_list_creator()
		for x in range(0, int(math.log(self.shape_count,2))):
			self.shape_count = self.shape_count/2   
			for x in range(0, self.shape_count):
				random = randint(0,1)
				if random == 0:
					a = self.shape_list.pop(0)
					b = self.shape_list.pop(0)
					self.shape_list.append(self.parallel_merge(a,b))
				else:
					a = self.shape_list.pop(0)
					b = self.shape_list.pop(0)
					self.shape_list.append(self.series_merge(a,b))
		return self.graph

	def get_head_node(self):
		for x in self.graph:
			if not self.graph.predecessors(x):
				return x

	def get_tail_node(self):
		for x in self.graph:
			if not self.graph.successors(x):
				return x














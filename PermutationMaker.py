'''
Created on Feb 21, 2016

@author: Noah Higa
'''
from networkx.algorithms.dag import is_directed_acyclic_graph
import threading

class permutationMaker (threading.Thread):
    
    def __init__(self, dag, clusterType = 0, threads = 1):
        self.clustering = clusterType
        self.dag = dag
        self.threadCap = threads
        self.activeThreads = 1
        self.counterMutex = threading.Lock()
        self.listMutex = threading.Lock()
        self.listOfPerm = []
        
    def run(self):
        if self.clustering == 0:
            if self.threadCap < 1:
                self.exhaustivePerm(self.dag)
            else:
                self.exhaustivePermMulti(self.dag, 0)
    
    #Recursively generates every possible permutation of
    #the dag given. NOTE: This does NOT add the original
    #unclustered DAG to the list of permutations
    def exhaustivePerm(self, dag):
        if len(dag.edges()) < 1:
            return #Base case: Only one node left
        nodes = dag.nodes()
        for node in nodes:
            for otherNode in nodes:
                if(node != otherNode): #Compare all pairs of nodes
                    newDag = dag.copy()
                    n1 = self.findJob(newDag,node)
                    n2 = self.findJob(newDag,otherNode)
                    if(n1 != None and n2 != None):
                        newDag = self.combine(newDag,n1,n2)
                        if (self.checkLegal(newDag)): #Check if it's still a DAG
                            if self.addWithoutDuplicates(self.listOfPerm,newDag):
                                self.exhaustivePerm(newDag) #IF so, add and keep going
    
    def exhaustivePermMulti(self, dag, depth):
        if len(dag.edges()) < 1:
            return #Base case: Only one node left
        threads = []
        nodes = dag.nodes()
        for node in nodes:
            for otherNode in nodes:
                if(node != otherNode): #Compare all pairs of nodes
                    newDag = dag.copy()
                    n1 = self.findJob(newDag,node)
                    n2 = self.findJob(newDag,otherNode)
                    if(n1 != None and n2 != None):
                        newDag = self.combine(newDag,n1,n2)
                        if (self.checkLegal(newDag)): #Check if it's still a DAG
                            self.listMutex.acquire()
                            added = self.addWithoutDuplicates(self.listOfPerm,newDag)
                            self.listMutex.release()
                            if added:
                                self.counterMutex.acquire()
                                makeNewThread = self.activeThreads < self.threadCap
                                if makeNewThread:
                                    self.activeThreads += 1
                                self.counterMutex.release()
                                if makeNewThread:
                                    t = threading.Thread(target = self.exhaustivePermMulti, args =  (newDag, 0))
                                    threads.append(t)
                                    t.start()
                                else:
                                    self.exhaustivePermMulti(newDag, depth+1)
        for t in threads:
            t.join()
        if(depth == 0):
            self.counterMutex.acquire()
            self.activeThreads -= 1
            self.counterMutex.release()
            
    def combine(self, G, node1, node2):
        #adds the work in node2 into node 1
        node1.job_id = min(node1.job_id,node2.job_id)
        node1.requested_time += node2.requested_time
        node1.actual_time += node2.actual_time
        preds = G.predecessors(node2)
        succs = G.successors(node2)
        for node in preds:
            G.remove_edge(node,node2)
            G.add_edge(node,node1)
        for node in succs:
            G.remove_edge(node2,node)
            G.add_edge(node1,node)
        G.remove_node(node2) #This also removes all edges associated with node2
        #If node1 and 2 are directly linked, a self-cycle could form but can be removed without consequence
        if G.has_edge(node1,node1):
            G.remove_edge(node1,node1)
        return G
        
        
        #Checks if the graph is still a DAG, ie, 
        #That there are no cycles in the graph
    def checkLegal(self, G):
        return is_directed_acyclic_graph(G)
    
    #    If the graph is a duplicate, it doesn't add it
    #    It returns false if it isn't added, true otherwise    
    def addWithoutDuplicates(self, c, dag):
        for graph in c:
                if self.checkAllNodes(graph, dag):
                    return False
        c.append(dag)
        return True
    
    #Returns true if all the nodes have a match with each other,
    #False if at least one node has no match with another
    #Assumes that graph and dag have the same number of nodes
    def checkAllNodes(self, dag1, dag2):
        edges1 = dag1.edges()
        edges2 = dag2.edges()
        if(len(edges1) != len(edges2)):
            return False
        if(len(dag1.nodes()) != len(dag2.nodes())):
            return False
        for e1 in edges1:
            found = False
            for e2 in edges2:
                if self.isSameJob(e1[0],e2[0]):
                    if self.isSameJob(e1[1],e2[1]):
                        found = True
            if not found: 
                return False
        return True
        
        #Basically checks if all its values are identical
        #Can't just check ID because of merging
    def isSameJob(self, job1, job2):
        if job1.job_id != job2.job_id:
            return False
        if job1.actual_time != job2.actual_time:
            return False
        if job1.requested_time != job2.requested_time:
            return False
        return True
    
        #Should only be used for finding
        #the same job after a G.copy() command
    def findJob(self, G,job):
        nodes = G.nodes()
        for node in nodes:
            if self.isSameJob(node,job):
                return node
    
    def findHead(self, G):
        nodes = G.nodes()
        for node in nodes:
            if len(node.predecessors()) < 1:
                return node
            
    def getList(self):
        return self.listOfPerm
    
    def dagToFile(self,dag,name):
        ##TODO: file format:
        # id req act procs(always 1))
        # Keep ids contiguous, so ignore the given job ID
        return

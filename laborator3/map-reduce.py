from mpi4py import MPI
import json
import base64
import os, fnmatch
import time
import random
from datetime import datetime

class Message:
	def __init__(self, srcLink, adjList=None):
		self.srcLink = srcLink
		if adjList is None:
			self.adjList = []
		else:
			self.adjList = adjList

class Process:
	def __init__(self, status, rank):
		self.status = status
		self.rank = rank
	def set_time(self, time):
		self.time = time

def map(parentLink, insideLinks):
	for insideLink in insideLinks:
		encodedNode =base64.b64encode(bytes(insideLink, 'utf-8'))
		encodedParent = base64.b64encode(bytes(parentLink, 'utf-8'))			
		encodedNodeStr = str(encodedNode, 'utf-8').replace('/','.')
		encodedParentStr = str(encodedParent, 'utf-8').replace('/','.')
		file = 'map/'+  encodedNodeStr + '_' + encodedParentStr
		f = open(file, 'w')
		f.close()

def reduce(insideLink):
	encodedTarget = base64.b64encode(bytes(insideLink, 'utf-8'))
	encodedTargetStr = str(encodedTarget,'utf-8')
	files = fnmatch.filter(os.listdir('map'), encodedTargetStr + '_*')
	list = []
	for file in files:
		encodedParentLink = file.split('_')[1]
		parentLink = base64.b64decode(bytes(encodedParentLink.replace('.','/'), 'utf-8'))
		list.append(str(parentLink,'utf-8'))
	return list

comm = MPI.COMM_WORLD
rank=comm.Get_rank()
size = comm.Get_size()
master = 0
processes = []
status = MPI.Status()

TM = 10
TR = 20
TE = 30

limit = 60

#map
if rank == master:
	for i in range(0, size-1):
		processes.append(Process("free",i+1)) 
	with open('result.json', 'r') as f:
		links = json.loads(f.read())
	unprocessedLinks = list(links.keys())
	while len(unprocessedLinks) > 0:
		freeProcesses = [process for process in processes if process.status == 'free']
		if freeProcesses is not None:
			process = random.choice(freeProcesses)
			insideLink = unprocessedLinks.pop(0)
			message = Message(insideLink, links)
			process.time = datetime.now()
			process.status = 'busy'
			req = comm.isend(message, dest=process.rank, tag=TM)
			req.wait()
			print('Process {} sent message to worker'+str(process.rank)+''.format(rank), message)
		else:
			for process in processes:
				if(process.status == 'busy' and ((datetime.now() - process.time).total_seconds > limit)):
					process.status = 'free'

		r = comm.irecv(source=MPI.ANY_SOURCE, tag=TM)
		completeRank = r.wait(status=status)
		for process in processes:
			if process.rank == completeRank:
				process.status = 'free'
		source = status.Get_source
		print('Process {} received complete from worker'+str(source)+':'.format(rank))

else:
	while True:
		r = comm.irecv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG)
		message = r.wait(status=status)
		print('Process {} received message from master:'.format(rank), message)
		if status.Get_tag() == TR:
			break
		parentLink = message.srcLink
		map(parentLink, message.adjList[parentLink])
		req = comm.isend(rank, dest=master, tag=TM)
		req.wait()
		print('Process {} sent complete to master'.format(rank))


#reduce
if rank == master:
	var = input("Please enter target links(comma-separated values): ")
	print ("You entered:", var)
	targetLinks = var.split(',')
	while len(targetLinks) > 0:
		freeProcesses = [process for process in processes if process.status == 'free']
		if freeProcesses is not None:
			process = random.choice(freeProcesses)
			targetLink = targetLinks.pop(0)
			message = Message(targetLink)
			process.time = datetime.now()
			process.status = 'busy'
			req = comm.isend(message, dest = process.rank, tag = TR)
			req.wait()
			print('Process {} sent meesage to workers '+str(process.rank)+' - reduce phase'.format(rank), message)
		else:
			for process in processes:
				if(process.status == 'busy' and ((datetime.now() - process.time).total_seconds > limit)):
					process.status = 'free'

		r = comm.irecv(source=MPI.ANY_SOURCE, tag=TR)
		completeRank = r.wait(status=status)
		for process in processes:
			if process.rank == completeRank:
				process.status = 'free'
		source = status.Get_source
		print('Process {} received complete from worker '+str(source)+' - reduce phase:'.format(rank))

	for i in range(1, size):
		req = comm.isend('dummy', dest = i, tag = TE)
		req.wait()
		print('Process {} sent ending message to workers'.format(rank))
else:
	while True:
		r = comm.irecv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG)
		message = r.wait(status=status)
		print('Process {} received message from master:'.format(rank), message)
		if status.Get_tag() == TE:
			break
		list = reduce(message.srcLink);
		for el in list:
			print (el)
		req = comm.isend(rank, dest=master, tag=TR)
		req.wait()
		print('Process {} sent complete to master'.format(rank))

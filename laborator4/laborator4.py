from mpi4py import MPI
from datetime import datetime
import itertools
import fnmatch
import os

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank=comm.Get_rank()
status = MPI.Status()
coordonator = 0;
successMapping = 0
successCombiner = 0
successReducer = 0
min_support = 2
combinerPath = 'F:/C/workspace-master-an1sem2/TBD/lab3/combiner/';

TM = 10
TC = 20
TR = 30

def storeSubsetsMap(data): 
    n = len(data)
    s = set(data)
    for i in range(n):
        subset = list(itertools.combinations(s, i+1))
        print (subset)
        for j in subset:
            now = datetime.now()
            filename = str(j) + '_' + str(1) + '_' + str(rank) + '_' + str(datetime.timestamp(now)) + '.txt'
            f = open('map/' + filename, "x")

def combinerPhase():
    pattern = '*1_'+ str(rank) + '*'
    filesCountMap = {}
    for file in os.listdir('F:/C/workspace-master-an1sem2/TBD/lab3/map/'):
        if fnmatch.fnmatch(file, pattern):
            fileName = file.rsplit('_',1)[0]
            if fileName in filesCountMap:
                filesCountMap[fileName] += 1
            else:
                filesCountMap[fileName] = 1
    storeSubsetsCombiner(filesCountMap)

def storeSubsetsCombiner(filesCountMap): 
    for key, value in filesCountMap.items():
        newKey = key.replace('_1_', '_'+str(value)+'_')
        print (str(key)+':'+str(value))
        f = open('F:/C/workspace-master-an1sem2/TBD/lab3/combiner/' + newKey, "x")

def getSubsets():
    subsets = []
    for file in os.listdir(combinerPath):
        subset = file.split('_')[0]
        if subset not in subsets:
            subsets.append(subset)
    return subsets

if rank == coordonator:
    lines = []
    with open('retail.dat.txt') as my_file:
        for line in my_file:
            lines.append(line)
            
    chunks = [[] for _ in range(size)]
    for i, chunk in enumerate(lines):
        chunks[i % size].append(chunk)
    print(chunks)
else:
    data = None
    chunks = None
    

receivedLines = comm.scatter(chunks, root=0)

for receiveLine in receivedLines:
    data = receiveLine.split()
    storeSubsetsMap(data)
if (rank != coordonator):
    req = comm.isend(rank, dest=coordonator, tag=TM)
    req.wait()
    print('Process ' + str(rank) + ' sent complete mapping msg to coordonator')

if rank == coordonator:
    while True:
        r = comm.irecv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG)
        msg = r.wait(status=status)
        source = status.Get_source()
        if status.Get_tag() == TM:
            print('Coord received success mapping msg from ' + str(source))
            successMapping = successMapping + 1
            if (successMapping == size-1):
                break
    dummy = 'start_combiner'
    for i in range(size):
        if i != coordonator:
            req = comm.isend(dummy, dest=i, tag=TC)
            req.wait()
            print('Process ' + str(rank) + ' sent starting combiner phase to process '.format(i))
    combinerPhase()
else:
    r = comm.irecv(source=coordonator, tag=MPI.ANY_TAG)
    msg = r.wait(status=status)
    if status.Get_tag() == TC:
        print('Process '+str(rank)+' received starting combiner phase msg')
        combinerPhase()
        if (rank != coordonator):
            req = comm.isend(rank, dest=coordonator, tag=TC)
            req.wait()
            print('Process ' + str(rank) + ' sent complete combiner msg to coordonator')
        
if rank == coordonator:
    subsets = []
    while True:
        r = comm.irecv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG)
        msg = r.wait(status=status)
        source = status.Get_source()
        if status.Get_tag() == TC:
            print('Coord received success combiner msg from ' + str(source))
            successCombiner = successCombiner + 1
            if (successCombiner == size-1):
                break
    subsets = getSubsets();
    for i in subsets:
        print(i)
    chunks = [[] for _ in range(size)]
    for i, chunk in enumerate(lines):
        chunks[i % size].append(chunk)
    print(chunks)
else:
    subsets = None
    chunks = None
receivedSubsets = comm.scatter(chunks, root=0)

reduceMap = {}
for receiveSubset in receivedSubsets:
    files = [f for f in os.listdir(combinerPath) if receiveSubset == f.split('_')[0]]
    count = 0
    for file in files:
        count += file.split('_')[1]
    reduceMap[receiveSubset] = count
    
if (rank != coordonator):
    req = comm.isend(reduceMap, dest=coordonator, tag=TR)
    req.wait()
    print('Process ' + str(rank) + ' sent reduce msg to coordonator')
else:
    for key, value in reduceMap.items():
        if value >= min_support:
            print('Result: ' + str(key))

if (rank == coordonator):
    while True:
        r = comm.irecv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG)
        reduceMap = r.wait(status=status)
        source = status.Get_source()
        if status.Get_tag() == TR:
            print('Coord received reduce msg from ' + str(source))
            print('ReduceMap ' + str(len(reduceMap)))
            successReducer = successReducer + 1
            for key, value in reduceMap.items():
                if value >= min_support:
                    print('Result: ' + str(key))
            if (successReducer == size-1):
                break
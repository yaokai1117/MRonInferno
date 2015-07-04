# MRonInferno
****
a small MapReduce framework on Inferno written in Limbo, just like many other MarReduce framework, our code is divided into two parts, a distrbuted file system and the mapreduce calculation model.

## MapReduce Calculation Model

this part is the main part of the MapReduce framework. The main modules of this part is JobTracker, TaskTracker, MapperWorker, and ReducerWorker.
    
* ioutil, mrutil, jobs:
	These modules includes important adts and functions in mapreduce model.

* jobtracker:
	The JobTracker module run the job.
	It assigns and coordinates all the tasktrackers.
	
* tasktracker:
	TaskTracker handle mappers and reducers .

* jobtrackerserver, tasktrackerserver:
	These two modules handle the communication between jobtracker and tasktrackers.

* mapperworker, reducerworker:
	Do mapping and reducing tasks.

* mapreduce:
	The MapReduce interface.

* submit, start:
	Users use these program to submit a job and start the job.

## DFS 
this part is the file partition,transport and scheduling module of our MapReduce framework.

* dfsutil:
	Definition and implementation of some basic datastructures used in DFS.

* dfsmaster:
	Maintain the infomation of every file, chunk and node in the file system.

* dfsserver:
		The server of dfsmaster, receives message from other computing equipment and call appropriate function in dfsmaster.

* dfsnodeserver:
		The server of dfsnode. Every data node in DFS use dfsnodeserver to transport file chunks and communicate with master.
		
* dfsclient:
		Provide APIs (such as createFile, getFile, readChunk, writeChunk and deleteChunk) of the distributed file system for Mapreduce Calculation Model.
	
* upload, list, download:
		These programs can be called in inferno shell, to upload and download file in distributed file system.
		
## UI
   A simple graphical user interface of our framework, **still in completion**.

## Logger
this part is the logger of the distributed file system and Mapreduce jobs.
	* logger:
		Set the log file and write logs into the log file and on the screen when the DFS and MapReduce jobs are working.
	 	The log file can record the state of the work and recover the work if it ends in failure.

## Xml
   Some tools to transform dfs datastructures into xml file, so that they can be transformed easily. 
    
## How to start

1. Download "MR.zip" and unpacked it to the directory "/appl/".

2. Open the file "config", and change the two addresses: the first address is the host address, the second address is the local address.

3. Implement "mapreduce.m".("wordcount.dis" is an example.) Do not forget to compile it.

4. The host open a new Inferno shell window, and start a dfsserver:     
    $sh fuck.sh
   
5. Other computers start a dfsnodeserver:   
	**first, you should change your working directory to /appl/MR**    
	$/dfsdfsnodeserver
	
6. If all computers are connected, you should upload the input file and the implemention of mapreduce : 
	$dfs/upload wctest   
	$cd mapreduce   
	$../dfs/upload wordcount.dis
	   
7. Then start a jobtrackerserver:   
	$jobtrackerserver
	
8. Other computers start a dfsnodeserver:   
	$tasktrackerserver
	
9. If all computers are connected,to submit a job:

       **you should edit jobconfig first !**       
	$submit
	
10. To start the job that you've already submit   
	$start
	
11. OK,now the job is running!

# MRonInferno
****
a small MapReduce framework on Inferno written in Limbo, just like many other MarReduce framework, our code is divided into two parts, a distrbuted file system and the mapreduce calculation model.

## MapReduce Calculation Model
this part is the main part of the MapReduce framework. The main modules of this part is JobTracker, TaskTracker, MapperWorker, and ReducerWorker.

	* ioutil: 
		The IOUtil module includes some often used adts and utilities when doing MapReduce.
	* mrutil:
		The MRUtil module includes adts of MapperTask, ReducerTask, TaskTrackerInfo and TaskTracker.
		They are important adts in mapreduce model.
	* jobs:
		The Jobs module includes adts of Job and JobConfig.
		The user set the JobConfig and submit the job.
	* jobtracker:
		The JobTracker module run the job.
		It assigns and coordinates all the tasktrackers.
		When the job is started, Jobtracker will produce mapperTasks and reducerTasks, and then shoot them to different tasktrackers.
		Jobtracker will monitor the states of tasktrackers and tasks, and do some steps ,such as change tasks to other tasktrackers if some tasks are failed or some tasktrackers are broken.
	* jobtrackerserver:
		The JobTrackerServer module is the server of jobtracker.
		It receives message about jobs from the client side, shoot mapper and reducer tasks to tasktrackers and get heartbeats from tasktrackers.
	* tasktracker:
		Tasktracker accept some mappers and reducers from jobtrackerserver, and run the mapperTasks and reducerTasks.
		It returns states of its tasks (succeed or failed) and the addresses of mapper result files to jobtrackerserver.
		Its workspace is "MR/mapreduce/task/".
	* tasktrackerserver:
		The server of tasktracker, receives message from jobtrackerserver and call appropriate function in tasktracker.
		It sends heartbeat to jobtrackerserver, so that jobtracker can know states of all tasktrackers (working, succeed or failed).
	* mapperworker:
		The MapperWorker module is the worker to do the mapperTask. It runs the user map function, saves the result to local and spawns a thread to send results to ReducerWorkers.
	* reducerworker:
		The ReducerWorker module is the worker to do the reducerTask. It gets its partitions of files from each mapper,and merges them outside the memory.
		Then reducerworker will run the user reduce function, save the result to local and upload the result to DFS.
	* mapreduce:
		The MapReduce module includes the MapReduce interface.
		When submitting a job, user should implement the map(), reduce(), combine(), keySpaceSize() and hashKey().
	* client:
		The client side of the MapReduce framework. The user of the MapReduce framework use this program to submit a job and start the job.

## DFS 
this part is the file partition,transport and scheduling module of our MapReduce framework.

	* dfsutil:
		Definition and implementation of some basic datastructures used in DFS.
	* xmlhandle:
		Some tools to transform dfs datastructures into xml file, so that they can be transformed easily.
		It also includes some tools to parse a xml file to DFS adts.
	* dfsmaster:
		Maintain the infomation of every file, chunk and node in the file system.
		These infomation are stored in the metadata.
	* dfsserver:
		The server of dfsmaster, receives message from other computing equipment and call appropriate function in dfsmaster.
		Its workspace is "MR/dfs/hostser/".
	* dfsnodeserver:
		The server of dfsnode. Every data node in DFS use dfsnodeserver to transport file chunks and communicate with master.
		It sends heartbeat to dfsserver, so that dfsmaster can know states of all nodes (working or died).
		Its workspace is "MR/dfs/ser/".
	* dfsclient:
		Provide APIs (such as createFile, getFile, readChunk, writeChunk and deleteChunk) of the distributed file system for Mapreduce Calculation Model.
		It creates some connections with DFSServer and DFSNodeServer,and communicate information and data with them.
		Its workspace is "MR/dfs/cli/".
	* upload, list, download:
		These programs can be called in inferno shell, to upload and download file in distributed file system.

## Logger
this part is the logger of the distributed file system and Mapreduce jobs.
	* logger:
		Set the log file and write logs into the log file and on the screen when the DFS and MapReduce jobs are working.
	 	The log file can record the state of the work and recover the work if it ends in failure.


# How to start
	1. Download "MR.zip" and unpacked it to the directory "/appl/".
	2. Open the file "config", and change the two addresses: the first address is the host address, the second address is the local address.
	3. Create a file in "/mapreduce/", and write an implementation of "mapreduce.m" in it.("wordcount.dis" is an example.) Do not forget to compile it.
	4. The host open a new Inferno shell window, and start a dfsserver:
		$sh fuck.sh
	5. Other computers start a dfsnodeserver:
		$cd /appl/MR/dfs
		$dfsnodeserver
	6. If all computers are connected, one computer open a new Inferno shell window and upload the input file and the mapreduce implemention:
		$cd /appl/MR/
		$dfs/upload -s 200000 -r 3 wctest
		$cd mapreduce
		$../dfs/upload wordcount.dis
	7. The host open a new Inferno shell window, and start a jobtrackerserver:
		$cd /appl/MR/mapreduce
		$jobtrackerserver
	8. Other computers start a dfsnodeserver:
		$cd /appl/MR/mapreduce
		$tasktrackerserver
	9. If all computers are connected, one computer open a new Inferno shell window and submit and start a job:
		$sh submit.sh
	10. OK,now the job is running!

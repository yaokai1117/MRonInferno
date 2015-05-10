# MRonInferno
****
a small MapReduce framework on Inferno written in Limbo, just like many other MarReduce framework, our code is divided into two parts, a distrbuted file system and the mapreduce calculation model.

## MapReduce Calculation Model
still in implementation.

## DFS 
this part is the file partition,transport and scheduling module of our MapReduce framework.

* dfsutil: definition and implementation of some basic datastructures used in DFS
* xmlhandle: some tools to transform dfs datastructures into xml file, so that they can be transformed easily  
* dfsmaster: mantain the infomation of every file, chunk and node in the file system
* dfsserver: the server of dfsmaster, receive message from other computing equipment and call appropriate function in dfsmaster
* dfsnodeserver: every data node in dfs use dfsnodeserver to transport file chunks and communicate with master
* dfsclient: provide APIs (such as createFile, getFile) for Mapreduce Calculation Model
* upload, list, download: these program can be called in inferno shell, to upload and download file in distributed file system   

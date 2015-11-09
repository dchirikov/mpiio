## Description

I/O benchmark for shared filesystems.  Only writes are supported now, as usually it slower that reads.

## Details

Shared filesystem usually can handle more IO than the local ones, so it is needed to test them using several nodes (clients). Because of the nature of concurrent access and probability theory, not all clients are operating with the same speed. Some are faster, other are slower.
The common approach of bencharking is to split desirable amount of data into equal relatively large pieces (number of pieces is equal of the number of clients) and ask client to write (or read) those pieces independently. For instance if we specify 10TB for 10 clients, each client will write (read) 1TB of the data from shared file system.
As a result fastest process will write its 1TB and go to idle and benchmark will finish when the slowest processes put their data onto the storage. In many cases I/O from one or several client processes cannot saturate the bandwidth of the server and benchmark will measure not the performance of the shares storage, but the performance of the slowest processes: "a herd of buffalo can only move as fast as the slowest buffalo".

The other approach is a operate like a team: "do work for your lazy colleagues".
Here the whole amount of the data is split onto equal small pieces (chunks), and a single dedicated process is tracking how many of those pieces are fallen down to the filesystem. As a result, steady speed of I/O is all along the benchmark.

## Some pictures
All pictures were captured from IEEL web-interface (Enterprise Lustre FS by Intel).

#### FIO test from 50 nodes
Here 2 slow processes are 1.5 slower that the fastest ones, and the test lasts 1.5 longer than it could be.
![fio heatmap](img/fio1.png)
![fio write](img/fio2.png)
Resulting speed was about 17 GB/sec

#### MPIio from 50 nodes
Slowest processes are not limiting the duration of the test.
![mpiio heatmap](img/mpiio1.png)
![mpiio write](img/mpiio2.png)
Resulting speed was 24 GB/sec

## Compilation
As MPI is using to track the chunks, so some flavour of MPI is required. Tested on OpenMPI only. 
```
mpicc -lrt mpiio.c
```
```-lrt```  - for posix AIO.

## CLI options
```
    -b <N>  Block size
    -r <N>  Max outstanding request
    -f <N>  File name
    -c <N>  Chunk size
    -s <N>  Number of bytes to write
    -h      This help
    N accepts multipliers [k,K,m,M,g,G,t,T,p,P,e,E], 1k=1000, 1K=1024
```

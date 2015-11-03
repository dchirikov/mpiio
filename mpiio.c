#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>                                                                                                                                                                         
#include <sys/stat.h>                                                                                                                                                                          
#include <fcntl.h>                                                                                                                                                                             
#include <mpi.h>
#include <limits.h>
#include <aio.h>
#include <stdbool.h>

#define M_IFNEWCHUNK    1               // message type 'ask for new chunk'
#define M_BYE           2               // message type 'bye'
#define M_TAG_SERVER    0               // tag message of the server
#define M_TAG_CLIENT    1               // tag message of the client
#define OUTS_REQ        4               // default number of outstanding requests
#define BLOCKSIZE       1048576         // default block size
#define MAXBYTES        1073741824      // default task size
#define CHUNKSIZE       104857600       // default chink size
#define PERCENT_LIM     90              // as we reacheed this % of current chunk done 
                                        //    ask fro another chunk


struct server_data_t {
    MPI_Comm *thread_comm;
    off_t    csize;             // chunk size
    off_t      num;             // number of chunks
    int      numranks;          // how mny ranks do we have
};

struct client_data_t {
    MPI_Comm  *thread_comm;
    off_t      bsize;           // block size
    off_t      csize;           // chunk size
    int        rank;            // runk of the client
    int        maxreqs;         // max outstanding requests
    char       fname[1024];     // file name

};

void *server_thread(void *ptr);
int client_thread(void *ptr);
off_t units_convert(char istr[]);
void print_help(int rank);

int main(int argc,char *argv[]) {
    char pathtobin[1024], testfile[1024];
    struct server_data_t server_data;
    struct client_data_t client_data;
    void *thr_exit_status = (int)0;
    int parm, provided, rank, numranks, data;
    off_t maxbytes;
    MPI_Comm thread_comm;
    pthread_t thread;

    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    MPI_Comm_dup(MPI_COMM_WORLD, &thread_comm);
    client_data.thread_comm = &thread_comm; 
    server_data.thread_comm = &thread_comm;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    //default parameters
    strcpy(testfile, "testmpiio");
    client_data.maxreqs = OUTS_REQ;
    client_data.bsize = BLOCKSIZE;
    client_data.csize = CHUNKSIZE;
    maxbytes = MAXBYTES;
    // reading input parameters
    while ((parm = getopt (argc, argv, "b:r:f:c:s:h")) != -1) {
        switch (parm) {
            case 'b':
                client_data.bsize = units_convert(optarg);
                break;
            case 'r':
                client_data.maxreqs = units_convert(optarg);
                break;
            case 'f':
                strcpy(testfile, optarg);
                break;
            case 'c':
                client_data.csize = units_convert(optarg);
                break;
            case 's':
                maxbytes = units_convert(optarg);
                break;
            case 'h':
                print_help(rank);
                MPI_Finalize();
                exit(0);
            default:
                print_help(rank);
                MPI_Finalize();
                exit(0);
        }
    }
    // check for correct data
    if ((client_data.bsize == 0) || (client_data.maxreqs == 0) || \
        (client_data.fname == "") || (client_data.csize == 0) || (maxbytes == 0) ) {
            print_help(rank);
            MPI_Finalize();
            exit(0);

    }
    if (client_data.bsize > client_data.csize) {
        if (rank == 0) {
            printf("Requested blocksize larger that chunk size. Exiting.\n");
        }
        MPI_Finalize();
        exit(0);
    }
    server_data.num = (off_t)maxbytes/client_data.csize;
    server_data.csize = client_data.csize;
    if (client_data.csize > client_data.csize*server_data.num) {
        if (rank == 0) {
            printf("Requested chunk size larger that requested bytes to write. Exiting.\n");
        }
        MPI_Finalize();
        exit(0);
    }
    // print what we are going to do
    snprintf(client_data.fname, sizeof client_data.fname, "%s.%05d", testfile ,rank);
    if (rank == 0) {
        printf("Rank %05d | Blocksize: %d\n", rank, client_data.bsize);
        printf("Rank %05d | Max outstanding requests: %d\n", rank, client_data.maxreqs);
        printf("Rank %05d | File name: %s\n", rank, client_data.fname);
        printf("Rank %05d | Chunk size %lli\n", rank, client_data.csize);
        printf("Rank %05d | Number of chunks %lli\n", rank, server_data.num);
        printf("Rank %05d | Requested bytes to write: %lli\n", rank, maxbytes);
        printf("Rank %05d | Bytes to write: %lli\n", rank, client_data.csize*server_data.num);
    }
    // how many ranks do we have?
    client_data.rank = rank; 
    MPI_Comm_size(MPI_COMM_WORLD,&numranks);
    server_data.numranks = numranks;
    // run manager process if rank==0
    if (rank == 0) {
        pthread_create(&thread, NULL, server_thread, &server_data);
    } 
    // run client on all ranks
    client_thread(&client_data);
    if (rank == 0 ) {
        pthread_join(thread, &thr_exit_status);
    }
    // exiting
    MPI_Barrier( MPI_COMM_WORLD );
    MPI_Finalize();
    exit(0);
}
void *server_thread(void *ptr) {
    
    int data_in, data_out;
    struct server_data_t server_data = *(struct server_data_t *)ptr;
    int chunks_per_time = 1;
    int rank;
    struct timeval tv_start, tv_finish;
    unsigned long time_sec;
    off_t num_of_chunks = server_data.num;
    double bw;
    
    MPI_Status status;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    printf("Server started\n");
    gettimeofday(&tv_start,NULL);
    // main server loop
    while(1) {
        MPI_Recv(&data_in, 1, MPI_INT, MPI_ANY_SOURCE, M_TAG_CLIENT, *server_data.thread_comm, &status);
        if (data_in == M_IFNEWCHUNK) {
            // printf("Got req for new chunk from %d\n", status.MPI_SOURCE);
            if (num_of_chunks > 0) {
                num_of_chunks -= 1;
            }  else {
                chunks_per_time = 0;
            }
            MPI_Send(&chunks_per_time, 1, MPI_INT, status.MPI_SOURCE, M_TAG_SERVER, *server_data.thread_comm );
            printf("Chunks left\t%d\n", num_of_chunks);
        } 
        if (data_in == M_BYE) {
            printf("Process on rank %d said 'bye'\n", status.MPI_SOURCE);
            server_data.numranks -= 1;
        }
        if (server_data.numranks <= 0) {
            break;
        }
    }
    gettimeofday(&tv_finish,NULL);
    time_sec = tv_finish.tv_sec - tv_start.tv_sec;
    if (time_sec != 0) {
        bw = (double) (((server_data.csize/1048576)*server_data.num)/time_sec);
        printf("BW:\t%.2f MB/sec\n", bw);
    } else {
        printf("BW:\tInf MB/sec\n");
    }
    pthread_exit(0);
}

int client_thread(void *ptr) {
    int data_in, data_out;
    struct client_data_t client_data = *(struct client_data_t *)ptr;
    // struct timeval tv;
    int percent_done = 0; 
    int chunks_lim = 0;
    int need_to_ask = 0;
    int last_chunk = 0;
    int req_was_issued = 0;
    int req_was_sent = 0;
    int isend_in_fly = 1;
    int wait_for_answer = 0;
    int got_answer = 0;
    off_t data_writen = 0;
    int chunks_writen = 0;
    int fd, ret, i;
    bool infly[256];
    struct aiocb my_aiocb[client_data.maxreqs];
    MPI_Status status;
    MPI_Request request;

    data_out = M_IFNEWCHUNK;
    fd = open( client_data.fname, O_WRONLY|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    if (fd < 0) {
        printf("Rank %05d | Error open() for \n", client_data.rank, strerror(errno), client_data.fname);
    }
    for (i=0; i < client_data.maxreqs; i++) {

        bzero( (char *)&my_aiocb[i], sizeof(struct aiocb) );

        // Allocate a data buffer for the aiocb request
        my_aiocb[i].aio_buf = calloc(client_data.bsize+1, 1);
        if (!my_aiocb[i].aio_buf) {
            printf("Rank %05d | Error calloc()[%d]: %s\n", client_data.rank, strerror(errno), i);
            exit(errno);
        }

        // Initialize the necessary fields in the aiocb
        my_aiocb[i].aio_fildes = fd;
        my_aiocb[i].aio_nbytes = client_data.bsize;
        infly[i] = 0;
    }
    
    data_in = -1;
    while(1) {
        /*if (client_data.rank == 0) {
            printf("%05d | Percent done %d %d %d %lli\n", client_data.rank, percent_done, chunks_lim, client_data.csize, data_writen);
        } */ 
        // what hve be done so far?
        percent_done = (int) ((100*(data_writen - client_data.csize*(chunks_lim-1)))/client_data.csize);
        // triggers only on the beginning. Ugly.
        if ((data_writen == 0) && (chunks_lim == 0) && (req_was_issued == 0)) {
            percent_done = 100;
        }
        // we are close to idle. need to ask for another portion of the data
        if ((percent_done > PERCENT_LIM) && (last_chunk == 0) && (req_was_sent == 0) ) {
            need_to_ask = 1;
            got_answer = 0;
        }
        // sending requests
        if ( (need_to_ask == 1) && (req_was_issued == 0 ) && (req_was_sent == 0)) {
            MPI_Isend(&data_out, 1, MPI_INT, 0, M_TAG_CLIENT, *client_data.thread_comm, &request);
            // printf("%05d | Asked for a new chunk\n", client_data.rank);
            req_was_issued = 1;
        }
        // checking if send is finished
        if (req_was_issued == 1) {
            MPI_Test(&request, &isend_in_fly, &status);
            if (isend_in_fly != 0 ) {
                            // printf("%05d req_was_issued \n",client_data.rank);
                req_was_sent = 1;
                req_was_issued = 0;
            }
        }
        // if send request gone, run async receive
        if ((req_was_sent == 1) && (wait_for_answer == 0)) {
            MPI_Irecv(&data_in, 1, MPI_INT, 0, M_TAG_SERVER, *client_data.thread_comm, &request);
                    // printf("%05d wait_for_answer \n",client_data.rank);
            wait_for_answer = 1;
        }
        // check the satatus of the irecv
        if (wait_for_answer == 1) {
            MPI_Test(&request, &isend_in_fly, &status);
            if (isend_in_fly != 0 ) {
                        // printf("%05d got_answer \n",client_data.rank);
                wait_for_answer = 0;
                got_answer = 1;
            }
        }
        // finaly got an answer
        if (got_answer == 1) {
                    // printf("%05d got_answer %d \n",client_data.rank, data_in);
                        if (data_in == -1) {
                            break;
                        }
                        if (data_in > 0) {
                            chunks_lim += data_in;
                        } else {
                            last_chunk = 1;
                        // printf("%05d last_chunk \n",client_data.rank);
                        }
            req_was_sent = 0;
            need_to_ask = 0;
            got_answer = 0;
        }
        // check if we done what needed
        if ((last_chunk == 1) && (data_writen >= chunks_lim * client_data.csize )) {
            break;
        }
        // we are here? that is bad. Probably needs  to decrease PERCENT_LIM
        if (percent_done >= 100) {
            continue;
        }
        // writing data to file
        for (i=0; i < client_data.maxreqs; i++) {
            if ( aio_error( &my_aiocb[i] ) == EINPROGRESS ) {
                continue;

            } 
            if ((ret = aio_return( &my_aiocb[i] )) < 0) {
                exit(ret);
            } 
            my_aiocb[i].aio_offset = data_writen;
            if ((ret = aio_write( &my_aiocb[i] )) < 0) {
                exit(errno);
            }

            data_writen += client_data.bsize;
        }
 
    }
    // waiting for the last bytes are in fly
    for  (i=0; i < client_data.maxreqs; i++) {
        while (aio_error( &my_aiocb[i] ) == EINPROGRESS) 
        if ((ret = aio_return( &my_aiocb[i] )) < 0) {
            exit(ret);
        }
    }
    // send 'bye' to manager thread.
    data_out = M_BYE;
    MPI_Send(&data_out, 1, MPI_INT, status.MPI_SOURCE, M_TAG_CLIENT, *client_data.thread_comm );
    return 0;
}
// converting input unis from 1k to 1000 and 1K to 1024
off_t units_convert(char istr[]) {
    off_t multiplier, num_part;
    int base=10;
    char *endptr;
    num_part = strtol(istr, &endptr, base);
    if ((errno == ERANGE && (num_part == LONG_MAX || num_part == LONG_MIN))
           || (errno != 0 && num_part == 0)) {
       return 0;
    }
    if (endptr == istr) {
        return 0;
    }
    multiplier = 1;
    if (*endptr != '\0') {
        if (strcmp(endptr, "k") ==0 ) {
            multiplier = 1000;
        } else  if (strcmp(endptr, "K") ==0 ) {
                multiplier = 1 << 10;
        } else  if (strcmp(endptr, "m") ==0 ) {
                multiplier = 1000000;
        } else  if (strcmp(endptr, "M") ==0 ) {
                multiplier = 1 << 20;
        } else  if (strcmp(endptr, "g") ==0 ) {
                multiplier = 1000000000;
        } else  if (strcmp(endptr, "G") ==0 ) {
                multiplier = 1 << 30;
        } else  if (strcmp(endptr, "t") ==0 ) {
                multiplier = 1000000000000;
        } else  if (strcmp(endptr, "T") ==0 ) {
                multiplier = 1UL << 40;
        } else  if (strcmp(endptr, "p") ==0 ) {
                multiplier = 1000000000000000;
        } else  if (strcmp(endptr, "P") ==0 ) {
                multiplier = 1UL << 50;
        } else  if (strcmp(endptr, "e") ==0 ) {
                multiplier = 1000000000000000000;
        } else  if (strcmp(endptr, "E") ==0 ) {
                multiplier = 1UL << 60;
        } else {
                multiplier = 0;
        }
    }
    return num_part*multiplier;
}
// print help for process rank==1
void print_help(int rank) {
    if (rank == 0) {
        printf("Options:\n");    
        printf("\t-b <N>\tBlock size\n");    
        printf("\t-r <N>\tMax outstanding request\n");
        printf("\t-f <N>\tFile name\n");
        printf("\t-c <N>\tChunk size\n");
        printf("\t-s <N>\tNumber of bytes to write\n");
        printf("\t-h    \tThis help\n");
        printf("\nN accepts multipliers [k,K,m,M,g,G,t,T,p,P,e,E], 1k=1000, 1K=1024\n\n");
    }
}

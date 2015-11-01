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

#define DMODE 0777
#define PERCENT_LIM 95
#define M_IFNEWCHUNK 1
#define M_BYE 2
#define M_TAG_SERVER 0
#define M_TAG_CLIENT 1
#define OUTS_REQ 4
#define BUFSIZE 1048576
#define MAXBYTES 1073741824
#define CHUNKSIZE 107374182400


struct server_data_t {
    MPI_Comm *thread_comm;
    off_t    csize;             //chunk size
    off_t      num;               //number of chunks
    int      numranks;
};
struct client_data_t {
    MPI_Comm  *thread_comm;
    off_t      bsize;             //block size
    off_t      csize;             //chunk size
    int        rank;
    int        maxreqs;
    char       fname[1024];

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
    client_data.bsize = BUFSIZE;
    client_data.csize = CHUNKSIZE;
    maxbytes = MAXBYTES;
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
    if ((client_data.bsize == 0) || (client_data.maxreqs == 0) || \
        (client_data.fname == "") || (client_data.csize == 0) || (maxbytes == 0) ) {
            print_help(rank);
            MPI_Finalize();
            exit(0);

    }
    server_data.num = (off_t)maxbytes/client_data.csize;
    snprintf(client_data.fname, sizeof client_data.fname, "%s.%05d", testfile ,rank);
    printf("Rank %05d | Blocksize: %d\n", rank, client_data.bsize);
    printf("Rank %05d | Max outstanding requests: %d\n", rank, client_data.maxreqs);
    printf("Rank %05d | File name: %s\n", rank, client_data.fname);
    printf("Rank %05d | Chunk size %lli\n", rank, client_data.csize);
    printf("Rank %05d | Requested bytes to write: %lli\n", rank, maxbytes);
    printf("Rank %05d | Bytes to write: %lli\n", rank, client_data.csize*server_data.num);
//    MPI_Finalize();
//    exit(0);
    /*
    if (rank == 0) {
        if ( argc < 4 ) {
            readlink("/proc/self/exe", pathtobin, sizeof(pathtobin));
            printf("Usage: %s block_size chunk_size N\n", pathtobin);
            MPI_Abort(MPI_COMM_WORLD, 0);
        }
    }
    */
/*    client_data.bsize = atoi(argv[1]);

    server_data.csize = atoi(argv[2]);
    client_data.csize = server_data.csize;
    server_data.num = atoi(argv[3]);*/
    client_data.rank = rank; 
    MPI_Comm_size(MPI_COMM_WORLD,&numranks);
    server_data.numranks = numranks;
    if (rank == 0) {
        pthread_create(&thread, NULL, server_thread, &server_data);
    } 
    client_thread(&client_data);
    if (rank == 0 ) {
        pthread_join(thread, &thr_exit_status);
    }
    MPI_Barrier( MPI_COMM_WORLD );
    printf("Rank %05d | exit\n",rank);
    MPI_Finalize();
    exit(0);
}
void *server_thread(void *ptr) {
    int data_in, data_out;
    struct server_data_t server_data = *(struct server_data_t *)ptr;
    int num_of_chunks = 1;
    int rank;
    MPI_Status status;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    printf("Rank %05d | server start", rank);
    while(1) {
        MPI_Recv(&data_in, 1, MPI_INT, MPI_ANY_SOURCE, M_TAG_CLIENT, *server_data.thread_comm, &status);
        printf("Rank %05d | server: receive %d from %d\n", rank, data_in, status.MPI_SOURCE);
        if (data_in == M_IFNEWCHUNK) {

            MPI_Send(&num_of_chunks, 1, MPI_INT, status.MPI_SOURCE, M_TAG_SERVER, *server_data.thread_comm );
            printf("Rank %05d | server: sent %d to %d\n", rank, num_of_chunks, status.MPI_SOURCE);
            if (server_data.num > 1) {
                server_data.num -= 1;
            } else {
                num_of_chunks = 0;
            }
        } 
        if (data_in == M_BYE) {
            server_data.numranks -= 1;
            printf("Rank %05d | server: new numranks %d\n", rank, server_data.numranks);
        }
        if (server_data.numranks <= 0) {
            break;
        }
    }
    printf("Rank %05d | server: done!\n", rank);
    pthread_exit(0);
}

int client_thread(void *ptr) {
    int data_in, data_out;
    struct client_data_t client_data = *(struct client_data_t *)ptr;
    struct timeval tv;
    int percent_done = 0; // percentage of current chunk wroten
    int chunks_lim = 0;
    int need_to_ask = 0;
    int last_chunk = 0;
    int req_was_sent = 0;
    int isend_in_fly = 1;
    off_t data_writen = 0;
    int chunks_writen = 0;
    int fd, ret, i;
    bool infly[256];
    struct aiocb my_aiocb[client_data.maxreqs];
    MPI_Status status;
    MPI_Request request;

    printf("Rank %05d | client start\n",client_data.rank);
//    gettimeofday(&tv,NULL);
    printf("Rank %05d | client_data.bsize %d\n", client_data.rank, client_data.bsize);
//    unsigned long time_in_micros = 1000000 * tv.tv_sec + tv.tv_usec;
    srand(time_in_micros);
    data_out = M_IFNEWCHUNK;
    fd = open( client_data.fname, O_WRONLY|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    if (fd < 0) {
        printf("Rank %05d | Error open() for \n", client_data.rank, strerror(errno), client_data.fname);
    }
    for (i=0; i < client_data.maxreqs; i++) {

        bzero( (char *)&my_aiocb[i], sizeof(struct aiocb) );

        // Allocate a data buffer for the aiocb request
        printf("Rank %05d | client_data.maxreqs %d\n", client_data.rank, client_data.maxreqs);
        my_aiocb[i].aio_buf = calloc(client_data.bsize+1, 1);
        // my_aiocb[i].aio_buf = malloc(client_data.bsize+1);
        if (!my_aiocb[i].aio_buf) {
            printf("Rank %05d | Error calloc()[%d]: %s\n", client_data.rank, strerror(errno), i);
            exit(errno);
        }

        // Initialize the necessary fields in the aiocb
        my_aiocb[i].aio_fildes = fd;
        my_aiocb[i].aio_nbytes = client_data.bsize;
        //printf("Rank %05d | ==================================================test %d\n", client_data.rank, i);
        //printf("Rank %05d | client_data.bsize  %d: %s\n", client_data.rank, client_data.bsize);
        infly[i] = 0;
    }
    while(1) {
        if ((percent_done > PERCENT_LIM) && (last_chunk == 0)) {
            need_to_ask = 1;
        }
        if ( need_to_ask == 1 ) {
            MPI_Isend(&data_out, 1, MPI_INT, 0, M_TAG_CLIENT, *client_data.thread_comm, &request);
            printf("Rank %05d | asked server for chunk\n",client_data.rank);
            need_to_ask = 0;
            req_was_sent = 1;
        }
        if (req_was_sent == 1) {
            MPI_Test(&request, &isend_in_fly, &status);
            if (isend_in_fly !=0 ) {
                MPI_Recv(&data_in, 1, MPI_INT, MPI_ANY_SOURCE, M_TAG_SERVER, *client_data.thread_comm, &status);
                printf("Rank %05d | got data_in %d\n",client_data.rank, data_in);
                if (data_in == -1) {
                    break;
                }
                if (data_in > 0) {
                    chunks_lim += data_in;
                } else {
                    last_chunk = 1;
                }
                printf("Rank %05d | new chunks_lim %d\n",client_data.rank, chunks_lim);
                req_was_sent = 0;
            }
        }
        for (i=0; i < client_data.maxreqs; i++) {
            if ( aio_error( &my_aiocb[i] ) == EINPROGRESS ) {
                continue;

            } 
            if ((ret = aio_return( &my_aiocb[i] )) < 0) {
                printf("Rank %05d | Error aio_return(): %d: %s\n", client_data.rank, ret, strerror(errno) );
                exit(ret);
            } 
            my_aiocb[i].aio_offset = data_writen;
            if ((ret = aio_write( &my_aiocb[i] )) < 0) {
                printf("Rank %05d | Error aio_write()[%d]: %s\n", client_data.rank, strerror(errno), i);
                exit(errno);
            }

            data_writen += client_data.bsize;
        }
 
        printf("Rank %05d | data_writen %d\n",client_data.rank, data_writen);
        if ((last_chunk == 1) && (data_writen >= chunks_lim * client_data.csize )) {
            break;
        }
        percent_done = (int) data_writen % client_data.csize;
        printf("Rank %05d | percent_done %d\n", client_data.rank, percent_done);
        if ((data_writen == 0) && (chunks_lim == 0)) {
            percent_done = 100;
            continue;
        }
    }
    for  (i=0; i < client_data.maxreqs; i++) {
        while (aio_error( &my_aiocb[i] ) == EINPROGRESS) 
        if ((ret = aio_return( &my_aiocb[i] )) < 0) {
            printf("Rank %05d | Error aio_return(): %d: %s\n", client_data.rank, ret, strerror(errno) );
            exit(ret);
        }
    }
    data_out = M_BYE;
    MPI_Send(&data_out, 1, MPI_INT, status.MPI_SOURCE, M_TAG_CLIENT, *client_data.thread_comm );
    printf("Rank %05d | sent master 'bye!'\n", client_data.rank);
    return 0;
}
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

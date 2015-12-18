#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <mpi.h>

#define BUFSIZE 1
#define MODE 0777

int mk2subleveldirs(const int i, const int lim_dir, const char *path0) {
    char path1[1024];
    snprintf(path1, sizeof path1, "level1_%02d",i);
    mkdir(path1, MODE);
    chdir(path1);
    int j;
    for( j=0; j < lim_dir; j++) {
        char path2[1024];
        snprintf(path2, sizeof path2, "level2_%02d", j);
        mkdir(path2, MODE);
        chdir(path2);
        int k;
#pragma omp parallel for
        for( k=0; k < lim_dir; k++ ) {
            char path3[1024];
            snprintf(path3, sizeof path3, "level3_%02d", k);
            int fd = open(path3, O_WRONLY|O_CREAT|O_TRUNC, 0666);
            write(fd,"/n",1);
            close(fd);
        }
        chdir("..");
    }
    chdir("..");
}

int main(int argc,char *argv[]) {

    int myid, numprocs;
    MPI_Init(&argc,&argv);
    MPI_Comm_size(MPI_COMM_WORLD,&numprocs);
    MPI_Comm_rank(MPI_COMM_WORLD,&myid);

    char pathtobin[1024];
    if(argc<2) {
        MPI_Finalize();
        readlink("/proc/self/exe", pathtobin, sizeof(pathtobin));
        printf("Usage: %s number_of_dirs_on_each_level\n", pathtobin);
        exit(0);
    }
    int lim_dir = atoi(argv[1]);
    char cwd[1024],path0[1024];
    getcwd(cwd, sizeof(cwd));

    snprintf(path0, sizeof path0, "%s/metadata_test", cwd);
    mkdir(path0, MODE);
    chdir(path0);

    if (numprocs>lim_dir) {
        printf("number of ranks more than the lim_dir\n");
        MPI_Finalize();

        return 1;
    }
    int step = (lim_dir - (lim_dir % numprocs) + numprocs)/numprocs;
    int start = myid * step;
    int end = (myid + 1 )*step - 1;
    if (end>=lim_dir) {
        end = lim_dir-1;
    }
    int i;
    for( i=start; i <= end; i++ ) {
        int res = mk2subleveldirs(i,lim_dir,path0);
    }
    MPI_Finalize();
    return 0;
}


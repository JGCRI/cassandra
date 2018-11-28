#include <stdio.h>
#include <mpi.h>

/*****************************************************************
 * mpi-thread-test
 *
 * Portably test the thread level supported by an MPI installation.
 *
 * to build:
 *     mpicc -o mpi-thread-test mpi-thread-test.c
 *
 * to run:
 *     mpirun -np 1 mpi-thread-test
 *
 * (some installations may use mpiexec instead of mpirun)
 *
 *****************************************************************/


int main(int argc, char *argv[])
{
  int reqd = MPI_THREAD_MULTIPLE;
  int provided=0;
  const char *tlvl;

  MPI_Init_thread(&argc, &argv, reqd, &provided);

  switch(provided) {
  case MPI_THREAD_SINGLE:
    tlvl = "single";
    break;
  case MPI_THREAD_FUNNELED:
    tlvl = "funneled";
    break;
  case MPI_THREAD_SERIALIZED:
    tlvl = "serialized";
    break;
  case MPI_THREAD_MULTIPLE:
    tlvl = "multiple";
    break;
  default:
    tlvl = "???";
  }


  printf("Supported thread level = %s\n", tlvl);

  MPI_Finalize();

  return 0;
}

  

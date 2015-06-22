#PBS -l nodes=1
#PBS -l walltime=3:00:00:00
#PBS -Agcam
#PBS -lpmem=20gb

cd /lustre/data/rpl/gcam-driver
date
tap -q matlab
tap java6
time ./gcam-driver ./wb-fio.cfg
date


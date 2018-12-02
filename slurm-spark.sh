#!/bin/bash                                                                                                                                                          
#SBATCH --job-name=test                                                                                                                                              
#SBATCH --output=t%j.out                                                                                                                                            
#SBATCH --error=t%j.err                                                                                                                                             
#SBATCH --time=00:10:00                                                                                                                                              
#SBATCH --nodes=2                                                                                                                                                    
#SBATCH --cpus-per-task=1                                                                                                                                            
#SBATCH --exclusive                                                                                                                                                  
##SBATCH --account=cse470f18                                                                                                                                         
#SBATCH --partition=debug                                                                                                                                            
##SBATCH --constraint=IB                                                                                                                                             
#SBATCH --mem=48000                                                                                                                                                  
#SBATCH --tasks-per-node=12                                                                                                                                          
#SBATCH --qos=debug

rm -d -R out
module load python
module load spark

# IF SET TO 1 SPARK MASTER RUNS ON A SEPARATE NODE
exclude_master=0

# IF SET TO 1 SCRATCH AND TMP WILL BE RM -RF (RECOMMENDED)
nodes_clean=1

# MAKE SURE THAT SPARK_LOG_DIR, SPARK_LOCAL_DIRS AND SPARK_WORKER_DIR
# ARE SET IN YOUR BASHRC, FOR EXAMPLE:
# export SPARK_LOG_DIR=/scratch/
# export SPARK_LOCAL_DIRS=/scratch/
# export SPARK_WORKER_DIR=/scratch/

# ADD EXTRA MODULES HERE IF NEEDED
# YOU MAY WANT TO CHANGE SPARK VERSION IF CCR MAKES UPDATE


# SET YOUR COMMAND AND ARGUMENTS
PROG="a2.py"
ARGS=""

# SET EXTRA OPTIONS TO spark-submit (CHECK SPARK DOCUMENTATION FOR DETAILS)
# EXAMPLE OPTIONS:
# --num-executors
# --executor-cores
# --executor-memory
# --driver-cores
# --driver-memory
# --py-files
SPARK_ARGS="--num-executors=24"



####### DO NOT EDIT BELOW (HERE CLUSTER IS DEPLOYED AND JOB CREATED)
SPARK_PATH=$SPARK_HOME

# GET LIST OF NODES
NODES=(`srun hostname | sort | uniq`)

NUM_NODES=${#NODES[@]}
LAST=$((NUM_NODES - 1))

# FIRST NODE IS MASTER
ssh ${NODES[0]} "cd $SPARK_PATH; ./sbin/start-master.sh"
MASTER="spark://${NODES[0]}:7077"

WHO=`whoami`
echo -e "you can use this:\n ssh $WHO@rush.ccr.buffalo.edu -L 4040:${NODES[0]}:4040 -N\nto enable local dasboard"

TEMP_OUT_DIR=$SLURM_SUBMIT_DIR/spark-$SLURM_JOB_ID

# ALL NODES ARE WORKERS
mkdir -p $TEMP_OUT_DIR
for i in `seq $exclude_master $LAST`; do
  ssh ${NODES[$i]} "cd $SPARK_PATH; nohup ./bin/spark-class org.apache.spark.deploy.worker.Worker $MASTER &> $TEMP_OUT_DIR/nohup-${NODES[$i]}.$i.out" &
done

# SUBMIT JOB
$SPARK_PATH/bin/spark-submit --master $MASTER $SPARK_ARGS $PROG $ARGS

# CLEAN SPARK JOB
ssh ${NODES[0]} "cd $SPARK_PATH; ./sbin/stop-master.sh"

for i in `seq 0 $LAST`; do
  ssh ${NODES[$i]} "killall java"
done

if [ $nodes_clean -eq 1 ]; then
  for i in `seq 0 $LAST`; do
    ssh ${NODES[$i]} "find /scratch ! -path /scratch/$SLURM_JOB_ID -user $(whoami) -delete; find /tmp ! -path /tmp/$SLURM_JOB_ID -user $(whoami) -delete"
  done
fi
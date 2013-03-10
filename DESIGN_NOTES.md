Names:  Herald (service discovery), Magistrate (configuration), Council (coordination), 

Task service 

Assumption:  
A Job is a set of Task(s) to be performed and has a unique ID.
Each task has a unique ID.
Ideally, tasks are idempotent.
If tasks are not idempotent, tasks must be all or nothing and be retry-able. 

For Job/Task workflow:
/sovereign/task/discrete/<job_type>/<job_id>/lock/<task_lock_or_work_partition_lock>
/sovereign/task/discrete/<job_type>/<job_id>/state/<task_id>

Simple work division for \"jobs\" that are continously running:
Same as above but the \"discrete\" will be \"continuous\".

Simple:
The unique task ID is hashed to a number.  This number is taken modulo N, where N is the current number of worker nodes:  call the result X. 
Each worker node grabs a semaphore lock under the task lock path.  The number of permits in the semaphore is equal to the number of worker nodes.
The ordering of the sequence nodes corresponds to the range 0...N, where N is the current number of worker nodes:  call this corresponding index Y.
The worker node that has lock Y which corresponds to X performs the work.
As worker nodes come up and down, the corresponding values of Y and X change dynamically, allowing sets of tasks to be parallelized and individual tasks will not be performed more than once as long as the number of worker nodes does not change while getting through the entire \"set\" of work.
To reduce chance of a task being performed multiple times, a "last_completed" property can be stored in Zookeeper or elsewhere and read by the worker nodes before performing a task.

More comprehensive:
If NOT repeating a task is very important, an exclusive lock can be acquired for each unique task ID.
State can be communicated in the node data for a given task.

Administration thread:
The task service can scan jobs for incomplete tasks and re-schedule them for retries if a task description is provided.
The task service cleans up job nodes and corresponding task children after they pass some configurable age.


//call RequestWork rpc
//if there is no work available, then retry after a timeout, fixed (100 ms)? vs exponential backoff?
//remember to add jitter as well
//if there is work, then call executor and after call report_result RPC
//keep in-flight jobs in memory as well so request_work is not called to overload

//tracking mechanism for in-flight jobs since jobs run concurrentlyt and call executor concurrently too
//semaphore or atomic int?

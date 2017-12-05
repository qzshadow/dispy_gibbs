maxIter = 10
localMode = True
workerNum = 3
masterVarNum = 5
workerVarNum = 4

def init_worker(F, num):
    import pickle
    with open('/tmp/F{0}.pickle'.format(num), 'wb') as f:
        pickle.dump(F, f)
    return "F successfully initialized on worker " + num

def init_master(F):
    import pickle
    with open('/tmp/F.pickle', 'wb') as f:
        pickle.dump(F, f)
    return "F successfully initialized on master", F

def gibbs_worker():
    return

if __name__ == '__main__':
    import dispy
    import numpy as np
    import time

    input_variable = {'B': {
        i: [
            0, [(j-1) * masterVarNum + i for j in range(1, 1 + workerNum * workerVarNum)]
        ] for i in range(1, masterVarNum + 1)
    }}

    input_variable.update({'D' + str(i): {
        masterVarNum + (i-1) * workerVarNum + j: [
                0, [k + masterVarNum * (j-1)  + (i-1) * workerVarNum * masterVarNum for k in range(1, masterVarNum + 1)]
            ] for j in range(1, workerVarNum + 1)
    } for i in range(1, workerNum + 1)})

    input_factor = {'F' + str(i): {
        (i - 1) * (workerVarNum * masterVarNum) + (j - 1) * masterVarNum + k: [
            j + masterVarNum + (i - 1) * workerVarNum, k, 'EQU', 0.9
        ] for j in range(1, workerVarNum + 1) for k in range(1, masterVarNum + 1)
    } for i in range(1, workerNum + 1)}
    # input_variable : class | node | value | factor_id_list
    # input_variable = {
    #     'B': {'f': [0, [5, 6, 9, 10]],
    #           'g': [0, [7, 8, 11, 12]]},
    #
    #     'D1': {'h': [0, [5, 7]],
    #            'i': [0, [6, 8]]},
    #
    #     'D2': {'j': [0, [9, 11]],
    #            'k': [0, [10, 12]]}}
    # input_factor : class | factor_id | [node1, node2, factor_type, weight]
    # input_factor = {
    #     'F3': {5: ['h', 'f', 'EQU', 0.9],
    #            6: ['i', 'f', 'EQU', 0.9],
    #            7: ['h', 'g', 'EQU', 0.9],
    #            8: ['i', 'g', 'EQU', 0.9]},
    #     'F4': {9: ['j', 'f', 'EQU', 0.9],
    #            10: ['k', 'f', 'EQU', 0.9],
    #            11: ['j', 'g', 'EQU', 0.9],
    #            12: ['k', 'g', 'EQU', 0.9]},
    # }
    if localMode:
        worker_map = {str(i):"127.0.0.1" for i in range(1, workerNum + 1)}
        master_ip = "127.0.0.1"
        cluster_init_worker = dispy.JobCluster(init_worker, nodes=list(set(worker_map.values())),
                                               ip_addr=master_ip, reentrant=True)
        cluster_gibbs_worker = dispy.JobCluster(gibbs_worker, nodes=list(set(worker_map.values())),
                                               ip_addr=master_ip, reentrant=True)
    else:
        worker_map = {"1": '18.217.70.175', "2": '18.217.35.186'}

        cluster_init_worker = dispy.JobCluster(init_worker, nodes=list(worker_map.values()),
                                               ext_ip_addr='18.221.159.28', reentrant=True)
        cluster_gibbs_worker = dispy.JobCluster(gibbs_worker, nodes=list(worker_map.values()),
                                                ext_ip_addr='18.221.159.28', reentrant=True)
    time.sleep(2) # wait for all workers discovered by master

    # initialize master
    factors = {}
    # initialize workers
    for key, value in input_factor.items():
        if key[0] == 'F':
            factors.update(value)
            job = cluster_init_worker.submit_node(worker_map[key[1:]], value, key[1:])
            n = job()
            print(n)



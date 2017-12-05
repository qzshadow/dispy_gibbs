maxIter = 100
localMode = True
workerNum = 2
masterVarNum = 1
workerVarNum = 2


def init_worker(F, D, num):
    import pickle
    with open('/tmp/F{0}.pickle'.format(num), 'wb') as f:
        pickle.dump(F, f)
    with open('/tmp/D{0}.pickle'.format(num), 'wb') as f:
        pickle.dump(D, f)
    return "D and F successfully initialized on worker " + num


def init_master(F):
    import pickle
    with open('/tmp/F.pickle', 'wb') as f:
        pickle.dump(F, f)
    return "F successfully initialized on master", F


def gibbs_worker(B, num):
    import pickle
    import numpy as np
    with open('/tmp/D{0}.pickle'.format(num), 'rb') as f:
        D = pickle.load(f)  # type: dict
    with open('/tmp/F{0}.pickle'.format(num), 'rb') as f:
        F = pickle.load(f)  # type: dict
    for var, [_, factor_list] in D.items():
        var_prob = {0: 0, 1: 0}
        for factor_id in factor_list:
            factor = F[factor_id]
            other_var = factor[0] if var == factor[1] else factor[1]
            other_var_val = B[other_var][0]  # TODO optimization
            for val in var_prob.keys():
                if factor[2] == 'EQU':
                    var_prob[val] += (factor[3] if val == other_var_val else 0)
        probability = np.exp(list(var_prob.values()))
        val = np.random.choice(list(var_prob.keys()), p=probability / sum(probability))
        D[var][0] = val

    with open('/tmp/D{0}.pickle'.format(num), 'wb') as f:
        pickle.dump(D, f)

    return D


if __name__ == '__main__':
    import dispy
    import numpy as np
    import time

    input_variable = {'B': {
        i: [
            0, [(j - 1) * masterVarNum + i for j in range(1, 1 + workerNum * workerVarNum)]
        ] for i in range(1, masterVarNum + 1)
    }}

    input_variable.update({'D' + str(i): {
        masterVarNum + (i - 1) * workerVarNum + j: [
            0, [k + masterVarNum * (j - 1) + (i - 1) * workerVarNum * masterVarNum for k in range(1, masterVarNum + 1)]
        ] for j in range(1, workerVarNum + 1)
    } for i in range(1, workerNum + 1)})

    input_factor = {'F' + str(i): {
        (i - 1) * (workerVarNum * masterVarNum) + (j - 1) * masterVarNum + k: [
            j + masterVarNum + (i - 1) * workerVarNum, k, 'EQU', 0.9
        ] for j in range(1, workerVarNum + 1) for k in range(1, masterVarNum + 1)
    } for i in range(1, workerNum + 1)}

    if localMode:
        worker_map = {str(i): "127.0.0.1" for i in range(1, workerNum + 1)}
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
    time.sleep(2)  # wait for all workers discovered by master

    # initialize master
    F = {}
    B = input_variable['B']
    count = {}
    for type, node_info in input_variable.items():
        for var, _ in node_info.items():
            count[var] = {0: 0, 1: 0}

    # initialize workers
    for key, value in input_factor.items():
        if key[0] == 'F':
            F.update(value)
            job = cluster_init_worker.submit_node(worker_map[key[1:]], value, input_variable['D' + key[1:]], key[1:])
            n = job()
            print(n)

    for i in range(maxIter):
        gibbs_worker_jobs = []
        D_variable = {}
        for num, node_ip in worker_map.items():
            job2 = cluster_gibbs_worker.submit_node(node_ip, B, num)
            gibbs_worker_jobs.append(job2)

        # get all the variable assignment on workers
        for job2 in gibbs_worker_jobs:
            D = job2()
            D_variable.update(D)

        # run gibbs sampling on master
        for var, [_, factor_list] in B.items():
            var_prob = {0: 1, 1: 0}
            for factor_id in factor_list:
                factor = F[factor_id]
                other_var = factor[0] if var == factor[1] else factor[1]
                other_var_val = D_variable[other_var][0]
                for val in var_prob.keys():
                    if factor[2] == 'EQU':
                        var_prob[val] += (factor[3] if val == other_var_val else 0)
                probability = np.exp(list(var_prob.values()))
                val = np.random.choice(list(var_prob.keys()), p=probability / sum(probability))
                B[var][0] = val

        # statistic
        for var, [val, factor_list] in B.items():
            count[var][val] += 1
        for var, [val, factor_list] in D_variable.items():
            count[var][val] += 1
        print(count)

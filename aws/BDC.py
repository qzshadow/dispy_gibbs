# maxIter = 10
# localMode = True
# workerNum = 5
# workerVarNum = 4


def init_worker(C, D, num):
    import pickle
    with open('/tmp/C{0}.pickle'.format(num), 'wb') as f:
        pickle.dump(C, f)
    with open('/tmp/D{0}.pickle'.format(num), 'wb') as f:
        pickle.dump(D, f)
    return "C and D initialized successfully on worker" + num


def init_master(B):
    import pickle
    with open('/tmp/B.pickle', 'wb') as f:
        pickle.dump(B, f)
    return "B initialized successfully."


def gibbs_worker(B, num):
    # sample variables in class C
    import pickle
    import numpy as np
    variables = {}
    factors = {}
    with open('/tmp/C{0}.pickle'.format(num), 'rb') as f:
        variables = pickle.load(f)
    with open('/tmp/D{0}.pickle'.format(num), 'rb') as f:
        factors = pickle.load(f)
    # c_var_prob = {0: 0, 1: 0}
    for var, [val, factor_list] in variables.items():
        c_var_prob = {0: 0, 1: 0}
        for factor_id in factor_list:
            factor = factors[factor_id]
            other_var = factor[0] if var == factor[1] else factor[1]
            other_var_val = B[other_var][0]
            for val in c_var_prob.keys():
                if factor[2] == 'EQU':
                    c_var_prob[val] = c_var_prob[val] + (factor[3] if val == other_var_val else 0)
        probability = np.exp(list(c_var_prob.values()))
        val = np.random.choice(list(c_var_prob.keys()), p=probability / sum(probability))
        variables[var][0] = val
    with open('/tmp/C{0}.pickle'.format(num), 'wb') as f:
        pickle.dump(variables, f)
    # calculate partial result for variables in class B
    b_var_prob = {}
    for var in B.keys():
        b_var_prob[var] = {0: 0, 1: 0}
    for var, [val, factor_list] in B.items():
        for factor_id in factor_list:
            if factor_id in factors.keys():
                factor = factors[factor_id]
                other_var = factor[0] if var == factor[1] else factor[1]
                other_var_val = variables[other_var][0]
                for val in b_var_prob[var].keys():
                    if factor[2] == 'EQU':
                        b_var_prob[var][val] = b_var_prob[var][val] + (factor[3] if val == other_var_val else 0)
    return b_var_prob, variables


if __name__ == '__main__':
    import argparse
    import dispy
    import numpy as np
    import time

    parser = argparse.ArgumentParser()
    parser.add_argument("workerNum", type=int)
    parser.add_argument("workerVarNum", type=int)
    parser.add_argument("maxIter", type=int)
    parser.add_argument("localMode", type=int)
    args = parser.parse_args()

    workerNum = args.workerNum
    workerVarNum = args.workerVarNum
    localMode = args.localMode
    maxIter = args.maxIter

    input_variable = {'B': {0: [1, [i for i in range(1, workerNum * workerVarNum + 1)]]}}
    input_variable.update({'C' + str(i): {(i - 1) * workerVarNum + j: [0, [(i - 1) * workerVarNum + j]]
                                          for j in range(1, workerVarNum + 1)} for i in range(1, workerNum + 1)})

    input_factor = {'D' + str(i): {
        j: [0, j, 'EQU', 0.9] for j in range((i - 1) * workerVarNum + 1, i * workerVarNum + 1)
    } for i in range(1, workerNum + 1)}

    if localMode:
        worker_map = {str(i): "127.0.0.1" for i in range(1, workerNum + 1)}
        master_ip = "127.0.0.1"
        cluster_init_worker = dispy.JobCluster(init_worker, nodes=list(set(worker_map.values())),
                                               ip_addr=master_ip, reentrant=True)
        cluster_gibbs_worker = dispy.JobCluster(gibbs_worker, nodes=list(set(worker_map.values())),
                                                ip_addr=master_ip, reentrant=True)
        count = {}
        for type, node_info in input_variable.items():
            for var, _ in node_info.items():
                count[var] = {0: 0, 1: 0}
    else:
        worker_map = {}
        with open("master_ip.conf", 'r') as f:
            master_ip = f.readline()
        with open("worker_ips.conf", 'r') as f:
            for idx, line in enumerate(f.readlines()):
                if idx >= workerNum: break
                worker_map[str(idx + 1)] = line.strip()

        # worker_map = {"1": '18.217.70.175', "2": '18.217.35.186'}

        cluster_init_worker = dispy.JobCluster(init_worker, nodes=list(worker_map.values()),
                                               ext_ip_addr=master_ip, reentrant=True)
        cluster_gibbs_worker = dispy.JobCluster(gibbs_worker, nodes=list(worker_map.values()),
                                                ext_ip_addr=master_ip, reentrant=True)
    time.sleep(2)  # wait for all workers discovered by master

    print(master_ip, worker_map)

    for key, value in input_variable.items():
        if key[0] == 'C':
            job = cluster_init_worker.submit_node(worker_map[key[1:]], value, input_factor['D' + key[1:]], key[1:])
            n = job()
            print(n)

    B = input_variable['B']

    for i in range(maxIter):
        gibbs_worker_jobs = []
        for num, node_ip in worker_map.items():
            # print(num, node_ip)
            job2 = cluster_gibbs_worker.submit_node(node_ip, B, num)
            gibbs_worker_jobs.append(job2)

        b_var_prob = {}
        # for var in B.keys():
        #     b_var_prob[var] = {0: 0, 1: 0}
        for job2 in gibbs_worker_jobs:
            part_b_var_prob, c_variables = job2()
            # print(part_b_var_prob, c_variables)

            for var, probs in part_b_var_prob.items():
                if var not in b_var_prob.keys():
                    b_var_prob[var] = probs
                else:
                    for val, prob in probs.items():
                        b_var_prob[var][val] += prob
            if localMode:
                for var, [val, _] in c_variables.items():
                    count[var][val] += 1
            # print(b_var_prob)
        for var, probs in b_var_prob.items():
            probability = np.exp(list(probs.values()))
            val = np.random.choice(list(probs.keys()), p=probability / sum(probability))
            B[var][0] = val
            # print(B)
            if localMode: count[var][val] += 1

        if localMode: print(count)

    cluster_gibbs_worker.print_status()

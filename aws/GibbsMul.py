maxIter = 500


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
    c_var_prob = {0: 0, 1: 0}
    for var, [val, factor_list] in variables.items():
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
    import dispy
    import numpy as np

    # input_variable : class | node | value | factor_id_list
    input_variable = {'B': {'a': [0, [1, 2, 3, 4]]},
                      'C1': {'b': [0, [1]],
                             'c': [0, [2]]},
                      'C2': {'d': [0, [3]],
                             'e': [0, [4]]}}
    # input_factor : class | factor_id | [node1, node2, factor_type, weight]
    input_factor = {'D1': {1: ['a', 'b', 'EQU', 0.9],
                           2: ['a', 'c', 'EQU', 0.9]},
                    'D2': {3: ['a', 'd', 'EQU', 0.9],
                           4: ['a', 'e', 'EQU', 0.9]}}

    worker_map = {1: '18.217.70.175', 2: '18.217.35.186'} 

    cluster_init_worker = dispy.JobCluster(init_worker, nodes=['18.217.70.175','18.217.35.186'], ext_ip_addr='18.221.159.28', reentrant=True)
    cluster_gibbs_worker = dispy.JobCluster(gibbs_worker, nodes=['18.217.70.175','18.217.35.186'], ext_ip_addr='18.221.159.28', reentrant=True)

    for key, value in input_variable.items():
        if key[0] == 'C':
            job = cluster_init_worker.submit(value, input_factor['D' + key[1]], key[1])
            n = job()
            print(n)

    B = input_variable['B']
    count = {}
    for type, node_info in input_variable.items():
        for var, _ in node_info.items():
            count[var] = {0:0, 1:0}

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
            # print(b_var_prob)
            for var, probs in part_b_var_prob.items():
                if var not in b_var_prob.keys():
                    b_var_prob[var] = probs
                else:
                    for val, prob in probs.items():
                        b_var_prob[var][val] += prob
            for var, [val, _] in c_variables.items():
                count[var][val] += 1
        for var, probs in b_var_prob.items():
            probability = np.exp(list(probs.values()))
            val = np.random.choice(list(probs.keys()), p=probability / sum(probability))
            B[var][0] = val
            # print(B)
            count[var][val] += 1

        print(count)

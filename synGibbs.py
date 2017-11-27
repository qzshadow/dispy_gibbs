maxIter = 100

def init_worker(C, D):
    import pickle
    with open('/tmp/C.pickle', 'wb') as f:
        pickle.dump(C, f)
    with open('/tmp/D.pickle', 'wb') as f:
        pickle.dump(D, f)
    return "C and D initialized successfully."

def init_master(B):
    import pickle
    with open('/tmp/B.pickle', 'wb') as f:
        pickle.dump(B, f)
    return "B initialized successfully."



def gibbs_worker(B):
    # sample variables in class C
    import pickle
    import numpy as np
    variables = {}
    factors = {}
    with open('/tmp/C.pickle', 'rb') as f:
        variables = pickle.load(f)
    with open('/tmp/D.pickle', 'rb') as f:
        factors = pickle.load(f)
    c_var_prob = {0:0, 1:0}
    for var, [val, factor_list] in variables.items():
        for factor_id in factor_list:
            factor = factors[factor_id]
            other_var = factor[0] if var == factor[1] else factor[1]
            other_var_val = B[other_var][0]
            for val in c_var_prob.keys():
                if factor[2] == 'EQU':
                    c_var_prob[val] = c_var_prob[val] + (factor[3] if val == other_var_val else 0)
        probability = np.exp(list(c_var_prob.values()))
        val = np.random.choice(list(c_var_prob.keys()), p = probability / sum(probability))
        variables[var][0] = val
    with open('/tmp/C.pickle', 'wb') as f:
        pickle.dump(variables, f)

    # calculate partial result for variables in class B
    b_var_prob = {}
    for var in B.keys():
        b_var_prob[var] = {0:0, 1:0}
    for var, [val, factor_list] in B.items():
        for factor_id in factor_list:
            factor = factors[factor_id]
            other_var = factor[0] if var == factor[1] else factor[1]
            other_var_val = variables[other_var][0]
            for val in b_var_prob[var].keys():
                if factor[2] == 'EQU':
                    b_var_prob[var][val] = b_var_prob[var][val] + (factor[3] if val == other_var_val else 0)

    return b_var_prob, variables


# def gibbs_master(C, factors):
#     import numpy as np
#     var_prob = {0: 0, 1:0}
#     for var, [val, factor_list] in variables.items():
#         for factor_id in factor_list:
#             factor = factors[factor_id]
#             other_var = factor[0] if var == factor[1] else factor[1]
#             other_var_val = C[other_var][0]
#             for val in var_prob.keys():
#                 if factor[2] == 'EQU':
#                     var_prob[val] = var_prob[val] + (factor[3] if val == other_var_val else 0)
#         probability = np.exp(list(var_prob.values()))
#         val = np.random.choice(list(var_prob.keys()), p=probability / sum(probability))


# def compute_factor(factor):





if __name__ == '__main__':
    import dispy
    import numpy as np
    # input_variable : class | node | value | factor_id_list
    input_variable = {'B': {'c': [0, [1,2]]},
                      'C1': {'a': [1, [1]],
                             'b': [1, [2]]}}
    # input_factor : class | factor_id | [node1, node2, factor_type, weight]
    input_factor = {'D1': {1:['a', 'c', 'EQU', 0.9],
                           2:['b', 'c', 'EQU', 0.8]}}

    cluster_init_worker = dispy.JobCluster(init_worker, nodes=['127.0.0.1'], ip_addr='127.0.0.1', reentrant=True)
    cluster_gibbs_worker = dispy.JobCluster(gibbs_worker, nodes=['127.0.0.1'], ip_addr='127.0.0.1', reentrant=True)

    job = cluster_init_worker.submit(input_variable['C1'], input_factor['D1'])
    n = job()
    # print(n)



    B = input_variable['B']
    count = {}
    for var in B.keys():
        count[var] = {0:0, 1:0}
    for var in input_variable['C1']:
        count[var] = {0:0, 1:0}
    for i in range(maxIter):
        job2 =  cluster_gibbs_worker.submit(B)
        b_var_prob, c_variables = job2()
        # print(b_var_prob, c_variables)

        # print(b_var_prob)
        for var, prob in b_var_prob.items():
            probability = np.exp(list(prob.values()))
            val = np.random.choice(list(prob.keys()), p=probability / sum(probability))
            B[var][0] = val
            # print(B)
            count[var][val] += 1
        for var, [val, _] in c_variables.items():
            count[var][val] += 1

        print(count)








maxIter = 10
localMode = True
workerNum = 1
masterVarNum = 2
workerVarNum = 4

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
    input_variable = {
        'B': {'f': [0, [5, 6, 9, 10]],
              'g': [0, [7, 8, 11, 12]]},

        'D1': {'h': [0, [5, 7]],
               'i': [0, [6, 8]]},

        'D2': {'j': [0, [9, 11]],
               'k': [0, [10, 12]]}}
    # input_factor : class | factor_id | [node1, node2, factor_type, weight]
    input_factor = {
        'F3': {5: ['h', 'f', 'EQU', 0.9],
               6: ['i', 'f', 'EQU', 0.9],
               7: ['h', 'g', 'EQU', 0.9],
               8: ['i', 'g', 'EQU', 0.9]},
        'F4': {9: ['j', 'f', 'EQU', 0.9],
               10: ['k', 'f', 'EQU', 0.9],
               11: ['j', 'g', 'EQU', 0.9],
               12: ['k', 'g', 'EQU', 0.9]},
    }

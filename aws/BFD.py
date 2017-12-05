if __name__ == '__main__':
    import dispy
    import numpy as np
    import time

    # input_variable : class | node | value | factor_id_list
    input_variable = {
                    # 'B' : {'a': [0, [1, 2, 3, 4]],
                    #        'f': [0, [5, 6, 9, 10]],
                    #        'g': [0, [7, 8, 11, 12]]},
                    # 'C1': {'b': [0, [1]],
                    #        'c': [0, [2]]},
                    #
                    # 'C2': {'d': [0, [3]],
                    #        'e': [0, [4]]},
                    'B' : {'f': [0, [5, 6, 9, 10]],
                           'g': [0, [7, 8, 11, 12]]},

                    'D1': {'h': [0, [5, 7]],
                           'i': [0, [6, 8]]},

                    'D2': {'j': [0, [9, 11]],
                           'k': [0, [10,12]]}}
    # input_factor : class | factor_id | [node1, node2, factor_type, weight]
    input_factor = {
                    # 'D1': {1: ['a', 'b', 'EQU', 0.9],
                    #        2: ['a', 'c', 'EQU', 0.9]},
                    # 'D2': {3: ['a', 'd', 'EQU', 0.9],
                    #        4: ['a', 'e', 'EQU', 0.9]},
                    'F3': {5: ['h', 'f', 'EQU', 0.9],
                           6: ['i', 'f', 'EQU', 0.9],
                           7: ['h', 'g', 'EQU', 0.9],
                           8: ['i', 'g', 'EQU', 0.9]},
                    'F4': {9:  ['j', 'f', 'EQU', 0.9],
                           10: ['k', 'f', 'EQU', 0.9],
                           11: ['j', 'g', 'EQU', 0.9],
                           12: ['k', 'g', 'EQU', 0.9]},
    }
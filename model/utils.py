import numpy as np

def find_factors(x):
    s = np.ceil(np.sqrt(x))
    for t in range(int(s))[::-1]:
        if x%t==0:
            return int(min(t, x/t)), int(max(t, x/t))
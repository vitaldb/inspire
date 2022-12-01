import numpy as np
import pandas as pd

# opid mapping 
opids = [y * 10**7 + m * 10**5 + d * 10**3 + n for y in range(11,21) for m in range(1,13) for d in range(1,32) for n in range(0, 1000)]
deopids = np.random.choice(10**8, len(opids), replace=False) + 3*10**8

pd.DataFrame({'opid':opids, 'deopid':deopids}).to_csv('opid_mapping.csv', index=False, encoding='utf-8-sig')
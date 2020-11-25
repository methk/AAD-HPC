from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
import numpy as np
import pandas as pd
import os, sys

PATH = os.path.join(os.sep, 'galileo', 'home', 'userexternal', 'mberti00', 'Scripts', 'Output')
data = pd.read_csv(os.path.join(PATH, 'DATA_FLTR.csv'))

X_std = StandardScaler().fit_transform(data.values)
pca = PCA(n_components = data.shape[1])
res = pca.fit(X_std)

# write pcs result on file
fl = open(os.path.join(PATH, 'PCA.csv'), 'w')
fl.write('VAR,' + ','.join([c for c in data.columns]) + '\n')
for i, var in enumerate(pca.explained_variance_ratio_):
    fl.write(str(round(var*100, 2)) + ',' + ','.join([str(c) for c in pca.components_[i]]) + '\n')
fl.close()

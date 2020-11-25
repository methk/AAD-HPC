import pandas as pd
import os, re, time

PATH = os.path.join(os.sep, 'galileo', 'home', 'userexternal', 'mberti00', 'Scripts', 'Output')

def natural_keys(text):
    return [ (int(c) if c.isdigit() else c) for c in re.split(r'(\d+)', text) ]

idx_to_remove = []
# node 1
for _ in range(8640):
    idx_to_remove.append(_)
# node 8 and 9
for _ in range(60480, 77760):
    idx_to_remove.append(_)
# node 16
for _ in range(129600, 138240):
    idx_to_remove.append(_)

sorted_dirs = sorted(os.listdir(PATH), key=natural_keys)
data = pd.DataFrame(index = range(len(sorted_dirs) * 354240))

for day_i, day_dir in enumerate(sorted_dirs):
    ddata = pd.DataFrame(index = range(day_i * 354240, (day_i + 1) * 354240))
    cpu_core_temp = pd.DataFrame(index = range(day_i * 354240, (day_i + 1) * 354240))
    dimm_temp = pd.DataFrame(index = range(day_i * 354240, (day_i + 1) * 354240))
    mem_buf_temp = pd.DataFrame(index = range(day_i * 354240, (day_i + 1) * 354240))

    for counter, metric_file in enumerate(sorted(os.listdir(os.path.join(PATH, day_dir)), key=natural_keys)):
        if 'power' in metric_file or 'ASETEK' in metric_file or 'LITEON' in metric_file or 'PWR_IO' in metric_file or 'PWR_STORE' in metric_file or 'WINKCNT_P0' in metric_file:
            continue

        stime = time.time()

        df = pd.read_csv(os.path.join(PATH, day_dir, metric_file))
        df = df.drop(df.index[idx_to_remove]) # remove node 1, 8, 9 and 16
        df.index = range(day_i * 354240, (day_i + 1) * 354240) # reindex

        # ----- MERGE COLUMNS IN ONE (THE MEAN) -----

        # get all cmps in the metric
        cmps = []
        for col in df.columns:
            if 'CMP_' in col:
                cmp = col.split('CMP_', 1)[1].split('#', 1)[0]
                if cmp not in cmps:
                    cmps.append(cmp)

        if len(cmps) == 0:
            if 'CPU_Core_Temp' in metric_file:
                cpu_core_temp = cpu_core_temp.join(df)
                print(metric_file + ' - ' + str(cpu_core_temp.shape))
            elif 'DIMM' in metric_file:
                dimm_temp = dimm_temp.join(df)
                print(metric_file + ' - ' + str(dimm_temp.shape))
            elif 'Mem_Buf_Temp' in metric_file:
                mem_buf_temp = mem_buf_temp.join(df)
                print(metric_file + ' - ' + str(mem_buf_temp.shape))
            else:
                new_col = df.columns[0].split('#', 1)[0]
                df_mean = pd.DataFrame({new_col : df.mean(axis=1)})
                df_mean.index = df.index
                ddata = ddata.join(df_mean)
        else:
            for cmp in cmps:
                cmp_cols = [col for col in df.columns if 'CMP_' + cmp in col]
                df_fltr = df.loc[:, cmp_cols]

                tmp = df_fltr.columns[0].split('#', 1)
                new_col = tmp[0] + ('' if tmp[1] == '' or len(cmps) == 1 else '#' + tmp[1].split('#', 1)[0])

                df_mean = pd.DataFrame({new_col : df_fltr.mean(axis=1)})
                df_mean.index = df.index
                ddata = ddata.join(df_mean)

        print(metric_file.replace('.parquet', '') + ' - SHAPE: ' + str(ddata.shape) + ' - TIME: ' + str(round(time.time() - stime, 2)) + 's')
        del df

    # Merge CPU Core Temps
    core_temp_mean = pd.DataFrame({'CPU_Core_Temp' : cpu_core_temp.mean(axis=1)})
    core_temp_mean.index = cpu_core_temp.index
    ddata = ddata.join(core_temp_mean)

    # Merge DIMM Temps
    dimm_temp_mean = pd.DataFrame({'DIMM_Temp' : dimm_temp.mean(axis=1)})
    dimm_temp_mean.index = dimm_temp.index
    ddata = ddata.join(dimm_temp_mean)

    # Merge Memory Buffer Temps
    mem_buf_temp_mean = pd.DataFrame({'Mem_Buf_Temp' : mem_buf_temp.mean(axis=1)})
    mem_buf_temp_mean.index = mem_buf_temp.index
    ddata = ddata.join(mem_buf_temp_mean)

    data = data.combine_first(ddata)
    del ddata

    print('DATA SHAPE: ' + str(data.shape))

# interpolate (max consequent nans = 4)
data = data.interpolate(method ='linear') 

data.to_csv(os.path.join(PATH, 'DATA_FLTR.csv'), header=True, index=False)


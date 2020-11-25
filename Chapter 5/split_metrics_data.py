import pandas as pd
import os, re, itertools, copy, datetime, json, time, sys

DATE = pd.to_datetime(sys.argv[1]).tz_localize('Europe/Rome')
DAY_STAMP = 'FROM_' + DATE.strftime('%d_%m_%Y_%H%M%S') + '_TO_' + (DATE + datetime.timedelta(days=1)).strftime('%d_%m_%Y_%H%M%S')
PATH = os.path.join(os.sep, 'gpfs', 'scratch', 'userexternal', 'mberti00', 'Backups')
METRICS = os.path.join(PATH, 'Metrics', DAY_STAMP)
OUTPUT = os.path.join(os.sep, 'galileo', 'home', 'userexternal', 'mberti00', 'Scripts', 'Output', DAY_STAMP)
if not os.path.exists(OUTPUT):
    os.makedirs(OUTPUT)

with open(os.path.join(os.sep, 'galileo', 'home', 'userexternal', 'mberti00', 'Scripts', 'output.min.json')) as json_file:
    metrics_data = json.load(json_file)

def natural_keys(text):
    return [ (int(c) if c.isdigit() else c) for c in re.split(r'(\d+)', text) ]

def prepare_data_and_combine(node, feature_name, p_data):
    stime = time.time()

    to_df = pd.DataFrame(data=p_data[['timestamp','value']])
    to_df.rename(columns={'value': feature_name}, inplace=True)
    to_df.set_index('timestamp', inplace=True)

    # remove duplicates
    to_df = to_df[~to_df.index.duplicated(keep='last')]

    # creare correct timestamp indices, reindex existing ones and interpolate Nan values
    correct_idx = pd.date_range(start=DATE, periods=8640, freq='10s')
    to_df = to_df.reindex(correct_idx, fill_value=None)
    to_df[feature_name] = pd.to_numeric(to_df[feature_name], errors='coerce')
    #to_df = to_df.interpolate(method='linear')

    # reset index from timestamp to numeric
    to_df.index = range(8640 * (node - 1), 8640 * node)

    global data
    data = data.combine_first(to_df)

    print(f'NODE {node} - {feature_name} - SHAPE {data.shape} - TIME: {round(time.time() - stime, 2)}s')


data = pd.DataFrame(index = range(8640))

for counter, metric in enumerate(sorted(os.listdir(METRICS), key=natural_keys)):
    metric = metric.replace('.parquet', '')
    print(f'{counter + 1}/160 - {metric}')

    if metric != 'power':
        metric_data = pd.read_parquet(os.path.join(METRICS, metric + '.parquet'), engine='pyarrow', columns=['timestamp', 'value', 'node', 'id', 'occ', 'cmp'])
        metric_data.columns = [col.decode('utf-8') for col in metric_data.columns]
        metric_data['timestamp'] = pd.to_datetime(metric_data['timestamp'], unit='ms').dt.tz_localize('GMT').dt.tz_convert('Europe/Rome').apply(lambda dt : dt.round('5s'))

    for node in range(1, 46):
        md = metrics_data[metric][node-1]

        if 'fltr' in md:
            for cmp in md['fltr']:
                cmp_val = list(cmp.keys())[0]
                for k, occ in enumerate(cmp[cmp_val]):
                    occ_val = list(occ.keys())[0]
                    for id_ in cmp[cmp_val][k][occ_val]:
                        qry = "node == 'davide" + str(md['node']) + "' "
                        qry += "& cmp == '" + cmp_val + "' "
                        qry += "& occ == '" + occ_val + "' "
                        qry += "& id == '" + id_ + "' "
                        feature_name = metric + '#CMP_' + cmp_val + '#OCC_' + occ_val + '#ID_' + id_
                        prepare_data_and_combine(node, feature_name, metric_data.query(qry))
        elif 'ts' in md: # power metric
            metric_data = pd.read_parquet(os.path.join(METRICS, metric, f'power.davide{str(node).zfill(2)}.parquet'), engine='pyarrow', columns=['timestamp', 'value', 'ts'])
            metric_data.columns = [col.decode('utf-8') for col in metric_data.columns]

            qry = "ts == '" + md['ts'] + "'"
            metric_data = metric_data.query(qry)
            metric_data['timestamp'] = pd.to_datetime(metric_data['timestamp'], unit='ms').dt.tz_localize('GMT').dt.tz_convert('Europe/Rome').apply(lambda dt : dt.round('s'))

            prepare_data_and_combine(node, metric, metric_data)
        else:
            # NOTE: Asetek and Liteon reuse the same columns for asetek-{1, 2, 3} and liteon-{1 - 2, 3 - 4, 5 - 6}
            node_val = md['node']
            if isinstance(node_val, list): # liteon
                for nv in node_val:
                    qry = "node == '" + nv + "' "
                    feature_name = metric + '#LITEON_' + ('A' if int(nv[-1]) % 2 != 0 else 'B')
                    prepare_data_and_combine(node, feature_name, metric_data.query(qry))
            else:
                qry = "node == 'davide" + str(node_val) + "' " if isinstance(node_val, int) else "node == '" + node_val + "' "
                m_data = metric_data.query(qry)

                if m_data.empty: # nodes with no value recorded
                    data = data.combine_first(pd.DataFrame(None, index=range(8640 * (node - 1), 8640 * node), columns=data.columns))
                else:
                    prepare_data_and_combine(node, metric, m_data)

    del metric_data
    # save metric data to file and clear data structure (improves performances)
    data.to_csv(os.path.join(OUTPUT, f'{counter}_{metric}.csv'), header=True, index=False)
    data = pd.DataFrame(index = range(8640))

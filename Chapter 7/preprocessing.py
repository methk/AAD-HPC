# usage: python preprocess.py 2019-12-8 2019-12-9 159

import os
import sys
import time
import json
import pprint
import pandas as pd
import datetime as dt
from datetime import datetime
#from tabulate import tabulate
from collections import defaultdict

# pandas setup
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

# pretty Print setup
pp = pprint.PrettyPrinter(indent=4)

FROM_DATE = pd.to_datetime(sys.argv[1] + ' 00:00:00').tz_localize('Europe/Rome')
TO_DATE = pd.to_datetime(sys.argv[2] + ' 00:00:00').tz_localize('Europe/Rome')

# paths
BACKUPS = os.path.join(os.sep, 'gss', 'gss_work', 'DRES_examon', 'Backups')
JOBS = os.path.join(BACKUPS, 'Jobs')
METRICS = os.path.join(BACKUPS, 'Metrics')
TIMESERIES = os.path.join(BACKUPS, 'Timeseries')
OLD = os.path.join(JOBS, 'FROM_' + FROM_DATE.strftime('%d_%m_%Y_%H%M%S') + '_TO_' + TO_DATE.strftime('%d_%m_%Y_%H%M%S'), 'old')
DAY = os.path.join(METRICS, 'FROM_' + FROM_DATE.strftime('%d_%m_%Y_%H%M%S') + '_TO_' + TO_DATE.strftime('%d_%m_%Y_%H%M%S'))

MONITORED_NODES = [17, 18, 19, 20, 21, 22, 23, 24, 34, 36, 37, 38, 39, 41, 42, 43, 44, 45]

with open('combs.min.json') as combs_json:
    COMBINATIONS = json.load(combs_json)

# create the directory which will contain all jobs divided into timeseries
if not os.path.exists(TIMESERIES):
    os.makedirs(TIMESERIES)
# create the directory which will contain the jobs of the selected time period divided into timeseries
ts_dir = os.path.join(TIMESERIES, 'FROM_' + FROM_DATE.strftime('%d_%m_%Y_%H%M%S') + '_TO_' + TO_DATE.strftime('%d_%m_%Y_%H%M%S'))
if not os.path.exists(ts_dir):
    os.makedirs(ts_dir)

print('\nCreating timeseries for jobs from ' + FROM_DATE.strftime('%d_%m_%Y_%H%M%S') + ' to ' + TO_DATE.strftime('%d_%m_%Y_%H%M%S'))

# ----- Get jobs info -----
# In this section jobs info that have been run in the selected day is collected.
# If a job starts in the selected day and ends the next day it is stored in a file in order to resume saving its data the next day.

# read jobs info
jobs_info_path = os.path.join(JOBS, 'FROM_' + FROM_DATE.strftime('%d_%m_%Y_%H%M%S') + '_TO_' + TO_DATE.strftime('%d_%m_%Y_%H%M%S'), 'davide_jobs_simplekey.parquet')
jobs_info = pd.read_parquet(jobs_info_path, engine='pyarrow', columns=['job_id', 'nodes', 'start_time', 'end_time'])

# convert timestamps to datetime objects
jobs_info['start_time'] = pd.to_datetime(jobs_info['start_time']).dt.tz_localize('Europe/Rome')
jobs_info['end_time'] = pd.to_datetime(jobs_info['end_time']).dt.tz_localize('Europe/Rome')

# filter jobs executed in valid nodes {node_id: [ [job_id, start_time, end_time], ... ]}
valid_jobs = defaultdict(list)
# format: row = [job_id, nodes, start_time, end_time]
for row in zip(jobs_info['job_id'], jobs_info['nodes'], jobs_info['start_time'], jobs_info['end_time']):
    delta = row[3] - row[2]
    # filter jobs with duration less than 8 hours (8 hours is the maxium time a job can run, jobs running more that 8 hours are launched by system admins)
    if delta.days == 0 and delta.seconds//3600 < 8:
        # if the job executed on more than one node
        if '[' in row[1]:
            used_nodes = []
            # for each node the job executed on (es. [davide10, davide15-18])
            for block in row[1].replace('davide', '')[1:-1].split(','):
                if '-' in block:
                    n, m = block.split('-')
                    used_nodes.extend(list(range(int(n), int(m) + 1)))
                else:
                    used_nodes.append(int(block))

            # check if used nodes have been monitored
            for n in used_nodes:
                if n in MONITORED_NODES:
                    valid_jobs[n].append([int(row[0]), row[2], row[3]])

        # if the job used a single node (which have been monitored) - jobs executed on login nodes are excluded
        elif 'fe' not in row[1] and int(row[1].replace('davide', '')) in MONITORED_NODES:
            n = int(row[1].replace('davide', ''))
            valid_jobs[n].append([int(row[0]), row[2], row[3]])

# if jobs started the day before (saved on the /old folder) add them too
if os.path.exists(OLD):
    for file in os.listdir(OLD):
        if file.startswith('node_') and file.endswith('.csv'):
            f = open(os.path.join(OLD, file), 'r')
            n = int(file.replace('node_', '').replace('.csv', ''))
            next(f) # skip header line
            for line in f:
                row = line.split(',')
                valid_jobs[n].append([int(row[0]), pd.to_datetime(row[1]), pd.to_datetime(row[2])])

# print jobs by execution node
# for key in valid_jobs.keys():
#     print('Node: ' + str(key))
#     print(tabulate(valid_jobs[key], headers='keys', tablefmt='psql'))
# print('\n')

# ----- Save metric values -----
# In this section all metrics values related to the selected metric are stored in separate files according to the combination of metric parameters.
# A distinction has been made based on the type of metric (power, asetek, liteon, others).

# jobs that end the next day are saved in a new file
def save_job_for_tomorrow(job, node):
    tomorrow = job[1].replace(hour=0, minute=0, second=0) + dt.timedelta(days=1)
    the_day_after = tomorrow + dt.timedelta(days=1)

    NEXT_DAY_OLD = os.path.join(JOBS, 'FROM_' + tomorrow.strftime('%d_%m_%Y_%H%M%S') + '_TO_' + the_day_after.strftime('%d_%m_%Y_%H%M%S'), 'old')
    if not os.path.exists(NEXT_DAY_OLD):
        os.makedirs(NEXT_DAY_OLD)

    file = open(os.path.join(NEXT_DAY_OLD, 'node_' + str(node) + '.csv'), 'a')
    # add file header if the file is new
    if os.stat(os.path.join(NEXT_DAY_OLD, 'node_' + str(node) + '.csv')).st_size == 0:
        file.write('job_id,start_time,end_time\n')
        file.flush()
    # if the file does not contain the job yet, write it down
    reader = open(os.path.join(NEXT_DAY_OLD, 'node_' + str(node) + '.csv'), 'r')
    if str(job[0]) not in reader.read():
        file.write(str(job[0]) + ',' + str(job[1]) + ',' + str(job[2]) + '\n')
        print('Saved job ' + str(job[0]) + ' which ends in the future')
    reader.close()

    file.close()

def open_pwr_files(timeseries_files, metric_dir, job_id, node):
    for ts in ['1S', '1MS']:
        file_name = 'JOB_' + str(job_id) + '_NODE_' + str(node) + '_TS_' + ts
        file = open(os.path.join(metric_dir, file_name + '.csv'), 'a')
        file.write('timestamp,value\n') # write header
        file.flush()

        hash_value = hash(file_name) % ((sys.maxsize + 1) * 2)
        timeseries_files[hash_value] = file

# open all files for asetek or liteon metrics
def simple_open_files(timeseries_files, metric_dir, job_id, node, param_name, param_value):
    file_name = 'JOB_' + str(job_id) + '_NODE_' + str(node) + '_' + param_name.upper() + '_' + str(param_value)
    file = open(os.path.join(metric_dir, file_name + '.csv'), 'a')
    file.write('timestamp,value\n') # write header
    file.flush()

    hash_value = hash(file_name) % ((sys.maxsize + 1) * 2)
    timeseries_files[hash_value] = file

# open all files according to all possible combinations of metric parameters
def open_files(timeseries_files, metric_dir, job_id, node):
    for comb in params[node]:
        file_name = 'JOB_' + str(job_id) + '_NODE_' + str(node) + '_CMP_' + comb[0] + '_OCC_' + comb[1] + '_ID_' + comb[2]

        file = open(os.path.join(metric_dir, file_name + '.csv'), 'a')
        file.write('timestamp,value\n') # write header
        file.flush()

        hash_value = hash(file_name) % ((sys.maxsize + 1) * 2)
        timeseries_files[hash_value] = file

metric = sorted(os.listdir(DAY))[int(sys.argv[3])]
selected_metric_file = os.path.join(DAY, metric)
selected_metric = metric.replace('.parquet', '') if metric.endswith('.parquet') else metric

print('Processing metric: ' + selected_metric)

jobs_ending_in_future = []

metric_data = None
params = {}

# ASETEK or LITEON metric
if 'ASETEK' in selected_metric or 'LITEON' in selected_metric:
    metric_data = pd.read_parquet(selected_metric_file, engine='pyarrow', columns=['timestamp', 'value', 'node'])
    metric_data.columns = [col_name.decode('utf-8') for col_name in metric_data] # parse column names to utf-8 strings
    metric_data['timestamp'] = pd.to_datetime(metric_data['timestamp'], unit='ms').dt.tz_localize('GMT').dt.tz_convert('Europe/Rome')
# other metrics except power (power data is already divided by node)
elif 'power' not in selected_metric:
    metric_data = pd.read_parquet(selected_metric_file, engine='pyarrow', columns=['timestamp', 'value', 'node', 'cmp', 'occ', 'id'])
    metric_data.columns = [col_name.decode('utf-8') for col_name in metric_data] # parse column names to utf-8 strings
    metric_data['timestamp'] = pd.to_datetime(metric_data['timestamp'], unit='ms').dt.tz_localize('GMT').dt.tz_convert('Europe/Rome')

# read metrics
for node in valid_jobs:
    asetek_num = (1 if node >= 1 and node <= 15 else (2 if node >= 16 and node <= 30 else 3)) if 'ASETEK' in selected_metric else None
    liteon_num = ([1, 2] if node >= 1 and node <= 15 else ([3, 4] if node >= 16 and node <= 30 else [5, 6])) if 'LITEON' in selected_metric else None

    timeseries_files = {}
    for job in valid_jobs[node]:
        # create directory for the job and for the chosen metric
        start_day = job[1].replace(hour=0, minute=0, second=0)
        next_day = start_day + dt.timedelta(days=1)

        ts_dir = os.path.join(TIMESERIES, 'FROM_' + start_day.strftime('%d_%m_%Y_%H%M%S') + '_TO_' + next_day.strftime('%d_%m_%Y_%H%M%S'))
        job_dir = os.path.join(ts_dir, 'JOB_' + str(job[0]))
        if not os.path.exists(job_dir):
            os.makedirs(job_dir)
        metric_dir = os.path.join(job_dir, selected_metric)
        if not os.path.exists(metric_dir):
            os.makedirs(metric_dir)

        # if a job in this node ends the next day, save it on /old directory
        if job[2] >= TO_DATE:
            # save job for the future only the first time
            if job not in jobs_ending_in_future:
                save_job_for_tomorrow(job, node)
                jobs_ending_in_future.append(job)

        # keep all files open
        if 'ASETEK' in selected_metric:
            simple_open_files(timeseries_files, metric_dir, job[0], node, 'asetek', asetek_num)
        elif 'LITEON' in selected_metric:
            for i in [0, 1]:
                simple_open_files(timeseries_files, metric_dir, job[0], node, 'liteon', liteon_num[i])
        elif 'power' in selected_metric:
            open_pwr_files(timeseries_files, metric_dir, job[0], node)
        else:
            # create combinations of cmp, occ and id for each node and each metric
            comb = []
            if 'fltr' in COMBINATIONS[selected_metric][node-1]:
                fltr = COMBINATIONS[selected_metric][node-1]['fltr']
                for cmp in fltr:
                    cmp_value = list(cmp.keys())[0]
                    for occ in cmp[cmp_value]:
                        occ_value = list(occ.keys())[0]
                        for id_value in occ[occ_value]:
                            comb.append((cmp_value, occ_value, id_value))
                params[node] = comb
                open_files(timeseries_files, metric_dir, job[0], node)
            else:
                file_name = 'JOB_' + str(job[0]) + '_NODE_' + str(node)

                file = open(os.path.join(metric_dir, file_name + '.csv'), 'a')
                file.write('timestamp,value\n') # write header
                file.flush()

                hash_value = hash(file_name) % ((sys.maxsize + 1) * 2)
                timeseries_files[hash_value] = file

    # filter data recorded for this node or (if metric is power) read it from file
    data = None
    if 'ASETEK' in selected_metric:
        data = metric_data.loc[metric_data['node'] == 'asetek-' + str(asetek_num)].sort_values(by='timestamp')
    elif 'LITEON' in selected_metric:
        data = metric_data.loc[(metric_data['node'] == 'liteon-' + str(liteon_num[0])) | (metric_data['node'] == 'liteon-' + str(liteon_num[1]))].sort_values(by='timestamp')
    elif 'power' in selected_metric:
        power_path = os.path.join(DAY, 'power', 'power.davide' + str(node) + '.parquet')
        data = pd.read_parquet(power_path, engine='pyarrow', columns=['timestamp', 'value', 'ts'])
        data.columns = [col_name.decode('utf-8') for col_name in data] # parse column names to utf-8 strings
        data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms').dt.tz_localize('GMT').dt.tz_convert('Europe/Rome')
    else:
        data = metric_data.loc[metric_data['node'] == 'davide' + str(node)].sort_values(by='timestamp')
    zipped = zip(*[data[col] for col in data.columns])
    del data # from now on it will be used zipped values

    t_start = time.time()
    last_date_printed = pd.to_datetime('1970-01-01 00:00:00').tz_localize('Europe/Rome')
    print('-------- NODE ' + str(node) + ' --------')
    # for each timestep in the selected day check if some valid job have been run, if true save its value
    # format: row = [timestamp, value, node, cmp, occ, id]
    for row in zipped:
        # for power metric every hour print info and flush files (it takes long)
        if ('power' in selected_metric and row[0].minute == 0 and row[0].second == 0 and row[0].microsecond == 0 and row[0] != last_date_printed):
            print(row[0])
            last_date_printed = row[0]

            # every hour write data buffered on file
            for file in timeseries_files:
                timeseries_files[file].flush()

            t_end = time.time()
            print('execution time: ' + str(round(t_end - t_start, 1)) + 's\n')
            t_start = t_end

        # format: job = [job_id, start_time, end_time]
        for job in valid_jobs[node]:
            if row[0] >= job[1] and row[0] < job[2]:
                file_name = ''
                if 'LITEON' in selected_metric:
                    file_name = 'JOB_' + str(job[0]) + '_NODE_' + str(node) + '_LITEON_' + row[2].replace('liteon-', '')
                elif 'ASETEK' in selected_metric:
                    file_name = 'JOB_' + str(job[0]) + '_NODE_' + str(node) + '_ASETEK_' + row[2].replace('asetek-', '')
                elif 'power' in selected_metric:
                    file_name = 'JOB_' + str(job[0]) + '_NODE_' + str(node) + '_TS_' + row[3].upper()
                elif 'fltr' in COMBINATIONS[selected_metric][node-1]:
                    file_name = 'JOB_' + str(job[0]) + '_NODE_' + str(node) + '_CMP_' + row[3] + '_OCC_' + row[4] + '_ID_' + row[5]
                else:
                    file_name = 'JOB_' + str(job[0]) + '_NODE_' + str(node)

                hash_value = hash(file_name) % ((sys.maxsize + 1) * 2)
                if hash_value in timeseries_files:
                    timeseries_files[hash_value].write(str(row[0]) + ',' + str(row[1]) + '\n')

    # release unused resources
    for file in timeseries_files:
        timeseries_files[file].close()
    del timeseries_files

print('Completed timeseries for jobs from ' + FROM_DATE.strftime('%d_%m_%Y_%H%M%S') + ' to ' + TO_DATE.strftime('%d_%m_%Y_%H%M%S') + ' - Metric: ' + selected_metric + '\n')

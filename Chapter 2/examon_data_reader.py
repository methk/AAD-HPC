from datetime import datetime, timedelta
from examon.examon import Examon, ExamonQL
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import sys
import time

today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
yesterday = (today - timedelta(1))


# KairosDB server IP
KAIROSDB_SERVER = 'XXX.XXX.XXX.XXX'
# KairosDB server port
KAIROSDB_PORT = 'XXXX'
# DB username
USER = 'XXXXXX'
# DB password
PWD = 'XXXXXX'
# List of metrics to query
METRICS = ['*']
# Time of the first data point
TSTART = yesterday.strftime('%d-%m-%Y') + ' 00:00:00'
# Time of the last data point
TSTOP = today.strftime('%d-%m-%Y') + ' 00:00:00'
# Time zone
TIME_ZONE = 'Europe/Rome'
# Chunk size per metric in ms.
BUFSIZE = 320000
# Output file compression.
COMP = 'pqt'
# Compression options
COMPRESSIONS = { None: '.csv',
                'gzip': '.csv.gz',
                'bz2': '.csv.bz',
                'pqt': '.parquet'}
# Path to Backups
BASEPATH = os.path.join(os.sep, 'davide_scratch', 'userexternal', USER) # os.environ['CINECA_SCRATCH']
# Cassandra cluster local ip
CLUSTER_IP = 'XXX.XXX.XXX.XXX'


if __name__ == '__main__':
    print("------------- JOBS -------------")

    JOBSPATH = os.path.join(BASEPATH, 'Backups', 'Jobs', 'FROM_' + TSTART.replace(' ', '_').replace(':', '').replace('-', '_') + '_TO_' + TSTOP.replace(' ', '_').replace(':', '').replace('-', '_'))
    if not os.path.exists(JOBSPATH):
        os.mkdir(JOBSPATH)

    auth_provider = PlainTextAuthProvider(username='davideuser', password='davideuser')
    cluster = Cluster(contact_points=(CLUSTER_IP,), auth_provider=auth_provider)
    session = cluster.connect('e4_jobs')
    session.row_factory = dict_factory
    session.default_timeout = 600 # 10m
    session.idle_heartbeat_timeout = 600 # 10m
    session.idle_heartbeat_interval = 0

    print("Start query to davide_jobs_simplekey...")
    s_time = time.time()

    result_jobs = session.execute('select * from davide_jobs_simplekey')
    filtered_jobs = []

    for row in result_jobs:
    	if row["start_time"] >= yesterday and row["start_time"] < today:
    		filtered_jobs.append(row)

    print("Saving file...")
    if (COMP == 'pqt'):
        df_jobs = pd.DataFrame(filtered_jobs)
        df_jobs.columns = df_jobs.columns.astype('str')
        df_jobs.to_parquet(os.path.join(JOBSPATH, 'davide_jobs_simplekey' + COMPRESSIONS[COMP]), index=False, engine='pyarrow')
    else:
        print("Compression not available.")

    cluster.shutdown()
    print("[Jobs] Completed! - Total time: %fs" % (time.time() - s_time))


    print("\n\n------------- METRICS -------------")

    if USER == '':
        print("KairosDB Login:")
        USER = raw_input("username: ")
    if PWD == '':
        PWD = raw_input("password: ")

    # ExaMon Client
    ex = Examon(KAIROSDB_SERVER, port=KAIROSDB_PORT, user=USER, password=PWD, verbose=False, proxy=True)
    # ExaMon Query Language
    sq = ExamonQL(ex)

    METRICSPATH = os.path.join(BASEPATH, 'Backups', 'Metrics', 'FROM_' + TSTART.replace(' ', '_').replace(':', '').replace('-', '_') + '_TO_' + TSTOP.replace(' ', '_').replace(':', '').replace('-', '_'))
    if not os.path.exists(METRICSPATH):
        os.mkdir(METRICSPATH)

    if METRICS == ['*']:
        metric_names = ex.query_metricsnames()['results']
    else:
        metric_names = METRICS

    print("Total Metrics: %d" % len(metric_names))

    nodes = ['davide'+ str(x).zfill(2) for x in range(1,46)]

    t0 = time.time()
    metric_counter = 0
    for metric in metric_names:
        if metric == 'power':
            if not os.path.exists(os.path.join(METRICSPATH, 'power')):
                os.mkdir(os.path.join(METRICSPATH, 'power'))

            node_counter = 0
            for node in nodes:
                try:
                    print("POWER metric: node %s" % (node,))
                    t0_q = time.time()
                    node_counter += 1
                    df = sq.SELECT('*')\
                        .FROM(metric)\
                        .WHERE(org='e4', node=node) \
                        .TSTART(TSTART)\
                        .TSTOP(TSTOP)\
                        .execute_async(n_workers=4, threads_per_worker=2, processes=True, batch_size=BUFSIZE, dashboard_address=':4040', interface='eth0', memory_limit='0')

                    print("[" + str(node_counter) + "/45] " + metric + " | Query node: " + node + " | Time: %fs" % (time.time() - t0_q))
                    print('[' + str((time.time() - t0) / 60.0) + 'm] Saving file: ' + metric + '.' + node + COMPRESSIONS[COMP] + '...')
                    df['timestamp'] = df['timestamp'].astype('int64', copy=False)

                    if (COMP == 'pqt'):
                        pathname = os.path.join(METRICSPATH, 'power', metric + '.' + node + COMPRESSIONS[COMP])
                        df.columns = df.columns.astype('str')
                        df.reset_index(inplace=True)
                        df.to_parquet(pathname, index=True, engine='pyarrow')
                    else:
                        print("Compression not available.")
                except Exception as e:
                    print("[ERROR] Excpetion in metric " + metric + " node " + node + ":")
                    print(e)
        else:
            t0_q = time.time()
            metric_counter += 1
            df = sq.SELECT('*')\
                .FROM(metric)\
                .WHERE(org='e4') \
                .TSTART(TSTART)\
                .TSTOP(TSTOP)\
                .execute_async(n_workers=4, threads_per_worker=2, processes=True, batch_size=BUFSIZE, dashboard_address=':4040', interface='eth0', memory_limit='0')

            print("[" + str(metric_counter) + "/159] " + metric + " | Time: %fs" % (time.time() - t0_q))
            print('[' + str((time.time() - t0) / 60.0) + 'm] Saving file: ' + metric + COMPRESSIONS[COMP] + '...')
            df['timestamp'] = df['timestamp'].astype('int64', copy=False)
            pathname = os.path.join(METRICSPATH, metric + COMPRESSIONS[COMP])

            if (COMP == 'pqt'):
                df.columns = df.columns.astype('str')
                df.reset_index(inplace=True)
                df.to_parquet(pathname, index=True, engine='pyarrow')
            else:
                print("Compression not available.")

        del df

    print("[Metrics] Completed! - Total time: %fm" % ((time.time() - t0) / 60.0))

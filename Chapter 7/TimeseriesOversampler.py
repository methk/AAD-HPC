from sklearn.neighbors import NearestNeighbors
from scipy.ndimage import uniform_filter
import matplotlib.pyplot as plt
import numpy as np
import sys, math

class TimeseriesOversampler:

    def generate_new_lengths(self, timeseries, ts_num=1, window_size=6, X=10, plot=True):
        window_ts_lengths = [len(ts) for ts in timeseries]

        windows = [[] for _ in range(int(max(window_ts_lengths)/window_size) + 1)]
        for ts_len in window_ts_lengths:
            window_pos = int(ts_len/window_size)
            windows[window_pos].append(ts_len)

        # compute the percentage of total timeseries in each window
        tot_ts = len(timeseries)
        prob = [len(window)/tot_ts for window in windows]

        # generate random lengths based on percentages computed above; new_lengths contains pairs in the form: (reference_ts_index_within_window, new_length)
        new_lengths = []
        for rand_window in np.random.choice(list(range(len(prob))), ts_num, p=prob):
            # choose a random reference ts within the chosen time window
            ts_in_window = windows[rand_window]
            reference_ts_pos = np.random.randint(len(ts_in_window))
            new_len = ts_in_window[reference_ts_pos] + np.random.uniform(-X, X)
            # length cannot be lower or higher than this window bounds
            if new_len < rand_window*window_size:
                new_len = rand_window*window_size
            elif new_len >= (rand_window + 1)*window_size:
                new_len = (rand_window + 1)*window_size - 1

            new_lengths.append((reference_ts_pos, int(new_len)))

        if plot:
            # plot timeseries lengths distribution for each window
            plt.figure(figsize=(10, 4))
            plt.subplot(1, 2, 1)
            plt.barh(list(range(len(prob))), prob, color='#30475e')
            plt.title('Timeseries lengths distribution')
            # plot actual timeseries lengths
            plt.subplot(1, 2, 2)
            plt.bar(list(range(len(window_ts_lengths))), window_ts_lengths, color='#222831')
            # plot randomly generated lengths
            plt.bar(list(range(len(window_ts_lengths), len(window_ts_lengths)+len(new_lengths))), [p[1] for p in new_lengths], color='#f2a365')
            plt.title('Time series lengths + synthetic lengths')

            plt.tight_layout()
            plt.show()

        return new_lengths

    def get_point_between_two_points(self, p1, p2, d=0.5):
        return [p1[axis] + (p2[axis] - p1[axis]) * d for axis, val in enumerate(p1)]

    def random_point_in_d_ball(self, point, radius=-1):
        # Muller algorithm
        d = len(point)
        u = np.random.normal(0, 1, d)  # an array of d normally distributed random variables
        norm = np.sum(u**2)**(0.5)
        if radius > -1:
            x = [ax*(point[i]*radius) for i, ax in enumerate(u)]/norm # r*u/norm
        else:
            r = np.random.uniform()**(1.0/d) # radius*np.random.uniform()**(1.0/d)
            x = r * u / norm

        return [x[i]+v for i, v in enumerate(point)]

    def get_centroid(self, points):
        centroid = [[] for _ in points[0]]
        l = len(points)

        for point in points:
            for axis, val in enumerate(point):
                centroid[axis].append(val)

        return [sum(values)/l for values in centroid]

    def oversample_timeseries(self, timeseries, window_size=60, ts_num=1, X=8, normal_sd=3.33, sliding_window=3, d=-1, plot_axis=-1):
        new_lengths = sorted(self.generate_new_lengths(timeseries, ts_num, window_size, X, False))
        # sort time series based on timeseries lengths
        timeseries.sort(key=len)

        synthetic_timeseries = []

        for w in range(int(len(timeseries[-1]) / window_size) + 1):
            window_ts = [ts for ts in timeseries if w * window_size <= len(ts) < (w + 1) * window_size] # original timeseries in this window
            window_ts_lengths = [len(ts) for ts in window_ts] # original timeseries lenghts
            window_new_lengths = []
            window_ts_references = []

            for ts_len in new_lengths: # for each new synthetic time series get thos in this window
                if w * window_size <= ts_len[1] < (w + 1) * window_size:
                    window_ts_references.append(ts_len[0])
                    window_new_lengths.append(ts_len[1])

            # skip windows where there are no reference timeseries and any new timeseries to create (second check should be always false if there are no reference ts)
            if len(window_ts) > 0 and len(window_new_lengths) > 0:
                # in the first snapshot for each reference ts get a random neighbour and compute a third point between these two
                first_snapshot_points = [ts[0] for ts in window_ts]
                knn = NearestNeighbors(n_neighbors=len(first_snapshot_points), p=2)
                knn.fit(first_snapshot_points)

                # ----- COMPUTE NEW TIMESERIES STARTING POINTS -----

                # array with starting points for each new timeseries to create
                starting_points = [None for _ in window_new_lengths]

                # for each reference timeseries assign its first point to its paired synthetic timeseries
                for i, _ in enumerate(window_ts_lengths):
                    for j, pos in enumerate(window_ts_references):
                        if pos == i:
                            starting_points[j] = first_snapshot_points[i]

                # ----- GENERATE POINTS FOR NEW TIMESERIES -----

                # values for new ts based on their reference ts
                generated_points = [[window_ts[window_ts_references[i]][0]] for i, _ in enumerate(window_new_lengths)]
                # value of new ts starting from the chosen starter
                new_ts = [[starting_points[i]] for i, _ in enumerate(window_new_lengths)]

                for snapshot in range(1, len(window_ts[-1])):
                    # all values from all the timeseries which have a value in this position
                    points = [ts[snapshot] for ts in window_ts if len(ts) > snapshot]

                    for ts_pos, ts_length in enumerate(window_new_lengths):
                        if snapshot < ts_length:
                            # reference ts for this new ts
                            reference_ts = window_ts[window_ts_references[ts_pos]]

                            # pick a reference value from reference ts with normal distribution around snapshot (both from past or from future)
                            pos = int(np.random.normal(snapshot, normal_sd, 1)[0])
                            if pos < 0:
                                pos *= -1
                            elif pos >= len(reference_ts):
                                pos -= pos - (len(reference_ts) - 1)
                            reference_ts_value = reference_ts[pos]

                            # sample a point around the randomly chosen one
                            dball_point = self.random_point_in_d_ball(reference_ts_value, 0.01)
                            # add the difference between this new point and the last generated to the actual new ts
                            new_point = [round(new_ts[ts_pos][-1][ax]+(dball_point[ax]-generated_points[ts_pos][-1][ax]), 4) for ax, _ in enumerate(dball_point)]

                            new_ts[ts_pos].append(new_point)
                            generated_points[ts_pos].append(dball_point)

                # ----- MOVING AVERAGE -----
                moving_averages = []
                for j in range(len(new_ts)):
                    moving_averages.append([self.get_centroid(new_ts[j][i-sliding_window:i]) for i in range(sliding_window, len(new_ts[j]))])

                synthetic_timeseries.extend(moving_averages)

                # ----- PLOT -----
                if plot_axis > -1:
                    print(f'WINDOW: [{w*window_size}, {(w+1)*window_size}] - {len(window_ts)} timeseries in this window and {len(window_new_lengths)} to be generated.')

                    higher_bound =  max(max([max([s[plot_axis] for s in sublist]) for sublist in window_ts]), \
                                    max([max([s[plot_axis] for s in sublist]) for sublist in moving_averages]))
                    lower_bound  =  min(min([min([s[plot_axis] for s in sublist]) for sublist in window_ts]), \
                                    min([min([s[plot_axis] for s in sublist]) for sublist in moving_averages]))
                    right_bound  =  max(max(map(len, window_ts)), max(map(len, moving_averages)))

                    plt.figure(figsize=(16, 2))
                    plt.title('Original timeseries')
                    plt.ylim(top=higher_bound*1.02, bottom=lower_bound*0.98)
                    plt.xlim(left=0, right=right_bound)
                    ax = plt.gca()
                    ax.set_facecolor('#2E2E2E')

                    dark_colors = ['#488f31', '#78ab63', '#dac767', '#e18745', '#de425b']
                    for i, ts in enumerate(window_ts):
                        data = [v[plot_axis:plot_axis+1] for v in ts[:]]
                        plt.plot(data, color=dark_colors[i%len(dark_colors)])
                    plt.show()

                    plt.figure(figsize=(16, 2))
                    plt.title('Synthetic timeseries')
                    plt.ylim(top=higher_bound*1.02, bottom=lower_bound*0.98)
                    plt.xlim(left=0, right=right_bound)
                    ax = plt.gca()
                    ax.set_facecolor('#2E2E2E')

                    light_colors = ['#ffa600', '#ff6361', '#bc5090', '#58508d', '#003f5c']
                    for i, ts in enumerate(moving_averages):
                        data = [v[plot_axis:plot_axis+1] for v in ts[:]]
                        plt.plot(data, color=light_colors[i%len(light_colors)])
                    plt.show()

        return synthetic_timeseries

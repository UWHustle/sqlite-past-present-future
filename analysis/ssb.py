import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from util import hardware, queries

sns.set()


def plot(bloom=True):
    for hw in hardware:
        df = pd.read_csv(f'data/ssb/{hw}.csv')
        df = df[(df.cache_size == '1 GB') & (df['scale'] == 1)]
        df = df[~((df.system == 'duckdb') & (df.threads == 2))]
        df.loc[(df.system == 'duckdb') & (df['threads'] == 4), 'system'] = 'duckdb_mt'
        df.loc[(df.system == 'sqlite') & df['bloom_filter'], 'system'] = 'sqlite_bloom'

        if not bloom:
            df = df[~(df.system == 'sqlite_bloom')]

        df = (df.groupby('system', sort=False)[queries]
              .agg(['mean', 'min', 'max'])
              .stack()
              .transpose()
              .swaplevel(0, 1, 1)
              .rename_axis('query')
              .reset_index())

        err_lo = df['mean'] - df['min']
        err_hi = df['max'] - df['mean']

        system_list = ['sqlite', 'sqlite_bloom', 'duckdb', 'duckdb_mt'] if bloom else ['sqlite', 'duckdb', 'duckdb_mt']

        df.plot(
            kind='bar',
            x='query',
            y='mean',
            yerr=[[err_lo[s], err_hi[s]] for s in system_list],
            logy=True,
            rot=0,
            figsize=(12, 3),
            legend=False
        )

        plt.grid(False, axis='x')
        plt.xlabel('Query')
        plt.ylabel('Latency (s)')
        plt.tight_layout()
        plt.savefig(f'plots/ssb_bloom_{hw}.pdf' if bloom else f'plots/ssb_{hw}.pdf')


plot(False)
plot(True)

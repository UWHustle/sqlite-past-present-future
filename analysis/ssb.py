import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from util import hardware, queries

sns.set()
sns.set_palette('colorblind')


def plot(bloom=True):
    for hw in hardware:
        sf = 1 if hw == 'rpi' else 5
        df = pd.read_csv(f'data/ssb/{hw}.csv')
        df = df[(df.cache_size == '1 GB') & (df['scale'] == sf)]
        df = df[~((df.system == 'duckdb') & (df.threads == 2))]
        df.loc[(df.system == 'duckdb') & (df['threads'] == 4), 'system'] = 'duckdb_mt'
        df.loc[(df.system == 'sqlite') & df['bloom_filter'], 'system'] = 'sqlite_bloom'

        df = df[~(df.system == 'duckdb_mt')]

        for q in queries:
            df[q] *= 1000

        if not bloom:
            df = df[~(df.system == 'sqlite_bloom')]

        df = (df.groupby('system', sort=False)[queries]
              .agg(['mean', 'min', 'max'])
              .stack()
              .transpose()
              .swaplevel(0, 1, 1)
              .rename_axis('query')
              .reset_index())

        if bloom:
            print('speedup:', df['mean', 'sqlite'].sum() / df['mean', 'sqlite_bloom'].sum())

        err_lo = df['mean'] - df['min']
        err_hi = df['max'] - df['mean']

        system_list = ['sqlite', 'sqlite_bloom', 'duckdb'] if bloom else ['sqlite', 'duckdb']

        ax = df.plot(
            kind='bar',
            x='query',
            y='mean',
            yerr=[[err_lo[s], err_hi[s]] for s in system_list],
            logy=True,
            yticks=[1e2, 1e3, 1e4],
            ylim=(6e1, 1e5),
            rot=0,
            width=0.92 if bloom else 0.75,
            figsize=(12.3, 2.5)
        )

        for patch in ax.patches:
            ax.annotate(
                '{:.0e}'.format(patch.get_height()).replace('+0', ''),
                (patch.get_x() + 0.5 * patch.get_width(), 0.8 * patch.get_height()),
                color='white',
                alpha=0.8,
                size='small',
                ha='center',
                va='top'
            )

        plt.grid(False, axis='x')
        plt.xlabel('Query')
        plt.ylabel('Latency (ms)')
        plt.legend(
            labels=['SQLite', 'SQLite-Bloom', 'DuckDB'] if bloom else ['SQLite', 'DuckDB'],
            ncol=3,
            loc='upper center'
        )
        plt.subplots_adjust(left=0.06, right=0.99, top=0.96, bottom=0.20)
        plt.savefig(f'plots/ssb_bloom_{hw}.pdf' if bloom else f'plots/ssb_{hw}.pdf')


plot(False)
plot(True)

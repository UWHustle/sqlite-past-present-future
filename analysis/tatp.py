import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from util import hardware

sns.set()

for hw in hardware:
    df = pd.read_csv(f'data/tatp/{hw}.csv')
    df = (df[df['cache_size'] == '1 GB']
          .groupby(['records', 'system'], sort=False)[['throughput']]
          .agg(['mean', 'min', 'max'])
          .unstack()
          .droplevel(0, axis=1)
          .reset_index())

    err_lo = df['mean'] - df['min']
    err_hi = df['max'] - df['mean']

    df.plot(
        kind='bar',
        x='records',
        y='mean',
        yerr=[[err_lo[s], err_hi[s]] for s in ['sqlite_WAL', 'sqlite_DELETE', 'duckdb']],
        logy=True,
        ylim=(1, 1e5),
        yticks=[1e1, 1e2, 1e3, 1e4, 1e5],
        rot=0,
        figsize=(5, 2.7),
        legend=False
    )

    plt.grid(False, axis='x')
    plt.xlabel('Subscriber Records')
    plt.ylabel('Throughput (TPS)')
    plt.tight_layout()
    plt.savefig(f'plots/tatp_{hw}.pdf')

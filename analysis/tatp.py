import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from util import hardware

sns.set()
sns.set_palette('colorblind')

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

    ax = df.plot.bar(
        x='records',
        y='mean',
        yerr=[[err_lo[s], err_hi[s]] for s in ['sqlite_WAL', 'sqlite_DELETE', 'duckdb']],
        logy=True,
        ylim=(1, 2e6),
        yticks=[1e1, 1e2, 1e3, 1e4, 1e5],
        rot=0,
        width=0.63,
        figsize=(5.8, 2.5)
    )

    for patch in ax.patches:
        ax.annotate(
            '{:.0e}'.format(patch.get_height()).replace('+0', ''),
            (patch.get_x() + 0.5 * patch.get_width(), 0.65 * patch.get_height()),
            color='white',
            alpha=0.8,
            size='small',
            ha='center',
            va='top'
        )

    plt.grid(False, axis='x')
    plt.xlabel('Subscriber Records')
    plt.ylabel('Throughput (TPS)')
    plt.legend(labels=['SQLite-WAL', 'SQLite-DELETE', 'DuckDB'], ncol=3, loc='upper center')
    plt.subplots_adjust(left=0.12, right=0.99, top=0.90, bottom=0.20)
    plt.savefig(f'plots/tatp_{hw}.pdf')

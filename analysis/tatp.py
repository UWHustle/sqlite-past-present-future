import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns

from util import hardware

mpl.rc('pdf', fonttype=42)
sns.set()

palette = sns.color_palette('colorblind')
palette[1], palette[2] = palette[2], palette[1]
sns.set_palette(palette)

for hw in hardware:
    df = pd.read_csv(f'data/tatp/{hw}.csv')
    df = (df
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
        # yerr=[[err_lo[s], err_hi[s]] for s in ['sqlite_WAL', 'sqlite_DELETE', 'duckdb']],
        logy=True,
        ylim=(2, 5e5),
        yticks=[1e1, 1e2, 1e3, 1e4],
        rot=0,
        width=0.63,
        figsize=(5.8, 2.5)
    )

    for patch in ax.patches:
        ax.annotate(
            '{:.0e}'.format(patch.get_height()).replace('+0', ''),
            (patch.get_x() + 0.5 * patch.get_width(), 2.8 * patch.get_height()),
            # color='white',
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

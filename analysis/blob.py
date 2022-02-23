import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from util import hardware, systems

pd.set_option('display.max_columns', None)

sns.set()
sns.set_palette('colorblind')

for hw in hardware:
    for size in ['100 KB', '10 MB']:
        df = pd.read_csv(f'data/blob/{hw}.csv')
        df = (df[df['size'] == size]
              .groupby(['mix', 'system'], sort=False)[['throughput']]
              .agg(['mean', 'min', 'max'])
              .unstack()
              .droplevel(0, axis=1)
              .reset_index())

        err_lo = df['mean'] - df['min']
        err_hi = df['max'] - df['mean']

        if size == '100 KB' and hw == 'c220g5':
            ylim = (0, 13000)
        elif size == '100 KB' and hw == 'rpi':
            ylim = (0, 2200)
        elif size == '10 MB' and hw == 'c220g5':
            ylim = (0, 260)
        else:
            ylim = (0, 38)

        ax = df.plot(
            kind='bar',
            x='mix',
            y='mean',
            yerr=[[err_lo[s], err_hi[s]] for s in ['sqlite-WAL', 'sqlite-DELETE', 'duckdb', 'filesystem']],
            ylim=ylim,
            rot=0,
            width=0.8,
            figsize=(5.8, 2.5)
        )

        # for patch in ax.patches:
        #     ax.annotate(
        #         '{:.0e}'.format(patch.get_height()).replace('+0', ''),
        #         (patch.get_x() + 0.5 * patch.get_width(), 0.7 * patch.get_height()),
        #         color='white',
        #         alpha=0.8,
        #         size='small',
        #         ha='center',
        #         va='top'
        #     )

        plt.grid(False, axis='x')
        plt.xticks([0, 1, 2], labels=['90%', '50%', '10%'])
        plt.xlabel('Read percentage')
        plt.ylabel('Throughput (TPS)')
        plt.legend(labels=['SQLite-WAL', 'SQLite-DELETE', 'DuckDB', 'Filesystem'], ncol=2, loc='upper center')
        plt.subplots_adjust(left=0.19, right=0.99, top=0.98, bottom=0.24)
        plt.savefig(f'plots/blob_{size}_{hw}.pdf')

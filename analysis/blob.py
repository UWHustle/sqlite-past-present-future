import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from util import hardware, systems

pd.set_option('display.max_columns', None)

sns.set()
sns.set_palette('colorblind')

for hw in hardware:
    for mix in [0.9, 0.5, 0.1]:
        df = pd.read_csv(f'data/blob/{hw}.csv')
        df = (df[df['mix'] == mix]
              .groupby(['size', 'system'], sort=False)[['throughput']]
              .agg(['mean', 'min', 'max'])
              .unstack()
              .droplevel(0, axis=1)
              .reset_index())

        err_lo = df['mean'] - df['min']
        err_hi = df['max'] - df['mean']

        ax = df.plot(
            kind='bar',
            x='size',
            y='mean',
            yerr=[[err_lo[s], err_hi[s]] for s in systems],
            logy=True,
            ylim=(0.4, 5e6),
            yticks=[1, 1e2, 1e4, 1e6],
            rot=0,
            width=0.78,
            figsize=(3.3, 2)
        )

        for patch in ax.patches:
            ax.annotate(
                '{:.0e}'.format(patch.get_height()).replace('+0', ''),
                (patch.get_x() + 0.5 * patch.get_width(), 0.7 * patch.get_height()),
                color='white',
                alpha=0.8,
                size='small',
                ha='center',
                va='top'
            )

        plt.grid(False, axis='x')
        plt.xlabel('Blob size')
        plt.ylabel('Throughput (TPS)')
        plt.legend(labels=['SQLite', 'DuckDB'], ncol=2, loc='upper center')
        plt.subplots_adjust(left=0.19, right=0.99, top=0.98, bottom=0.24)
        plt.savefig(f'plots/blob_{mix}_{hw}.pdf')

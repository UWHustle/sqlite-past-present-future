import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from util import hardware, systems

pd.set_option('display.max_columns', None)

sns.set()

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

        df.plot(
            kind='bar',
            x='size',
            y='mean',
            yerr=[[err_lo[s], err_hi[s]] for s in systems],
            logy=True,
            ylim=(1, 5e5),
            yticks=[1e1, 1e2, 1e3, 1e4, 1e5],
            rot=0,
            figsize=(5, 2.7),
            legend=False
        )

        plt.grid(False, axis='x')
        plt.xlabel('Blob Size')
        plt.ylabel('Throughput')
        plt.tight_layout()
        plt.savefig(f'plots/blob_{mix}_{hw}.pdf')

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from util import hardware

pd.set_option('display.max_columns', None)

sns.set()

palette = sns.color_palette('colorblind')
palette[1], palette[2] = palette[2], palette[1]
sns.set_palette(palette)

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
            ylim = (0, 11000)
            h = 920
        elif size == '100 KB' and hw == 'rpi':
            ylim = (0, 1700)
            h = 140
        elif size == '10 MB' and hw == 'c220g5':
            ylim = (0, 210)
            h = 17
        else:
            ylim = (0, 35)
            h = 3

        ax = df.plot(
            kind='bar',
            x='mix',
            y='mean',
            yerr=[[err_lo[s], err_hi[s]] for s in ['sqlite-WAL', 'sqlite-DELETE', 'duckdb', 'filesystem']],
            ylim=ylim,
            rot=0,
            width=0.85,
            figsize=(5.8, 2.5)
        )

        for i, patch in enumerate(ax.patches):
            height = patch.get_height()
            # label = str(round(height)) if height < 1000 else '{:.0e}'.format(height).replace('+0', '')

            if height > 0:
                label = '{:.0e}'.format(height).replace('+0', '')
            else:
                label = ''

            # if height > 0:
            #     label = str(int(round(height, -int(math.floor(math.log10(height))) + 1)))
            # else:
            #     label = ''

            if size == '100 KB' and hw == 'rpi' and i == 6:
                d = 60
            elif size == '10 MB' and hw == 'rpi' and i == 6:
                d = 1.5
            else:
                d = 0

            ax.annotate(
                label,
                (patch.get_x() + 0.5 * patch.get_width(), patch.get_height() + h + d),
                # color='white',
                alpha=0.8,
                size='small',
                ha='center',
                va='top'
            )

        plt.grid(False, axis='x')
        plt.xticks([0, 1, 2], labels=['90%', '50%', '10%'])
        plt.xlabel('Read percentage')
        plt.ylabel('Throughput (TPS)')
        plt.legend(labels=['SQLite-WAL', 'SQLite-DELETE', 'DuckDB', 'Filesystem'])
        plt.subplots_adjust(left=0.14, right=0.99, top=0.98, bottom=0.2)
        plt.savefig(f'plots/blob_{size}_{hw}.pdf'.replace(' ', '_'))

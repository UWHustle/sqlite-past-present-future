from collections import defaultdict
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from util import queries

sns.set()

for config in ['vanilla', 'bloom']:
    if config == 'vanilla':
        palette = sns.color_palette('colorblind')
        del palette[6]
        sns.set_palette(palette)
    else:
        sns.set_palette('colorblind')

    data = {}
    for query in queries:
        profile = defaultdict(int)
        with open(f'data/ssb/profiles/{config}/{query}.txt', 'r') as f:
            for line in f:
                tokens = line.split()
                profile[tokens[4]] += int(tokens[1])

        data[query] = dict(profile)

    opcodes = ['SeekRowid', 'Column', 'SorterSort', 'Ne', 'Eq', 'Next'] \
        if config == 'vanilla' \
        else ['SeekRowid', 'Column', 'SorterSort', 'Ne', 'Eq', 'Next', 'Filter']

    df = pd.DataFrame.from_dict(data, orient='index')

    print(df.reindex(df.max().sort_values().index, axis=1).columns)

    df['Other'] = df[df.columns.difference(opcodes)].sum(axis=1)
    df = df[opcodes + ['Other']]

    ax = df.plot(
        kind='bar',
        stacked=True,
        ylim=(0, 6e10) if config == 'vanilla' else (0, 2e10),
        yticks=[0, 1e10, 2e10, 3e10, 4e10] if config == 'vanilla' else [0, 1e10],
        rot=0,
        width=0.6,
        figsize=(5.8, 3)
    )

    plt.grid(False, axis='x')
    plt.xlabel('Query')
    plt.ylabel('CPU cycles')
    plt.legend(ncol=4, loc='upper center')
    plt.subplots_adjust(left=0.08, right=0.99, top=0.92, bottom=0.19)
    plt.savefig(f'plots/profile_{config}.pdf')

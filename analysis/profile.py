from collections import defaultdict
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from util import queries

sns.set()

for config in ['vanilla', 'bloom']:
    data = {}
    for query in queries:
        profile = defaultdict(int)
        with open(f'data/ssb/c220g5/profiles/{config}/{query}.txt', 'r') as f:
            for line in f:
                tokens = line.split()
                profile[tokens[4]] += int(tokens[1])

        data[query] = dict(profile)

    df = pd.DataFrame.from_dict(data, orient='index')
    df0 = df.loc[:, (df.max(axis=0) < 5e8)]
    df1 = df.loc[:, (df.max(axis=0) >= 5e8)]
    df1 = df1.assign(other=df0.sum(axis=1))

    df1.plot(kind='bar', stacked=True, figsize=(8, 4))
    plt.xlabel('query')
    plt.ylabel('TSC ticks')
    plt.tight_layout()
    plt.savefig(f'plots/profile_{config}.pdf')

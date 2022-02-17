from collections import defaultdict
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sns.set()

figsize = (8, 4)

for config in ['vanilla', 'bloom']:
    data = {}
    for name in ['1.1', '1.2', '1.3', '2.1', '2.2', '2.3', '3.1', '3.2', '3.3', '3.4', '4.1', '4.2', '4.3']:
        profile = defaultdict(int)
        with open(f'data/ssb/c220g5/profiles/{config}/Q{name}.txt', 'r') as f:
            for line in f:
                tokens = line.split()
                profile[tokens[4]] += int(tokens[1])

        data[name] = dict(profile)

    df = pd.DataFrame.from_dict(data, orient='index')
    df0 = df.loc[:, (df.max(axis=0) < 5e8)]
    df1 = df.loc[:, (df.max(axis=0) >= 5e8)]
    df1 = df1.assign(other=df0.sum(axis=1))

    df1.plot(kind='bar', stacked=True, figsize=figsize)
    plt.xlabel('query')
    plt.ylabel('TSC ticks')
    plt.tight_layout()
    plt.savefig(f'plots/{config}_profile.pdf')

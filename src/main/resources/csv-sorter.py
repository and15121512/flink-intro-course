import pandas as pd

df = pd.read_csv("acronyms.csv")
df.columns = ['commentId', 'time', 'user', 'topic', 'acronym']

dfSorted = df.sort_values(['time'])
dfSorted.to_csv("acronyms_input.csv", header=False, index=False)

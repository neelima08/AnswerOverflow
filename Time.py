import pandas as pd
import numpy as np
from datetime import datetime

df_posts = pd.read_csv('/tmp/posts.csv', delimiter='|', usecols= ['PostTypeId','OwnerUserId', 'LastActivityDate'])

df_posts1 = df_posts.copy()

df_posts.dropna(inplace = True)

df_posts.info()

df = df_posts.loc[df_posts['PostTypeId'] == 2]

df['user'] = df["OwnerUserId"].astype(int)

df.drop('OwnerUserId', axis = 1, inplace = True)

df.drop('PostTypeId', axis = 1, inplace = True)

df.head()

df['Time'] = df['LastActivityDate'].astype(str)
df.drop('LastActivityDate', axis = 1, inplace = True)

df['Time'] = df['Time'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%f"))

sample = df.head()




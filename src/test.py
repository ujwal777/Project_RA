import pandas as pd
df=pd.read_csv("../data/A3.csv")
df2=pd.read_csv("../data/C3.csv")
a=[]
count=0
a=df["a"]
c=df2["c"].values
final=[]
for i in range(len(df)):
    for j in range(len(df2)):
        if(a[i]<=c[j]):
            count+=1



print(count)
# m=-2000
# k=1000
# while(m<k):
#     print(m//600)
#     m=m+600
# df=pd.read_csv("answer.csv")
# df2=pd.read_csv("../data/T.csv")
# df.set_index(list(df.columns)[0],inplace=True)
# vals1=df.values
# vals2=df2.values
# for i in vals2:
#     if i not in vals1:
#         print(i)


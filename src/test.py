import pandas as pd

data = [
    {
        "a": 1,
        "b": 2
    },
    {
        "a": 3,
        "b": 4
    }
    
]
df = pd.DataFrame(data)
cond = df["a"].eq(True)
print(cond.sum())

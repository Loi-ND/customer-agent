from matplotlib import pyplot as plt
from typing import List, Any
import pandas as pd

def agents_categories_stats(agents: pd.DataFrame, cols: List[List[str] | str]) -> None:
    val = []
    columns = []
    for col in cols:
        if isinstance(col, list):
            cond = agents[col].eq(True).all(axis=1)
            val.append(cond.sum())
            column = "--".join(col)
        else:
            cond = agents[col].eq(True)
            val.append(cond.sum())
            column = col
        
        columns.append(column)
    
    fig, ax = plt.subplots()

    bars = ax.bar(columns, val)

    ax.set_ylabel("Number agents")
    ax.set_title("Number of agents for each group")

    for bar in bars:
        height = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width()/2,
            height,
            f"{height}",
            ha='center',
            va='bottom'
        )

    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.show()

def agents_capacity_stats(agents: pd.DataFrame) -> None:

    fig, ax = plt.subplots()
    val = agents["capacity"]
    ax.hist(x=val, bins=50)
    ax.set_ylabel("Number agents")
    ax.set_xlabel("Agent's capacity")
    ax.set_title("Capacity of each agent")
    plt.tight_layout()
    plt.show()

def customers_categories_stats(customers: pd.DataFrame) -> None:
    val = []
    columns = ["life_health", "motor", "travel"]
    for column in columns:
        cond = customers["demand"].eq(column)
        val.append(cond.sum())
    fig, ax = plt.subplots()

    bars = ax.bar(columns, val)

    ax.set_ylabel("Number customers")
    ax.set_title("Number of customers for each group")

    for bar in bars:
        height = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width()/2,
            height,
            f"{height}",
            ha='center',
            va='bottom'
        )

    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.show()
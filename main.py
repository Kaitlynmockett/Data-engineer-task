import json
import pandas as pd

# Load JSON 
with open("orders.json", "r") as file:
    data = json.load(file)

print(data)


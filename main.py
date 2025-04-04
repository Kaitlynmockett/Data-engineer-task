import json
import uuid
import pandas as pd

# Load JSON 
with open("orders.json", "r") as file:
    data = json.load(file)

# Create arrays for eachtable
order = []
customer_details = []
products = []
order_items = []
shipping_details = []
shipping_lines = []
payment_methods = []
billing_details = []
order_discounts = []
discounts = []
charges = []
refunds = []
event = []
store = []
tax_lines = []
fulfillments = []

for record in data:
    # Establish primary key variables
    order_id = record["orderId"]
    customer_id = record["customerReference"]
    product_id = record["productId"]
    order_items_id = record["lineItems"]["id"]

    
    
    
    for item in record["lineItems"]:
            order_items_id = item["id"]
            order_items.append({
                 "orderItemsID": order_id,
                 "orderID": record["orderId"],
                 "productID": item["productId"],
                 "parentLineItemID": item["parentLineItemId"],
                 "pricingQuantity": item["pricingQuantity"],
                 "quantity": item["quantity"],
                 "reference": item["reference"],
                 "lineItemGID": item["references"]["lineItemGid"],
                 "subscription": item["subscription"],
                 "subscriptionRequest": item["subscriptionRequest"],
                 "subtotal": item["amounts"]["subtotal"],
                 "total": item["amounts"]["total"],
                 

            })
            
    

    for item in record["billingDetails"]:
        billing_details_id = str(uuid.uuid4())

    

    
   

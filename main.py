from collections import defaultdict
import json
import uuid
import pandas as pd
from math import isnan


# Load JSON 
with open("orders.json", "r") as file:
    data = json.load(file)

# Create arrays for eachtable
orders = []
customer_details = []
order_items = []
shipping_details = []
shipping_lines = []
billing_details = []
discounts = []
charges = []
event = []
stores = []
tax_lines = []


 # Dictionary for storing order to billingID and shippingDetailsID mapping
order_billing_mapping = {}
order_shipping_mapping = {}
order_charge_mapping = {}

# Create DataFrames for each table
events_df = pd.DataFrame()
orders_df = pd.DataFrame()
master_df = pd.DataFrame()

parsed_data = defaultdict(list)

df_parsed = pd.DataFrame()

# Iterate through the JSON data

for record in data:  # For each event
    event_name = record["event_name"]
    event_id = str(uuid.uuid4())  # Generate a unique event ID

    # Extract order_id from the record
    order_id = record["event_payload"]["order"]["orderId"] 
    billing_details_id = str(uuid.uuid4())# generate GUID
    # Variables for order items
    customer_id = record["event_payload"]["order"]["customerReference"]
    tax_included = record["event_payload"]["order"]["taxIncluded"]
    discount_id = record["event_payload"]["order"]["discountCode"]


    # Add the event to the event table
    event.append({
        "event_id": event_id,
        "event_name": event_name,
        "order_id": order_id
    })

    # Order Items data extraction
    if "lineItems" in record["event_payload"]["order"]:
        for item in record["event_payload"]["order"]["lineItems"]:
            order_items_id = item["id"]
            order_items.append({
                "order_id": order_id,  # Add the foreign key
                "orderItemsID": item["id"],
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
                "appliedDiscounts": item["appliedDiscounts"]
            })

    if "charges" in record["event_payload"]["order"]:
        for charge in record["event_payload"]["order"]["charges"]:
            charges.append({
                "order_id": order_id,  # Add the foreign key
                "charge_id": charge["id"],
                "amount": charge["amount"],
                "currency": charge["currency"],
                "status": charge["status"]
            })

    
    # Check if customerDetails is a dictionary
    if isinstance(record["event_payload"]["order"]["customerDetails"], dict):
        # Extract customer details directly
        customer = record["event_payload"]["order"]["customerDetails"]
        email = customer.get("email")
        firstName = customer.get("firstName")
        lastName = customer.get("lastName")

        # Append to the customer_details table
        customer_details.append({
            "customerID": customer_id,
            "email": email,
            "firstName": firstName,
            "lastName": lastName
        })
    

    # Store data extraction
    if isinstance(record["event_payload"]["store"], dict):
        store = record["event_payload"]["store"]
        store_id = store.get("id")
        name = store.get("name")

        stores.append({"storeID": store_id, "name": name})

        
    # Billing details extraction
    if isinstance(record["event_payload"]["order"]["billingDetails"], dict):
        billingItem = record["event_payload"]["order"]["billingDetails"]
        firstName = billingItem["firstName"]
        lastName = billingItem["lastName"]
        company = billingItem["company"]
        phone = billingItem["phone"]
        city = billingItem["address"]["city"]
        country = billingItem["address"]["country"]
        county = billingItem["address"]["county"]
        line1 = billingItem["address"]["line1"]
        line2 = billingItem["address"]["line2"]
        postcode = billingItem["address"]["postcode"]
        order_billing_mapping[order_id] = billing_details_id

        billing_details.append({
            "billingDetailsID": billing_details_id,
            "orderID": order_id,
            "firstName": firstName,
            "lastName": lastName,
            "company": company,
            "phone": phone,
            "city": city,
            "country": country,
            "county": county,
            "line1": line1,
            "line2": line2,
            "postcode": postcode
    })

    # Shipping Details data extraction
    if isinstance(record["event_payload"]["order"]["shippingDetails"], dict):
        shippingDetail = record["event_payload"]["order"]["shippingDetails"]
        shipping_detail_id = str(uuid.uuid4()) # Generate GUID
        firstName = shippingDetail["firstName"]
        lastName = shippingDetail["lastName"]
        company = shippingDetail["company"]
        phone = shippingDetail["phone"]
        city = shippingDetail["address"]["city"]
        country = shippingDetail["address"]["country"]
        county = shippingDetail["address"]["county"]
        line1 = shippingDetail["address"]["line1"]
        line2 = shippingDetail["address"]["line2"]
        postcode = shippingDetail["address"]["postcode"]
        order_billing_mapping[order_id] = shipping_detail_id
        
        shipping_details.append({
            "shippingDetailID": shipping_detail_id,
            "orderID": order_id,
            "firstName": firstName,
            "lastName": lastName,
            "company": company,
            "phone": phone,
            "city": city,
            "country": country,
            "county": county,
            "line1": line1,
            "line2": line2,
            "postcode": postcode
        })

    # Tax Lines data extraction
    for taxLine in record["event_payload"]["order"]["taxLines"]:
        tax_line_id = taxLine["id"]
        amount = taxLine["amount"]
        currency = taxLine["currency"]
        name = taxLine["name"]
        rate = taxLine["rate"]
        type = taxLine["rateType"]
        reference = taxLine["reference"]
        
        tax_lines.append({
            "taxLineID": tax_line_id, 
            "shippingLineID": shipping_line_id, 
            "orderItemsID": order_items_id, 
            "amount": amount, 
            "currency": currency, 
            "name": name, 
            "rate": rate, 
            "type": type, 
            "reference": reference
        })
    

    # Shipping Lines data extraction
    for item in record["event_payload"]["order"]["shippingLines"]:
        shipping_line_id = str(uuid.uuid4()) # Generate GUID
        amount = item["amount"]
        currency = item["currency"]
        name = item["name"]
        handle= item["handle"]
        reference = item["reference"]

        shipping_lines.append({
            "shipping_id": shipping_line_id,
            "shipping_details_id": shipping_detail_id, # Add foreign key
            "amount": amount,
            "currency": currency,
            "name": name,
            "handle": handle,
            "reference": reference,
            "taxIncluded": tax_included
        })
    # Charges data extraction
    for charge in record["event_payload"]["order"]["charges"]:
        chargeID = charge["id"]
        amount = charge["amount"]
        currency = charge["currency"]
        gateway = charge["gateway"]
        gatewayPaymentMethodReference = charge["gatewayPaymentMethodReference"]
        gatewayReference = charge["gatewayReference"]
        payment_method_id = charge["paymentMethodId"]
        reference = charge["reference"]
        status = charge["status"]
        refunds = charge["refunds"]
        order_charge_mapping[order_id] = chargeID

        charges.append({
            "chargeID": chargeID,
            "orderID": order_id,
            "amount": amount,
            "currency": currency,
            "gateway": gateway,
            "gatewayPaymentMethodReference": gatewayPaymentMethodReference,
            "gatewayReference": gatewayReference,
            "paymentMethodID": payment_method_id,
            "reference": reference,
            "refunds": refunds,
            "status": status
        })
    # Discount codes data extraction
    for discount in record["event_payload"]["order"]["discountCodes"]:
        order = record["event_payload"]["order"]
        code = discount["code"]
        type = order["discountType"]

        discounts.append({
            "discountID": discount_id,
            "code": code, 
            "type": type
        })

    # Initialise orders
    orders.append({
        "orderID": order_id,
        "billingID": None,
        "chargeID": None,
        "shippingDetailsID": None,
        "customerID": customer_id,
        "discountID": discount_id,
        "orderItemsID": order_items_id,
        "storeID": store_id,
        "eventID": event_id,
        "total": record["event_payload"]["order"]["amounts"]["total"],
        "subtotal": record["event_payload"]["order"]["amounts"]["subtotal"],
        "discount": record["event_payload"]["order"]["amounts"]["discount"],
        "channel": record["event_payload"]["order"]["channel"],
        "currency": record["event_payload"]["order"]["currency"],
        "isManual": record["event_payload"]["order"]["isManual"],
        "isTest": record["event_payload"]["order"]["isTest"],
        "note": record["event_payload"]["order"]["note"],
        "date": record["event_payload"]["order"]["placedAt"],
        "risk":record["event_payload"]["order"]["risk"],
        "source": record["event_payload"]["order"]["source"],
        "sourceID": record["event_payload"]["order"]["sourceId"],
        "tags": record["event_payload"]["order"]["tags"],
        "weight": record["event_payload"]["order"]["weight"],
        "fulfillments": record["event_payload"]["order"]["fulfillments"]
    })


# Update the orders with the billingID, chargeID and shippingDetailsID
for ord in orders:
    order_id = ord["orderID"]
    
    # Assign the billingID and shippingDetailsID to the corresponding order
    if order_id in order_billing_mapping:
        ord["billingID"] = order_billing_mapping[order_id]
    
    if order_id in order_shipping_mapping:
        ord["shippingDetailsID"] = order_shipping_mapping[order_id] 
    
    if order_id in order_charge_mapping:
        ord["chargeID"] = order_charge_mapping[order_id]


# List of all table arrays
table_arrays = {
    "orders": orders,
    "customer_details": customer_details,
    "order_items": order_items,
    "shipping_details": shipping_details,
    "shipping_lines": shipping_lines,
    "billing_details": billing_details,
    "discounts": discounts,
    "charges": charges,
    "event": event,
    "stores": stores,
    "tax_lines": tax_lines
}

# List of all table arrays
table_arrays = {
    "orders": orders,
    "customer_details": customer_details,
    "order_items": order_items,
    "shipping_details": shipping_details,
    "shipping_lines": shipping_lines,
    "billing_details": billing_details,
    "discounts": discounts,
    "charges": charges,
    "event": event,
    "stores": stores,
    "tax_lines": tax_lines
}
# Find the maximum length among all arrays
max_length = max(len(table) for table in table_arrays.values())

# Normalize the length of each array
for table_name, table in table_arrays.items():
    if len(table) == 0:
        print(f"Table '{table_name}' is empty. Adding a placeholder row.")
        # If the table is empty, add a placeholder row with None values
        table.append({})
    while len(table) < max_length:
        # Append an empty row with None values
        table.append({key: None for key in table[0].keys()})

# Convert each array to a DataFrame
dataframes = {table_name: pd.DataFrame(table) for table_name, table in table_arrays.items()}

# Access individual DataFrames
df_orders = dataframes["orders"]
df_customer_details = dataframes["customer_details"]
df_order_items = dataframes["order_items"]
df_shipping_details = dataframes["shipping_details"]
df_shipping_lines = dataframes["shipping_lines"]
df_billing_details = dataframes["billing_details"]
df_discounts = dataframes["discounts"]
df_charges = dataframes["charges"]
df_event = dataframes["event"]
df_stores = dataframes["stores"]
df_tax_lines = dataframes["tax_lines"]

df_orders.to_csv("orders.csv", index=False)
df_customer_details.to_csv("customer_details.csv", index=False)
df_order_items.to_csv("order_items.csv", index=False)
df_shipping_details.to_csv("shipping_details.csv", index=False)
df_shipping_lines.to_csv("shipping_lines.csv", index=False)
df_billing_details.to_csv("billing_details.csv", index=False)
df_discounts.to_csv("discounts.csv", index=False)
df_charges.to_csv("charges.csv", index=False)
df_event.to_csv("event.csv", index=False)
df_stores.to_csv("stores.csv", index=False)
df_tax_lines.to_csv("tax_lines.csv", index=False)

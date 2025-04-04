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
billing_details = []
discounts = []
charges = []
event = []
store = []
tax_lines = []


 # Dictionary for storing order to billingID and shippingDetailsID mapping
order_billing_mapping = {}
order_shipping_mapping = {}
print(data["orderId"])
for record in data: # For event
    print("Record: ", record)
   
    # Event data extraction
    event_id = str(uuid.uuid4())
    event_name = record["event_name"]
    

    event.append({"eventID": event_id, "name": event_name})

    for order in record["event_payload"]["order"]:

        # Establish variables
        print("Order: ", order)
        order_id = order["orderId"]
        customer_id = order["customerReference"]
        order_items_id = order["lineItems"]["id"]
        shipping_line_id = order["shippingLines"]["id"]
        tax_included = order["taxIncluded"]
        discount_id = order["discountCode"]
        chargeID = order["charges"]["id"]
        store_id = order["store"]["id"]

        # Order items data extraction
        for item in record["lineItems"]:
                order_items_id = item["id"]
                order_items.append({
                    "orderItemsID": order_items_id,
                    "orderID": order_id,
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
        
        # Billing details extraction
        for billingItem in record["billingDetails"]:
            billing_details_id = str(uuid.uuid4())# generate GUID 
            firstName = billingItem["firstName"]
            lastName = billingItem["lastName"]
            company = billingItem["company"]
            phone = billingItem["phone"]
            city = billingItem["city"]
            country = billingItem["country"]
            county = billingItem["county"]
            line1 = billingItem["line1"]
            line2 = billingItem["line2"]
            postcode = billingItem["postcode"]

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
        
        # Store the mapping between orderID and billingDetailsID
        order_billing_mapping[billingItem["orderId"]] = billing_details_id
            
        
        
        # Store data extraction
        for store in record["store"]:
            store_id = store["id"]
            name = store["name"]

            store.append({"storeID": store_id, "name": name})

        # Customer data extaction
        for customer in record["customerDetails"]:
            email = customer["email"]
            firstName = customer["firstName"]
            lastName = customer["lastName"]
        
        # Shipping Details data extraction
        for shippingDetail in record["shippingDetails"]:
            shipping_detail_id = str(uuid.uuid4()) # Generate GUID
            firstName = shippingDetail["firstName"]
            lastName = shippingDetail["lastName"]
            company = shippingDetail["company"]
            phone = shippingDetail["phone"]
            city = shippingDetail["city"]
            country = shippingDetail["country"]
            county = shippingDetail["county"]
            line1 = shippingDetail["line1"]
            line2 = shippingDetail["line2"]
            postcode = shippingDetail["postcode"]
            
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

            # Store the mapping between orderID and shippingDetailsID
            order_shipping_mapping[shippingDetail["orderId"]] = shipping_detail_id

        # Tax Lines data extraction
        for taxLine in record["taxLines"]:
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
        
        # Products data extraction
        for item in record["lineItems"]:
            product_id = item["productId"]
            sku = item["sku"]
            variant_id = item["variantId"]
            weight = item["weight"]
            taxLineID = item["taxLines"]["id"]

            products.append({
                "productID": product_id, 
                "sku": sku, 
                "variantID": variant_id, 
                "weight": weight,
                "taxLineID": taxLineID
            })

        # Shipping Lines data extraction
        for item in record["shippingLines"]:
            amount = item["amount"]
            currency = item["currency"]
            name = item["name"]
            handle= item["handle"]
            reference = item["reference"]

            shipping_lines.append({
                "amount": amount,
                "currency": currency,
                "name": name,
                "handle": handle,
                "reference": reference,
                "taxIncluded": tax_included
            })
        # Charges data extraction
        for charge in record["charges"]:
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

            charges.append({
                "chargeID": chargeID,
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
        for discount in record["discountCodes"]:
            code = discount["code"]
            type = order["discountType"]

            discounts.append({
                "discountID": discount_id,
                "code": code, 
                "type": type
            })


        # Initialise orders
        order.append({
            "orderID": order_id,
            "billingID": None,
            "chargeID": charge,
            "shippingDetailsID": None,
            "customerID": customer_id,
            "discountID": discount_id,
            "orderItemsID": order_items_id,
            "storeID": store_id,
            "eventID": event_id,
            "total": order["order"]["amounts"]["total"],
            "subtotal": order["amounts"]["subtotal"],
            "discount": order["amounts"]["discount"],
            "discountCode": order["discountCode"],
            "channel": order["channel"],
            "currency": order["currency"],
            "isManual": order["isManual"],
            "isTest": order["isTest"],
            "note": order["note"],
            "date": order["date"],
            "risk":order["risk"],
            "source": order["source"],
            "sourceID": order["sourceId"],
            "tags": order["tags"],
            "weight": order["weight"],
            "fulfillments": order["fulfillments"]
        })

# Update the orders with the billingID and shippingDetailsID
for ord in order:
    order_id = ord["orderID"]
    
    # Assign the billingID and shippingDetailsID to the corresponding order
    if order_id in order_billing_mapping:
        ord["billingID"] = order_billing_mapping[order_id]
    
    if order_id in order_shipping_mapping:
        ord["shippingDetailsID"] = order_shipping_mapping[order_id]   

# Convert tables to dataFrames
df_orders = pd.DataFrame(order)
customer_details = pd.DataFrame(customer_details)
products = pd.DataFrame(products)
order_items = pd.DataFrame(order_items)
shipping_details = pd.DataFrame(shipping_details)
shipping_lines = pd.DataFrame(shipping_lines)
billing_details = pd.DataFrame(billing_details)
discounts = pd.DataFrame(discounts)
charges = pd.DataFrame(charges)
event = pd.DataFrame(event)
store = pd.DataFrame(store)
tax_lines = pd.DataFrame(tax_lines)

# Function to generate SQL insert statement for each DataFrame
def generate_sql_insert(table_name, df):
    insert_statements = []
    for _, row in df.iterrows():
        columns = ", ".join(row.index)
        values = ", ".join([f"'{str(val)}'" if val is not None else "NULL" for val in row.values])
        insert_statements.append(f"INSERT INTO {table_name} ({columns}) VALUES ({values});")
    return insert_statements

# Create SQL inserts for each table
order_inserts = generate_sql_insert("orders", df_orders)
customer_details_inserts = generate_sql_insert("customer_details", customer_details)
products_inserts = generate_sql_insert("products", products)
order_items_inserts = generate_sql_insert("order_items", order_items)
shipping_details_inserts = generate_sql_insert("shipping_details", shipping_details)
shipping_lines_inserts = generate_sql_insert("shipping_lines", shipping_lines)
billing_details_inserts = generate_sql_insert("billing_details", billing_details)
discounts_inserts = generate_sql_insert("discounts", discounts)
charges_inserts = generate_sql_insert("charges", charges)
event_inserts = generate_sql_insert("event", event)
store_inserts = generate_sql_insert("store", store)
tax_lines_inserts = generate_sql_insert("tax_lines", tax_lines)

# Print SQL insert statements
print("Orders table SQL inserts: ")
for statement in order_inserts:
    print(statement)

print("\nCustomer Details table SQL inserts: ")
for statement in customer_details_inserts:
    print(statement)

print("\nProducts table SQL inserts: ")
for statement in products_inserts:
    print(statement)

print("\nOrder Items table SQL inserts: ")
for statement in order_items_inserts:
    print(statement)

print("\nShipping Details table SQL inserts: ")
for statement in shipping_details_inserts:
    print(statement)

print("\nShipping Lines table SQL inserts: ")
for statement in shipping_lines_inserts:
    print(statement)

print("\nBilling Details table SQL inserts: ")
for statement in billing_details_inserts:
    print(statement)

print("\nDiscounts table SQL inserts: ")
for statement in discounts_inserts:
    print(statement)

print("\nCharges table SQL inserts: ")
for statement in charges_inserts:
    print(statement)

print("\nEvent table SQL inserts: ")
for statement in event_inserts:
    print(statement)

print("\nStore table SQL inserts: ")
for statement in store_inserts:
    print(statement)

print("\nTax Lines table SQL inserts: ")
for statement in tax_lines_inserts:
    print(statement)

    
   

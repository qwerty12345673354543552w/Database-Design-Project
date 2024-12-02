import random
from decimal import Decimal

# import boto3
from faker import Faker
import Hypergraph

# Initialize DynamoDB resource and Faker
"""
dynamodb = boto3.resource(
    "dynamodb",
    region_name="us-east-2",
    aws_access_key_id="",
    aws_secret_access_key="",
)
table = dynamodb.Table("ECommerceTable")
"""
fake = Faker()
# hy = Hypergraph()


def generate_consumer_data(user_id):
    """Generate a consumer entry."""
    return {
        "pk": "USER",
        "sk": f"USER#{user_id}",
        "username": fake.user_name(),
        "email": fake.email(),
        "billing_address": {
            "street": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "country": "USA",
            "zip": fake.zipcode(),
        },
        "shipping_address": {
            "street": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "country": "USA",
            "zip": fake.zipcode(),
        },
    }


def generate_order_data(user_id, order_id, product_ids):
    """Generate an order entry, with products matching the provided product_ids."""
    products = [
        {"product_id": f"PROD#{pid}", "quantity": random.randint(1, 5)}
        for pid in product_ids
    ]
    return {
        "pk": f"USER#{user_id}",
        "sk": f"ORDER#{order_id}",
        "order_id": order_id,
        "order_status": "Shipped",
        "products": products,
        "payment_method": "Credit Card",
        "amount": Decimal(
            str(
                round(sum(p["quantity"] * random.uniform(10, 100) for p in products), 2)
            )
        ),
        "shipping_address": {
            "street": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "country": "USA",
            "zip": fake.zipcode(),
        },
        "feedback": fake.sentence(),
    }


def generate_product_data(product_id, supplier_id):
    """Generate a product entry with a specific supplier."""
    return {
        "pk": "PRODUCT",
        "sk": f"PRODUCT#{product_id}",
        "product_id": product_id,
        "product_name": fake.word().capitalize(),
        "price": Decimal(str(round(random.uniform(50, 2000), 2))),
        "stock": random.randint(10, 100),
        "supplier_id": f"SUPPLIER#{supplier_id}",
        "supplier_name": fake.company(),
        "billing_address": {
            "street": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "country": "USA",
            "zip": fake.zipcode(),
        },
    }


def generate_supplier_data(supplier_id):
    """Generate a supplier entry."""
    return {
        "pk": "SUPPLIER",
        "sk": f"SUPPLIER#{supplier_id}",
        "supplier_name": fake.company(),
        "billing_address": {
            "street": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "country": "USA",
            "zip": fake.zipcode(),
        },
        "shipping_address": {
            "street": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "country": "USA",
            "zip": fake.zipcode(),
        },
    }


def upload_data(n=100):
    """Generate and insert synchronized data for consumers, orders, products, and suppliers."""
    Hypergraph.init_hypergraph()
    for _ in range(n):
        # Generate common IDs to ensure synchronization
        user_id = random.randint(10000, 99999)
        order_id = random.randint(10000, 99999)
        product_ids = [random.randint(1000, 9999) for _ in range(random.randint(1, 3))]
        supplier_id = random.randint(1, 100)

        # Generate data for each entity type using the common IDs
        consumer_data = generate_consumer_data(user_id)
        order_data = generate_order_data(user_id, order_id, product_ids)
        product_data = [generate_product_data(pid, supplier_id) for pid in product_ids]
        supplier_data = generate_supplier_data(supplier_id)

        """
        Skipped because already done
        # Insert each item into DynamoDB
        table.put_item(Item=consumer_data)
        
        print(f"Inserted consumer: {consumer_data}")

        table.put_item(Item=order_data)
        print(f"Inserted order: {order_data}")

        for product in product_data:
            table.put_item(Item=product)
            print(f"Inserted product: {product}")

        table.put_item(Item=supplier_data)
        print(f"Inserted supplier: {supplier_data}")
        """
        u = Hypergraph.create_new_customer(consumer_data)
        s = Hypergraph.create_new_supplier(supplier_data)
        plist = []
        for pd in product_data:
            pd["supplier_id"] = s
            p = Hypergraph.add_product(pd)
            plist.append(p)
        Hypergraph.place_order(u, plist)


# Run the function to upload data
upload_data(1000)

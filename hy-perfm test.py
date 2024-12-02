import time
import Hypergraph
import random

# import boto3
# from boto3.dynamodb.conditions import Key


"""
# Initialize DynamoDB resource
dynamodb = boto3.resource(
    "dynamodb",
    region_name="us-east-2",
    aws_access_key_id="",
    aws_secret_access_key="",
)
table = dynamodb.Table("ECommerceTable")
"""


# hy = Hypergraph()
rand = random.random()


def add_product(
    product_id, product_name, price, stock, supplier_id, supplier_name, billing_address
):
    """
    Add a new product to the DynamoDB table.

    Args:
        product_id (str): Unique ID for the product.
        product_name (str): Name of the product.
        price (float): Price of the product.
        stock (int): Stock quantity of the product.
        supplier_id (str): Supplier ID associated with the product.
        supplier_name (str): Name of the supplier.
        billing_address (dict): Billing address of the supplier.

    Returns:
        bool: True if the product was added successfully, False otherwise.
    """
    try:
        product_item = {
            "pk": "PRODUCT",
            "sk": f"PRODUCT#{product_id}",
            "product_id": product_id,
            "product_name": product_name,
            "price": price,
            "stock": stock,
            "supplier_id": supplier_id,
            "supplier_name": supplier_name,
            "billing_address": billing_address,
        }
        # table.put_item(Item=product_item)
        Hypergraph.add_product(product_item)
        print(f"Product '{product_name}' added successfully!")
        return True
    except Exception as e:
        print(f"Error adding product '{product_name}': {e}")
        return False


'''
def get_orders_by_user(customer_id):
    """Fetch all orders placed by a specific user, including product details."""
    try:
        # Query orders by user
        response = table.query(
            KeyConditionExpression=Key("pk").eq(f"USER#{customer_id}")
            & Key("sk").begins_with("ORDER#")
        )
        orders = response.get("Items", [])

        # For each order, add product information
        for order in orders:
            enriched_products = []
            for item in order.get("products", []):  # Changed from Cart to products
                product_id = item["product_id"]  # Changed from ProductID
                product_response = table.get_item(
                    Key={"pk": "PRODUCT", "sk": f"PRODUCT#{product_id}"}  # Updated keys
                )
                product_details = product_response.get("Item", {})
                enriched_products.append(
                    {
                        "ProductID": product_id,
                        "Quantity": item["quantity"],  # Changed from Quantity
                        "Details": product_details,
                    }
                )
            order["Products"] = enriched_products

        return orders
    except Exception as e:
        print(f"Error retrieving orders: {e}")
        return []


def get_product_by_name(product_name):
    """Fetch product details by product ID (name)."""
    try:
        # Use the product ID directly in pk
        response = table.get_item(
            Key={
                "pk": "PRODUCT",  # Assuming pk is PRODUCT#<product_id>
                "sk": f"PRODUCT#{product_name}",  # Assuming sk matches pk for product entries
            }
        )

        product = response.get("Item")
        if product:
            print(f"Product details for '{product_name}': {product}")
            return product
        else:
            print(f"No product found with ID '{product_name}'.")
            return None
    except Exception as e:
        print(f"Error retrieving product by ID: {e}")
        return None


def get_order_cost(user_id, order_id):
    """
    Fetch the cost of a specific order.

    Args:
        user_id (str): The ID of the user who placed the order
        order_id (str): The ID of the order

    Returns:
        float: The total cost of the order, or None if order not found
    """
    try:
        response = table.get_item(
            Key={"pk": f"USER#{user_id}", "sk": f"ORDER#{order_id}"}
        )

        order = response.get("Item")
        if order:
            amount = order.get("amount")
            print(f"Order {order_id} cost: ${amount}")
            return float(amount)
        else:
            print(f"No order found with ID {order_id} for user {user_id}")
            return None

    except Exception as e:
        print(f"Error retrieving order cost: {e}")
        return None


def get_supplier_by_name(supplier_name):
    """Fetch supplier details by name."""
    try:
        # Scan for a supplier by name
        response = table.scan(
            FilterExpression=Key("pk").eq("SUPPLIER")
            & Key("sk").begins_with("SUPPLIER#"),
            ProjectionExpression="supplier_name, billing_address, shipping_address",
        )
        suppliers = [
            item
            for item in response.get("Items", [])
            if item.get("supplier_name") == supplier_name
        ]
        if suppliers:
            print(f"Supplier details for '{supplier_name}': {suppliers}")
            return suppliers
        else:
            print(f"No supplier found with name '{supplier_name}'.")
            return None
    except Exception as e:
        print(f"Error retrieving supplier by name: {e}")
        return None


def get_customer_by_name(username):
    """Fetch customer details by username."""
    try:
        # Scan for a customer by username
        response = table.scan(
            FilterExpression=Key("pk").eq("USER") & Key("sk").begins_with("USER#"),
            ProjectionExpression="username, email, billing_address, shipping_address",
        )
        customers = [
            item
            for item in response.get("Items", [])
            if item.get("username") == username
        ]
        if customers:
            print(f"Customer details for '{username}': {customers}")
            return customers
        else:
            print(f"No customer found with username '{username}'.")
            return None
    except Exception as e:
        print(f"Error retrieving customer by name: {e}")
        return None

'''


def create_new_supplier(supplier_id, supplier_name, billing_address, shipping_address):
    """
    Create a new supplier in the DynamoDB table.

    Args:
        supplier_id (str): Unique ID for the supplier.
        supplier_name (str): Name of the supplier.
        billing_address (dict): Billing address of the supplier.
        shipping_address (dict): Shipping address of the supplier.

    Returns:
        bool: True if the supplier was created successfully, False otherwise.
    """
    try:
        supplier_item = {
            "pk": "SUPPLIER",
            "sk": f"SUPPLIER#{supplier_id}",
            "supplier_id": supplier_id,
            "supplier_name": supplier_name,
            "billing_address": billing_address,
            "shipping_address": shipping_address,
        }
        Hypergraph.create_new_supplier(supplier_item)
        # table.put_item(Item=supplier_item)
        print(f"Supplier '{supplier_name}' created successfully!")
        return True
    except Exception as e:
        print(f"Error creating supplier '{supplier_name}': {e}")
        return False


'''

def get_all_feedback():
    """Fetch all product feedback from orders."""
    try:
        # Scan for all order items with feedback
        response = table.scan(
            FilterExpression=Key("pk").begins_with("USER#")
            & Key("sk").begins_with("ORDER#"),
            ProjectionExpression="feedback",  # Retrieve only feedback field
        )
        feedbacks = [
            item["feedback"] for item in response.get("Items", []) if "feedback" in item
        ]
        print(f"Retrieved feedback: {feedbacks}")
        return feedbacks
    except Exception as e:
        print(f"Error retrieving feedback: {e}")
        return []
'''


def measure_get_orders_performance(num_queries, customer_id):
    """Measure time to fetch orders for a specific customer."""
    start_time = time.time()
    for _ in range(num_queries):
        Hypergraph.get_orders(customer_id)
    elapsed_time = time.time() - start_time
    average_time = elapsed_time / num_queries
    print(
        f"Get Orders ({num_queries} queries): Total time = {elapsed_time:.4f}s, Average time = {average_time:.6f}s"
    )


def measure_product_search_performance(num_queries, product_name):
    """Measure time to search for products by name."""
    start_time = time.time()
    for _ in range(num_queries):
        Hypergraph.get_product(product_name)
    elapsed_time = time.time() - start_time
    average_time = elapsed_time / num_queries
    print(
        f"Product Search ({num_queries} queries): Total time = {elapsed_time:.4f}s, Average time = {average_time:.6f}s"
    )


def measure_order_cost_performance(num_queries, user_id, order_id):
    """Measure time to fetch order cost."""
    start_time = time.time()
    for _ in range(num_queries):
        Hypergraph.get_cost_of_order(order_id, user_id)
    elapsed_time = time.time() - start_time
    average_time = elapsed_time / num_queries
    print(
        f"Order Cost Retrieval ({num_queries} queries): "
        f"Total time = {elapsed_time:.4f}s, "
        f"Average time = {average_time:.6f}s"
    )


def measure_feedback_retrieval_performance(num_queries):
    """Measure time to fetch all feedback."""
    start_time = time.time()
    for i in range(num_queries):
        if i % 100 == 0:
            print("Checkpoint:", i)
        Hypergraph.get_all_product_feedback()
    elapsed_time = time.time() - start_time
    average_time = elapsed_time / num_queries
    print(
        f"Feedback Retrieval ({num_queries} queries): Total time = {elapsed_time:.4f}s, Average time = {average_time:.6f}s"
    )


def measure_supplier_retrieval_performance(num_queries, supplier_name):
    """Measure time to fetch supplier by name."""
    start_time = time.time()
    for _ in range(num_queries):
        Hypergraph.get_supplier(supplier_name)
    elapsed_time = time.time() - start_time
    average_time = elapsed_time / num_queries
    print(
        f"Supplier Retrieval ({num_queries} queries): Total time = {elapsed_time:.4f}s, Average time = {average_time:.6f}s"
    )


def measure_customer_retrieval_performance(num_queries, username):
    """Measure time to fetch customer by name."""
    start_time = time.time()
    for _ in range(num_queries):
        Hypergraph.get_customer(username)
    elapsed_time = time.time() - start_time
    average_time = elapsed_time / num_queries
    print(
        f"Customer Retrieval ({num_queries} queries): Total time = {elapsed_time:.4f}s, Average time = {average_time:.6f}s"
    )


def measure_create_supplier_performance(supplier_id_base, supplier_name_base):
    """
    Measure the time to create new suppliers in the DynamoDB table.

    Args:
        num_queries (int): Number of suppliers to create.
        supplier_id_base (str): Base supplier ID for unique supplier IDs.
        supplier_name_base (str): Base supplier name for unique supplier names.
    """
    start_time = time.time()
    create_new_supplier(
        supplier_id=f"{supplier_id_base}",
        supplier_name=f"{supplier_name_base}",
        billing_address={
            "street": "Billing Street",
            "city": "CityName",
            "state": "StateName",
            "country": "CountryName",
            "zip": "ZIP",
        },
        shipping_address={
            "street": "Shipping Street",
            "city": "CityName",
            "state": "StateName",
            "country": "CountryName",
            "zip": "ZIP",
        },
    )
    elapsed_time = time.time() - start_time
    average_time = elapsed_time / num_queries
    print(
        f"Create Supplier ({num_queries} queries): Total time = {elapsed_time:.4f}s, Average time = {average_time:.6f}s"
    )


def measure_add_product_performance(product_id_base, product_name_base, num_queries=1):
    """
    Measure the time to add products to the DynamoDB table.

    Args:
        num_queries (int): Number of products to add.
        product_id_base (str): Base product ID for unique product IDs.
        product_name_base (str): Base product name for unique product names.
    """
    start_time = time.time()
    add_product(
        product_id=f"{product_id_base}",
        product_name=f"{product_name_base}",
        price=100,  # Example: Increment price by i
        stock=50,  # Example: Increment stock by i
        supplier_id=1,
        supplier_name="Supplier",
        billing_address={
            "street": "Street",
            "city": "CityName",
            "state": "StateName",
            "country": "CountryName",
            "zip": "ZIP",
        },
    )
    elapsed_time = time.time() - start_time
    average_time = elapsed_time / num_queries
    print(
        f"Add Product ({num_queries} queries): Total time = {elapsed_time:.4f}s, Average time = {average_time:.6f}s"
    )


if __name__ == "__main__":
    # Test parameters
    num_queries = 1000
    test_customer_id = random.randint(1, 1000)
    test_customer_name = Hypergraph.username_by_id(test_customer_id)
    test_customer_id_2 = test_customer_id - 1  # Example customer ID
    test_product_id = random.randint(1, 1000)
    test_product_name = Hypergraph.product_name_by_id(test_product_id)
    test_order_id = Hypergraph.get_orders(test_customer_id_2)[0]["orderid"]
    test_supplier_id = random.randint(1, 1000)
    # Example product name
    test_supplier_name = Hypergraph.supplier_name_by_id(test_supplier_id)
    base_supplier_name = "TEST_SUPPLIER_NAME"  # Example product name
    base_supplier_id = random.randint(1, 1000)
    base_product_id, base_product_name = random.randint(1, 1000), "TEST_PRODUCT_NAME"

    print("\nQuery Performance Test Results:")
    measure_get_orders_performance(num_queries, test_customer_id)
    measure_product_search_performance(num_queries, test_product_name)
    measure_order_cost_performance(
        num_queries,
        test_customer_id_2,
        test_order_id,
    )

    measure_feedback_retrieval_performance(num_queries)
    measure_customer_retrieval_performance(num_queries, test_customer_name)
    measure_supplier_retrieval_performance(num_queries, test_supplier_name)
    measure_create_supplier_performance(base_supplier_id, base_supplier_name)
    measure_add_product_performance(base_product_id, base_product_name)

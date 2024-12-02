import time
import boto3
from boto3.dynamodb.conditions import Key
import matplotlib.pyplot as plt

# Initialize DynamoDB resource
dynamodb = boto3.resource(
    "dynamodb",
    region_name="us-east-2",
    aws_access_key_id="",
    aws_secret_access_key="",
)
table = dynamodb.Table("ECommerceTable")


def add_item_to_cart(user_id, product_id, quantity):
    """Add an item to the cart."""
    try:
        item = {
            "pk": f"USER#{user_id}",
            "sk": f"CART#{product_id}",
            "product_id": product_id,
            "quantity": quantity,
        }
        table.put_item(Item=item)
    except Exception as e:
        print(f"Error adding item to cart: {e}")

def measure_performance_for_updates(num_updates_list):
    """
    Measure and plot the average time for the add_item_to_cart operation with varying number of updates.
    
    Args:
        num_updates_list (list): List of update sizes to test.
        
    Returns:
        results (dict): Dictionary containing number of updates, elapsed time, and average time per update.
    """
    results = {"num_updates": [], "elapsed_time": [], "average_time": []}
    
    for num_updates in num_updates_list:
        start_time = time.time()
        
        # Perform updates
        for i in range(num_updates):
            add_item_to_cart(user_id="12345", product_id=f"67890-{i}", quantity=1)
        
        elapsed_time = time.time() - start_time
        average_time = elapsed_time / num_updates
        results["num_updates"].append(num_updates)
        results["elapsed_time"].append(elapsed_time)
        results["average_time"].append(average_time)
        
        print(f"Updates: {num_updates}, Elapsed Time: {elapsed_time:.4f}s, Average Time: {average_time:.6f}s")
    
    return results

def plot_results(results):
    """
    Plot the results of the performance test.
    
    Args:
        results (dict): Dictionary containing performance test results.
    """
    plt.figure(figsize=(10, 6))
    plt.plot(results["num_updates"], results["average_time"], marker='o', label="Avg Time per Update")
    plt.title("Average Time per Update vs. Number of Updates")
    plt.xlabel("Number of Updates")
    plt.ylabel("Average Time per Update (seconds)")
    plt.grid(True)
    plt.legend()
    plt.show()

if __name__ == "__main__":
    # Define varying numbers of updates
    num_updates_list = [1000, 2000, 3000, 4000, 5000, 6000, 7000]

    # Measure performance
    performance_results = measure_performance_for_updates(num_updates_list)

    # Plot the results
    plot_results(performance_results)

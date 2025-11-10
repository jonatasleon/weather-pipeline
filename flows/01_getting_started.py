from prefect import flow, task
import random


@task
def get_customer_id() -> list[str]:
    """Fetch customer IDs from a database or API"""
    return [f"customer_{n}" for n in random.choices(range(100), k=10)]


@task
def process_customer_data(customer_id: str) -> str:
    """Process customer data"""
    return f"Processed {customer_id}"


@flow(log_prints=True, name="getting-started-flow")
def main():
    print("Starting weather automation")
    print("Getting customer IDs")
    customer_ids = get_customer_id()
    print(f"Customer IDs: {customer_ids}")
    processed_data = process_customer_data.map(customer_ids)
    return processed_data


if __name__ == "__main__":
    main.serve(
        name="getting-started",
        cron="0 0 * * *",  # Run every day at midnight
    )

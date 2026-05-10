import itertools
import os
import random
import time
import uuid

import psycopg

PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_USER = os.environ.get("PG_USER", "flink")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "flink")
PG_DATABASE = os.environ.get("PG_DATABASE", "source")
MAX_MESSAGES = int(os.environ.get("MAX_MESSAGES", "100"))

JOB_TITLES = [
    "Software Engineer",
    "Data Scientist",
    "Product Manager",
    "DevOps Engineer",
    "UX Designer",
    "QA Engineer",
    "Solutions Architect",
    "Technical Writer",
]

ORDER_STATUSES = ["pending", "shipped", "delivered", "canceled"]


def generate_user():
    return {
        "id": str(uuid.uuid4()),
        "created_at": int(time.time() * 1000),
        "latitude": round(random.uniform(-90.0, 90.0), 6),
        "job_title": random.choice(JOB_TITLES),
        "random_identifier": random.randint(0, 2**63 - 1),
        "active": random.choice([True, False]),
    }


def generate_order(order_id, user_ids):
    return {
        "id": order_id,
        "user_id": random.choice(user_ids) if user_ids else str(uuid.uuid4()),
        "amount": round(random.uniform(1.0, 1000.0), 2),
        "status": random.choice(ORDER_STATUSES),
        "created_at": int(time.time() * 1000),
    }


def wait_for_postgres(conn_str, retries=30, delay=2):
    for attempt in range(retries):
        try:
            with psycopg.connect(conn_str) as conn:
                conn.execute("SELECT 1")
            return
        except psycopg.OperationalError:
            if attempt == retries - 1:
                raise
            time.sleep(delay)


def main():
    conn_str = (
        f"host={PG_HOST} port={PG_PORT} user={PG_USER} "
        f"password={PG_PASSWORD} dbname={PG_DATABASE}"
    )
    wait_for_postgres(conn_str)

    user_ids = []
    order_ids = []
    order_id_seq = itertools.count(1)

    print(f"Producing {MAX_MESSAGES} change events across users + orders in {PG_DATABASE}...")
    with psycopg.connect(conn_str, autocommit=True) as conn:
        for i in range(MAX_MESSAGES):
            target_table = "orders" if (i % 2 == 1 and user_ids) else "users"

            if target_table == "users":
                if user_ids and random.random() < 0.2:
                    target = random.choice(user_ids)
                    conn.execute(
                        "UPDATE users SET active = NOT active, created_at = %s WHERE id = %s",
                        (int(time.time() * 1000), target),
                    )
                elif user_ids and random.random() < 0.1:
                    target = user_ids.pop(random.randrange(len(user_ids)))
                    conn.execute("DELETE FROM users WHERE id = %s", (target,))
                else:
                    user = generate_user()
                    conn.execute(
                        """
                        INSERT INTO users (id, created_at, latitude, job_title, random_identifier, active)
                        VALUES (%(id)s, %(created_at)s, %(latitude)s, %(job_title)s, %(random_identifier)s, %(active)s)
                        """,
                        user,
                    )
                    user_ids.append(user["id"])
            else:
                if order_ids and random.random() < 0.2:
                    target = random.choice(order_ids)
                    conn.execute(
                        "UPDATE orders SET status = %s WHERE id = %s",
                        (random.choice(ORDER_STATUSES), target),
                    )
                elif order_ids and random.random() < 0.1:
                    target = order_ids.pop(random.randrange(len(order_ids)))
                    conn.execute("DELETE FROM orders WHERE id = %s", (target,))
                else:
                    order = generate_order(next(order_id_seq), user_ids)
                    conn.execute(
                        """
                        INSERT INTO orders (id, user_id, amount, status, created_at)
                        VALUES (%(id)s, %(user_id)s, %(amount)s, %(status)s, %(created_at)s)
                        """,
                        order,
                    )
                    order_ids.append(order["id"])

            time.sleep(0.5)

    print(f"Done. Produced {MAX_MESSAGES} change events.")


if __name__ == "__main__":
    main()

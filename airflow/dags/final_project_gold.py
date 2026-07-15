from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta

from sqlalchemy import inspect, text


POSTGRES_CONN_ID = "postgres_dwh"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
MART_TABLE = "mart_store_performance"


def run_transaction(sql_statements: list[str]) -> None:
    """Jalankan beberapa SQL statement dalam satu transaksi."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    with engine.begin() as connection:
        for sql_statement in sql_statements:
            connection.execute(text(sql_statement))


with DAG(
    dag_id="final_project_gold",
    start_date=datetime(2026, 7, 1),
    # Dipicu otomatis oleh final_project_silver.
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "mugi",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=[
        "final_project",
        "gold",
        "store_performance",
        "data_mart",
    ],
) as dag:

    start = EmptyOperator(task_id="start")

    @task(task_id="init_gold")
    def init_gold():
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        hook.run(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA};")

    @task(task_id="build_dimensions")
    def build_dimensions():
        sql_statements = [
            f"DROP TABLE IF EXISTS {GOLD_SCHEMA}.dim_store CASCADE;",
            f"DROP TABLE IF EXISTS {GOLD_SCHEMA}.dim_customer CASCADE;",
            f"""
            CREATE TABLE {GOLD_SCHEMA}.dim_store AS
            SELECT
                ROW_NUMBER() OVER (ORDER BY store_id)::BIGINT AS store_key,
                store_id,
                store_name,
                city,
                opened_date::DATE AS opened_date,
                silver_updated_at
            FROM {SILVER_SCHEMA}.stores;
            """,
            f"""
            ALTER TABLE {GOLD_SCHEMA}.dim_store
            ADD CONSTRAINT pk_dim_store PRIMARY KEY (store_key);
            """,
            f"""
            ALTER TABLE {GOLD_SCHEMA}.dim_store
            ADD CONSTRAINT uq_dim_store_id UNIQUE (store_id);
            """,
            f"""
            CREATE TABLE {GOLD_SCHEMA}.dim_customer AS
            SELECT
                ROW_NUMBER() OVER (ORDER BY customer_id)::BIGINT AS customer_key,
                customer_id,
                name AS customer_name,
                city AS customer_city,
                segment,
                join_date::DATE AS join_date,
                silver_updated_at
            FROM {SILVER_SCHEMA}.customers;
            """,
            f"""
            ALTER TABLE {GOLD_SCHEMA}.dim_customer
            ADD CONSTRAINT pk_dim_customer PRIMARY KEY (customer_key);
            """,
            f"""
            ALTER TABLE {GOLD_SCHEMA}.dim_customer
            ADD CONSTRAINT uq_dim_customer_id UNIQUE (customer_id);
            """,
        ]
        run_transaction(sql_statements)

    @task(task_id="build_fact_sales")
    def build_fact_sales():
        sql_statements = [
            f"DROP TABLE IF EXISTS {GOLD_SCHEMA}.fact_sales CASCADE;",
            f"""
            CREATE TABLE {GOLD_SCHEMA}.fact_sales AS
            SELECT
                oi.order_item_id AS sales_key,
                o.order_id,
                ds.store_key,
                dc.customer_key,
                o.order_date::TIMESTAMP AS order_timestamp,
                o.order_date::DATE AS order_date,
                oi.quantity::INTEGER AS quantity,
                oi.unit_price::NUMERIC(18, 2) AS unit_price,
                (
                    oi.quantity::NUMERIC
                    * oi.unit_price::NUMERIC
                )::NUMERIC(18, 2) AS gross_sales_amount,
                o.payment_method,
                o.status AS order_status
            FROM {SILVER_SCHEMA}.order_items oi
            INNER JOIN {SILVER_SCHEMA}.orders o
                ON oi.order_id = o.order_id
            INNER JOIN {GOLD_SCHEMA}.dim_store ds
                ON o.store_id = ds.store_id
            INNER JOIN {GOLD_SCHEMA}.dim_customer dc
                ON o.customer_id = dc.customer_id;
            """,
            f"""
            ALTER TABLE {GOLD_SCHEMA}.fact_sales
            ADD CONSTRAINT pk_fact_sales PRIMARY KEY (sales_key);
            """,
            f"""
            ALTER TABLE {GOLD_SCHEMA}.fact_sales
            ADD CONSTRAINT fk_fact_sales_store
            FOREIGN KEY (store_key)
            REFERENCES {GOLD_SCHEMA}.dim_store (store_key);
            """,
            f"""
            ALTER TABLE {GOLD_SCHEMA}.fact_sales
            ADD CONSTRAINT fk_fact_sales_customer
            FOREIGN KEY (customer_key)
            REFERENCES {GOLD_SCHEMA}.dim_customer (customer_key);
            """,
        ]
        run_transaction(sql_statements)

    @task(task_id="build_store_performance_mart")
    def build_store_performance_mart():
        sql_statements = [
            f"DROP TABLE IF EXISTS {GOLD_SCHEMA}.{MART_TABLE};",
            f"""
            CREATE TABLE {GOLD_SCHEMA}.{MART_TABLE} AS
            SELECT
                ds.store_id,
                ds.store_name,
                ds.city,
                ds.opened_date,
                COUNT(DISTINCT fs.order_id)::BIGINT AS total_orders,
                COALESCE(SUM(fs.quantity), 0)::BIGINT AS total_items_sold,
                COUNT(DISTINCT fs.customer_key)::BIGINT AS unique_customers,
                COALESCE(SUM(fs.gross_sales_amount), 0)::NUMERIC(18, 2)
                    AS gross_sales,
                CASE
                    WHEN COUNT(DISTINCT fs.order_id) > 0
                    THEN (
                        SUM(fs.gross_sales_amount)
                        / COUNT(DISTINCT fs.order_id)
                    )::NUMERIC(18, 2)
                    ELSE 0::NUMERIC(18, 2)
                END AS average_order_value
            FROM {GOLD_SCHEMA}.dim_store ds
            LEFT JOIN {GOLD_SCHEMA}.fact_sales fs
                ON ds.store_key = fs.store_key
            GROUP BY
                ds.store_id,
                ds.store_name,
                ds.city,
                ds.opened_date;
            """,
            f"""
            ALTER TABLE {GOLD_SCHEMA}.{MART_TABLE}
            ADD CONSTRAINT pk_mart_store_performance
            PRIMARY KEY (store_id);
            """,
        ]
        run_transaction(sql_statements)

    @task(task_id="validate_store_performance")
    def validate_store_performance():
        required_tables = [
            "dim_store",
            "dim_customer",
            "fact_sales",
            MART_TABLE,
        ]

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = hook.get_sqlalchemy_engine()
        inspector = inspect(engine)

        missing_tables = [
            table_name
            for table_name in required_tables
            if not inspector.has_table(
                table_name=table_name,
                schema=GOLD_SCHEMA,
            )
        ]

        if missing_tables:
            raise ValueError(f"Tabel Gold belum terbentuk: {missing_tables}")

        with engine.connect() as connection:
            fact_sales_rows = connection.execute(
                text(f"SELECT COUNT(*) FROM {GOLD_SCHEMA}.fact_sales")
            ).scalar_one()

            mart_rows = connection.execute(
                text(f"SELECT COUNT(*) FROM {GOLD_SCHEMA}.{MART_TABLE}")
            ).scalar_one()

            store_rows = connection.execute(
                text(f"SELECT COUNT(*) FROM {GOLD_SCHEMA}.dim_store")
            ).scalar_one()

            invalid_sales_rows = connection.execute(
                text(
                    f"""
                    SELECT COUNT(*)
                    FROM {GOLD_SCHEMA}.fact_sales
                    WHERE quantity <= 0
                       OR unit_price < 0
                       OR gross_sales_amount < 0
                    """
                )
            ).scalar_one()

            orphan_rows = connection.execute(
                text(
                    f"""
                    SELECT COUNT(*)
                    FROM {GOLD_SCHEMA}.fact_sales fs
                    LEFT JOIN {GOLD_SCHEMA}.dim_store ds
                        ON fs.store_key = ds.store_key
                    LEFT JOIN {GOLD_SCHEMA}.dim_customer dc
                        ON fs.customer_key = dc.customer_key
                    WHERE ds.store_key IS NULL
                       OR dc.customer_key IS NULL
                    """
                )
            ).scalar_one()

            invalid_mart_rows = connection.execute(
                text(
                    f"""
                    SELECT COUNT(*)
                    FROM {GOLD_SCHEMA}.{MART_TABLE}
                    WHERE total_orders < 0
                       OR total_items_sold < 0
                       OR unique_customers < 0
                       OR gross_sales < 0
                       OR average_order_value < 0
                    """
                )
            ).scalar_one()

        if fact_sales_rows == 0:
            raise ValueError("fact_sales kosong.")

        if mart_rows == 0:
            raise ValueError("mart_store_performance kosong.")

        if mart_rows != store_rows:
            raise ValueError(
                "Jumlah store pada mart tidak sama dengan dim_store. "
                f"mart={mart_rows}, dim_store={store_rows}"
            )

        if invalid_sales_rows > 0:
            raise ValueError(
                "Terdapat nilai sales tidak valid: "
                f"{invalid_sales_rows} baris."
            )

        if orphan_rows > 0:
            raise ValueError(
                "Terdapat orphan foreign key: "
                f"{orphan_rows} baris."
            )

        if invalid_mart_rows > 0:
            raise ValueError(
                "Terdapat nilai mart tidak valid: "
                f"{invalid_mart_rows} baris."
            )

        print(
            "VALIDATION SUCCESS | "
            f"fact_sales_rows={fact_sales_rows} | "
            f"store_rows={store_rows} | "
            f"mart_rows={mart_rows} | "
            "invalid_sales_rows=0 | "
            "orphan_rows=0 | "
            "invalid_mart_rows=0"
        )

        return {
            "status": "SUCCESS",
            "fact_sales_rows": fact_sales_rows,
            "store_rows": store_rows,
            "mart_rows": mart_rows,
        }

    end = EmptyOperator(task_id="end")

    initialized = init_gold()
    dimensions = build_dimensions()
    fact_sales = build_fact_sales()
    store_mart = build_store_performance_mart()
    validation = validate_store_performance()

    (
        start
        >> initialized
        >> dimensions
        >> fact_sales
        >> store_mart
        >> validation
        >> end
    )

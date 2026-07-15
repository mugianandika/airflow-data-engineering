#!/usr/bin/env python3
"""
generator.py - Continuous multi-table transaction data generator for
Multi-user synthetic retail data generator (v2 - richer schema for datamart building).

Simulates a "live" retail data source. Runs until stopped (Ctrl+C) and
writes new CSV batches at random intervals.

SCHEMA DESIGN
=============
CORE tables - every user gets the SAME structure (guaranteed overlap
so the class can still be compared/graded on a common baseline):

    categories(category_id PK)
    products(product_id PK, category_id FK -> categories)
    stores(store_id PK)
    customers(customer_id PK)
    orders(order_id PK, customer_id FK -> customers, store_id FK -> stores)
    order_items(order_item_id PK, order_id FK -> orders, product_id FK -> products)

EXTENSION units - each user is randomly assigned a SUBSET (4-6 out of 8)
of these. This is what makes each user's datamart genuinely different,
while still relating back to the core tables via FK:

    payments            -> payments(order_id FK)
    shipments           -> shipments(order_id FK)
    product_reviews     -> product_reviews(customer_id FK, product_id FK)
    promotions_module    -> promotions(promo_id PK) + order_promotions(order_id FK, promo_id FK)
    suppliers_module     -> suppliers(supplier_id PK) + product_suppliers(product_id FK, supplier_id FK)
    employees            -> employees(employee_id PK, store_id FK)
    support_tickets      -> support_tickets(customer_id FK, order_id FK nullable)
    loyalty_points       -> loyalty_points(customer_id FK, order_id FK)

Category and store REFERENCE DATA is also sampled from a shared global pool
(12 categories, 10 stores) so different users partially overlap in their
dimension content too, not just schema.

Usage:
    python generator.py --unique-id U01 --output-dir ./incoming
    python generator.py --unique-id U01 --min-interval 30 --max-interval 90
    python generator.py --unique-id U01 --once      # single batch, for testing

Stop anytime with Ctrl+C.
"""

import argparse
import csv
import random
import signal
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

try:
    from faker import Faker
except ImportError:
    sys.exit("Missing dependency. Install with: pip install faker --break-system-packages")

# ---------------------------------------------------------------------------
# Global pools (shared reference universe - users sample subsets from
# these, which is what creates partial overlap between users)
# ---------------------------------------------------------------------------

CATEGORY_POOL = [
    "Elektronik", "Fashion", "Kebutuhan Rumah", "Makanan & Minuman",
    "Kesehatan", "Olahraga", "Mainan", "Buku", "Otomotif", "Kecantikan",
    "Perlengkapan Bayi", "Alat Tulis",
]

STORE_POOL = [
    ("Jakarta Pusat", "Jakarta"), ("Bandung Kota", "Bandung"),
    ("Surabaya Timur", "Surabaya"), ("Medan Baru", "Medan"),
    ("Semarang Tengah", "Semarang"), ("Makassar Selatan", "Makassar"),
    ("Palembang Ilir", "Palembang"), ("Yogyakarta Kota", "Yogyakarta"),
    ("Denpasar Utara", "Denpasar"), ("Malang Kota", "Malang"),
]

SUPPLIER_POOL = [
    "PT Sumber Makmur", "CV Anugerah Jaya", "PT Karya Sejahtera",
    "CV Berkah Abadi", "PT Mitra Utama", "CV Sentosa Prima",
    "PT Cahaya Nusantara", "CV Harapan Baru",
]

PAYMENT_METHOD_POOL = ["credit_card", "bank_transfer", "e-wallet", "cod", "qris"]
COURIER_POOL = ["JNE", "J&T", "SiCepat", "AnterAja", "Ninja Express"]
ISSUE_TYPE_POOL = ["produk_rusak", "salah_kirim", "keterlambatan", "refund", "komplain_kualitas"]

EXTENSION_UNITS = [
    "payments", "shipments", "product_reviews", "promotions_module",
    "suppliers_module", "employees", "support_tickets", "loyalty_points",
]

# Per-table candidate data-quality issues. Each user gets a random subset
# per table (roughly half of the applicable pool), so no two users face
# an identical cleaning checklist.
ISSUE_CATALOG = {
    "customers": ["missing_email", "whitespace_fields", "inconsistent_city_case", "duplicate_row"],
    "products": ["decimal_comma_price", "missing_category", "negative_price"],
    "orders": ["inconsistent_date_format", "missing_payment_method", "invalid_status", "duplicate_row"],
    "order_items": ["negative_or_zero_qty", "decimal_comma_price", "price_drift"],
    "payments": ["amount_mismatch", "missing_paid_at", "duplicate_row", "invalid_status"],
    "shipments": ["delivered_before_shipped", "missing_courier", "orphan_order_id"],
    "product_reviews": ["rating_out_of_range", "missing_review_text", "duplicate_row"],
    "promotions": ["overlapping_dates", "invalid_discount_pct"],
    "order_promotions": ["orphan_promo_id"],
    "suppliers": ["missing_city", "whitespace_fields"],
    "product_suppliers": ["negative_cost_price"],
    "employees": ["missing_store_id", "invalid_hire_date"],
    "support_tickets": ["orphan_order_id", "missing_issue_type", "inconsistent_date_format"],
    "loyalty_points": ["negative_points", "orphan_order_id"],
}


def build_profile(unique_id: str) -> dict:
    """Derive a stable but unique data profile for a given unique_id."""
    seed = int.from_bytes(unique_id.encode(), "little") % (2**32)
    rnd = random.Random(seed)

    n_categories = rnd.randint(6, 9)
    categories = rnd.sample(CATEGORY_POOL, n_categories)
    unused_categories = [c for c in CATEGORY_POOL if c not in categories]

    n_stores = rnd.randint(4, 7)
    stores = rnd.sample(STORE_POOL, n_stores)
    unused_stores = [s for s in STORE_POOL if s not in stores]

    n_units = rnd.randint(4, 6)
    extension_units = set(rnd.sample(EXTENSION_UNITS, n_units))

    n_suppliers = rnd.randint(3, 6)
    suppliers = rnd.sample(SUPPLIER_POOL, n_suppliers) if "suppliers_module" in extension_units else []
    unused_suppliers = [s for s in SUPPLIER_POOL if s not in suppliers]

    issues = {}
    for table, pool in ISSUE_CATALOG.items():
        issues[table] = {issue for issue in pool if rnd.random() < 0.5}

    return {
        "seed": seed,
        "categories": categories,
        "unused_categories": unused_categories,
        "stores": stores,
        "unused_stores": unused_stores,
        "extension_units": extension_units,
        "suppliers": suppliers,
        "unused_suppliers": unused_suppliers,
        "issues": issues,
        "price_min": rnd.choice([5_000, 10_000, 20_000]),
        "price_max": rnd.choice([300_000, 500_000, 1_000_000, 2_000_000]),
        "batch_orders_min": rnd.randint(3, 8),
        "batch_orders_max": rnd.randint(10, 20),
        "null_rate": rnd.uniform(0.03, 0.10),
        "duplicate_rate": rnd.uniform(0.02, 0.08),
        # Per-batch probability that a given SLOWLY-CHANGING dimension gets a
        # new row. Kept low and table-specific so growth mimics how often
        # these things realistically change in a real business.
        "growth_prob": {
            "categories": 0.02,   # taxonomy almost never changes
            "stores": 0.03,       # new store opens rarely
            "products": 0.12,     # new SKUs added fairly often
            "suppliers": 0.05,
            "employees": 0.08,    # hiring happens periodically
            "promotions": 0.10,   # marketing creates new promos often
        },
        # Probability that an EXISTING master row gets updated in place this
        # batch (same PK, different value) - simulates real CDC/master-data
        # updates, not just brand-new rows.
        "update_prob": {
            "products": 0.15,     # repricing
            "customers": 0.15,    # segment upgrade, moved city, changed email
            "employees": 0.06,    # promotion / store transfer
            "suppliers": 0.05,    # updated city/contact
            "promotions": 0.08,   # discount adjusted / extended
        },
    }


def has_issue(profile, table, issue):
    return issue in profile["issues"].get(table, set())


def maybe(prob):
    return random.random() < prob


class DataState:
    """Holds all generated entities + running counters across the run."""

    def __init__(self, profile: dict, fake: Faker):
        self.p = profile
        self.fake = fake
        self.categories = []
        self.products = []
        self.stores = []
        self.customers = []
        self.suppliers = []
        self.employees = []
        self.promotions = []
        self.product_suppliers = []
        self.order_history = []   # list of dicts: order_id, customer_id, order_date, total_amount
        # Remaining pool items that haven't been used yet - drawn from when a
        # dimension "grows" later (new store opens, new supplier onboarded, etc).
        self.unused_categories = list(profile["unused_categories"])
        self.unused_stores = list(profile["unused_stores"])
        self.unused_suppliers = list(profile["unused_suppliers"])
        # Live records that can still change state later (same PK, new
        # values in a future batch file) - this is what makes the output
        # CDC-like instead of pure insert-only. Removed once terminal.
        self.orders_state = {}
        self.payments_state = {}
        self.shipments_state = {}
        self.tickets_state = {}
        self.counters = {k: 1 for k in [
            "product", "store", "customer", "order", "order_item", "payment",
            "shipment", "review", "promo", "supplier", "employee", "ticket", "loyalty",
            "category",
        ]}
        self._seed_categories()
        self._seed_stores()
        self._seed_products()
        self._seed_customers(n=30)
        if "suppliers_module" in profile["extension_units"]:
            self._seed_suppliers()
            self.product_suppliers = self.build_product_suppliers()
        if "employees" in profile["extension_units"]:
            self._seed_employees()
        if "promotions_module" in profile["extension_units"]:
            self._seed_promotions()

    # ---- static dimensions -------------------------------------------------

    def _seed_categories(self):
        for name in self.p["categories"]:
            cid = f"CAT{self.counters['category']:03d}"
            self.counters["category"] += 1
            self.categories.append({"category_id": cid, "category_name": name})

    def _seed_stores(self):
        for name, city in self.p["stores"]:
            sid = f"ST{self.counters['store']:03d}"
            self.counters["store"] += 1
            self.stores.append({
                "store_id": sid,
                "store_name": name,
                "city": city,
                "opened_date": self.fake.date_between(start_date="-5y", end_date="-1y").isoformat(),
            })

    def _seed_products(self):
        for cat in self.categories:
            for _ in range(self.fake.random_int(3, 6)):
                self._new_product(cat)

    def _new_product(self, cat):
        pid = f"P{self.counters['product']:04d}"
        self.counters["product"] += 1
        price = self.fake.random_int(self.p["price_min"], self.p["price_max"])
        category_id = cat["category_id"]
        if has_issue(self.p, "products", "missing_category") and maybe(0.05):
            category_id = ""
        if has_issue(self.p, "products", "negative_price") and maybe(0.03):
            price = -price
        price_out = str(price).replace(".", ",") if (
            has_issue(self.p, "products", "decimal_comma_price") and maybe(0.15)
        ) else price
        product = {
            "product_id": pid,
            "product_name": f"{cat['category_name']} - {self.fake.word().capitalize()} {pid}",
            "category_id": category_id,
            "unit_price": price_out,
        }
        self.products.append(product)
        # New product gets linked to 1-2 existing suppliers right away, if any.
        if self.suppliers:
            self._link_product_suppliers(product)
        return product

    def _link_product_suppliers(self, product):
        n_sup = random.randint(1, min(2, len(self.suppliers)))
        for sup in random.sample(self.suppliers, n_sup):
            try:
                base_price = float(str(product["unit_price"]).replace(",", "."))
            except ValueError:
                base_price = 0
            cost = round(base_price * random.uniform(0.5, 0.8))
            if has_issue(self.p, "product_suppliers", "negative_cost_price") and maybe(0.05):
                cost = -cost
            self.product_suppliers.append({
                "product_id": product["product_id"],
                "supplier_id": sup["supplier_id"],
                "cost_price": cost,
            })

    def _seed_suppliers(self):
        for name in self.p["suppliers"]:
            self._new_supplier(name)

    def _new_supplier(self, name):
        sid = f"SUP{self.counters['supplier']:03d}"
        self.counters["supplier"] += 1
        city = "" if (has_issue(self.p, "suppliers", "missing_city") and maybe(0.1)) else self.fake.city()
        supplier = {"supplier_id": sid, "supplier_name": name, "city": city}
        self.suppliers.append(supplier)
        # Link the new supplier to a few existing products right away.
        if self.products:
            n_prod = random.randint(2, min(5, len(self.products)))
            for prod in random.sample(self.products, n_prod):
                try:
                    base_price = float(str(prod["unit_price"]).replace(",", "."))
                except ValueError:
                    base_price = 0
                cost = round(base_price * random.uniform(0.5, 0.8))
                if has_issue(self.p, "product_suppliers", "negative_cost_price") and maybe(0.05):
                    cost = -cost
                self.product_suppliers.append({
                    "product_id": prod["product_id"],
                    "supplier_id": supplier["supplier_id"],
                    "cost_price": cost,
                })
        return supplier

    def build_product_suppliers(self):
        rows = []
        for prod in self.products:
            n_sup = random.randint(1, min(2, len(self.suppliers))) if self.suppliers else 0
            for sup in random.sample(self.suppliers, n_sup) if n_sup else []:
                cost = round(float(str(prod["unit_price"]).replace(",", ".") or 0) * random.uniform(0.5, 0.8))
                if has_issue(self.p, "product_suppliers", "negative_cost_price") and maybe(0.05):
                    cost = -cost
                rows.append({
                    "product_id": prod["product_id"],
                    "supplier_id": sup["supplier_id"],
                    "cost_price": cost,
                })
        return rows

    def _seed_employees(self):
        for _ in range(random.randint(8, 15)):
            self._new_employee()

    def _new_employee(self):
        eid = f"EMP{self.counters['employee']:04d}"
        self.counters["employee"] += 1
        store_id = random.choice(self.stores)["store_id"] if self.stores else ""
        if has_issue(self.p, "employees", "missing_store_id") and maybe(0.08):
            store_id = ""
        hire_date = self.fake.date_between(start_date="-4y", end_date="today")
        if has_issue(self.p, "employees", "invalid_hire_date") and maybe(0.05):
            hire_date = hire_date + timedelta(days=random.randint(400, 900))  # future date
        employee = {
            "employee_id": eid,
            "name": self.fake.name(),
            "store_id": store_id,
            "role": random.choice(["Kasir", "Sales Associate", "Store Manager", "Stock Clerk"]),
            "hire_date": hire_date.isoformat(),
        }
        self.employees.append(employee)
        return employee

    def _seed_promotions(self):
        for _ in range(random.randint(4, 8)):
            self._new_promotion()

    def _new_promotion(self):
        promo_id = f"PROMO{self.counters['promo']:03d}"
        self.counters["promo"] += 1
        start = self.fake.date_between(start_date="-6M", end_date="today")
        end = start + timedelta(days=random.randint(3, 30))
        if has_issue(self.p, "promotions", "overlapping_dates") and maybe(0.15):
            start = start - timedelta(days=random.randint(1, 10))
        discount = random.choice([5, 10, 15, 20, 25, 30])
        if has_issue(self.p, "promotions", "invalid_discount_pct") and maybe(0.05):
            discount = random.choice([-10, 120])
        promotion = {
            "promo_id": promo_id,
            "promo_code": f"PROMO{random.randint(100,999)}",
            "discount_pct": discount,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
        }
        self.promotions.append(promotion)
        return promotion

    # ---- growth events for slowly-changing dimensions -------------------
    # Called once per batch from run(). Each returns True if that table
    # actually changed this batch, so the caller knows which CSVs to rewrite.

    def maybe_grow_category(self):
        prob = self.p["growth_prob"]["categories"]
        if not self.unused_categories or not maybe(prob):
            return False
        name = self.unused_categories.pop(0)
        cid = f"CAT{self.counters['category']:03d}"
        self.counters["category"] += 1
        cat = {"category_id": cid, "category_name": name}
        self.categories.append(cat)
        # A brand new category usually launches with a couple of products.
        for _ in range(random.randint(1, 3)):
            self._new_product(cat)
        return True

    def maybe_grow_store(self):
        prob = self.p["growth_prob"]["stores"]
        if not self.unused_stores or not maybe(prob):
            return False
        name, city = self.unused_stores.pop(0)
        sid = f"ST{self.counters['store']:03d}"
        self.counters["store"] += 1
        self.stores.append({
            "store_id": sid,
            "store_name": name,
            "city": city,
            "opened_date": datetime.now().strftime("%Y-%m-%d"),
        })
        return True

    def maybe_grow_product(self):
        prob = self.p["growth_prob"]["products"]
        if not self.categories or not maybe(prob):
            return False
        cat = random.choice(self.categories)
        self._new_product(cat)
        return True

    def maybe_grow_supplier(self):
        prob = self.p["growth_prob"]["suppliers"]
        if not self.unused_suppliers or not maybe(prob):
            return False
        name = self.unused_suppliers.pop(0)
        self._new_supplier(name)
        return True

    def maybe_grow_employee(self):
        prob = self.p["growth_prob"]["employees"]
        if not maybe(prob):
            return False
        self._new_employee()
        return True

    def maybe_grow_promotion(self):
        prob = self.p["growth_prob"]["promotions"]
        if not maybe(prob):
            return False
        self._new_promotion()
        return True

    # ---- update events for existing master rows (same PK, new value) ----
    # This is the CDC-style scenario: file batch N has product P0001 at
    # price 100000, batch N+3 has the SAME product_id P0001 at price
    # 115000. Downstream pipelines must handle this as an UPDATE (upsert
    # by PK), not just append. Each method mutates ONE existing row and
    # returns True if a change actually happened this batch.

    def maybe_update_product(self):
        prob = self.p["update_prob"]["products"]
        if not self.products or not maybe(prob):
            return False
        product = random.choice(self.products)
        try:
            current = float(str(product["unit_price"]).replace(",", "."))
        except ValueError:
            return False
        new_price = round(current * random.uniform(0.85, 1.25))
        product["unit_price"] = (
            str(new_price).replace(".", ",")
            if (has_issue(self.p, "products", "decimal_comma_price") and maybe(0.15))
            else new_price
        )
        return True

    def maybe_update_customer(self):
        prob = self.p["update_prob"]["customers"]
        if not self.customers or not maybe(prob):
            return False
        customer = random.choice(self.customers)
        field = random.choice(["segment", "city", "email", "phone"])
        if field == "segment":
            customer["segment"] = random.choice(["Regular", "Silver", "Gold", "Platinum"])
        elif field == "city":
            new_city = self.fake.city()
            if has_issue(self.p, "customers", "inconsistent_city_case") and maybe(0.25):
                new_city = random.choice([new_city.upper(), new_city.lower()])
            customer["city"] = new_city
        elif field == "email":
            customer["email"] = self.fake.email()
        else:
            customer["phone"] = self.fake.phone_number()
        return True

    def maybe_update_employee(self):
        prob = self.p["update_prob"]["employees"]
        if not self.employees or not maybe(prob):
            return False
        employee = random.choice(self.employees)
        if maybe(0.5) and self.stores:
            employee["store_id"] = random.choice(self.stores)["store_id"]  # transferred
        else:
            employee["role"] = random.choice(["Kasir", "Sales Associate", "Store Manager", "Stock Clerk"])
        return True

    def maybe_update_supplier(self):
        prob = self.p["update_prob"]["suppliers"]
        if not self.suppliers or not maybe(prob):
            return False
        supplier = random.choice(self.suppliers)
        supplier["city"] = self.fake.city()
        return True

    def maybe_update_promotion(self):
        prob = self.p["update_prob"]["promotions"]
        if not self.promotions or not maybe(prob):
            return False
        promotion = random.choice(self.promotions)
        if maybe(0.5):
            promotion["discount_pct"] = random.choice([5, 10, 15, 20, 25, 30, 40])
        else:
            try:
                end = datetime.strptime(promotion["end_date"], "%Y-%m-%d")
                promotion["end_date"] = (end + timedelta(days=random.randint(5, 20))).strftime("%Y-%m-%d")
            except ValueError:
                pass
        return True

    # ---- growing dimension ---------------------------------------------

    def _seed_customers(self, n):
        for _ in range(n):
            self._new_customer()

    def _new_customer(self):
        cid = f"C{self.counters['customer']:05d}"
        self.counters["customer"] += 1
        city = self.fake.city()
        if has_issue(self.p, "customers", "inconsistent_city_case") and maybe(0.25):
            city = random.choice([city.upper(), city.lower()])
        email = self.fake.email()
        if has_issue(self.p, "customers", "missing_email") and maybe(self.p["null_rate"]):
            email = ""
        name = self.fake.name()
        if has_issue(self.p, "customers", "whitespace_fields") and maybe(0.15):
            name = f"  {name}  "
        customer = {
            "customer_id": cid,
            "name": name,
            "email": email,
            "phone": self.fake.phone_number(),
            "city": city,
            "join_date": self.fake.date_between(start_date="-2y", end_date="today").isoformat(),
            "segment": random.choice(["Regular", "Silver", "Gold", "Platinum"]),
        }
        self.customers.append(customer)
        return customer

    def random_customer(self):
        if maybe(0.08):
            return self._new_customer()
        return random.choice(self.customers)

    # ---- transactional fact tables (new rows every batch) --------------

    def _dirty_date(self, dt: datetime, table: str) -> str:
        if has_issue(self.p, table, "inconsistent_date_format") and maybe(0.3):
            return dt.strftime("%d/%m/%Y %H:%M:%S")
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    def new_batch(self):
        """Generate one batch across all applicable tables. Returns a dict
        table_name -> list[dict] of only the tables that have new rows."""
        units = self.p["extension_units"]
        out = {"orders": [], "order_items": []}
        if "payments" in units:
            out["payments"] = []
        if "shipments" in units:
            out["shipments"] = []
        if "product_reviews" in units:
            out["product_reviews"] = []
        if "promotions_module" in units:
            out["order_promotions"] = []
        if "support_tickets" in units:
            out["support_tickets"] = []
        if "loyalty_points" in units:
            out["loyalty_points"] = []

        n_orders = random.randint(self.p["batch_orders_min"], self.p["batch_orders_max"])
        now = datetime.now()

        for _ in range(n_orders):
            customer = self.random_customer()
            store = random.choice(self.stores)
            order_id = f"ORD{self.counters['order']:06d}"
            self.counters["order"] += 1
            order_dt = now - timedelta(seconds=random.randint(0, 3000))
            payment_method = random.choice(PAYMENT_METHOD_POOL)
            if has_issue(self.p, "orders", "missing_payment_method") and maybe(0.05):
                payment_method = ""
            status = "pending"
            if has_issue(self.p, "orders", "invalid_status") and maybe(0.03):
                status = "unknown"
            elif maybe(0.05):
                status = "cancelled"  # cancelled right away (edge case)

            order_row = {
                "order_id": order_id,
                "customer_id": customer["customer_id"],
                "store_id": store["store_id"],
                "order_date": self._dirty_date(order_dt, "orders"),
                "payment_method": payment_method,
                "status": status,
            }
            out["orders"].append(order_row)
            if status == "pending":
                self.orders_state[order_id] = order_row
            if has_issue(self.p, "orders", "duplicate_row") and maybe(self.p["duplicate_rate"]):
                out["orders"].append(dict(order_row))

            # order_items
            n_items = random.randint(1, 4)
            order_total = 0
            for _ in range(n_items):
                product = random.choice(self.products)
                try:
                    base_price = float(str(product["unit_price"]).replace(",", "."))
                except ValueError:
                    base_price = 0
                qty = random.randint(1, 5)
                if has_issue(self.p, "order_items", "negative_or_zero_qty") and maybe(0.04):
                    qty = random.choice([0, -1])
                unit_price = base_price
                if has_issue(self.p, "order_items", "price_drift") and maybe(0.08):
                    unit_price = round(base_price * random.uniform(0.85, 1.15))
                order_total += max(qty, 0) * unit_price
                price_out = str(unit_price).replace(".", ",") if (
                    has_issue(self.p, "order_items", "decimal_comma_price") and maybe(0.1)
                ) else unit_price
                out["order_items"].append({
                    "order_item_id": f"OI{self.counters['order_item']:07d}",
                    "order_id": order_id,
                    "product_id": product["product_id"],
                    "quantity": qty,
                    "unit_price": price_out,
                })
                self.counters["order_item"] += 1

            self.order_history.append({
                "order_id": order_id, "customer_id": customer["customer_id"],
                "order_date": order_dt, "total_amount": order_total,
            })

            # payments
            if "payments" in units:
                amount = order_total
                if has_issue(self.p, "payments", "amount_mismatch") and maybe(0.06):
                    amount = round(amount * random.uniform(0.5, 0.9))
                pay_status = "pending"
                if has_issue(self.p, "payments", "invalid_status") and maybe(0.05):
                    pay_status = random.choice(["failed", "refunded"])
                pay_row = {
                    "payment_id": f"PAY{self.counters['payment']:06d}",
                    "order_id": order_id,
                    "method": payment_method or random.choice(PAYMENT_METHOD_POOL),
                    "amount": amount,
                    "status": pay_status,
                    "paid_at": "",
                }
                self.counters["payment"] += 1
                out["payments"].append(pay_row)
                if pay_status == "pending":
                    self.payments_state[pay_row["payment_id"]] = pay_row
                if has_issue(self.p, "payments", "duplicate_row") and maybe(self.p["duplicate_rate"]):
                    out["payments"].append(dict(pay_row))

            # shipments
            if "shipments" in units and status != "cancelled" and maybe(0.9):
                shipped = order_dt + timedelta(hours=random.randint(1, 24))
                courier = "" if (has_issue(self.p, "shipments", "missing_courier") and maybe(0.05)) \
                    else random.choice(COURIER_POOL)
                ship_order_id = order_id
                if has_issue(self.p, "shipments", "orphan_order_id") and maybe(0.03):
                    ship_order_id = f"ORD{random.randint(900000, 999999)}"
                ship_row = {
                    "shipment_id": f"SHP{self.counters['shipment']:06d}",
                    "order_id": ship_order_id,
                    "courier": courier,
                    "shipped_date": shipped.strftime("%Y-%m-%d"),
                    "delivered_date": "",
                    "status": "in_transit",
                }
                out["shipments"].append(ship_row)
                self.shipments_state[ship_row["shipment_id"]] = ship_row
                self.counters["shipment"] += 1

            # promotions applied to this order (bridge)
            if "promotions_module" in units and self.promotions and maybe(0.3):
                promo = random.choice(self.promotions)
                promo_id = promo["promo_id"]
                if has_issue(self.p, "order_promotions", "orphan_promo_id") and maybe(0.05):
                    promo_id = "PROMO999"
                out["order_promotions"].append({"order_id": order_id, "promo_id": promo_id})

            # loyalty points
            if "loyalty_points" in units:
                earned = int(order_total // 10000)
                if has_issue(self.p, "loyalty_points", "negative_points") and maybe(0.03):
                    earned = -earned
                loy_order_id = order_id
                if has_issue(self.p, "loyalty_points", "orphan_order_id") and maybe(0.03):
                    loy_order_id = f"ORD{random.randint(900000, 999999)}"
                out["loyalty_points"].append({
                    "loyalty_id": f"LOY{self.counters['loyalty']:06d}",
                    "customer_id": customer["customer_id"],
                    "order_id": loy_order_id,
                    "points_earned": earned,
                    "points_redeemed": random.choice([0, 0, 0, 50, 100]),
                })
                self.counters["loyalty"] += 1

            # support tickets (only sometimes, not every order)
            if "support_tickets" in units and maybe(0.08):
                issue_type = "" if (has_issue(self.p, "support_tickets", "missing_issue_type") and maybe(0.05)) \
                    else random.choice(ISSUE_TYPE_POOL)
                tix_order_id = order_id
                if has_issue(self.p, "support_tickets", "orphan_order_id") and maybe(0.04):
                    tix_order_id = f"ORD{random.randint(900000, 999999)}"
                tix_row = {
                    "ticket_id": f"TIX{self.counters['ticket']:05d}",
                    "customer_id": customer["customer_id"],
                    "order_id": tix_order_id,
                    "issue_type": issue_type,
                    "status": "open",
                    "created_at": self._dirty_date(now, "support_tickets"),
                }
                out["support_tickets"].append(tix_row)
                self.tickets_state[tix_row["ticket_id"]] = tix_row
                self.counters["ticket"] += 1

        # product reviews - not tied to a specific new order, sampled from
        # customers/products seen so far (simulates reviews trickling in)
        if "product_reviews" in units:
            for _ in range(random.randint(1, 5)):
                customer = random.choice(self.customers)
                product = random.choice(self.products)
                rating = random.randint(1, 5)
                if has_issue(self.p, "product_reviews", "rating_out_of_range") and maybe(0.04):
                    rating = random.choice([0, 6])
                review_text = "" if (
                    has_issue(self.p, "product_reviews", "missing_review_text") and maybe(0.1)
                ) else self.fake.sentence(nb_words=10)
                row = {
                    "review_id": f"REV{self.counters['review']:06d}",
                    "customer_id": customer["customer_id"],
                    "product_id": product["product_id"],
                    "rating": rating,
                    "review_text": review_text,
                    "review_date": now.strftime("%Y-%m-%d"),
                }
                self.counters["review"] += 1
                out["product_reviews"].append(row)
                if has_issue(self.p, "product_reviews", "duplicate_row") and maybe(self.p["duplicate_rate"]):
                    out["product_reviews"].append(dict(row))

        self._advance_lifecycles(out, units, now)
        return out

    def _advance_lifecycles(self, out, units, now):
        """CDC-style updates: pick existing records that are still in a
        non-terminal state and flip them forward. The updated row is
        re-emitted (SAME primary key, new values) into THIS batch's file,
        so the same order_id/payment_id/etc can legitimately appear across
        multiple batch files with different column values over time -
        forcing an upsert/merge downstream instead of blind append."""

        # orders: pending -> completed / cancelled
        for oid in list(self.orders_state.keys()):
            row = self.orders_state[oid]
            if maybe(0.35):
                row["status"] = random.choices(["completed", "cancelled"], weights=[85, 15])[0]
                out["orders"].append(dict(row))
                del self.orders_state[oid]

        # payments: pending -> success / failed ; success -> refunded (rare, simulates a return)
        for pid in list(self.payments_state.keys()):
            row = self.payments_state[pid]
            if row["status"] == "pending" and maybe(0.4):
                new_status = random.choices(["success", "failed"], weights=[85, 15])[0]
                row["status"] = new_status
                if new_status == "success":
                    row["paid_at"] = "" if (
                        has_issue(self.p, "payments", "missing_paid_at") and maybe(0.08)
                    ) else self._dirty_date(now, "payments")
                out["payments"].append(dict(row))
                if new_status == "failed":
                    del self.payments_state[pid]
            elif row["status"] == "success" and maybe(0.03):
                row["status"] = "refunded"
                out["payments"].append(dict(row))
                del self.payments_state[pid]

        # shipments: in_transit -> delivered / returned
        for sid in list(self.shipments_state.keys()):
            row = self.shipments_state[sid]
            if maybe(0.3):
                new_status = random.choices(["delivered", "returned"], weights=[90, 10])[0]
                row["status"] = new_status
                try:
                    shipped_dt = datetime.strptime(row["shipped_date"], "%Y-%m-%d")
                except ValueError:
                    shipped_dt = now
                delivered_dt = shipped_dt + timedelta(days=random.randint(1, 5))
                if has_issue(self.p, "shipments", "delivered_before_shipped") and maybe(0.05):
                    delivered_dt = shipped_dt - timedelta(hours=random.randint(1, 5))
                row["delivered_date"] = delivered_dt.strftime("%Y-%m-%d")
                out["shipments"].append(dict(row))
                del self.shipments_state[sid]

        # tickets: open -> in_progress -> resolved (two possible re-emissions per ticket)
        for tid in list(self.tickets_state.keys()):
            row = self.tickets_state[tid]
            if row["status"] == "open" and maybe(0.3):
                row["status"] = "in_progress"
                out["support_tickets"].append(dict(row))
            elif row["status"] == "in_progress" and maybe(0.3):
                row["status"] = "resolved"
                out["support_tickets"].append(dict(row))
                del self.tickets_state[tid]



FIELD_MAP = {
    "categories": ["category_id", "category_name"],
    "stores": ["store_id", "store_name", "city", "opened_date"],
    "products": ["product_id", "product_name", "category_id", "unit_price"],
    "customers": ["customer_id", "name", "email", "phone", "city", "join_date", "segment"],
    "suppliers": ["supplier_id", "supplier_name", "city"],
    "product_suppliers": ["product_id", "supplier_id", "cost_price"],
    "employees": ["employee_id", "name", "store_id", "role", "hire_date"],
    "promotions": ["promo_id", "promo_code", "discount_pct", "start_date", "end_date"],
    "orders": ["order_id", "customer_id", "store_id", "order_date", "payment_method", "status"],
    "order_items": ["order_item_id", "order_id", "product_id", "quantity", "unit_price"],
    "payments": ["payment_id", "order_id", "method", "amount", "status", "paid_at"],
    "shipments": ["shipment_id", "order_id", "courier", "shipped_date", "delivered_date", "status"],
    "product_reviews": ["review_id", "customer_id", "product_id", "rating", "review_text", "review_date"],
    "order_promotions": ["order_id", "promo_id"],
    "support_tickets": ["ticket_id", "customer_id", "order_id", "issue_type", "status", "created_at"],
    "loyalty_points": ["loyalty_id", "customer_id", "order_id", "points_earned", "points_redeemed"],
}


def write_csv(path: Path, rows: list, fieldnames: list):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def run(unique_id: str, output_dir: str, min_interval: int, max_interval: int, once: bool):
    profile = build_profile(unique_id)
    fake = Faker("id_ID")
    fake.seed_instance(profile["seed"])
    random.seed(profile["seed"])

    state = DataState(profile, fake)
    out_base = Path(output_dir) / unique_id

    # Dimensions get their initial snapshot written now. They are NOT frozen
    # after this though - maybe_grow_*() calls inside the batch loop below
    # can add new rows to any of them over time, and whichever file actually
    # changed gets rewritten. So every table in the schema eventually moves,
    # either by a slowly-changing dimension snapshot update or a new
    # incremental file, depending on the table.
    #
    # Master/dimension tables now follow the SAME folder + timestamped-file
    # pattern as the transactional tables (e.g. products/products_<ts>.csv)
    # instead of a single file overwritten in place. Every time a master
    # table changes, a NEW snapshot file is dropped - so comparing two
    # consecutive files literally shows the same PK with a different value,
    # exactly like the transactional tables. This is also how many real
    # source systems behave: periodic full dumps of master/reference tables
    # that the pipeline has to diff itself (no CDC feed available).
    init_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    def write_snapshot(table, rows):
        write_csv(out_base / table / f"{table}_{init_ts}.csv", rows, FIELD_MAP[table])

    write_snapshot("categories", state.categories)
    write_snapshot("stores", state.stores)
    write_snapshot("products", state.products)
    if state.suppliers:
        write_snapshot("suppliers", state.suppliers)
        write_snapshot("product_suppliers", state.product_suppliers)
    if state.employees:
        write_snapshot("employees", state.employees)
    if state.promotions:
        write_snapshot("promotions", state.promotions)
    write_snapshot("customers", state.customers)

    print(f"[{unique_id}] seed={profile['seed']}")
    print(f"[{unique_id}] categories={profile['categories']}")
    print(f"[{unique_id}] stores={[s[0] for s in profile['stores']]}")
    print(f"[{unique_id}] extension_units={sorted(profile['extension_units'])}")
    print(f"[{unique_id}] writing to {out_base.resolve()}")
    print(f"[{unique_id}] press Ctrl+C to stop.\n")

    stop_flag = {"stop": False}

    def handle_sigint(signum, frame):
        stop_flag["stop"] = True
        print(f"\n[{unique_id}] stop signal received, shutting down gracefully...")

    signal.signal(signal.SIGINT, handle_sigint)

    units = profile["extension_units"]
    batch_num = 0
    while not stop_flag["stop"]:
        batch_num += 1
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        def write_batch_snapshot(table, rows):
            write_csv(out_base / table / f"{table}_{timestamp}.csv", rows, FIELD_MAP[table])

        # Fact/transactional tables: always a brand-new file this batch.
        batch = state.new_batch()
        for table, rows in batch.items():
            if not rows:
                continue
            write_batch_snapshot(table, rows)

        # customers is a growing snapshot, so a new file drops every batch -
        # _new_customer() can fire inside new_batch() above, and
        # maybe_update_customer() below mutates an existing customer_id's
        # values (segment/city/email/phone), same CDC pattern as everything
        # else in this schema.
        state.maybe_update_customer()
        write_batch_snapshot("customers", state.customers)

        # Slowly-changing dimensions: each has its own probability of
        # growing (new row) or updating (existing row, changed value) this
        # batch. Only drop a new snapshot file for the tables that actually
        # changed - no-op batches don't spam empty/duplicate files.
        changed = []
        products_changed = False
        if state.maybe_grow_category():
            write_batch_snapshot("categories", state.categories)
            products_changed = True
            changed.append("categories(new)+products(new)")
        if state.maybe_grow_product():
            products_changed = True
            changed.append("products(new)")
        if state.maybe_update_product():
            products_changed = True
            changed.append("products(updated price)")
        if products_changed:
            write_batch_snapshot("products", state.products)

        if state.maybe_grow_store():
            write_batch_snapshot("stores", state.stores)
            changed.append("stores(new)")

        suppliers_changed = False
        if "suppliers_module" in units and state.maybe_grow_supplier():
            suppliers_changed = True
            changed.append("suppliers(new)")
        if "suppliers_module" in units and state.maybe_update_supplier():
            suppliers_changed = True
            changed.append("suppliers(city updated)")
        if suppliers_changed:
            write_batch_snapshot("suppliers", state.suppliers)
        if "suppliers_module" in units and (suppliers_changed or products_changed):
            # product_suppliers needs a flush whenever either side of the
            # bridge changed (new/updated product, or new supplier linked).
            write_batch_snapshot("product_suppliers", state.product_suppliers)

        employees_changed = False
        if "employees" in units and state.maybe_grow_employee():
            employees_changed = True
            changed.append("employees(new)")
        if "employees" in units and state.maybe_update_employee():
            employees_changed = True
            changed.append("employees(role/store change)")
        if employees_changed:
            write_batch_snapshot("employees", state.employees)

        promotions_changed = False
        if "promotions_module" in units and state.maybe_grow_promotion():
            promotions_changed = True
            changed.append("promotions(new)")
        if "promotions_module" in units and state.maybe_update_promotion():
            promotions_changed = True
            changed.append("promotions(discount/date changed)")
        if promotions_changed:
            write_batch_snapshot("promotions", state.promotions)

        counts = ", ".join(f"{t}={len(r)}" for t, r in batch.items() if r)
        dim_note = f" | dims changed: {', '.join(changed)}" if changed else ""
        print(f"[{unique_id}] batch #{batch_num} @ {timestamp}: {counts}{dim_note}")

        if once:
            break

        wait_seconds = random.randint(min_interval, max_interval)
        for _ in range(wait_seconds):
            if stop_flag["stop"]:
                break
            time.sleep(1)

    print(f"[{unique_id}] stopped after {batch_num} batch(es).")


def main():
    parser = argparse.ArgumentParser(description="Continuous multi-table retail data generator per user.")
    parser.add_argument("--unique-id", required=True, help="Unique ID for this data source, e.g. U01, a username, or any identifier")
    parser.add_argument("--output-dir", default="./incoming", help="Base output directory")
    parser.add_argument("--min-interval", type=int, default=60, help="Min seconds between batches")
    parser.add_argument("--max-interval", type=int, default=300, help="Max seconds between batches")
    parser.add_argument("--once", action="store_true", help="Generate a single batch and exit")
    args = parser.parse_args()

    if args.min_interval > args.max_interval:
        parser.error("--min-interval cannot be greater than --max-interval")
    if args.min_interval < 1:
        parser.error("--min-interval must be >= 1")

    run(args.unique_id, args.output_dir, args.min_interval, args.max_interval, args.once)


if __name__ == "__main__":
    main()
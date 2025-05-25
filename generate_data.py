import os
import random
import datetime
import psycopg2
from decimal import Decimal # Import Decimal for explicit checks if needed, though we convert to float
from faker import Faker
from dotenv import load_dotenv
import logging
import re # For basic phone cleaning

# --- Configuration ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Database Connection ---
def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        logging.info("Database Connection Successful.")
        return conn
    except Exception as e:
        logging.error(f"Database Connection Error: {e}")
        return None

# --- Faker Initialization ---
try:
    fake = Faker(['fr_FR', 'ar_SA', 'en_US'])
    fake.name()
except ImportError:
    logging.warning("Could not initialize Faker with 'ar_SA', falling back to fr_FR and en_US.")
    fake = Faker(['fr_FR', 'en_US'])


# --- Data Generation Configuration ---
NUM_CATEGORIES = 5
NUM_PRODUCTS = 60
NUM_CUSTOMERS = 2200
NUM_ADDRESSES_PER_CUSTOMER = (1, 2)
NUM_PROMOTIONS = 15
NUM_ORDERS = 2500
NUM_WEB_SESSIONS = 3000
MAX_ITEMS_PER_ORDER = 4
ORDER_START_DATE = datetime.date.today() - datetime.timedelta(days=180)
ORDER_END_DATE = datetime.date.today()

NA_LOCATIONS = {
    'Algeria': ['Algiers', 'Oran', 'Constantine', 'Annaba', 'Blida', 'Setif', 'Tlemcen'],
    'Morocco': ['Rabat', 'Casablanca', 'Marrakech', 'Fes', 'Tangier', 'Agadir', 'Meknes'],
    'Tunisia': ['Tunis', 'Sfax', 'Sousse', 'Kairouan', 'Bizerte', 'Gabes'],
    'Libya': ['Tripoli', 'Benghazi', 'Misrata', 'Tobruk', 'Sabha'],
    'Egypt': ['Cairo', 'Alexandria', 'Giza', 'Shubra El Kheima', 'Port Said', 'Suez', 'Luxor']
}
COUNTRIES = list(NA_LOCATIONS.keys())

CATEGORIES_DATA = [
    ('Keyboards', 'Mechanical, Membrane, Gaming Keyboards'),
    ('Mice & Mousepads', 'Gaming Mice, Ergonomic Mice, Wireless Mice, Large Mousepads'),
    ('Audio', 'Gaming Headsets, Headphones, Microphones, Speakers'),
    ('PC Components', 'CPU Coolers, Case Fans, RAM Modules, SSDs, Power Supplies'),
    ('Peripherals & Accessories', 'Controllers, Webcams, Monitor Stands, Cable Management')
]

PRODUCT_NAME_TEMPLATES = {
    'Keyboards': ['MechaniKey {} Pro', 'StealthType {} Silent', 'RGB Warrior Keyboard {}', 'Compact Tactile {}', 'ErgoBoard {}'],
    'Mice & Mousepads': ['Laser Precision Mouse {}', 'Swift Glide Pad XL {}', 'RGB Gaming Mouse {}', 'ErgoVertical Mouse {}', 'Control Surface Pad {}'],
    'Audio': ['Surround Sound Headset {}', 'Crystal Clear Comm {} Mic', 'Pro Gamer Headset {}', 'Studio USB Mic {}', 'Bass Boost Speakers {}'],
    'PC Components': ['HyperFrost CPU Cooler {}', 'Arctic Flow Fan {} Pack', 'Velocity RAM {}GB Kit', 'Inferno SSD {}TB NVMe', 'Reliant {}W PSU'],
    'Peripherals & Accessories': ['Elite Pro Controller {}', 'Streamer HD Webcam {}', 'Dual Monitor Arm {}', 'CableSleeve Kit {}', 'Gaming Chair {} Series']
}

ORDER_STATUS_DISTRIBUTION = [
    ('Delivered', 0.65),
    ('Refused Delivery', 0.08),
    ('Delivery Failed', 0.07),
    ('Cancelled by Customer', 0.10),
    ('Cancelled by Admin', 0.04),
    ('Shipped', 0.03),
    ('Processing', 0.02),
    ('Pending Confirmation', 0.01)
]

CANCELLATION_REASONS = [
    'Customer changed mind', 'Found cheaper elsewhere', 'Delivery took too long',
    'Incorrect item ordered', 'No longer needed', 'Address validation failed',
    'No answer at door (Courier)', 'Refused - Damaged package', 'Refused - Did not order',
    'Refused - Wrong item', 'Refused - Unexpected COD amount', 'Out of stock (Admin)',
    'Fraud check failed (Admin)', 'Verification call failed (Admin)'
]

REFERRER_SOURCES = ['Google', 'Facebook', 'Instagram', 'YouTube', 'TikTok', 'Direct', 'Friend Referral', 'Other Website']

# --- Helper Functions ---
def clean_phone_number(phone):
    cleaned = re.sub(r'[^\d+]+', '', phone)
    if not cleaned.startswith('+'):
        cleaned = re.sub(r'[^\d]+', '', cleaned)
    return cleaned[:15]

def get_random_location():
    country = random.choice(COUNTRIES)
    city = random.choice(NA_LOCATIONS[country])
    return country, city

def random_date_between(start_date, end_date):
    if isinstance(start_date, datetime.datetime):
        start_date = start_date.date()
    if isinstance(end_date, datetime.datetime):
        end_date = end_date.date()

    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    if days_between_dates < 0: days_between_dates = 0
    random_number_of_days = random.randrange(days_between_dates + 1)
    random_dt = start_date + datetime.timedelta(days=random_number_of_days)
    random_time = datetime.time(random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))
    return datetime.datetime.combine(random_dt, random_time, tzinfo=datetime.timezone.utc)

def get_weighted_status(status_distribution):
    statuses, weights = zip(*status_distribution)
    return random.choices(statuses, weights=weights, k=1)[0]

# --- Main Data Generation Functions ---

def insert_categories(cursor):
    logging.info("Inserting categories...")
    category_ids = []
    for name, desc in CATEGORIES_DATA:
        try:
            cursor.execute(
                "INSERT INTO public.categories (category_name, description) VALUES (%s, %s) ON CONFLICT (category_name) DO NOTHING RETURNING category_id",
                (name, desc)
            )
            result = cursor.fetchone()
            if result:
                category_ids.append(result[0])
            else:
                 cursor.execute("SELECT category_id FROM public.categories WHERE category_name = %s", (name,))
                 existing_id = cursor.fetchone()
                 if existing_id:
                     category_ids.append(existing_id[0])
        except psycopg2.Error as e:
            logging.error(f"Error processing category {name}: {e}")
            cursor.connection.rollback()
    category_ids = list(set(category_ids))
    logging.info(f"Using Category IDs: {category_ids}")
    return category_ids

def insert_products(cursor, category_ids):
    logging.info("Inserting products...")
    product_data = [] # Changed name to avoid confusion with product_ids list
    if not category_ids:
        logging.error("No category IDs available, cannot insert products.")
        return []
    cat_names = {} # Cache category names
    for cat_id in category_ids:
         cursor.execute("SELECT category_name FROM public.categories WHERE category_id = %s", (cat_id,))
         res = cursor.fetchone()
         if res: cat_names[cat_id] = res[0]

    for i in range(NUM_PRODUCTS):
        try:
            cat_id = random.choice(category_ids)
            cat_name = cat_names.get(cat_id)
            if not cat_name: continue # Skip if somehow category name not found

            template = random.choice(PRODUCT_NAME_TEMPLATES[cat_name])
            variation = random.choice(['', ' X', ' RGB', ' Wireless', ' Mini', ' Elite', ' Plus'])
            modifier = random.choice(['', f' {random.randint(1, 9)}00', f' V{random.randint(2, 7)}', f'-{random.choice(["BLK","WHT","BLU","RED"])}'])
            product_name = template.format(modifier) + variation
            product_name = product_name.replace(' {}','').strip()

            if cat_name == 'PC Components':
              unit_price = round(random.uniform(50.00, 500.00), 2) 
            elif cat_name == 'Keyboards':
              unit_price = round(random.uniform(40.00, 350.00), 2) 
            elif cat_name == 'Audio':
               unit_price = round(random.uniform(25.00, 400.00), 2) 
            else: 
              unit_price = round(random.uniform(25.00, 150.00), 2) 
              
            unit_cost = round(unit_price * random.uniform(0.4, 0.7), 2)
            sku = fake.unique.bothify(text='SKU-???-#####').upper()
            is_active = random.choices([True, False], weights=[0.95, 0.05], k=1)[0]

            cursor.execute(
                """INSERT INTO public.products
                   (product_name, category_id, unit_price, unit_cost, sku, is_active)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (sku) DO NOTHING
                   RETURNING product_id, unit_price, unit_cost""",
                (product_name, cat_id, unit_price, unit_cost, sku, is_active)
            )
            result = cursor.fetchone()
            if result:
                
                product_data.append({'id': result[0], 'price': result[1], 'cost': result[2]})

        except psycopg2.Error as e:
            logging.warning(f"Skipping product due to DB error: {e}")
            cursor.connection.rollback()
            continue
        except Exception as e:
             logging.warning(f"Generic error generating product data: {e}")
             continue

    logging.info(f"Inserted/Processed {len(product_data)} products.")
    return product_data


def insert_customers(cursor):
    logging.info("Inserting customers...")
    customer_ids = []
    for _ in range(NUM_CUSTOMERS):
        try:
            fname = fake['fr_FR'].first_name()
            lname = fake['fr_FR'].last_name()
            if random.random() < 0.2: fname = fake['ar_SA'].first_name(); lname = fake['ar_SA'].last_name()
            elif random.random() < 0.1: fname = fake['en_US'].first_name(); lname = fake['en_US'].last_name()

            email = fake.unique.email()
            phone = clean_phone_number(fake.phone_number())
            if not phone: phone = f"{random.randint(100000000, 999999999)}"

            s_date_start = datetime.date(2022, 6, 1)
            s_date_end = datetime.date.today()
            signup_date = random_date_between(s_date_start, s_date_end).date()

            cursor.execute(
                """INSERT INTO public.customers (first_name, last_name, email, phone, signup_date)
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (email) DO NOTHING
                   RETURNING customer_id""",
                (fname, lname, email, phone, signup_date)
            )
            result = cursor.fetchone()
            if result: customer_ids.append(result[0])
        except psycopg2.Error as e:
             logging.warning(f"Skipping customer due to DB error: {e}")
             cursor.connection.rollback()
             continue
        except Exception as e:
             logging.warning(f"Generic error generating customer data: {e}")
             continue
    logging.info(f"Inserted/Processed {len(customer_ids)} customers.")
    return customer_ids

def insert_addresses(cursor, customer_ids):
    logging.info("Inserting addresses...")
    address_ids = {}
    address_count = 0
    for cust_id in customer_ids:
        address_ids[cust_id] = []
        num_addr = random.randint(NUM_ADDRESSES_PER_CUSTOMER[0], NUM_ADDRESSES_PER_CUSTOMER[1])
        for i in range(num_addr):
            try:
                address_type = random.choice(['Shipping', 'Billing', 'Shipping'])
                country, city = get_random_location()
                street = fake['fr_FR'].street_address()
                postal_code = fake.postcode()
                is_default = (i == 0)

                cursor.execute(
                    """INSERT INTO public.addresses
                       (customer_id, address_type, street_address, city, postal_code, country, is_default)
                       VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING address_id""",
                    (cust_id, address_type, street, city, postal_code, country, is_default)
                )
                addr_id = cursor.fetchone()[0]
                address_ids[cust_id].append(addr_id)
                address_count += 1
            except psycopg2.Error as e:
                logging.error(f"Error inserting address for customer {cust_id}: {e}")
                cursor.connection.rollback()
                continue
            except Exception as e:
                logging.warning(f"Generic error generating address data: {e}")
                continue
    logging.info(f"Inserted {address_count} addresses.")
    return address_ids

def insert_promotions(cursor):
    logging.info("Inserting promotions...")
    promo_details_map = {} # Changed name to avoid confusion
    for i in range(NUM_PROMOTIONS):
        try:
            code = fake.unique.bothify(text=random.choice(['SALE##??', 'GAMER###', 'COD##OFF', 'NA###SALE'])).upper()
            dtype = random.choice(['Percentage', 'Fixed Amount'])
            dvalue = round(random.uniform(5.0, 30.0) if dtype == 'Percentage' else random.uniform(3.0, 50.0), 2)
            desc = f"{dvalue}{'%' if dtype == 'Percentage' else '$'} off {random.choice(['selected items', 'your order', 'gaming gear', 'first order'])}"

            days_offset_start = random.randint(-270, 45)
            days_offset_end = days_offset_start + random.randint(15, 120)
            start_date = datetime.date.today() + datetime.timedelta(days=days_offset_start)
            end_date = datetime.date.today() + datetime.timedelta(days=days_offset_end)
            if random.random() < 0.15: start_date = None
            if random.random() < 0.25: end_date = None

            cursor.execute(
                """INSERT INTO public.promotions (promo_code, description, discount_type, discount_value, start_date, end_date)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (promo_code) DO NOTHING
                   RETURNING promo_id, promo_code, discount_type, discount_value""",
                 (code, desc, dtype, dvalue, start_date, end_date)
            )
            result = cursor.fetchone()
            if result:
                 promo_details_map[result[1]] = {'id': result[0], 'type': result[2], 'value': result[3]}
        except psycopg2.Error as e:
            logging.warning(f"Skipping promotion due to DB error: {e}")
            cursor.connection.rollback()
            continue
        except Exception as e:
            logging.warning(f"Generic error generating promotion data: {e}")
            continue
    logging.info(f"Inserted/Processed {len(promo_details_map)} promotions.")
    return promo_details_map # Return the map


def insert_orders_and_items(cursor, customer_ids, product_data, address_ids, promo_details_map): # Use renamed map
    logging.info("Inserting orders and order items...")
    if not customer_ids or not product_data:
        logging.error("Cannot generate orders without customers or products.")
        return {}

    order_id_map = {}
    order_insert_count = 0

    for i in range(NUM_ORDERS):
        cust_id = random.choice(customer_ids)
        if not address_ids.get(cust_id): continue
        ship_addr_id = random.choice(address_ids[cust_id])
        bill_addr_id = ship_addr_id

        order_date = random_date_between(ORDER_START_DATE, ORDER_END_DATE)
        order_status = get_weighted_status(ORDER_STATUS_DISTRIBUTION)
        shipped_at, delivered_at, cancelled_at, cancellation_reason = None, None, None, None
        last_updated_at = order_date + datetime.timedelta(minutes=random.randint(5, 120))

        # Simulate Lifecycle
        if order_status not in ['Pending Confirmation', 'Cancelled by Customer', 'Cancelled by Admin']:
            processing_delay = datetime.timedelta(hours=random.uniform(1, 48))
            if last_updated_at < order_date + processing_delay: last_updated_at = order_date + processing_delay
            if order_status != 'Processing':
                ship_delay = datetime.timedelta(days=random.uniform(0.5, 4))
                shipped_at = last_updated_at + ship_delay
                last_updated_at = shipped_at
                if order_status != 'Shipped':
                    final_event_delay = datetime.timedelta(days=random.uniform(1, 10))
                    final_event_time = shipped_at + final_event_delay
                    if order_status == 'Delivered': delivered_at = final_event_time; last_updated_at = delivered_at
                    elif order_status in ['Refused Delivery', 'Delivery Failed']:
                        cancelled_at = final_event_time; last_updated_at = cancelled_at
                        if order_status == 'Refused Delivery': cancellation_reason = random.choice([r for r in CANCELLATION_REASONS if 'Refused' in r])
                        else: cancellation_reason = random.choice([r for r in CANCELLATION_REASONS if 'Courier' in r or 'Address' in r or 'answer' in r])
                    elif order_status == 'Returned':
                         delivery_delay = random.uniform(1, 7)
                         delivered_at = shipped_at + datetime.timedelta(days=delivery_delay)
                         return_delay = random.uniform(1, 5)
                         cancelled_at = delivered_at + datetime.timedelta(days=return_delay)
                         last_updated_at = cancelled_at
                         cancellation_reason = "Item returned post-delivery"
        elif order_status in ['Cancelled by Customer', 'Cancelled by Admin']:
            cancel_delay = datetime.timedelta(days=random.uniform(0.1, 3))
            cancelled_at = order_date + cancel_delay
            last_updated_at = cancelled_at
            if order_status == 'Cancelled by Customer': cancellation_reason = random.choice([r for r in CANCELLATION_REASONS if 'Customer' in r or 'needed' in r or 'item' in r or 'delayed' in r or 'cheaper' in r])
            else: cancellation_reason = random.choice([r for r in CANCELLATION_REASONS if 'Admin' in r or 'stock' in r])

        applied_promo_id = None
        if promo_details_map and random.random() < 0.30:
            promo_code = random.choice(list(promo_details_map.keys()))
            applied_promo_id = promo_details_map[promo_code]['id']

        order_subtotal_placeholder, order_total_placeholder = 0.0, 0.0
        try:
            cursor.execute(
                """INSERT INTO public.orders
                   (customer_id, order_date, order_status, shipped_at, delivered_at, cancelled_at, last_updated_at,
                    cancellation_reason, subtotal, discount_amount, shipping_cost, tax_amount, order_total,
                    shipping_address_id, billing_address_id, promo_id)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING order_id""",
                 (cust_id, order_date, order_status, shipped_at, delivered_at, cancelled_at, last_updated_at,
                  cancellation_reason, order_subtotal_placeholder, 0.0, 0.0, 0.0, order_total_placeholder,
                  ship_addr_id, bill_addr_id, applied_promo_id)
            )
            order_id = cursor.fetchone()[0]
            order_id_map[order_id] = {'promo_id': applied_promo_id, 'items_total': 0.0}
            order_insert_count += 1
        except psycopg2.Error as e:
            logging.error(f"Error inserting order {i+1} for customer {cust_id}: {e}")
            cursor.connection.rollback()
            continue

        num_items = random.randint(1, MAX_ITEMS_PER_ORDER)
        order_items_subtotal = 0.0
        items_inserted_count = 0
        for _ in range(num_items):
            if not product_data: continue
            product = random.choice(product_data)
            prod_id = product['id']
            qty = random.randint(1, 3)
            # *** Convert potential Decimal from product_data to float ***
            price_unit = float(product['price'])
            cost_unit = float(product['cost']) if product['cost'] is not None else None # Handle potential NULL cost

            try:
                cursor.execute(
                    """INSERT INTO public.order_items (order_id, product_id, quantity, price_per_unit, cost_per_unit)
                       VALUES (%s, %s, %s, %s, %s)""",
                     (order_id, prod_id, qty, price_unit, cost_unit)
                )
                # *** Use float for calculation ***
                order_items_subtotal += qty * price_unit
                items_inserted_count += 1
            except psycopg2.Error as e:
                 logging.error(f"Error inserting order item for order {order_id}: {e}")
                 cursor.connection.rollback()
                 continue
            except ValueError:
                 logging.error(f"Error converting price/cost for item in order {order_id}")
                 continue

        if items_inserted_count > 0:
             order_id_map[order_id]['items_total'] = round(order_items_subtotal, 2)
        else:
            logging.warning(f"Order {order_id} created with no items due to errors.")

    logging.info(f"Finished inserting {order_insert_count} orders and their items.")
    return order_id_map

def update_order_totals(cursor, order_id_map, promo_details_map): # Use renamed map
    logging.info("Updating order totals...")
    if not promo_details_map:
        logging.warning("Promo data unavailable for discount calculation.")

    updated_count = 0
    skipped_count = 0
    for order_id, order_data in order_id_map.items():
        # *** Ensure subtotal is float ***
        subtotal = float(order_data.get('items_total', 0.0))

        if subtotal <= 0:
             skipped_count += 1
             continue

        discount_amount = 0.0
        promo_id = order_data.get('promo_id')

        if promo_id and promo_details_map:
            # Find promo details using the correct map name
            promo_detail = next((p for p_code, p in promo_details_map.items() if p['id'] == promo_id), None)
            if promo_detail:
                try:
                    # *** Ensure discount value is float ***
                    discount_value_float = float(promo_detail['value'])
                    if promo_detail['type'] == 'Percentage':
                        discount_amount = round(subtotal * (discount_value_float / 100.0), 2)
                    elif promo_detail['type'] == 'Fixed Amount':
                        discount_amount = discount_value_float
                    discount_amount = round(min(subtotal, discount_amount), 2)
                except (ValueError, TypeError):
                     logging.warning(f"Invalid discount value '{promo_detail['value']}' for promo_id {promo_id}, skipping discount.")
                     discount_amount = 0.0

        # *** Calculations using floats ***
        shipping_cost = round(random.uniform(3.0, 15.0), 2) if subtotal < 75 else 0.0
        taxable_amount = subtotal - discount_amount
        tax_amount = round(taxable_amount * 0.07, 2) if taxable_amount > 0 else 0.0
        order_total = round(subtotal - discount_amount + shipping_cost + tax_amount, 2)
        order_total = max(0.0, order_total)

        try:
            cursor.execute(
                """UPDATE public.orders
                   SET subtotal = %s, discount_amount = %s, shipping_cost = %s, tax_amount = %s, order_total = %s
                   WHERE order_id = %s""",
                 (subtotal, discount_amount, shipping_cost, tax_amount, order_total, order_id)
            )
            updated_count += 1
        except psycopg2.Error as e:
            logging.error(f"Error updating totals for order {order_id}: {e}")
            cursor.connection.rollback()

    logging.info(f"Updated totals for {updated_count} orders. Skipped {skipped_count} orders with zero subtotal.")


def insert_web_sessions(cursor, customer_ids):
    logging.info("Inserting web sessions...")
    if not customer_ids: customer_ids = [None]

    session_count = 0
    for _ in range(NUM_WEB_SESSIONS):
        try:
            cust_id = random.choice(customer_ids + [None]*int(len(customer_ids)*0.6))
            session_start = random_date_between(ORDER_START_DATE, ORDER_END_DATE)
            session_end = session_start + datetime.timedelta(minutes=random.randint(1, 180))
            ip_address = fake.ipv4()
            user_agent = fake.user_agent()
            referrer = random.choice(REFERRER_SOURCES + [None]*2)
            utm_campaign = fake.slug() if random.random() < 0.25 else None
            utm_medium = random.choice(['cpc', 'social', 'email', 'referral']) if utm_campaign else None

            cursor.execute(
                 """INSERT INTO public.web_sessions
                    (customer_id, session_start, session_end, ip_address, user_agent, referrer_source, utm_campaign, utm_medium)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                 (cust_id, session_start, session_end, ip_address, user_agent, referrer, utm_campaign, utm_medium)
            )
            session_count += 1
        except psycopg2.Error as e:
            logging.error(f"Error inserting web session: {e}")
            cursor.connection.rollback()
        except Exception as e:
            logging.warning(f"Generic error generating session data: {e}")
            continue
    logging.info(f"Inserted {session_count} web sessions.")


# --- Main Execution ---
if __name__ == "__main__":
    start_time = datetime.datetime.now()
    logging.info("Starting data generation script...")
    conn = get_db_connection()
    if conn:
        cur = conn.cursor()

        # --- Optional: Add data clearing logic here if desired ---
        # logging.info("Clearing existing data...")
        # Add DELETE FROM ... commands and sequence resets if needed
        # Remember conn.commit() after clearing
        # ---

        try:
            # Generate in order of dependency
            category_ids = insert_categories(cur)
            conn.commit()

            product_data = insert_products(cur, category_ids)
            conn.commit()

            customer_ids = insert_customers(cur)
            conn.commit()

            address_data = insert_addresses(cur, customer_ids)
            conn.commit()

            promo_data_map = insert_promotions(cur) # Use correct variable name
            conn.commit()

            order_map = insert_orders_and_items(cur, customer_ids, product_data, address_data, promo_data_map) # Pass correct map
            conn.commit()

            update_order_totals(cur, order_map, promo_data_map) # Pass correct map
            conn.commit()

            insert_web_sessions(cur, customer_ids)
            conn.commit()

            end_time = datetime.datetime.now()
            logging.info(f"Sample data generation completed successfully in {end_time - start_time}!")

        except Exception as e:
            logging.exception(f"An critical error occurred during data generation:") # Log traceback
            conn.rollback()
        finally:
            cur.close()
            conn.close()
            logging.info("Database connection closed.")
    else:
        logging.error("Could not establish database connection. Exiting.")
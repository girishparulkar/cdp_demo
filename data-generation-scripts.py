# generate_data.py
"""
Complete Data Generation for StreamMart E-commerce Demo
Generates realistic historical and real-time data for the demo

Usage:
    python generate_data.py --mode historical --events 10000
    python generate_data.py --mode reference
"""

import json
import random
import argparse
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd

fake = Faker()

# ==============================================
# PRODUCT CATALOG
# ==============================================

PRODUCTS = [
    # Electronics (15 products)
    {"id": 1523, "name": "Wireless Headphones Pro", "category": "electronics", "price": 149.99, "brand": "SoundMax", "rating": 4.5},
    {"id": 1524, "name": "Bluetooth Speaker", "category": "electronics", "price": 79.99, "brand": "SoundMax", "rating": 4.3},
    {"id": 2341, "name": "Smart Watch Series 5", "category": "electronics", "price": 299.99, "brand": "TechWear", "rating": 4.7},
    {"id": 2342, "name": "Fitness Tracker", "category": "electronics", "price": 89.99, "brand": "TechWear", "rating": 4.2},
    {"id": 3421, "name": "USB-C Hub Adapter", "category": "electronics", "price": 19.99, "brand": "TechConnect", "rating": 4.0},
    {"id": 3422, "name": "Wireless Charging Pad", "category": "electronics", "price": 29.99, "brand": "TechConnect", "rating": 4.1},
    {"id": 8834, "name": "Premium Headphone Case", "category": "electronics", "price": 24.99, "brand": "SoundMax", "rating": 4.4},
    {"id": 8835, "name": "Phone Stand Holder", "category": "electronics", "price": 15.99, "brand": "TechConnect", "rating": 4.0},
    {"id": 9912, "name": "Replacement Ear Pads", "category": "electronics", "price": 14.99, "brand": "SoundMax", "rating": 4.2},
    {"id": 9913, "name": "Screen Protector", "category": "electronics", "price": 9.99, "brand": "TechShield", "rating": 4.1},
    {"id": 4567, "name": "Portable Power Bank", "category": "electronics", "price": 39.99, "brand": "PowerPlus", "rating": 4.6},
    {"id": 4568, "name": "Wireless Mouse", "category": "electronics", "price": 29.99, "brand": "TechConnect", "rating": 4.3},
    {"id": 4569, "name": "Mechanical Keyboard", "category": "electronics", "price": 89.99, "brand": "TechConnect", "rating": 4.7},
    {"id": 4570, "name": "Webcam HD", "category": "electronics", "price": 59.99, "brand": "TechVision", "rating": 4.4},
    {"id": 4571, "name": "Gaming Headset", "category": "electronics", "price": 79.99, "brand": "SoundMax", "rating": 4.5},
    
    # Fashion (15 products)
    {"id": 4423, "name": "Running Shoes Pro", "category": "fashion", "price": 89.99, "brand": "SportFit", "rating": 4.6},
    {"id": 4424, "name": "Training Sneakers", "category": "fashion", "price": 79.99, "brand": "SportFit", "rating": 4.4},
    {"id": 6643, "name": "Winter Jacket Premium", "category": "fashion", "price": 129.99, "brand": "WarmWear", "rating": 4.8},
    {"id": 6644, "name": "Rain Coat", "category": "fashion", "price": 69.99, "brand": "WarmWear", "rating": 4.2},
    {"id": 7789, "name": "Casual T-Shirt", "category": "fashion", "price": 24.99, "brand": "UrbanStyle", "rating": 4.1},
    {"id": 7790, "name": "Denim Jeans", "category": "fashion", "price": 59.99, "brand": "UrbanStyle", "rating": 4.3},
    {"id": 7791, "name": "Sports Shorts", "category": "fashion", "price": 29.99, "brand": "SportFit", "rating": 4.0},
    {"id": 7792, "name": "Hoodie Sweatshirt", "category": "fashion", "price": 44.99, "brand": "UrbanStyle", "rating": 4.4},
    {"id": 7793, "name": "Backpack Travel", "category": "fashion", "price": 49.99, "brand": "TravelPro", "rating": 4.5},
    {"id": 7794, "name": "Leather Wallet", "category": "fashion", "price": 34.99, "brand": "UrbanStyle", "rating": 4.6},
    {"id": 7795, "name": "Baseball Cap", "category": "fashion", "price": 19.99, "brand": "SportFit", "rating": 4.2},
    {"id": 7796, "name": "Sunglasses", "category": "fashion", "price": 39.99, "brand": "StyleVision", "rating": 4.3},
    {"id": 7797, "name": "Watch Band", "category": "fashion", "price": 14.99, "brand": "TechWear", "rating": 4.0},
    {"id": 7798, "name": "Belt Leather", "category": "fashion", "price": 29.99, "brand": "UrbanStyle", "rating": 4.1},
    {"id": 7799, "name": "Socks Athletic", "category": "fashion", "price": 12.99, "brand": "SportFit", "rating": 4.0},
    
    # Home (10 products)
    {"id": 7721, "name": "Coffee Maker Deluxe", "category": "home", "price": 79.99, "brand": "BrewMaster", "rating": 4.5},
    {"id": 7722, "name": "Electric Kettle", "category": "home", "price": 39.99, "brand": "BrewMaster", "rating": 4.3},
    {"id": 1124, "name": "Kitchen Blender Pro", "category": "home", "price": 69.99, "brand": "BlendMaster", "rating": 4.6},
    {"id": 1125, "name": "Food Processor", "category": "home", "price": 99.99, "brand": "BlendMaster", "rating": 4.7},
    {"id": 3345, "name": "Toaster Oven", "category": "home", "price": 89.99, "brand": "BrewMaster", "rating": 4.4},
    {"id": 3346, "name": "Air Fryer", "category": "home", "price": 119.99, "brand": "CookPro", "rating": 4.8},
    {"id": 3347, "name": "Vacuum Cleaner", "category": "home", "price": 149.99, "brand": "CleanHome", "rating": 4.5},
    {"id": 3348, "name": "Iron Steam", "category": "home", "price": 49.99, "brand": "CleanHome", "rating": 4.2},
    {"id": 3349, "name": "Dish Rack", "category": "home", "price": 29.99, "brand": "HomeOrganize", "rating": 4.0},
    {"id": 3350, "name": "Storage Containers Set", "category": "home", "price": 24.99, "brand": "HomeOrganize", "rating": 4.3},
    
    # Fitness (10 products)
    {"id": 5532, "name": "Yoga Mat Premium", "category": "fitness", "price": 34.99, "brand": "FitLife", "rating": 4.6},
    {"id": 5533, "name": "Resistance Bands Set", "category": "fitness", "price": 24.99, "brand": "FitLife", "rating": 4.4},
    {"id": 5534, "name": "Dumbbells Set", "category": "fitness", "price": 79.99, "brand": "StrongGear", "rating": 4.7},
    {"id": 5535, "name": "Exercise Ball", "category": "fitness", "price": 19.99, "brand": "FitLife", "rating": 4.2},
    {"id": 5536, "name": "Jump Rope", "category": "fitness", "price": 12.99, "brand": "FitLife", "rating": 4.1},
    {"id": 5537, "name": "Foam Roller", "category": "fitness", "price": 29.99, "brand": "FitLife", "rating": 4.5},
    {"id": 5538, "name": "Water Bottle", "category": "fitness", "price": 14.99, "brand": "HydroFlow", "rating": 4.3},
    {"id": 5539, "name": "Gym Bag", "category": "fitness", "price": 39.99, "brand": "SportFit", "rating": 4.4},
    {"id": 5540, "name": "Workout Gloves", "category": "fitness", "price": 19.99, "brand": "StrongGear", "rating": 4.2},
    {"id": 5541, "name": "Yoga Block Set", "category": "fitness", "price": 16.99, "brand": "FitLife", "rating": 4.0},
]

# ==============================================
# USER GENERATION
# ==============================================

def generate_users(num_users=1000):
    """Generate user profiles"""
    users = []
    for i in range(num_users):
        signup_date = fake.date_between(start_date='-3y', end_date='-1m')
        user = {
            "user_id": 1000 + i,
            "email": fake.email(),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "signup_date": signup_date.isoformat(),
            "customer_segment": random.choice(["premium", "standard", "basic"]),
            "lifetime_value": round(random.uniform(50, 5000), 2),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "country": "US"
        }
        users.append(user)
    return users

# ==============================================
# CLICKSTREAM GENERATION
# ==============================================

def generate_clickstream_event(user_id=None, session_id=None, event_type=None, product=None, timestamp=None):
    """Generate a single clickstream event"""
    if user_id is None:
        user_id = random.randint(1000, 1999)
    
    if session_id is None:
        session_id = random.randint(100000, 999999)
    
    if event_type is None:
        event_type = random.choices(
            ["page_view", "product_view", "add_to_cart", "remove_from_cart", "checkout"],
            weights=[35, 30, 15, 5, 15]
        )[0]
    
    if product is None:
        product = random.choice(PRODUCTS)
    
    if timestamp is None:
        timestamp = datetime.now()
    
    event = {
        "event_id": fake.uuid4(),
        "user_id": user_id,
        "session_id": session_id,
        "event_type": event_type,
        "product_id": product["id"],
        "product_name": product["name"],
        "product_category": product["category"],
        "product_price": product["price"],
        "product_brand": product["brand"],
        "event_timestamp": timestamp.isoformat(),
        "device": random.choice(["mobile", "desktop", "tablet"]),
        "browser": random.choice(["Chrome", "Safari", "Firefox", "Edge"]),
        "page_url": f"/products/{product['category']}/{product['id']}",
        "referrer": random.choice(["google", "facebook", "instagram", "direct", "email"]),
        "cart_value": round(random.uniform(50, 500), 2) if event_type == "checkout" else None,
        "session_duration": random.randint(30, 1800) if event_type == "checkout" else None
    }
    
    return event

def generate_user_session(user_id=None, session_start_time=None):
    """Generate a realistic user session with multiple events"""
    if user_id is None:
        user_id = random.randint(1000, 1999)
    
    if session_start_time is None:
        session_start_time = datetime.now() - timedelta(hours=random.randint(0, 48))
    
    session_id = random.randint(100000, 999999)
    events = []
    
    # Start with page views
    num_page_views = random.randint(2, 6)
    current_time = session_start_time
    
    for _ in range(num_page_views):
        events.append(generate_clickstream_event(
            user_id=user_id,
            session_id=session_id,
            event_type="page_view",
            timestamp=current_time
        ))
        current_time += timedelta(seconds=random.randint(5, 30))
    
    # Product views
    num_product_views = random.randint(1, 5)
    viewed_products = random.sample(PRODUCTS, min(num_product_views, len(PRODUCTS)))
    
    for product in viewed_products:
        events.append(generate_clickstream_event(
            user_id=user_id,
            session_id=session_id,
            event_type="product_view",
            product=product,
            timestamp=current_time
        ))
        current_time += timedelta(seconds=random.randint(10, 60))
    
    # Maybe add to cart
    if random.random() > 0.4:  # 60% add to cart
        num_cart_adds = random.randint(1, 3)
        cart_products = random.sample(viewed_products, min(num_cart_adds, len(viewed_products)))
        
        for product in cart_products:
            events.append(generate_clickstream_event(
                user_id=user_id,
                session_id=session_id,
                event_type="add_to_cart",
                product=product,
                timestamp=current_time
            ))
            current_time += timedelta(seconds=random.randint(5, 20))
        
        # Maybe checkout (32% conversion rate overall)
        if random.random() > 0.68:
            events.append(generate_clickstream_event(
                user_id=user_id,
                session_id=session_id,
                event_type="checkout",
                product=cart_products[0],
                timestamp=current_time
            ))
    
    return events

# ==============================================
# TRANSACTION GENERATION
# ==============================================

def generate_transaction(user_id=None, timestamp=None):
    """Generate a transaction event"""
    if user_id is None:
        user_id = random.randint(1000, 1999)
    
    if timestamp is None:
        timestamp = datetime.now()
    
    num_items = random.randint(1, 5)
    items = random.sample(PRODUCTS, num_items)
    
    transaction_items = []
    subtotal = 0
    
    for item in items:
        quantity = random.randint(1, 3)
        item_total = item["price"] * quantity
        subtotal += item_total
        
        transaction_items.append({
            "product_id": item["id"],
            "product_name": item["name"],
            "quantity": quantity,
            "unit_price": item["price"],
            "item_total": round(item_total, 2)
        })
    
    tax = round(subtotal * 0.08, 2)
    shipping = 9.99 if subtotal < 100 else 0
    total = round(subtotal + tax + shipping, 2)
    
    transaction = {
        "transaction_id": fake.uuid4(),
        "user_id": user_id,
        "transaction_timestamp": timestamp.isoformat(),
        "items": transaction_items,
        "num_items": num_items,
        "subtotal": round(subtotal, 2),
        "tax": tax,
        "shipping": shipping,
        "discount": 0,
        "total": total,
        "payment_method": random.choice(["credit_card", "debit_card", "paypal", "apple_pay", "google_pay"]),
        "shipping_address": {
            "street": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "zip": fake.zipcode(),
            "country": "US"
        },
        "order_status": "completed"
    }
    
    return transaction

# ==============================================
# MAIN GENERATION FUNCTIONS
# ==============================================

def generate_historical_data(num_events=10000, num_transactions=500, output_dir="output"):
    """Generate historical data for loading into lakehouse"""
    import os
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"🔄 Generating {num_events} historical clickstream events...")
    clickstream_events = []
    
    # Generate sessions (each session has 3-15 events)
    num_sessions = num_events // 6  # Average 6 events per session
    for i in range(num_sessions):
        if i % 100 == 0:
            print(f"  Generated {i}/{num_sessions} sessions...")
        
        # Random timestamp in past 30 days
        session_time = datetime.now() - timedelta(
            days=random.randint(0, 30),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        
        session_events = generate_user_session(session_start_time=session_time)
        clickstream_events.extend(session_events)
    
    # Save clickstream
    clickstream_file = f"{output_dir}/clickstream_historical.json"
    with open(clickstream_file, 'w') as f:
        for event in clickstream_events[:num_events]:  # Limit to requested number
            f.write(json.dumps(event) + '\n')
    
    print(f"✅ Saved {len(clickstream_events[:num_events])} events to {clickstream_file}")
    
    # Generate transactions
    print(f"🔄 Generating {num_transactions} historical transactions...")
    transactions = []
    
    for i in range(num_transactions):
        txn_time = datetime.now() - timedelta(
            days=random.randint(0, 30),
            hours=random.randint(0, 23)
        )
        transactions.append(generate_transaction(timestamp=txn_time))
    
    # Save transactions
    transactions_file = f"{output_dir}/transactions_historical.json"
    with open(transactions_file, 'w') as f:
        for txn in transactions:
            f.write(json.dumps(txn) + '\n')
    
    print(f"✅ Saved {len(transactions)} transactions to {transactions_file}")
    
    return clickstream_events[:num_events], transactions

def generate_reference_data(num_users=1000, output_dir="output"):
    """Generate reference data (products, users)"""
    import os
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Save product catalog
    products_file = f"{output_dir}/products.json"
    with open(products_file, 'w') as f:
        json.dump(PRODUCTS, f, indent=2)
    
    print(f"✅ Saved {len(PRODUCTS)} products to {products_file}")
    
    # Generate and save users
    print(f"🔄 Generating {num_users} users...")
    users = generate_users(num_users)
    
    users_file = f"{output_dir}/users.json"
    with open(users_file, 'w') as f:
        json.dump(users, f, indent=2)
    
    print(f"✅ Saved {num_users} users to {users_file}")
    
    return PRODUCTS, users

# ==============================================
# CLI
# ==============================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate StreamMart demo data')
    parser.add_argument('--mode', choices=['historical', 'reference', 'all'], 
                       default='all', help='Type of data to generate')
    parser.add_argument('--events', type=int, default=10000, 
                       help='Number of clickstream events')
    parser.add_argument('--transactions', type=int, default=500,
                       help='Number of transactions')
    parser.add_argument('--users', type=int, default=1000,
                       help='Number of users')
    parser.add_argument('--output', default='output',
                       help='Output directory')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("StreamMart Data Generation")
    print("=" * 60)
    
    if args.mode in ['reference', 'all']:
        generate_reference_data(num_users=args.users, output_dir=args.output)
    
    if args.mode in ['historical', 'all']:
        generate_historical_data(
            num_events=args.events,
            num_transactions=args.transactions,
            output_dir=args.output
        )
    
    print("\n" + "=" * 60)
    print("✅ Data generation complete!")
    print("=" * 60)
    print(f"\nFiles created in '{args.output}/' directory")
    print("\nNext steps:")
    print("1. Upload files to cloud storage (S3/ADLS/GCS)")
    print("2. Create Iceberg tables in CDW")
    print("3. Load historical data into tables")

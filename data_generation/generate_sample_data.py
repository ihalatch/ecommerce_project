import random
from faker import Faker
from datetime import datetime, timedelta
import json

fake = Faker()
Faker.seed(42)

NUM_SUPPLIERS = 10
NUM_CUSTOMERS = 100
NUM_PRODUCTS = 50
NUM_ORDERS = 200
NUM_ORDER_ITEMS = 400
NUM_REVIEWS = 120

# Generate suppliers
def generate_suppliers():
    suppliers = []
    for supplier_id in range(1, NUM_SUPPLIERS + 1):
        suppliers.append({
            'supplier_id': supplier_id,
            'company_name': fake.company(),
            'contact_name': fake.name(),
            'email': fake.company_email(),
            'phone': fake.phone_number(),
            'country': fake.country(),
            'lead_time': random.randint(2, 30)
        })
    return suppliers

# Generate customers
def generate_customers():
    customers = []
    for customer_id in range(1, NUM_CUSTOMERS + 1):
        registration_date = fake.date_between(start_date='-3y', end_date='today')
        last_login_date = fake.date_between(start_date=registration_date, end_date='today')
        customers.append({
            'customer_id': customer_id,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'age': random.randint(18, 80),
            'email': fake.email(),
            'country': fake.country(),
            'city': fake.city(),
            'postal_code': fake.postcode(),
            'phone_number': fake.phone_number(),
            'registration_date': registration_date,
            'last_login_date': last_login_date
        })
    return customers

# Generate products
def generate_products(suppliers):
    products = []
    for product_id in range(1, NUM_PRODUCTS + 1):
        supplier = random.choice(suppliers)
        price = round(random.uniform(10, 2000), 2)
        cost = round(price * random.uniform(0.6, 0.9), 2)
        products.append({
            'product_id': product_id,
            'name': fake.catch_phrase(),
            'category': random.choice(['Electronics', 'Books', 'Clothing', 'Home', 'Sports']),
            'subcategory': fake.word(),
            'price': price,
            'cost': cost,
            'supplier_id': supplier['supplier_id'],
            'stock_quantity': random.randint(0, 100),
            'weight': round(random.uniform(0.1, 10.0), 2),
            'dimensions': f"{random.randint(10, 100)}x{random.randint(10, 100)}x{random.randint(1, 50)} cm"
        })
    return products

# Generate orders
def generate_orders(customers):
    orders = []
    order_statuses = ['pending', 'processing', 'shipped', 'delivered', 'cancelled', 'returned']
    payment_methods = ['credit_card', 'paypal', 'bank_transfer', 'cash_on_delivery']
    currencies = ['USD', 'EUR', 'GBP']
    for order_id in range(1, NUM_ORDERS + 1):
        customer = random.choice(customers)
        order_date = fake.date_between(start_date=customer['registration_date'], end_date='today')
        status = random.choice(order_statuses)
        orders.append({
            'order_id': order_id,
            'customer_id': customer['customer_id'],
            'order_date': order_date,
            'total_amount': 0,  # Will be updated after order_items
            'status': status,
            'shipping_address': fake.address().replace('\n', ', '),
            'payment_method': random.choice(payment_methods),
            'currency': random.choice(currencies)
        })
    return orders

# Generate order_items and update order total_amount
def generate_order_items(orders, products):
    order_items = []
    for _ in range(NUM_ORDER_ITEMS):
        order = random.choice(orders)
        product = random.choice(products)
        quantity = random.randint(1, 5)
        unit_price = product['price']
        discount_amount = round(unit_price * random.uniform(0, 0.2), 2)
        total_price = round((unit_price - discount_amount) * quantity, 2)
        order_items.append({
            'order_id': order['order_id'],
            'product_id': product['product_id'],
            'quantity': quantity,
            'unit_price': unit_price,
            'discount_amount': discount_amount,
            'total_price': total_price
        })
        order['total_amount'] += total_price
    # Round order totals
    for order in orders:
        order['total_amount'] = round(order['total_amount'], 2)
    return order_items

# Generate product_reviews
def generate_product_reviews(products, customers):
    reviews = []
    for review_id in range(1, NUM_REVIEWS + 1):
        product = random.choice(products)
        customer = random.choice(customers)
        review_date = fake.date_between(start_date=customer['registration_date'], end_date='today')
        reviews.append({
            'review_id': review_id,
            'product_id': product['product_id'],
            'customer_id': customer['customer_id'],
            'rating': random.randint(1, 5),
            'review_text': fake.sentence(nb_words=15),
            'review_date': review_date
        })
    return reviews

if __name__ == '__main__':
    suppliers = generate_suppliers()
    customers = generate_customers()
    products = generate_products(suppliers)
    orders = generate_orders(customers)
    order_items = generate_order_items(orders, products)
    reviews = generate_product_reviews(products, customers)

    # Print sample data for verification
    import pprint
    pp = pprint.PrettyPrinter(indent=2)
    print('Suppliers:'); pp.pprint(suppliers[:2])
    print('\nCustomers:'); pp.pprint(customers[:2])
    print('\nProducts:'); pp.pprint(products[:2])
    print('\nOrders:'); pp.pprint(orders[:2])
    print('\nOrder Items:'); pp.pprint(order_items[:2])
    print('\nProduct Reviews:'); pp.pprint(reviews[:2])

    # Save customers to JSON for Kafka producer
    with open('customers.json', 'w') as f:
        json.dump(customers, f, default=str, indent=2) 
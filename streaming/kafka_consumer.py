import json
from kafka import KafkaConsumer
import mysql.connector
from loguru import logger
from config import KAFKA_BROKER, KAFKA_TOPIC, MYSQL_CONFIG
import re
from datetime import datetime
import pycountry
import phonenumbers
try:
    from postal.parser import parse_address
    LIBPOSTAL_AVAILABLE = True
except ImportError:
    LIBPOSTAL_AVAILABLE = False
# import requests  # For external email validation API (optional)

# Regex for names and cities (allow letters, spaces, hyphens, apostrophes)
NAME_REGEX = re.compile(r"^[A-Za-z\s\-']+$")
CITY_REGEX = re.compile(r"^[A-Za-z\s\-']+$")

def is_valid_email(email):
    # Stricter email regex
    pattern = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    return bool(re.match(pattern, email))

# Optional: External email validation API (commented)
def is_email_deliverable(email):
    # Example using ZeroBounce or similar
    # api_key = 'YOUR_API_KEY'
    # url = f'https://api.zerobounce.net/v2/validate?api_key={api_key}&email={email}'
    # try:
    #     resp = requests.get(url, timeout=5)
    #     data = resp.json()
    #     return data.get('status') == 'valid'
    # except Exception as e:
    #     logger.error(f'Email API error: {e}')
    #     return False
    return True  # Always true unless API is enabled

def get_country_info(country_name):
    try:
        country = pycountry.countries.lookup(country_name)
        return country.name, country.alpha_2
    except LookupError:
        return None, None

def normalize_and_validate_phone(phone, country_code):
    try:
        parsed = phonenumbers.parse(phone, country_code)
        if phonenumbers.is_valid_number(parsed):
            return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
        else:
            return None
    except Exception as e:
        logger.error(f'Phone normalization error: {e}')
        return None

def is_valid_date(date_str):
    # Accepts YYYY-MM-DD or YYYY-MM-DD HH:MM:SS
    try:
        if 'T' in date_str:
            date_str = date_str.replace('T', ' ')
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except Exception:
        try:
            datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
            return True
        except Exception:
            return False

# --- GDPR Masking Utilities ---
def mask_email(email):
    # Mask everything except first char and domain
    if '@' in email:
        name, domain = email.split('@', 1)
        return name[0] + '***@' + domain
    return '***'

def mask_phone(phone):
    # Mask all but last 2 digits
    digits = re.sub(r'\D', '', phone)
    if len(digits) > 2:
        return '*' * (len(digits)-2) + digits[-2:]
    return '**'

def mask_address(address):
    # Mask all but first 5 chars
    return address[:5] + '***' if address else '***'

# --- Address Validation ---
def is_valid_address(address, country=None):
    if not address or len(address) < 5:
        return False
    if LIBPOSTAL_AVAILABLE:
        try:
            parsed = parse_address(address)
            # Check for at least a road or house number
            components = dict(parsed)
            if 'road' in components or 'house_number' in components:
                return True
            return False
        except Exception as e:
            logger.error(f'Libpostal error: {e}')
            return False
    else:
        # Fallback: basic check for numbers and letters
        if re.search(r'[A-Za-z]', address) and re.search(r'\d', address):
            return True
        return False

def validate_customer(customer, db_conn):
    required_fields = [
        'customer_id', 'first_name', 'last_name', 'age', 'email', 'country',
        'city', 'postal_code', 'phone_number', 'registration_date', 'last_login_date'
    ]
    # shipping_address is optional, but validate if present
    for field in required_fields:
        if field not in customer or customer[field] in [None, '']:
            logger.error(f"Missing or empty field: {field} in customer (masked email: {mask_email(customer.get('email',''))})")
            return False
    # Name fields (allow spaces, hyphens, apostrophes)
    if not NAME_REGEX.match(customer['first_name']) or not NAME_REGEX.match(customer['last_name']):
        logger.error(f"Invalid name: {mask_email(customer['first_name'])} {mask_email(customer['last_name'])}")
        return False
    # Country validation
    country_name, country_code = get_country_info(customer['country'])
    if not country_name or not country_code:
        logger.error(f"Invalid country: {customer['country']} (masked email: {mask_email(customer['email'])})")
        return False
    customer['country'] = country_name  # Use canonical name
    customer['country_code'] = country_code  # For phone normalization
    # City (allow spaces, hyphens, apostrophes)
    if not CITY_REGEX.match(customer['city']):
        logger.error(f"Invalid city: {customer['city']} (masked email: {mask_email(customer['email'])})")
        return False
    # Email
    if not is_valid_email(customer['email']):
        logger.error(f"Invalid email: {mask_email(customer['email'])}")
        return False
    if not is_email_deliverable(customer['email']):
        logger.error(f"Undeliverable email (API): {mask_email(customer['email'])}")
        return False
    # Check for duplicate email in DB
    try:
        cursor = db_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM customers WHERE email=%s", (customer['email'],))
        if cursor.fetchone()[0] > 0:
            logger.error(f"Duplicate email in DB: {mask_email(customer['email'])}")
            cursor.close()
            return False
        cursor.close()
    except Exception as e:
        logger.error(f"DB error during duplicate email check: {e} (masked email: {mask_email(customer['email'])})")
        return False
    # Phone normalization/validation
    normalized_phone = normalize_and_validate_phone(customer['phone_number'], customer['country_code'])
    if not normalized_phone:
        logger.error(f"Invalid or unmatchable phone: {mask_phone(customer['phone_number'])} for country {customer['country_code']} (masked email: {mask_email(customer['email'])})")
        return False
    customer['phone_number'] = normalized_phone
    # Address validation (optional)
    if 'shipping_address' in customer and customer['shipping_address']:
        if not is_valid_address(customer['shipping_address'], customer['country_code']):
            logger.error(f"Invalid address: {mask_address(customer['shipping_address'])} (masked email: {mask_email(customer['email'])})")
            return False
    # Age
    try:
        age = int(customer['age'])
        if not (13 <= age <= 120):
            logger.error(f"Invalid age: {customer['age']} (masked email: {mask_email(customer['email'])})")
            return False
    except Exception:
        logger.error(f"Non-integer age: {customer['age']} (masked email: {mask_email(customer['email'])})")
        return False
    # Dates
    for date_field in ['registration_date', 'last_login_date']:
        if not is_valid_date(str(customer[date_field])):
            logger.error(f"Invalid date format for {date_field}: {customer[date_field]} (masked email: {mask_email(customer['email'])})")
            return False
    return True

def transform_customer(customer):
    # Trim whitespace
    for k, v in customer.items():
        if isinstance(v, str):
            customer[k] = v.strip()
    # Capitalize names and city
    customer['first_name'] = customer['first_name'].capitalize()
    customer['last_name'] = customer['last_name'].capitalize()
    customer['city'] = customer['city'].title()
    # Lowercase email
    customer['email'] = customer['email'].lower()
    # Normalize date format
    for date_field in ['registration_date', 'last_login_date']:
        if isinstance(customer[date_field], str):
            customer[date_field] = customer[date_field].replace('T', ' ')
            # If only date, add time
            if len(customer[date_field]) == 10:
                customer[date_field] += ' 00:00:00'
    return customer

def insert_customer(db_conn, customer):
    query = '''
        INSERT INTO customers
        (customer_id, first_name, last_name, age, email, country, city, postal_code, phone_number, registration_date, last_login_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            first_name=VALUES(first_name),
            last_name=VALUES(last_name),
            age=VALUES(age),
            email=VALUES(email),
            country=VALUES(country),
            city=VALUES(city),
            postal_code=VALUES(postal_code),
            phone_number=VALUES(phone_number),
            registration_date=VALUES(registration_date),
            last_login_date=VALUES(last_login_date)
    '''
    data = (
        customer['customer_id'], customer['first_name'], customer['last_name'], customer['age'],
        customer['email'], customer['country'], customer['city'], customer['postal_code'],
        customer['phone_number'], customer['registration_date'], customer['last_login_date']
    )
    cursor = db_conn.cursor()
    try:
        cursor.execute(query, data)
        db_conn.commit()
        logger.info(f"Inserted/updated customer {customer['customer_id']} in DB.")
    except mysql.connector.Error as e:
        logger.error(f"MySQL error inserting customer {customer['customer_id']}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error inserting customer {customer['customer_id']}: {e}")
    finally:
        cursor.close()

def main():
    logger.info('Starting Kafka consumer...')
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='customer-consumer-group'
    )
    db_conn = mysql.connector.connect(**MYSQL_CONFIG)
    logger.info('Connected to MySQL.')
    for msg in consumer:
        customer = msg.value
        logger.info(f'Received customer {customer.get("customer_id")}.')
        try:
            customer = transform_customer(customer)
            if not validate_customer(customer, db_conn):
                logger.error(f'Validation failed for customer: {customer}')
                continue
            insert_customer(db_conn, customer)
        except Exception as e:
            logger.error(f'Error processing customer {customer.get("customer_id")}: {e}')
    db_conn.close()
    logger.info('Consumer finished.')

if __name__ == '__main__':
    main() 
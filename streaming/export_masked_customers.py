import mysql.connector
import csv
from config import MYSQL_CONFIG
from kafka_consumer import mask_email, mask_phone

def export_masked_customers(output_file='masked_customers.csv'):
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT customer_id, first_name, last_name, age, email, country, city, postal_code, phone_number, registration_date, last_login_date FROM customers")
    customers = cursor.fetchall()
    cursor.close()
    conn.close()

    with open(output_file, 'w', newline='') as csvfile:
        fieldnames = ['customer_id', 'first_name', 'last_name', 'age', 'masked_email', 'country', 'city', 'postal_code', 'masked_phone', 'registration_date', 'last_login_date']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for c in customers:
            writer.writerow({
                'customer_id': c['customer_id'],
                'first_name': c['first_name'],
                'last_name': c['last_name'],
                'age': c['age'],
                'masked_email': mask_email(c['email']),
                'country': c['country'],
                'city': c['city'],
                'postal_code': c['postal_code'],
                'masked_phone': mask_phone(c['phone_number']),
                'registration_date': c['registration_date'],
                'last_login_date': c['last_login_date']
            })
    print(f"Exported {len(customers)} masked customers to {output_file}")

if __name__ == '__main__':
    export_masked_customers() 
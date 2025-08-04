import psycopg2

## Bu değeri localinde çalışırken kendi passwordün yap. Ama kodu pushlarken 'postgres' olarak bırak.
password = 'postgres'

def connect_db():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="postgres",
        user="postgres",
        password=password
    )

def create_view_completed_orders():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE VIEW completed as SELECT * FROM orders WHERE status = 'completed'")
            conn.commit()

def create_view_electronics_products():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE VIEW electr AS (SELECT * FROM products WHERE category = 'Electronics')")
            conn.commit()

def total_spending_per_customer():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                        WITH musteri_harcama as (

                            SELECT o.customer_id, SUM(p.price * o.quantity) as total_spend FROM orders as o 
                            JOIN products as p 
                            ON o.product_id = p.product_id
                            WHERE o.status IN ('completed')
                            GROUP BY o.customer_id

                        )
                        SELECT c.customer_id, c.full_name, mh.total_spend FROM musteri_harcama as mh 
                        JOIN customers as c 
                        ON mh.customer_id = c.customer_id
                        ORDER BY mh.total_spend DESC""")
            return cur.fetchall()

def order_details_with_total():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""WITH toplam_tutar as (

                                SELECT SUM(p.price * o.quantity) as toplam_ucret FROM orders as o 
                                JOIN products as p ON o.product_id = p.product_id


                        ) SELECT toplam_ucret FROM toplam_tutar""")
            return cur.fetchall()

def get_customer_who_bought_most_expensive_product():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""SELECT c.full_name, p.price
                    FROM customers c
                    JOIN orders o ON c.customer_id = o.customer_id
                    JOIN products p ON o.product_id = p.product_id
                    WHERE p.price = (
                        SELECT MAX(price)
                        FROM products
                    );""")
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result

# 2. Sipariş durumlarına göre açıklama
def get_order_status_descriptions():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""SELECT order_id, status,
                        CASE
                            WHEN status = 'completed' THEN 'Tamamlandı'
                            WHEN status = 'cancelled' THEN 'İptal Edildi'
                            ELSE 'Diğer'
                        END AS "status_description"
                    FROM orders
                    """)
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result

# 3. Ortalama fiyatın üstündeki ürünler
def get_products_above_average_price():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""SELECT product_name, price FROM products WHERE price > (SELECT AVG(price) FROM products)""")
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result

# 4. Müşteri kategorileri
def get_customer_categories():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""SELECT c.full_name,
                    CASE 
                        WHEN o.quantity > 5 THEN 'Sadık Müşteri'
                        WHEN o.quantity > 2 AND o.quantity <=5 THEN 'Orta Seviye'
                        ELSE 'Yeni Müşteri'
                    END as "customer_category"

                FROM customers as c JOIN orders as o ON c.customer_id = o.customer_id""")
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result

# 5. Son 30 gün içinde sipariş veren müşteriler
def get_recent_customers():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""SELECT c.full_name FROM customers as c JOIN orders as o ON c.customer_id = o.customer_id
                    WHERE o.order_date >= CURRENT_DATE - INTERVAL '30 day'""")
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result

# 6. En çok sipariş verilen ürün
def get_most_ordered_product():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""SELECT p.product_name, o.quantity FROM orders as o 
                    JOIN products as p 
                    ON o.product_id = p.product_id 
                    WHERE o.quantity = (SELECT MAX(quantity) FROM orders)""")
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result

# 7. Ürün fiyatlarına göre etiketleme
def get_product_price_categories():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""SELECT product_name, price, 
                    CASE
                        WHEN price > 1000 THEN 'Pahalı'
                        WHEN price > 500 AND price<= 1000 THEN 'Orta'
                        ELSE 'Ucuz'
                    END as price_category

                FROM products """)
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result
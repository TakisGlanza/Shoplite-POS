# app.py
import hashlib
import os
import sys
import sqlite3
import threading
import time
import queue
from datetime import datetime, timedelta
from flask import Flask, render_template, request, jsonify, Response, redirect, url_for, make_response
import webview
import ctypes.wintypes
import json
import csv
from io import StringIO
from flask import Flask, render_template, make_response

# =========================================
# Configure folder in My Documents
# =========================================
try:
    CSIDL_PERSONAL = 5  # My Documents
    SHGFP_TYPE_CURRENT = 0
    buf = ctypes.create_unicode_buffer(ctypes.wintypes.MAX_PATH)
    ctypes.windll.shell32.SHGetFolderPathW(None, CSIDL_PERSONAL, None, SHGFP_TYPE_CURRENT, buf)
    USER_FOLDER = os.path.join(buf.value, "ShopLite_POS")
except:
    USER_FOLDER = os.path.join(os.path.expanduser("~"), "Documents", "ShopLite_POS")

os.makedirs(USER_FOLDER, exist_ok=True)
EXPORTS_DIR = os.path.join(USER_FOLDER, "Exports")
os.makedirs(EXPORTS_DIR, exist_ok=True)

DB_PATH = os.path.join(USER_FOLDER, "warehouse.db")
OPEN_FLAG = os.path.join(USER_FOLDER, "open_pos.flag")
CLOSE_FLAG = os.path.join(USER_FOLDER, "close_pos.flag")
TRIAL_DAYS = 5
LICENSE_FILE = os.path.join(USER_FOLDER, "license.json")
LICENSE_SECRET = "ShoplitePOS_2025_BubblePaws"  # secret for offline license keys

print("ShopLite_POS data folder:", USER_FOLDER)

# =========================================
# Flask & WebView
# =========================================
app = Flask(__name__)
app.config["SECRET_KEY"] = "ShopLite_POS-secret"
pos_window = None

# =========================================
# Database connection
# =========================================
def get_db_connection():
    conn = sqlite3.connect(DB_PATH, timeout=10, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA busy_timeout = 5000;")   # wait up to 5 seconds when DB is locked
    conn.execute("PRAGMA journal_mode = WAL;")    # better concurrent reads/writes
    return conn


def init_database():
    conn = get_db_connection()
    c = conn.cursor()

    c.execute("""
    CREATE TABLE IF NOT EXISTS suppliers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        phone TEXT,
        email TEXT,
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS categories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        barcode TEXT UNIQUE NOT NULL,
        name TEXT NOT NULL,
        description TEXT,
        quantity INTEGER DEFAULT 0,
        cost_price REAL DEFAULT 0.0,
        retail_price REAL DEFAULT 0.0,
        min_stock INTEGER DEFAULT 1,
        category_id INTEGER,
        supplier_id INTEGER,
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE SET NULL,
        FOREIGN KEY (supplier_id) REFERENCES suppliers(id) ON DELETE SET NULL
    )
    """)

    # Migration: add supplier_code to products if missing
    try:
        c.execute("ALTER TABLE products ADD COLUMN supplier_code TEXT")
    except Exception:
        # Column already exists, ignore
        pass

    c.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER,
        barcode TEXT,
        transaction_type TEXT NOT NULL,
        quantity INTEGER NOT NULL,
        price REAL DEFAULT 0.0,
        total_value REAL DEFAULT 0.0,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        notes TEXT,
        FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE SET NULL
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS purchase_orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        supplier_id INTEGER,
        order_number TEXT UNIQUE NOT NULL,
        order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        expected_date TIMESTAMP,
        status TEXT DEFAULT 'pending', -- pending, ordered, received, cancelled
        total_amount REAL DEFAULT 0.0,
        notes TEXT,
        FOREIGN KEY (supplier_id) REFERENCES suppliers(id) ON DELETE SET NULL
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS purchase_order_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id INTEGER,
        product_id INTEGER,
        barcode TEXT,
        product_name TEXT NOT NULL,
        quantity_ordered INTEGER NOT NULL,
        quantity_received INTEGER DEFAULT 0,
        unit_cost REAL DEFAULT 0.0,
        total_cost REAL DEFAULT 0.0,
        FOREIGN KEY (order_id) REFERENCES purchase_orders(id) ON DELETE CASCADE,
        FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE SET NULL
    )
    """)

    # Ensure extra columns on purchase_orders
    try:
        c.execute("ALTER TABLE purchase_orders ADD COLUMN invoice_number TEXT")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE purchase_orders ADD COLUMN invoice_date TEXT")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE purchase_orders ADD COLUMN date_received TEXT")
    except Exception:
        pass

    conn.commit()
    conn.close()
    print("Database initialized successfully")

# =========================================
# LICENSE / TRIAL SYSTEM (LOCAL)
# =========================================

def _load_license():
    data = {}
    if os.path.exists(LICENSE_FILE):
        try:
            with open(LICENSE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = {}
    return data


def _save_license(data: dict):
    try:
        with open(LICENSE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print("Error writing license file:", e)


def validate_license_key(key: str) -> bool:
    """
    Simple offline check:
    Key format: SHOPLITE-XXXX-YYYY
    - XXXX: 4 chars (alphanumeric)
    - YYYY: last 4 hex of SHA1(LICENSE_SECRET + XXXX)
    This is the security for now until a real server is added.
    """
    try:
        key = key.strip().upper()
        if not key.startswith("SHOPLITE-"):
            return False
        body = key[len("SHOPLITE-"):]  # e.g. "ABCD-1F2A"
        parts = body.split("-")
        if len(parts) != 2:
            return False
        code, checksum = parts
        if len(code) != 4 or len(checksum) != 4:
            return False

        h = hashlib.sha1((LICENSE_SECRET + code).encode("utf-8")).hexdigest().upper()
        expected = h[-4:]
        return expected == checksum
    except Exception:
        return False


def get_license_status():
    """
    Returns dict:
    - activated: bool
    - first_run: ISO string or None
    - trial_days: TRIAL_DAYS
    - days_used: int
    - trial_valid: bool
    """
    today = datetime.now().date()
    data = _load_license()

    activated = bool(data.get("activated", False))
    first_run_str = data.get("first_run")
    days_used = 0
    trial_valid = False

    if first_run_str:
        try:
            first_run_date = datetime.fromisoformat(first_run_str).date()
        except Exception:
            first_run_date = today
        days_used = (today - first_run_date).days + 1
        trial_valid = days_used <= TRIAL_DAYS

    return {
        "activated": activated,
        "first_run": first_run_str,
        "trial_days": TRIAL_DAYS,
        "days_used": days_used,
        "trial_valid": trial_valid
    }


def start_trial():
    """
    Set the first trial day if not already set.
    If the folder is deleted, trial starts again.
    """
    today = datetime.now().date()
    data = _load_license()

    if not data.get("first_run"):
        data["first_run"] = today.isoformat()
        _save_license(data)

    return get_license_status()


# =========================================
# ROUTES
# =========================================
@app.after_request
def ensure_html_content_type(resp):
    ct = resp.headers.get("Content-Type", "")
    if "text/plain" in ct:
        data = resp.get_data(as_text=True)
        if data.lstrip().startswith("<!DOCTYPE html") or data.lstrip().startswith("<html"):
            resp.headers["Content-Type"] = "text/html; charset=utf-8"
    return resp


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/pos")
def pos():
    return render_template("pos.html")


@app.route("/analytics")
def analytics():
    return render_template("analytics.html")


from flask import make_response, render_template

@app.route("/purchase-orders")
def purchase_orders():
    html = render_template("purchase_orders.html")
    resp = make_response(html)
    resp.headers["Content-Type"] = "text/html; charset=utf-8"
    return resp


@app.route("/license")
def license_page():
    # This is the page that webview will open first
    return render_template("license.html")


@app.route("/api/license/status", methods=["GET"])
def api_license_status():
    status = get_license_status()
    return jsonify(status)


@app.route("/api/license/start_trial", methods=["POST"])
def api_license_start_trial():
    status = start_trial()
    return jsonify(status)


@app.route("/api/license/activate", methods=["POST"])
def api_license_activate():
    """
    Local activation with license key.
    In the future:
      - We can first call a remote server,
        get the OK there, and then set activated=True.
    """
    try:
        payload = request.get_json() or {}
        key = (payload.get("license_key") or "").strip()
        if not key:
            return jsonify({"success": False, "message": "License key is required"}), 400

        if not validate_license_key(key):
            return jsonify({"success": False, "message": "Invalid license key"}), 400

        data = _load_license()
        data["activated"] = True
        data["license_key"] = key
        _save_license(data)

        return jsonify({"success": True, "message": "License activated"})
    except Exception as e:
        print("License activation error:", e)
        return jsonify({"success": False, "message": "Internal error"}), 500

# =========================================
# PRODUCTS API
# =========================================

@app.route("/api/products", methods=["GET"])
def api_get_products():
    try:
        conn = get_db_connection()
        products = conn.execute("""
            SELECT p.*, c.name as category_name, s.name as supplier_name 
            FROM products p 
            LEFT JOIN categories c ON p.category_id = c.id 
            LEFT JOIN suppliers s ON p.supplier_id = s.id 
            ORDER BY p.name
        """).fetchall()
        conn.close()
        return jsonify([dict(product) for product in products])
    except Exception as e:
        print(f"Error getting products: {e}")
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500


@app.route("/api/products", methods=["POST"])
def api_add_product():
    try:
        data = request.get_json()

        if not data:
            return jsonify({"success": False, "message": "No data received"}), 400

        required_fields = ['barcode', 'name']
        for field in required_fields:
            if not data.get(field):
                return jsonify({"success": False, "message": f"Field {field} is required"}), 400

        conn = get_db_connection()

        # Check if barcode already exists
        existing = conn.execute("SELECT id FROM products WHERE barcode = ?", (data['barcode'],)).fetchone()
        if existing:
            conn.close()
            return jsonify({"success": False, "message": "A product with this barcode already exists"}), 400

        # Add new product
        conn.execute("""
            INSERT INTO products (
                barcode, name, description, quantity, cost_price, retail_price, 
                min_stock, category_id, supplier_id, supplier_code
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data['barcode'].strip(),
            data['name'].strip(),
            data.get('description', '').strip(),
            int(data.get('quantity', 0)),
            float(data.get('cost_price', 0)),
            float(data.get('retail_price', 0)),
            int(data.get('min_stock', 1)),
            data.get('category_id') or None,
            data.get('supplier_id') or None,
            data.get('supplier_code', '').strip(),
        ))

        conn.commit()
        conn.close()
        return jsonify({"success": True, "message": "Product added successfully"})

    except Exception as e:
        print(f"Error adding product: {e}")
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500


@app.route("/api/products/<barcode>", methods=["GET"])
def api_get_product(barcode):
    """Get specific product"""
    try:
        conn = get_db_connection()
        product = conn.execute("""
            SELECT p.*, c.name as category_name, s.name as supplier_name 
            FROM products p 
            LEFT JOIN categories c ON p.category_id = c.id 
            LEFT JOIN suppliers s ON p.supplier_id = s.id 
            WHERE p.barcode = ?
        """, (barcode,)).fetchone()
        conn.close()

        if not product:
            return jsonify({"success": False, "message": "Product not found"}), 404

        return jsonify({"success": True, "product": dict(product)})
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500


@app.route("/api/products/<barcode>", methods=["PUT"])
def api_update_product(barcode):
    """Update product"""
    try:
        data = request.get_json()

        conn = get_db_connection()

        # Check if product exists
        product = conn.execute("SELECT id FROM products WHERE barcode = ?", (barcode,)).fetchone()
        if not product:
            conn.close()
            return jsonify({"success": False, "message": "Product not found"}), 404

        # Update product
        conn.execute("""
            UPDATE products 
            SET name = ?, description = ?, quantity = ?, cost_price = ?, 
                retail_price = ?, min_stock = ?, category_id = ?, supplier_id = ?, 
                supplier_code = ?
            WHERE barcode = ?
        """, (
            data.get('name', '').strip(),
            data.get('description', '').strip(),
            int(data.get('quantity', 0)),
            float(data.get('cost_price', 0)),
            float(data.get('retail_price', 0)),
            int(data.get('min_stock', 1)),
            data.get('category_id') or None,
            data.get('supplier_id') or None,
            data.get('supplier_code', '').strip(),
            barcode
        ))

        conn.commit()
        conn.close()

        return jsonify({"success": True, "message": "Product updated successfully"})

    except Exception as e:
        print(f"Error updating product: {e}")
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500


@app.route("/api/products/<barcode>", methods=["DELETE"])
def api_delete_product(barcode):
    """Delete product"""
    try:
        conn = get_db_connection()

        # Check if product exists
        product = conn.execute("SELECT id FROM products WHERE barcode = ?", (barcode,)).fetchone()
        if not product:
            conn.close()
            return jsonify({"success": False, "message": "Product not found"}), 404

        # Delete product
        conn.execute("DELETE FROM products WHERE barcode = ?", (barcode,))
        conn.commit()
        conn.close()

        return jsonify({"success": True, "message": "Product deleted successfully"})

    except Exception as e:
        print(f"Error deleting product: {e}")
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500

# =========================================
# CATEGORIES API
# =========================================

@app.route("/api/categories", methods=["GET"])
def api_get_categories():
    """Get all categories"""
    try:
        conn = get_db_connection()
        categories = conn.execute("SELECT * FROM categories ORDER BY name").fetchall()
        conn.close()
        return jsonify([dict(category) for category in categories])
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500


@app.route("/api/categories", methods=["POST"])
def api_add_category():
    """Add new category"""
    try:
        data = request.get_json()
        name = data.get('name', '').strip()

        if not name:
            return jsonify({"success": False, "message": "Category name is required"}), 400

        conn = get_db_connection()

        # Check if category already exists
        existing = conn.execute("SELECT id FROM categories WHERE name = ?", (name,)).fetchone()
        if existing:
            conn.close()
            return jsonify({"success": False, "message": "Category with this name already exists"}), 400

        # Add new category
        conn.execute("INSERT INTO categories (name) VALUES (?)", (name,))
        conn.commit()
        conn.close()

        return jsonify({"success": True, "message": "Category added successfully"})

    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500


@app.route("/api/categories/<int:category_id>", methods=["DELETE"])
def api_delete_category(category_id):
    """Delete category"""
    try:
        conn = get_db_connection()

        # Check if category has products
        products_count = conn.execute(
            "SELECT COUNT(*) as count FROM products WHERE category_id = ?", (category_id,)
        ).fetchone()['count']
        if products_count > 0:
            conn.close()
            return jsonify({"success": False, "message": "You cannot delete a category that contains products"}), 400

        # Delete category
        conn.execute("DELETE FROM categories WHERE id = ?", (category_id,))
        conn.commit()
        conn.close()

        return jsonify({"success": True, "message": "Category deleted successfully"})

    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500


@app.route("/api/purchase-orders/<int:order_id>", methods=["DELETE"])
def delete_purchase_order(order_id):
    import sqlite3
    conn = get_db_connection()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    row = cur.execute("SELECT status FROM purchase_orders WHERE id=?", (order_id,)).fetchone()
    if not row:
        conn.close()
        return jsonify({"success": False, "error": "Purchase order not found"}), 404

    # Protection: do not delete orders that have been received
    if (row["status"] or "").lower() in ("received", "ολοκληρωμένη"):
        conn.close()
        return jsonify({"success": False, "error": "The purchase order has been received and cannot be deleted"}), 400

    cur.execute("DELETE FROM purchase_order_items WHERE order_id=?", (order_id,))
    cur.execute("DELETE FROM purchase_orders WHERE id=?", (order_id,))
    conn.commit()
    conn.close()
    return jsonify({"success": True})


@app.route("/api/export/purchase-orders", methods=["GET"])
def export_purchase_orders():
    import csv, io, sqlite3
    from flask import Response

    conn = get_db_connection()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    rows = cur.execute("""
        SELECT 
            po.id,
            po.order_date,
            po.status,
            po.total_amount,
            po.invoice_number,
            po.invoice_date,
            po.date_received,
            s.name AS supplier_name
        FROM purchase_orders po
        LEFT JOIN suppliers s ON s.id = po.supplier_id
        ORDER BY po.id DESC
    """).fetchall()
    conn.close()

    # Write to StringIO and use UTF-8 BOM (utf-8-sig) for proper Excel import on Windows
    output = io.StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow([
        "id", "supplier", "order_date", "status",
        "total_amount", "invoice_number", "invoice_date", "date_received"
    ])
    for r in rows:
        writer.writerow([
            r["id"],
            r["supplier_name"],
            r["order_date"],
            r["status"],
            ("%.2f" % (r["total_amount"] or 0)),
            r["invoice_number"] or "",
            r["invoice_date"] or "",
            r["date_received"] or "",
        ])

    csv_text = output.getvalue()
    # Add BOM
    csv_bytes = csv_text.encode("utf-8-sig")

    return Response(
        csv_bytes,
        mimetype="application/octet-stream",
        headers={
            'X-Content-Type-Options': 'nosniff',
            'Cache-Control': 'no-store',
            "Content-Disposition": "attachment; filename=purchase_orders.csv"
        }
    )


@app.route("/api/suppliers/<int:supplier_id>", methods=["DELETE"])
def api_delete_supplier(supplier_id):
    """Delete supplier"""
    try:
        conn = get_db_connection()

        # Check if there are products with this supplier
        products_count = conn.execute(
            "SELECT COUNT(*) as count FROM products WHERE supplier_id = ?", (supplier_id,)
        ).fetchone()['count']
        if products_count > 0:
            conn.close()
            return jsonify({"success": False, "message": "You cannot delete a supplier that has products"}), 400

        # Delete supplier
        conn.execute("DELETE FROM suppliers WHERE id = ?", (supplier_id,))
        conn.commit()
        conn.close()

        return jsonify({"success": True, "message": "Supplier deleted successfully"})

    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500

# =========================================
# RECEIVING & STOCK MANAGEMENT API
# =========================================
@app.route("/api/receiving/quick-add", methods=["POST"])
def api_quick_add_stock():
    """Quick stock receiving"""
    try:
        data = request.get_json()
        barcode = data.get('barcode', '').strip()
        quantity = int(data.get('quantity', 1))

        if not barcode:
            return jsonify({"success": False, "message": "Barcode is required"}), 400

        conn = get_db_connection()

        # Find product
        product = conn.execute("""
            SELECT p.*, s.name as supplier_name, c.name as category_name 
            FROM products p 
            LEFT JOIN suppliers s ON p.supplier_id = s.id 
            LEFT JOIN categories c ON p.category_id = c.id 
            WHERE p.barcode = ?
        """, (barcode,)).fetchone()

        if not product:
            conn.close()
            return jsonify({"success": False, "message": f"Product with barcode {barcode} not found"}), 404

        # Update quantity
        conn.execute("UPDATE products SET quantity = quantity + ? WHERE barcode = ?", (quantity, barcode))

        # Log transaction
        conn.execute("""
            INSERT INTO transactions (product_id, barcode, transaction_type, quantity, price, total_value, notes)
            VALUES (?, ?, 'receiving', ?, ?, ?, ?)
        """, (
            product['id'],
            barcode,
            quantity,
            product['cost_price'],
            product['cost_price'] * quantity,
            f"Quick receiving - {quantity} units"
        ))

        conn.commit()
        conn.close()

        return jsonify({
            "success": True,
            "message": f"Added {quantity} units to {product['name']}",
            "product_name": product['name']
        })

    except Exception as e:
        print(f"Error in quick add: {e}")
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500


@app.route("/api/scan/out", methods=["POST"])
def api_scan_out():
    """Remove stock (sale/loss)"""
    try:
        data = request.get_json()
        barcode = data.get('barcode', '').strip()
        quantity = int(data.get('quantity', 1))

        if not barcode:
            return jsonify({"success": False, "message": "Barcode is required"}), 400

        conn = get_db_connection()

        # Find product
        product = conn.execute("SELECT * FROM products WHERE barcode = ?", (barcode,)).fetchone()
        if not product:
            conn.close()
            return jsonify({"success": False, "message": f"Product with barcode {barcode} not found"}), 404

        # Check stock
        if product['quantity'] < quantity:
            conn.close()
            return jsonify({"success": False, "message": "Insufficient stock"}), 400

        # Update quantity
        conn.execute("UPDATE products SET quantity = quantity - ? WHERE barcode = ?", (quantity, barcode))

        # Log transaction
        conn.execute("""
            INSERT INTO transactions (product_id, barcode, transaction_type, quantity, price, total_value, notes)
            VALUES (?, ?, 'sale', ?, ?, ?, ?)
        """, (
            product['id'],
            barcode,
            quantity,
            product['retail_price'],
            product['retail_price'] * quantity,
            f"Stock removal - {quantity} units"
        ))

        conn.commit()
        conn.close()

        return jsonify({
            "success": True,
            "message": f"Removed {quantity} units from {product['name']}",
            "product_name": product['name']
        })

    except Exception as e:
        print(f"Error in scan out: {e}")
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500

# =========================================
# STATISTICS API
# =========================================

@app.route("/api/stats")
def api_get_stats():
    """System statistics"""
    try:
        conn = get_db_connection()

        # Basic stats
        stats = conn.execute("""
            SELECT 
                COUNT(*) as total_products,
                SUM(quantity) as total_stock,
                SUM(CASE WHEN quantity <= min_stock THEN 1 ELSE 0 END) as low_stock,
                SUM(quantity * cost_price) as total_cost,
                SUM(quantity * retail_price) as total_retail,
                SUM(quantity * (retail_price - cost_price)) as total_profit
            FROM products
        """).fetchone()

        # Today's transactions from transactions table (sales only)
        today_tx_row = conn.execute("""
            SELECT COUNT(*) AS cnt
            FROM transactions
            WHERE transaction_type = 'sale'
              AND DATE(timestamp) = DATE('now','localtime')
        """).fetchone()
        today_transactions = int(today_tx_row["cnt"] if today_tx_row and today_tx_row["cnt"] is not None else 0)

        conn.close()

        return jsonify({
            "total_products": stats['total_products'] or 0,
            "total_stock": stats['total_stock'] or 0,
            "low_stock": stats['low_stock'] or 0,
            "total_cost": float(stats['total_cost'] or 0),
            "total_retail": float(stats['total_retail'] or 0),
            "total_profit": float(stats['total_profit'] or 0),
            "today_transactions": today_transactions   # New field
        })

    except Exception as e:
        print(f"Error getting stats: {e}")
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500

# =========================================
# POS SYSTEM API
# =========================================

@app.route("/api/pos/add-to-cart", methods=["POST"])
def api_pos_add_to_cart():
    """Add product to POS cart"""
    try:
        data = request.get_json()
        barcode = data.get('barcode', '').strip()
        quantity = int(data.get('quantity', 1))

        if not barcode:
            return jsonify({"success": False, "message": "Barcode is required"}), 400

        conn = get_db_connection()

        # Find product
        product = conn.execute("""
            SELECT p.*, s.name as supplier_name, c.name as category_name 
            FROM products p 
            LEFT JOIN suppliers s ON p.supplier_id = s.id 
            LEFT JOIN categories c ON p.category_id = c.id 
            WHERE p.barcode = ?
        """, (barcode,)).fetchone()

        if not product:
            conn.close()
            return jsonify({"success": False, "message": f"Product with barcode {barcode} not found"}), 404

        # Check stock
        if product['quantity'] < quantity:
            conn.close()
            return jsonify({"success": False, "message": "Insufficient stock"}), 400

        conn.close()

        return jsonify({
            "success": True,
            "product": dict(product)
        })

    except Exception as e:
        print(f"Error in POS add to cart: {e}")
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500


@app.route("/api/pos/complete-sale", methods=["POST"])
def api_pos_complete_sale():
    """Complete POS sale"""
    try:
        data = request.get_json()
        cart_items = data.get('cart_items', [])
        payment_method = data.get('payment_method', 'CASH')
        total_amount = float(data.get('total_amount', 0))
        payment_amount = float(data.get('payment_amount', total_amount))

        if not cart_items:
            return jsonify({"success": False, "message": "Cart is empty"}), 400

        conn = get_db_connection()

        # Create unique receipt number
        receipt_number = f"R{int(datetime.now().timestamp())}"

        # Process each product in cart
        for item in cart_items:
            barcode = item.get('barcode')
            quantity = int(item.get('quantity', 1))

            # Find product
            product = conn.execute("SELECT * FROM products WHERE barcode = ?", (barcode,)).fetchone()
            if not product:
                continue

            # Check stock
            if product['quantity'] < quantity:
                conn.close()
                return jsonify({"success": False, "message": f"Insufficient stock for: {product['name']}"}), 400

            # Update quantity
            conn.execute("UPDATE products SET quantity = quantity - ? WHERE barcode = ?", (quantity, barcode))

            # Log sale transaction
            conn.execute("""
                INSERT INTO transactions (product_id, barcode, transaction_type, quantity, price, total_value, notes)
                VALUES (?, ?, 'sale', ?, ?, ?, ?)
            """, (
                product['id'],
                barcode,
                quantity,
                product['retail_price'],
                product['retail_price'] * quantity,
                f"POS Sale - Receipt: {receipt_number} - {payment_method}"
            ))

        conn.commit()
        conn.close()

        return jsonify({
            "success": True,
            "message": "Sale completed successfully",
            "receipt_number": receipt_number,
            "total_amount": total_amount,
            "change": payment_amount - total_amount if payment_method == 'CASH' else 0
        })

    except Exception as e:
        print(f"Error completing POS sale: {e}")
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500

# =========================================
# ADVANCED ANALYTICS API
# =========================================

@app.route("/api/analytics/sales-overview")
def api_sales_overview():
    """Sales statistics for dashboard"""
    try:
        conn = get_db_connection()

        current_year = datetime.now().year
        last_year = current_year - 1

        # This year's sales (with count)
        sales_this_year = conn.execute("""
            SELECT 
                strftime('%m', timestamp) AS month,
                SUM(total_value)         AS monthly_sales,
                COUNT(*)                 AS transactions_count
            FROM transactions
            WHERE transaction_type = 'sale'
              AND strftime('%Y', timestamp) = ?
            GROUP BY strftime('%m', timestamp)
            ORDER BY month
        """, (str(current_year),)).fetchall()

        # Last year's sales (amounts only)
        sales_last_year = conn.execute("""
            SELECT 
                strftime('%m', timestamp) AS month,
                SUM(total_value)          AS monthly_sales
            FROM transactions
            WHERE transaction_type = 'sale'
              AND strftime('%Y', timestamp) = ?
            GROUP BY strftime('%m', timestamp)
            ORDER BY month
        """, (str(last_year),)).fetchall()

        # Top products this year
        top_products = conn.execute("""
            SELECT 
                p.name,
                p.barcode,
                SUM(t.quantity)                         AS total_sold,
                SUM(t.total_value)                      AS total_revenue,
                (p.retail_price - p.cost_price) * SUM(t.quantity) AS total_profit
            FROM transactions t
            JOIN products p ON t.product_id = p.id
            WHERE t.transaction_type = 'sale'
              AND strftime('%Y', t.timestamp) = ?
            GROUP BY p.id
            ORDER BY total_sold DESC
            LIMIT 10
        """, (str(current_year),)).fetchall()

        # Sales by category this year
        sales_by_category = conn.execute("""
            SELECT 
                COALESCE(c.name, 'Uncategorized') AS category_name,
                SUM(t.total_value)                AS total_sales,
                COUNT(*)                          AS transactions_count
            FROM transactions t
            JOIN products p ON t.product_id = p.id
            LEFT JOIN categories c ON p.category_id = c.id
            WHERE t.transaction_type = 'sale'
              AND strftime('%Y', t.timestamp) = ?
            GROUP BY c.id
            ORDER BY total_sales DESC
        """, (str(current_year),)).fetchall()

        # Last 30 days daily sales
        daily_sales = conn.execute("""
            SELECT 
                DATE(timestamp)      AS date,
                SUM(total_value)     AS daily_sales,
                COUNT(*)             AS daily_transactions
            FROM transactions
            WHERE transaction_type = 'sale'
              AND timestamp >= date('now', '-30 days')
            GROUP BY DATE(timestamp)
            ORDER BY date
        """).fetchall()

        conn.close()

        # Datasets for charts
        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

        this_year_data = [0.0] * 12
        last_year_data = [0.0] * 12
        this_year_tx = [0] * 12

        for row in sales_this_year:
            idx = int(row['month']) - 1
            this_year_data[idx] = float(row['monthly_sales'] or 0)
            this_year_tx[idx] = int(row['transactions_count'] or 0)

        for row in sales_last_year:
            idx = int(row['month']) - 1
            last_year_data[idx] = float(row['monthly_sales'] or 0)

        return jsonify({
            "sales_comparison": {
                "labels": months,
                "this_year": this_year_data,
                "last_year": last_year_data,
                "this_year_tx": this_year_tx
            },
            "top_products": [dict(x) for x in top_products],
            "sales_by_category": [dict(x) for x in sales_by_category],
            "daily_sales": [dict(x) for x in daily_sales],
            "current_year": current_year,
            "last_year": last_year
        })
    except Exception as e:
        print(f"Analytics error: {e}")
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500


@app.route("/api/analytics/inventory-metrics")
def api_inventory_metrics():
    """Inventory metrics"""
    try:
        conn = get_db_connection()

        # Total inventory value
        inventory_value = conn.execute("""
            SELECT 
                SUM(quantity * cost_price) as total_cost_value,
                SUM(quantity * retail_price) as total_retail_value,
                COUNT(*) as total_products,
                SUM(quantity) as total_units
            FROM products
        """).fetchone()

        # Inventory distribution by category
        inventory_by_category = conn.execute("""
            SELECT 
                c.name as category_name,
                COUNT(p.id) as product_count,
                SUM(p.quantity) as total_units,
                SUM(p.quantity * p.cost_price) as cost_value,
                SUM(p.quantity * p.retail_price) as retail_value
            FROM products p
            LEFT JOIN categories c ON p.category_id = c.id
            GROUP BY c.id
            ORDER BY cost_value DESC
        """).fetchall()

        # Fast/slow moving products (last 30 days)
        product_turnover = conn.execute("""
            SELECT 
                p.name,
                p.barcode,
                p.quantity,
                COALESCE(SUM(CASE WHEN t.transaction_type = 'sale' THEN t.quantity ELSE 0 END), 0) as units_sold,
                COALESCE(SUM(CASE WHEN t.transaction_type = 'receiving' THEN t.quantity ELSE 0 END), 0) as units_received,
                CASE 
                    WHEN p.quantity > 0 THEN 
                        COALESCE(SUM(CASE WHEN t.transaction_type = 'sale' THEN t.quantity ELSE 0 END), 0) / p.quantity 
                    ELSE 0 
                END as turnover_ratio
            FROM products p
            LEFT JOIN transactions t ON p.id = t.product_id 
                AND t.timestamp >= date('now', '-30 days')
            GROUP BY p.id
            HAVING units_sold > 0
            ORDER BY turnover_ratio DESC
            LIMIT 15
        """).fetchall()

        conn.close()

        return jsonify({
            "inventory_value": dict(inventory_value),
            "inventory_by_category": [dict(cat) for cat in inventory_by_category],
            "product_turnover": [dict(product) for product in product_turnover]
        })

    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500


@app.route("/api/analytics/profit-analysis")
def api_profit_analysis():
    """Profit analysis"""
    try:
        conn = get_db_connection()

        # Monthly profits (last 12 months)
        monthly_profits = conn.execute("""
            SELECT 
                strftime('%Y-%m', timestamp) as month,
                SUM(total_value) as revenue,
                SUM(quantity * (
                    SELECT cost_price FROM products WHERE id = transactions.product_id
                )) as cost,
                SUM(total_value) - SUM(quantity * (
                    SELECT cost_price FROM products WHERE id = transactions.product_id
                )) as profit
            FROM transactions 
            WHERE transaction_type = 'sale'
            AND timestamp >= date('now', '-12 months')
            GROUP BY strftime('%Y-%m', timestamp)
            ORDER BY month
        """).fetchall()

        # Profit by category (last 6 months)
        profit_by_category = conn.execute("""
            SELECT 
                c.name as category_name,
                SUM(t.total_value) as revenue,
                SUM(t.quantity * p.cost_price) as cost,
                SUM(t.total_value) - SUM(t.quantity * p.cost_price) as profit,
                CASE 
                    WHEN SUM(t.quantity * p.cost_price) > 0 
                    THEN (SUM(t.total_value) - SUM(t.quantity * p.cost_price)) / SUM(t.quantity * p.cost_price) * 100
                    ELSE 0
                END as profit_margin
            FROM transactions t
            JOIN products p ON t.product_id = p.id
            LEFT JOIN categories c ON p.category_id = c.id
            WHERE t.transaction_type = 'sale'
            AND t.timestamp >= date('now', '-6 months')
            GROUP BY c.id
            HAVING revenue > 0
            ORDER BY profit DESC
        """).fetchall()

        # Profit by product (last 6 months)
        top_profitable_products = conn.execute("""
            SELECT 
                p.name,
                p.barcode,
                SUM(t.quantity) as units_sold,
                SUM(t.total_value) as revenue,
                SUM(t.quantity * p.cost_price) as cost,
                SUM(t.total_value) - SUM(t.quantity * p.cost_price) as profit,
                CASE 
                    WHEN SUM(t.quantity * p.cost_price) > 0 
                    THEN (SUM(t.total_value) - SUM(t.quantity * p.cost_price)) / SUM(t.quantity * p.cost_price) * 100
                    ELSE 0
                END as profit_margin
            FROM transactions t
            JOIN products p ON t.product_id = p.id
            WHERE t.transaction_type = 'sale'
            AND t.timestamp >= date('now', '-6 months')
            GROUP BY p.id
            HAVING profit > 0
            ORDER BY profit DESC
            LIMIT 15
        """).fetchall()

        conn.close()

        return jsonify({
            "monthly_profits": [dict(month) for month in monthly_profits],
            "profit_by_category": [dict(cat) for cat in profit_by_category],
            "top_profitable_products": [dict(product) for product in top_profitable_products]
        })

    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500

# =========================================
# PURCHASE ORDERS API
# =========================================

@app.route("/api/export/purchase-orders/<int:order_id>", methods=["GET"])
def api_export_purchase_order_single(order_id):
    import csv
    from io import StringIO
    from flask import Response

    conn = get_db_connection()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Order header
    cur.execute("""
        SELECT po.*, s.name AS supplier_name
        FROM purchase_orders po
        LEFT JOIN suppliers s ON s.id = po.supplier_id
        WHERE po.id = ?
    """, (order_id,))
    order = cur.fetchone()
    if not order:
        conn.close()
        return jsonify({"success": False, "message": "Purchase order not found"}), 404

    # Order lines
    cur.execute("""
        SELECT poi.*, p.name AS actual_product_name, p.supplier_code
        FROM purchase_order_items poi
        LEFT JOIN products p ON p.id = poi.product_id
        WHERE poi.order_id = ?
        ORDER BY poi.id
    """, (order_id,))
    items = cur.fetchall()
    conn.close()

    # Create CSV (with BOM for proper Excel support)
    out = StringIO(newline="")
    w = csv.writer(out)
    w.writerow(["Purchase Order"])
    w.writerow(["Order ID", order["id"]])
    w.writerow(["Order Number", order["order_number"] or ""])
    w.writerow(["Supplier", order["supplier_name"] or ""])
    w.writerow(["Order Date", order["order_date"] or ""])
    w.writerow(["Status", order["status"] or ""])
    w.writerow(["Total Amount", f'{(order["total_amount"] or 0):.2f}'])
    w.writerow(["Notes", order["notes"] or ""])
    w.writerow([])

    w.writerow(["#", "Supplier Code", "Product ID", "Product Name", "Barcode",
                "Qty Ordered", "Qty Received", "Unit Cost", "Line Total"])
    total = 0.0
    for i, it in enumerate(items, start=1):
        qtyo = it["quantity_ordered"] or 0
        qyr = it["quantity_received"] or 0
        cost = float(it["unit_cost"] or 0)
        line = float(it["total_cost"] or (qtyo * cost))
        total += line
        w.writerow([
            i,
            it["supplier_code"] or "",
            it["product_id"] or "",
            it["product_name"] or it["actual_product_name"] or "",
            it["barcode"] or "",
            qtyo,
            qyr,
            f"{cost:.2f}",
            f"{line:.2f}",
        ])
    w.writerow([])
    w.writerow(["Computed Total", f"{total:.2f}"])

    csv_text = out.getvalue()
    csv_bytes = csv_text.encode("utf-8-sig")  # BOM for Excel on Windows
    filename = f'purchase_order_{order["id"]}.csv'

    return Response(
        csv_bytes,
        mimetype="application/octet-stream",
        headers={
            'X-Content-Type-Options': 'nosniff',
            'Cache-Control': 'no-store',
            "Content-Disposition": f'attachment; filename="{filename}"'
        }
    )


@app.route("/api/purchase-orders", methods=["GET"])
def api_get_purchase_orders():
    """Get all purchase orders"""
    try:
        conn = get_db_connection()
        orders = conn.execute("""
            SELECT po.*, s.name as supplier_name
            FROM purchase_orders po
            LEFT JOIN suppliers s ON po.supplier_id = s.id
            ORDER BY po.order_date DESC
        """).fetchall()
        conn.close()
        return jsonify([dict(order) for order in orders])
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500


@app.route("/api/purchase-orders", methods=["POST"])
def api_create_purchase_order():
    """Create new purchase order"""
    try:
        data = request.get_json()
        supplier_id = data.get('supplier_id')
        items = data.get('items', [])

        if not supplier_id or not items:
            return jsonify({"success": False, "message": "Supplier and products are required"}), 400

        conn = get_db_connection()

        # Create unique order number
        order_number = f"PO{int(datetime.now().timestamp())}"

        # Calculate total amount
        total_amount = sum(item.get('quantity_ordered', 0) * item.get('unit_cost', 0) for item in items)

        # Insert order
        cursor = conn.execute("""
            INSERT INTO purchase_orders (supplier_id, order_number, total_amount, notes)
            VALUES (?, ?, ?, ?)
        """, (supplier_id, order_number, total_amount, data.get('notes', '')))

        order_id = cursor.lastrowid

        # Insert order items
        for item in items:
            conn.execute("""
                INSERT INTO purchase_order_items 
                (order_id, product_id, barcode, product_name, quantity_ordered, unit_cost, total_cost)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                order_id,
                item.get('product_id'),
                item.get('barcode'),
                item.get('product_name'),
                item.get('quantity_ordered', 0),
                item.get('unit_cost', 0),
                item.get('quantity_ordered', 0) * item.get('unit_cost', 0)
            ))

        conn.commit()
        conn.close()

        return jsonify({
            "success": True,
            "message": "Purchase order created successfully",
            "order_number": order_number,
            "order_id": order_id
        })

    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500


@app.route("/api/purchase-orders/<int:order_id>", methods=["GET"])
def api_get_purchase_order(order_id):
    """Get specific purchase order"""
    try:
        conn = get_db_connection()

        # Order header
        order = conn.execute("""
            SELECT po.*, s.name as supplier_name, s.phone, s.email
            FROM purchase_orders po
            LEFT JOIN suppliers s ON po.supplier_id = s.id
            WHERE po.id = ?
        """, (order_id,)).fetchone()

        if not order:
            conn.close()
            return jsonify({"success": False, "message": "Purchase order not found"}), 404

        # Order items
        items = conn.execute("""
            SELECT poi.*, p.name as actual_product_name, p.quantity as current_stock,
                   p.supplier_code as supplier_code
            FROM purchase_order_items poi
            LEFT JOIN products p ON poi.product_id = p.id
            WHERE poi.order_id = ?
        """, (order_id,)).fetchall()

        conn.close()

        return jsonify({
            "order": dict(order),
            "items": [dict(item) for item in items]
        })

    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500


# --- Status update + receiving with invoice ---
from datetime import date
import sqlite3
from flask import request, jsonify

@app.route("/api/purchase-orders/<int:order_id>/status", methods=["PUT"])
def api_update_order_status(order_id):
    """Update status + receiving with invoice & optional cost_price update per product"""
    from datetime import date
    data = request.get_json(silent=True) or {}

    new_status = (data.get("status") or "").strip().lower()
    invoice_number = data.get("invoice_number") or None
    invoice_date = data.get("invoice_date") or None
    update_buy = bool(data.get("update_buy_price"))

    # items: [{product_id: 26, new_cost: 1.25}, ...] for cost update per product
    overrides_list = data.get("items") or []
    override_prices = {}
    for it in overrides_list:
        try:
            pid = int(it.get("product_id"))
            nc = float(it.get("new_cost"))
            override_prices[pid] = nc
        except Exception:
            pass

    valid_statuses = ['pending', 'ordered', 'received', 'cancelled', 'ολοκληρωμένη', 'σε εξέλιξη']
    if new_status not in valid_statuses:
        return jsonify({"success": False, "message": "Invalid status"}), 400

    conn = get_db_connection()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    po = cur.execute("SELECT id, status FROM purchase_orders WHERE id=?", (order_id,)).fetchone()
    if not po:
        conn.close()
        return jsonify({"success": False, "message": "Purchase order not found"}), 404

    # Receiving
    if new_status in ("received", "ολοκληρωμένη"):
        # Get order items
        items = cur.execute("""
            SELECT product_id, barcode, quantity_ordered, unit_cost
            FROM purchase_order_items
            WHERE order_id = ?
        """, (order_id,)).fetchall()

        for it in items:
            pid = it["product_id"]
            bc = it["barcode"]
            qty = it["quantity_ordered"]
            cost = override_prices.get(pid, it["unit_cost"])  # use new cost if provided

            if pid is None:
                continue

            # If cost changed in modal, update order item line
            if pid in override_prices:
                cur.execute("""
                    UPDATE purchase_order_items
                    SET unit_cost = ?, total_cost = ?
                    WHERE order_id = ? AND product_id = ?
                """, (cost, cost * qty, order_id, pid))

            # Increase stock
            cur.execute("UPDATE products SET quantity = IFNULL(quantity,0) + ? WHERE id = ?", (qty, pid))

            # Update product cost_price if requested
            if update_buy:
                cur.execute("UPDATE products SET cost_price = ? WHERE id = ?", (cost, pid))

            # Log transaction
            cur.execute("""
                INSERT INTO transactions (product_id, barcode, transaction_type, quantity, price, total_value, notes)
                VALUES (?, ?, 'receiving', ?, ?, ?, ?)
            """, (pid, bc, qty, cost, qty * cost, f"Purchase order receiving #{order_id}"))

        # Lock order + invoice + receiving date
        cur.execute("""
            UPDATE purchase_orders
            SET status='received',
                invoice_number=?,
                invoice_date=?,
                date_received=?,
                expected_date=?
            WHERE id=?
        """, (invoice_number, invoice_date, date.today().isoformat(), data.get('expected_date'), order_id))

        conn.commit()
        conn.close()
        return jsonify({"success": True})

    # Other status changes (without receiving)
    cur.execute("""
        UPDATE purchase_orders
        SET status = ?, expected_date = ?
        WHERE id = ?
    """, (new_status, data.get('expected_date'), order_id))

    conn.commit()
    conn.close()
    return jsonify({"success": True, "message": "Status updated"})

# =========================================
# PRODUCTS BY SUPPLIER API (for orders)
# =========================================

@app.route("/api/products/supplier/<int:supplier_id>")
def api_get_products_by_supplier(supplier_id):
    """Get products by supplier"""
    try:
        conn = get_db_connection()
        products = conn.execute("""
            SELECT p.*, c.name as category_name, s.name as supplier_name 
            FROM products p 
            LEFT JOIN categories c ON p.category_id = c.id 
            LEFT JOIN suppliers s ON p.supplier_id = s.id 
            WHERE p.supplier_id = ?
            ORDER BY p.name
        """, (supplier_id,)).fetchall()
        conn.close()
        return jsonify([dict(product) for product in products])
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500

# =========================================
# SUPPLIERS API (for purchase_orders page)
# =========================================

@app.route("/api/suppliers", methods=["POST"])
def api_add_supplier():
    """Add new supplier"""
    try:
        data = request.get_json(force=True) or {}
        name = (data.get("name") or "").strip()
        phone = (data.get("phone") or "").strip()
        email = (data.get("email") or "").strip()

        if not name:
            return jsonify({"success": False, "message": "Supplier name is required"}), 400

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO suppliers (name, phone, email) VALUES (?, ?, ?)",
            (name, phone if phone else None, email if email else None),
        )
        conn.commit()
        new_id = cur.lastrowid
        conn.close()

        return jsonify({"success": True, "supplier": {"id": new_id, "name": name, "phone": phone, "email": email}}), 201

    except sqlite3.IntegrityError as e:
        # UNIQUE constraint on name
        return jsonify({"success": False, "message": "A supplier with this name already exists"}), 409
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500


@app.route("/api/suppliers", methods=["GET"])
def api_get_suppliers():
    """Return all suppliers"""
    try:
        conn = get_db_connection()
        suppliers = conn.execute("SELECT id, name FROM suppliers ORDER BY name").fetchall()
        conn.close()
        return jsonify([dict(s) for s in suppliers])
    except Exception as e:
        print("Error in api_get_suppliers:", e)
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500

# =========================================
# SEARCH PRODUCTS BY BARCODE API
# =========================================

@app.route("/api/products/search/<barcode>")
def api_search_product_by_barcode(barcode):
    """Search product by barcode"""
    try:
        conn = get_db_connection()
        product = conn.execute("""
            SELECT p.*, c.name as category_name, s.name as supplier_name 
            FROM products p 
            LEFT JOIN categories c ON p.category_id = c.id 
            LEFT JOIN suppliers s ON p.supplier_id = s.id 
            WHERE p.barcode LIKE ? OR p.barcode = ?
            ORDER BY p.name
            LIMIT 1
        """, (f"%{barcode}%", barcode)).fetchone()
        conn.close()

        if not product:
            return jsonify({"success": False, "message": "Product not found"}), 404

        return jsonify({"success": True, "product": dict(product)})
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500

# =========================================
# EXPORT API - IMPROVED REPORTS WITH SUPPLIER ID
# =========================================

@app.route("/api/export/products")
def api_export_products():
    """Export all products"""
    try:
        conn = get_db_connection()
        products = conn.execute("""
            SELECT p.*, c.name as category_name, s.name as supplier_name, s.id as supplier_id
            FROM products p 
            LEFT JOIN categories c ON p.category_id = c.id 
            LEFT JOIN suppliers s ON p.supplier_id = s.id 
            ORDER BY p.name
        """).fetchall()
        conn.close()

        # Create CSV
        output = StringIO()
        writer = csv.writer(output)

        # Header with supplier ID
        writer.writerow([
            'Supplier ID', 'Barcode', 'Name', 'Description', 'Category', 'Supplier',
            'Quantity', 'Min Stock', 'Cost Price', 'Retail Price'
        ])

        # Data
        for product in products:
            writer.writerow([
                product['supplier_id'] or '',
                product['barcode'],
                product['name'],
                product['description'] or '',
                product['category_name'] or '',
                product['supplier_name'] or '',
                product['quantity'],
                product['min_stock'],
                f"€{product['cost_price']:.2f}",
                f"€{product['retail_price']:.2f}"
            ])

        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={
                'X-Content-Type-Options': 'nosniff',
                'Cache-Control': 'no-store',
                "Content-disposition": "attachment; filename=products_export.csv"
            }
        )

    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500


@app.route("/api/export/supplier/<int:supplier_id>")
def api_export_supplier_products(supplier_id):
    """Export products by supplier (English headers)"""
    try:
        conn = get_db_connection()

        # Check supplier exists
        supplier = conn.execute("SELECT name FROM suppliers WHERE id = ?", (supplier_id,)).fetchone()
        if not supplier:
            conn.close()
            return jsonify({"success": False, "message": "Supplier not found"}), 404

        products = conn.execute("""
            SELECT p.*, c.name as category_name, s.name as supplier_name, s.id as supplier_id
            FROM products p 
            LEFT JOIN categories c ON p.category_id = c.id 
            LEFT JOIN suppliers s ON p.supplier_id = s.id 
            WHERE p.supplier_id = ?
            ORDER BY p.name
        """, (supplier_id,)).fetchall()
        conn.close()

        # Create CSV
        output = StringIO()
        writer = csv.writer(output)

        # English header with supplier code
        writer.writerow([
            'Supplier ID', 'Supplier Code', 'Barcode', 'Name', 'Description',
            'Category', 'Supplier', 'Quantity', 'Min Stock', 'Cost Price', 'Retail Price'
        ])

        # Data
        for product in products:
            writer.writerow([
                supplier_id,
                product['supplier_code'] or '',
                product['barcode'],
                product['name'],
                product['description'] or '',
                product['category_name'] or '',
                product['supplier_name'] or '',
                product['quantity'],
                product['min_stock'],
                f"{product['cost_price']:.2f}",
                f"{product['retail_price']:.2f}"
            ])

        filename = f"products_supplier_{supplier_id}_{supplier['name'].replace(' ', '_')}.csv"
        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={
                'X-Content-Type-Options': 'nosniff',
                'Cache-Control': 'no-store',
                "Content-disposition": f"attachment; filename={filename}"
            }
        )

    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500


@app.route("/api/export/low-stock")
def api_export_low_stock():
    """Export low stock products"""
    try:
        conn = get_db_connection()
        products = conn.execute("""
            SELECT p.*, c.name as category_name, s.name as supplier_name, s.id as supplier_id
            FROM products p 
            LEFT JOIN categories c ON p.category_id = c.id 
            LEFT JOIN suppliers s ON p.supplier_id = s.id 
            WHERE p.quantity <= p.min_stock
            ORDER BY p.quantity ASC
        """).fetchall()
        conn.close()

        # Create CSV
        output = StringIO()
        writer = csv.writer(output)

        # Header with supplier ID
        writer.writerow([
            'Supplier ID', 'Barcode', 'Name', 'Description', 'Category', 'Supplier',
            'Quantity', 'Min Stock', 'Cost Price', 'Retail Price', 'Status'
        ])

        # Data
        for product in products:
            status = "CRITICAL" if product['quantity'] == 0 else "LOW"
            writer.writerow([
                product['supplier_id'] or '',
                product['barcode'],
                product['name'],
                product['description'] or '',
                product['category_name'] or '',
                product['supplier_name'] or '',
                product['quantity'],
                product['min_stock'],
                f"€{product['cost_price']:.2f}",
                f"€{product['retail_price']:.2f}",
                status
            ])

        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={
                'X-Content-Type-Options': 'nosniff',
                'Cache-Control': 'no-store',
                "Content-disposition": "attachment; filename=low_stock_products.csv"
            }
        )

    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500


@app.route("/api/export/purchase-orders")
def api_export_purchase_orders():
    """Export all purchase orders with items"""
    try:
        conn = get_db_connection()
        orders = conn.execute("""
            SELECT po.*, s.name as supplier_name, s.phone, s.email, s.id as supplier_id
            FROM purchase_orders po
            LEFT JOIN suppliers s ON po.supplier_id = s.id
            ORDER BY po.order_date DESC
        """).fetchall()

        # Get items for each order
        orders_with_items = []
        for order in orders:
            items = conn.execute("""
                SELECT poi.*, p.name as actual_product_name
                FROM purchase_order_items poi
                LEFT JOIN products p ON poi.product_id = p.id
                WHERE poi.order_id = ?
            """, (order['id'],)).fetchall()
            orders_with_items.append({
                'order': dict(order),
                'items': [dict(item) for item in items]
            })

        conn.close()

        # Create CSV
        output = StringIO()
        writer = csv.writer(output)

        # Header with supplier ID
        writer.writerow([
            'Supplier ID', 'Order Number', 'Supplier', 'Order Date',
            'Expected Delivery', 'Status', 'Total Amount', 'Notes',
            'Product', 'Barcode', 'Ordered Quantity', 'Received Quantity',
            'Unit Price', 'Total Cost'
        ])

        # Data
        for order_data in orders_with_items:
            order = order_data['order']
            items = order_data['items']

            if items:
                for item in items:
                    writer.writerow([
                        order['supplier_id'] or '',
                        order['order_number'],
                        order['supplier_name'] or '',
                        order['order_date'],
                        order['expected_date'] or '',
                        order['status'],
                        f"€{order['total_amount']:.2f}",
                        order['notes'] or '',
                        item['product_name'] or item['actual_product_name'] or '',
                        item['barcode'] or '',
                        item['quantity_ordered'],
                        item['quantity_received'],
                        f"€{item['unit_cost']:.2f}",
                        f"€{item['total_cost']:.2f}"
                    ])
            else:
                # If no items, write only basic fields
                writer.writerow([
                    order['supplier_id'] or '',
                    order['order_number'],
                    order['supplier_name'] or '',
                    order['order_date'],
                    order['expected_date'] or '',
                    order['status'],
                    f"€{order['total_amount']:.2f}",
                    order['notes'] or '',
                    '', '', '', '', '', ''
                ])

        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={
                'X-Content-Type-Options': 'nosniff',
                'Cache-Control': 'no-store',
                "Content-disposition": "attachment; filename=purchase_orders_export.csv"
            }
        )

    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500


@app.route("/api/export/purchase-orders/supplier/<int:supplier_id>")
def api_export_purchase_orders_by_supplier(supplier_id):
    """Export purchase orders by supplier"""
    try:
        conn = get_db_connection()

        # Check supplier exists
        supplier = conn.execute("SELECT name FROM suppliers WHERE id = ?", (supplier_id,)).fetchone()
        if not supplier:
            conn.close()
            return jsonify({"success": False, "message": "Supplier not found"}), 404

        orders = conn.execute("""
            SELECT po.*, s.name as supplier_name, s.phone, s.email, s.id as supplier_id
            FROM purchase_orders po
            LEFT JOIN suppliers s ON po.supplier_id = s.id
            WHERE po.supplier_id = ?
            ORDER BY po.order_date DESC
        """, (supplier_id,)).fetchall()

        # Get items for each order
        orders_with_items = []
        for order in orders:
            items = conn.execute("""
                SELECT poi.*, p.name as actual_product_name
                FROM purchase_order_items poi
                LEFT JOIN products p ON poi.product_id = p.id
                WHERE poi.order_id = ?
            """, (order['id'],)).fetchall()
            orders_with_items.append({
                'order': dict(order),
                'items': [dict(item) for item in items]
            })

        conn.close()

        # Create CSV
        output = StringIO()
        writer = csv.writer(output)

        # Header with supplier ID
        writer.writerow([
            'Supplier ID', 'Order Number', 'Supplier', 'Order Date',
            'Expected Delivery', 'Status', 'Total Amount', 'Notes',
            'Product', 'Barcode', 'Ordered Quantity', 'Received Quantity',
            'Unit Price', 'Total Cost'
        ])

        # Data
        for order_data in orders_with_items:
            order = order_data['order']
            items = order_data['items']

            if items:
                for item in items:
                    writer.writerow([
                        order['supplier_id'] or '',
                        order['order_number'],
                        order['supplier_name'] or '',
                        order['order_date'],
                        order['expected_date'] or '',
                        order['status'],
                        f"€{order['total_amount']:.2f}",
                        order['notes'] or '',
                        item['product_name'] or item['actual_product_name'] or '',
                        item['barcode'] or '',
                        item['quantity_ordered'],
                        item['quantity_received'],
                        f"€{item['unit_cost']:.2f}",
                        f"€{item['total_cost']:.2f}"
                    ])
            else:
                writer.writerow([
                    order['supplier_id'] or '',
                    order['order_number'],
                    order['supplier_name'] or '',
                    order['order_date'],
                    order['expected_date'] or '',
                    order['status'],
                    f"€{order['total_amount']:.2f}",
                    order['notes'] or '',
                    '', '', '', '', '', ''
                ])

        filename = f"purchase_orders_{supplier['name'].replace(' ', '_')}.csv"
        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={
                'X-Content-Type-Options': 'nosniff',
                'Cache-Control': 'no-store',
                "Content-disposition": f"attachment; filename={filename}"
            }
        )

    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500


@app.route("/api/export/purchase-orders/status/<status>")
def api_export_purchase_orders_by_status(status):
    """Export purchase orders by status"""
    try:
        valid_statuses = ['pending', 'ordered', 'received', 'cancelled']
        if status not in valid_statuses:
            return jsonify({"success": False, "message": "Invalid status"}), 400

        conn = get_db_connection()
        orders = conn.execute("""
            SELECT po.*, s.name as supplier_name, s.phone, s.email, s.id as supplier_id
            FROM purchase_orders po
            LEFT JOIN suppliers s ON po.supplier_id = s.id
            WHERE po.status = ?
            ORDER BY po.order_date DESC
        """, (status,)).fetchall()

        # Get items for each order
        orders_with_items = []
        for order in orders:
            items = conn.execute("""
                SELECT poi.*, p.name as actual_product_name
                FROM purchase_order_items poi
                LEFT JOIN products p ON poi.product_id = p.id
                WHERE poi.order_id = ?
            """, (order['id'],)).fetchall()
            orders_with_items.append({
                'order': dict(order),
                'items': [dict(item) for item in items]
            })

        conn.close()

        # Create CSV
        output = StringIO()
        writer = csv.writer(output)

        # Header with supplier ID
        writer.writerow([
            'Supplier ID', 'Order Number', 'Supplier', 'Order Date',
            'Expected Delivery', 'Status', 'Total Amount', 'Notes',
            'Product', 'Barcode', 'Ordered Quantity', 'Received Quantity',
            'Unit Price', 'Total Cost'
        ])

        # Data
        for order_data in orders_with_items:
            order = order_data['order']
            items = order_data['items']

            if items:
                for item in items:
                    writer.writerow([
                        order['supplier_id'] or '',
                        order['order_number'],
                        order['supplier_name'] or '',
                        order['order_date'],
                        order['expected_date'] or '',
                        order['status'],
                        f"€{order['total_amount']:.2f}",
                        order['notes'] or '',
                        item['product_name'] or item['actual_product_name'] or '',
                        item['barcode'] or '',
                        item['quantity_ordered'],
                        item['quantity_received'],
                        f"€{item['unit_cost']:.2f}",
                        f"€{item['total_cost']:.2f}"
                    ])
            else:
                writer.writerow([
                    order['supplier_id'] or '',
                    order['order_number'],
                    order['supplier_name'] or '',
                    order['order_date'],
                    order['expected_date'] or '',
                    order['status'],
                    f"€{order['total_amount']:.2f}",
                    order['notes'] or '',
                    '', '', '', '', '', ''
                ])

        status_text = {
            'pending': 'pending',
            'ordered': 'ordered',
            'received': 'received',
            'cancelled': 'cancelled'
        }
        filename = f"purchase_orders_{status_text[status]}.csv"
        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={
                'X-Content-Type-Options': 'nosniff',
                'Cache-Control': 'no-store',
                "Content-disposition": f"attachment; filename={filename}"
            }
        )

    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {str(e)}"}), 500

# =========================================
# POS CONTROL
# =========================================

@app.route("/open_pos_window")
def open_pos_window():
    open(OPEN_FLAG, "w").close()
    return Response(status=204)


@app.route("/close_pos_window", methods=["POST", "GET"])
def close_pos_window():
    open(CLOSE_FLAG, "w").close()
    return Response(status=204)


@app.route("/api/export/purchase-orders/<int:order_id>", methods=["GET"])
def export_single_order(order_id):
    import csv, io, sqlite3
    from flask import Response

    conn = get_db_connection()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    order = cur.execute("""
        SELECT po.id, po.order_date, po.status, po.total_amount,
               po.invoice_number, po.invoice_date, po.date_received,
               s.name AS supplier_name
        FROM purchase_orders po
        LEFT JOIN suppliers s ON s.id = po.supplier_id
        WHERE po.id = ?
    """, (order_id,)).fetchone()

    items = cur.execute("""
        SELECT p.name, p.barcode, i.quantity_ordered, i.unit_cost, i.total_cost
        FROM purchase_order_items i
        LEFT JOIN products p ON p.id = i.product_id
        WHERE i.order_id = ?
    """, (order_id,)).fetchall()
    conn.close()

    if not order:
        return Response("Order not found", status=404)

    output = io.StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(["Order ID", order["id"]])
    writer.writerow(["Supplier", order["supplier_name"]])
    writer.writerow(["Order Date", order["order_date"]])
    writer.writerow(["Status", order["status"]])
    writer.writerow(["Total Amount", "%.2f" % (order["total_amount"] or 0)])
    writer.writerow(["Invoice Number", order["invoice_number"] or ""])
    writer.writerow(["Invoice Date", order["invoice_date"] or ""])
    writer.writerow(["Date Received", order["date_received"] or ""])
    writer.writerow([])
    writer.writerow(["Product", "Barcode", "Quantity", "Unit Cost", "Line Total"])
    for i in items:
        writer.writerow([
            i["name"], i["barcode"], i["quantity_ordered"],
            i["unit_cost"], i["total_cost"]
        ])
    csv_bytes = output.getvalue().encode("utf-8-sig")

    return Response(
        csv_bytes,
        mimetype="application/octet-stream",
        headers={
            'X-Content-Type-Options': 'nosniff',
            'Cache-Control': 'no-store',
            "Content-Disposition": f"attachment; filename=order_{order_id}.csv"
        }
    )

# =========================================
# WATCHER & SERVER
# =========================================

def watch_pos_trigger():
    global pos_window
    last_open = False
    last_close = False
    while True:
        try:
            should_open = os.path.exists(OPEN_FLAG)
            should_close = os.path.exists(CLOSE_FLAG)

            if should_open and not last_open:
                try:
                    pos_window = webview.create_window(
                        "ShopLite_POS POS",
                        "http://localhost:5000/pos",
                        width=1000,
                        height=700,
                        resizable=True,
                        js_api=BRIDGE_API
                    )
                finally:
                    if os.path.exists(OPEN_FLAG):
                        os.remove(OPEN_FLAG)

            if should_close and not last_close:
                try:
                    if pos_window is not None:
                        pos_window.destroy()
                        pos_window = None
                finally:
                    if os.path.exists(CLOSE_FLAG):
                        os.remove(CLOSE_FLAG)

            last_open = should_open
            last_close = should_close
        except Exception as e:
            print("Watcher error:", e)
        time.sleep(0.3)


def run_flask():
    try:
        from waitress import serve
        serve(app, host="127.0.0.1", port=5000)
    except Exception as e:
        print(f"Server error: {e}")
        app.run(host="127.0.0.1", port=5000, debug=False)

# =========================================
# Bridge API for pywebview
# =========================================

class BridgeAPI:
    """pywebview JS bridge: saves CSV reports locally and returns the file path"""

    def export_supplier_report(self, supplier_id: int):
        import csv
        from io import StringIO

        conn = get_db_connection()
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Check supplier exists
        supplier = cur.execute("SELECT name FROM suppliers WHERE id = ?", (supplier_id,)).fetchone()
        if not supplier:
            conn.close()
            return {"success": False, "message": "Supplier not found"}

        products = cur.execute("""
            SELECT p.*, c.name as category_name, s.name as supplier_name, s.id as supplier_id
            FROM products p 
            LEFT JOIN categories c ON p.category_id = c.id 
            LEFT JOIN suppliers s ON p.supplier_id = s.id 
            WHERE p.supplier_id = ?
            ORDER BY p.name
        """, (supplier_id,)).fetchall()
        conn.close()

        # Create CSV
        out = StringIO(newline="")
        w = csv.writer(out)

        # English header with supplier code
        w.writerow([
            'Supplier ID', 'Supplier Code', 'Barcode', 'Name', 'Description',
            'Category', 'Supplier', 'Quantity', 'Min Stock', 'Cost Price', 'Retail Price'
        ])

        # Data
        for product in products:
            w.writerow([
                supplier_id,
                product['supplier_code'] or '',
                product['barcode'],
                product['name'],
                product['description'] or '',
                product['category_name'] or '',
                product['supplier_name'] or '',
                product['quantity'],
                product['min_stock'],
                f"{product['cost_price']:.2f}",
                f"{product['retail_price']:.2f}"
            ])

        csv_text = out.getvalue()
        csv_bytes = csv_text.encode("utf-8-sig")  # BOM for Excel on Windows

        safe_name = supplier['name'].replace(' ', '_')
        fname = f"supplier_{supplier_id}_{safe_name}_report.csv"
        fpath = os.path.join(EXPORTS_DIR, fname)

        with open(fpath, "wb") as f:
            f.write(csv_bytes)

        return {"success": True, "path": fpath.replace("\\\\", "\\"), "message": f"Report saved to: {fpath}"}


class BridgeAPI:
    """pywebview JS bridge: saves CSV purchase order files locally and returns the file path"""

    def export_purchase_order_file(self, order_id: int):
        import csv
        from io import StringIO

        conn = get_db_connection()
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        cur.execute("""
            SELECT po.*, s.name AS supplier_name
            FROM purchase_orders po
            LEFT JOIN suppliers s ON s.id = po.supplier_id
            WHERE po.id = ?
        """, (order_id,))
        order = cur.fetchone()
        if not order:
            conn.close()
            return {"success": False, "message": "Purchase order not found"}

        cur.execute("""
            SELECT poi.*, p.name AS actual_product_name, p.supplier_code
            FROM purchase_order_items poi
            LEFT JOIN products p ON p.id = poi.product_id
            WHERE poi.order_id = ?
            ORDER BY poi.id
        """, (order_id,))
        items = cur.fetchall()
        conn.close()

        out = StringIO(newline="")
        w = csv.writer(out)
        w.writerow(["Purchase Order"])
        w.writerow(["Order ID", order["id"]])
        w.writerow(["Order Number", order["order_number"] or ""])
        w.writerow(["Supplier", order["supplier_name"] or ""])
        w.writerow(["Order Date", order["order_date"] or ""])
        w.writerow(["Status", order["status"] or ""])
        w.writerow(["Total Amount", f'{(order["total_amount"] or 0):.2f}'])
        w.writerow(["Notes", order["notes"] or ""])
        w.writerow([])

        w.writerow(["#", "Supplier Code", "Product ID", "Product Name", "Barcode",
                    "Qty Ordered", "Qty Received", "Unit Cost", "Line Total"])
        total = 0.0
        for i, it in enumerate(items, start=1):
            qtyo = it["quantity_ordered"] or 0
            qyr = it["quantity_received"] or 0
            cost = float(it["unit_cost"] or 0)
            line = float(it["total_cost"] or (qtyo * cost))
            total += line
            w.writerow([
                i,
                it["supplier_code"] or "",
                it["product_id"] or "",
                it["product_name"] or it["actual_product_name"] or "",
                it["barcode"] or "",
                qtyo,
                qyr,
                f"{cost:.2f}",
                f"{line:.2f}",
            ])
        w.writerow([])
        w.writerow(["Computed Total", f"{total:.2f}"])

        csv_text = out.getvalue()
        csv_bytes = csv_text.encode("utf-8-sig")  # BOM for Excel on Windows

        safe_num = order["order_number"] or f"PO{order['id']}"
        fname = f"purchase_order_{safe_num}.csv"
        fpath = os.path.join(EXPORTS_DIR, fname)
        with open(fpath, "wb") as f:
            f.write(csv_bytes)

        return {"success": True, "path": fpath.replace("\\\\", "\\")}

# MAIN
# =========================================

if __name__ == "__main__":
    print("Starting ShopLite_POS Inventory System")
    print("=" * 50)

    # Initialize DB
    init_database()

    # Bridge for webview (if available)
    try:
        BRIDGE_API = BridgeAPI()
    except Exception as e:
        print("Error creating BridgeAPI:", e)
        BRIDGE_API = None

    # Flask server in thread
    t = threading.Thread(target=run_flask, daemon=True)
    t.start()

    time.sleep(2)

    try:
        # Always open /license first
        window = webview.create_window(
            "ShopLite POS - License",
            "http://localhost:5000/license",
            width=1200,
            height=800,
            resizable=True,
            js_api=BRIDGE_API
        )

        threading.Thread(target=watch_pos_trigger, daemon=True).start()

        webview.start(debug=False)

    except Exception as e:
        print(f"Error starting application: {e}")
        print("Please make sure all required packages are installed:")
        print("pip install flask pywebview waitress")

# ================================
# Safe / Dynamic product update helpers
# ================================
from flask import request, jsonify
import sqlite3


def _to_int(x):
    try:
        return int(x) if x is not None and str(x).strip() != "" else None
    except Exception:
        return None


def _to_float(x):
    try:
        return float(x) if x is not None and str(x).strip() != "" else None
    except Exception:
        return None

# [REMOVED duplicate int PUT route]

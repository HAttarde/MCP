import os
import pyodbc
import psycopg2
from typing import Any

# MCP server
from fastmcp import FastMCP
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

def must_get(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Missing required env var {key}")
    return val

# ————————————————
# 1. MySQL Configuration
# ————————————————
MYSQL_HOST = must_get("MYSQL_HOST")
MYSQL_PORT = int(must_get("MYSQL_PORT"))
MYSQL_USER = must_get("MYSQL_USER")
MYSQL_PASSWORD = must_get("MYSQL_PASSWORD")
MYSQL_DB = must_get("MYSQL_DB")


def get_mysql_conn(db: str | None = MYSQL_DB):
    """If db is None we connect to the server only (needed to CREATE DATABASE)."""
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=db,
        ssl_disabled=False,  # Aiven requires TLS; keep this False
        autocommit=True,
    )


# ————————————————
# 2. PostgreSQL Configuration (Products)
# ————————————————
PG_HOST = must_get("PG_HOST")
PG_PORT = int(must_get("PG_PORT"))
PG_DB = os.getenv("PG_DB", "postgres")  # db name can default
PG_USER = must_get("PG_USER")
PG_PASS = must_get("PG_PASSWORD")


def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
        sslmode="require",  # Supabase enforces TLS
    )


# ————————————————
# 3. PostgreSQL Configuration (Sales)
# ————————————————
PG_SALES_HOST = must_get("PG_SALES_HOST")
PG_SALES_PORT = int(must_get("PG_SALES_PORT"))
PG_SALES_DB = os.getenv("PG_SALES_DB", "sales_db")
PG_SALES_USER = must_get("PG_SALES_USER")
PG_SALES_PASS = must_get("PG_SALES_PASSWORD")


def get_pg_sales_conn():
    return psycopg2.connect(
        host=PG_SALES_HOST,
        port=PG_SALES_PORT,
        dbname=PG_SALES_DB,
        user=PG_SALES_USER,
        password=PG_SALES_PASS,
        sslmode="require",
    )


# ————————————————
# 4. Instantiate your MCP server
# ————————————————
mcp = FastMCP("CRUDServer")


# ————————————————
# 5. Synchronous Setup: Create & seed tables
# ————————————————
def seed_databases():
    # ---------- MySQL (Customers) ----------
    root_cnx = get_mysql_conn(db=None)
    root_cur = root_cnx.cursor()
    root_cur.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DB}`;")
    root_cur.close()
    root_cnx.close()

    sql_cnx = get_mysql_conn()
    sql_cur = sql_cnx.cursor()

    # Disable foreign key checks temporarily
    sql_cur.execute("SET FOREIGN_KEY_CHECKS = 0;")

    # Drop tables in reverse dependency order (Sales first, then referenced tables)
    sql_cur.execute("DROP TABLE IF EXISTS Sales;")
    sql_cur.execute("DROP TABLE IF EXISTS ProductsCache;")
    sql_cur.execute("DROP TABLE IF EXISTS Customers;")

    # Re-enable foreign key checks
    sql_cur.execute("SET FOREIGN_KEY_CHECKS = 1;")

    # Create Customers table with FirstName and LastName
    sql_cur.execute("""
                    CREATE TABLE Customers
                    (
                        Id        INT AUTO_INCREMENT PRIMARY KEY,
                        FirstName VARCHAR(50) NOT NULL,
                        LastName  VARCHAR(50) NOT NULL,
                        Name      VARCHAR(100) NOT NULL,
                        Email     VARCHAR(100),
                        CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    """)

    # Insert sample customers with FirstName and LastName
    sql_cur.executemany(
        "INSERT INTO Customers (FirstName, LastName, Name, Email) VALUES (%s, %s, %s, %s)",
        [("Alice", "Johnson", "Alice Johnson", "alice@example.com"),
         ("Bob", "Smith", "Bob Smith", "bob@example.com"),
         ("Charlie", "Brown", "Charlie Brown", None)]  # Charlie has no email for null handling demo
    )

    # Create ProductsCache table (copy of PostgreSQL products for easier joins)
    sql_cur.execute("""
                    CREATE TABLE ProductsCache
                    (
                        id          INT PRIMARY KEY,
                        name        VARCHAR(100) NOT NULL,
                        price       DECIMAL(10, 4) NOT NULL,
                        description TEXT
                    );
                    """)

    # Insert sample products cache
    sql_cur.executemany(
        "INSERT INTO ProductsCache (id, name, price, description) VALUES (%s, %s, %s, %s)",
        [(1, "Widget", 9.99, "A standard widget."),
         (2, "Gadget", 14.99, "A useful gadget."),
         (3, "Tool", 24.99, None)]  # Tool has no description for null handling demo
    )

    # Create Sales table in MySQL with foreign key constraints
    sql_cur.execute("""
                    CREATE TABLE Sales
                    (
                        Id           INT AUTO_INCREMENT PRIMARY KEY,
                        customer_id  INT            NOT NULL,
                        product_id   INT            NOT NULL,
                        quantity     INT            NOT NULL DEFAULT 1,
                        unit_price   DECIMAL(10, 4) NOT NULL,
                        total_price  DECIMAL(10, 4) NOT NULL,
                        sale_date    TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (customer_id) REFERENCES Customers(Id) ON DELETE CASCADE,
                        FOREIGN KEY (product_id) REFERENCES ProductsCache(id) ON DELETE CASCADE
                    );
                    """)

    # Insert sample sales data
    sql_cur.executemany(
        "INSERT INTO Sales (customer_id, product_id, quantity, unit_price, total_price) VALUES (%s, %s, %s, %s, %s)",
        [(1, 1, 2, 9.99, 19.98),  # Alice bought 2 Widgets
         (2, 2, 1, 14.99, 14.99),  # Bob bought 1 Gadget
         (3, 3, 3, 24.99, 74.97)]  # Charlie bought 3 Tools
    )

    sql_cnx.close()

    # ---------- PostgreSQL (Products) ----------
    pg_cnxn = get_pg_conn()
    pg_cnxn.autocommit = True
    pg_cur = pg_cnxn.cursor()

    pg_cur.execute("DROP TABLE IF EXISTS products CASCADE;")
    pg_cur.execute("""
                   CREATE TABLE products
                   (
                       id          SERIAL PRIMARY KEY,
                       name        TEXT           NOT NULL,
                       price       NUMERIC(10, 4) NOT NULL,
                       description TEXT
                   );
                   """)

    pg_cur.executemany(
        "INSERT INTO products (name, price, description) VALUES (%s, %s, %s)",
        [("Widget", 9.99, "A standard widget."),
         ("Gadget", 14.99, "A useful gadget."),
         ("Tool", 24.99, "A handy tool.")]
    )
    pg_cnxn.close()

    # ---------- PostgreSQL Sales Database ----------
    sales_cnxn = get_pg_sales_conn()
    sales_cnxn.autocommit = True
    sales_cur = sales_cnxn.cursor()

    sales_cur.execute("DROP TABLE IF EXISTS sales;")
    sales_cur.execute("""
                      CREATE TABLE sales
                      (
                          id           SERIAL PRIMARY KEY,
                          customer_id  INT            NOT NULL,
                          product_id   INT            NOT NULL,
                          quantity     INT            NOT NULL DEFAULT 1,
                          unit_price   NUMERIC(10, 4) NOT NULL,
                          total_amount NUMERIC(10, 4) NOT NULL,
                          sale_date    TIMESTAMP               DEFAULT CURRENT_TIMESTAMP
                      );
                      """)

    # Sample sales data
    sales_cur.executemany(
        "INSERT INTO sales (customer_id, product_id, quantity, unit_price, total_amount) VALUES (%s, %s, %s, %s, %s)",
        [(1, 1, 2, 9.99, 19.98),  # Alice bought 2 Widgets
         (2, 2, 1, 14.99, 14.99),  # Bob bought 1 Gadget
         (3, 3, 3, 24.99, 74.97)]  # Charlie bought 3 Tools
    )
    sales_cnxn.close()


# ————————————————
# 6. Helper Functions for Cross-Database Queries and Name Resolution
# ————————————————
def get_customer_name(customer_id: int) -> str:
    """Fetch customer name from MySQL database"""
    try:
        mysql_cnxn = get_mysql_conn()
        mysql_cur = mysql_cnxn.cursor()
        mysql_cur.execute("SELECT Name FROM Customers WHERE Id = %s", (customer_id,))
        result = mysql_cur.fetchone()
        mysql_cnxn.close()
        return result[0] if result else f"Unknown Customer ({customer_id})"
    except Exception:
        return f"Unknown Customer ({customer_id})"


def get_product_details(product_id: int) -> dict:
    """Fetch product name and price from PostgreSQL products database"""
    try:
        pg_cnxn = get_pg_conn()
        pg_cur = pg_cnxn.cursor()
        pg_cur.execute("SELECT name, price FROM products WHERE id = %s", (product_id,))
        result = pg_cur.fetchone()
        pg_cnxn.close()
        if result:
            return {"name": result[0], "price": float(result[1])}
        else:
            return {"name": f"Unknown Product ({product_id})", "price": 0.0}
    except Exception:
        return {"name": f"Unknown Product ({product_id})", "price": 0.0}


def validate_customer_exists(customer_id: int) -> bool:
    """Check if customer exists in MySQL database"""
    try:
        mysql_cnxn = get_mysql_conn()
        mysql_cur = mysql_cnxn.cursor()
        mysql_cur.execute("SELECT COUNT(*) FROM Customers WHERE Id = %s", (customer_id,))
        result = mysql_cur.fetchone()
        mysql_cnxn.close()
        return result[0] > 0 if result else False
    except Exception:
        return False


def validate_product_exists(product_id: int) -> bool:
    """Check if product exists in PostgreSQL products database"""
    try:
        pg_cnxn = get_pg_conn()
        pg_cur = pg_cnxn.cursor()
        pg_cur.execute("SELECT COUNT(*) FROM products WHERE id = %s", (product_id,))
        result = pg_cur.fetchone()
        pg_cnxn.close()
        return result[0] > 0 if result else False
    except Exception:
        return False


def find_customer_by_name_enhanced(name: str) -> dict:
    """Enhanced customer search that handles multiple matches intelligently"""
    try:
        mysql_cnxn = get_mysql_conn()
        mysql_cur = mysql_cnxn.cursor()
        
        # Search strategy with priorities:
        # 1. Exact full name match (case insensitive)
        # 2. Exact first name or last name match  
        # 3. Partial name matches
        
        all_matches = []
        
        # 1. Try exact full name match (case insensitive)
        mysql_cur.execute("SELECT Id, Name, Email FROM Customers WHERE LOWER(Name) = LOWER(%s)", (name,))
        exact_matches = mysql_cur.fetchall()
        
        if exact_matches:
            # If only one exact match, return it immediately
            if len(exact_matches) == 1:
                mysql_cnxn.close()
                return {
                    "found": True,
                    "multiple_matches": False,
                    "customer_id": exact_matches[0][0],
                    "customer_name": exact_matches[0][1],
                    "customer_email": exact_matches[0][2]
                }
            else:
                # Multiple exact matches (rare but possible)
                for match in exact_matches:
                    all_matches.append({
                        "id": match[0],
                        "name": match[1], 
                        "email": match[2],
                        "match_type": "exact_full_name"
                    })
        
        # 2. Try exact first name or last name match if no exact full name match
        if not exact_matches:
            mysql_cur.execute("""
                SELECT Id, Name, Email FROM Customers 
                WHERE LOWER(FirstName) = LOWER(%s) 
                   OR LOWER(LastName) = LOWER(%s)
            """, (name, name))
            name_matches = mysql_cur.fetchall()
            
            for match in name_matches:
                all_matches.append({
                    "id": match[0],
                    "name": match[1],
                    "email": match[2], 
                    "match_type": "exact_name_part"
                })
        
        # 3. Try partial matches only if no exact matches found
        if not all_matches:
            mysql_cur.execute("""
                SELECT Id, Name, Email FROM Customers 
                WHERE LOWER(Name) LIKE LOWER(%s)
                   OR LOWER(FirstName) LIKE LOWER(%s)
                   OR LOWER(LastName) LIKE LOWER(%s)
            """, (f"%{name}%", f"%{name}%", f"%{name}%"))
            partial_matches = mysql_cur.fetchall()
            
            for match in partial_matches:
                all_matches.append({
                    "id": match[0],
                    "name": match[1],
                    "email": match[2],
                    "match_type": "partial"
                })
        
        mysql_cnxn.close()
        
        # Handle results
        if not all_matches:
            return {"found": False, "error": f"Customer '{name}' not found"}
        
        if len(all_matches) == 1:
            match = all_matches[0]
            return {
                "found": True,
                "multiple_matches": False,
                "customer_id": match["id"],
                "customer_name": match["name"],
                "customer_email": match["email"]
            }
        
        # Multiple matches found
        return {
            "found": True,
            "multiple_matches": True,
            "matches": all_matches,
            "error": f"Multiple customers found matching '{name}'"
        }
        
    except Exception as e:
        return {"found": False, "error": f"Database error: {str(e)}"}

def find_product_by_name(name: str) -> dict:
    """Find product by name (supports partial matching)"""
    try:
        pg_cnxn = get_pg_conn()
        pg_cur = pg_cnxn.cursor()
        
        # Try exact match first
        pg_cur.execute("SELECT id, name FROM products WHERE name = %s", (name,))
        result = pg_cur.fetchone()
        
        if result:
            pg_cnxn.close()
            return {"id": result[0], "name": result[1], "found": True}
        
        # Try case-insensitive exact match
        pg_cur.execute("SELECT id, name FROM products WHERE LOWER(name) = LOWER(%s)", (name,))
        result = pg_cur.fetchone()
        
        if result:
            pg_cnxn.close()
            return {"id": result[0], "name": result[1], "found": True}
        
        # Try partial match
        pg_cur.execute("SELECT id, name FROM products WHERE LOWER(name) LIKE LOWER(%s)", (f"%{name}%",))
        result = pg_cur.fetchone()
        
        if result:
            pg_cnxn.close()
            return {"id": result[0], "name": result[1], "found": True}
        
        pg_cnxn.close()
        return {"found": False, "error": f"Product '{name}' not found"}
        
    except Exception as e:
        return {"found": False, "error": f"Database error: {str(e)}"}


# ————————————————
# 7. Enhanced MySQL CRUD Tool (Customers) with Smart Name Resolution
# ————————————————
# Fixed sqlserver_crud function with proper variable initialization
@mcp.tool()
async def sqlserver_crud(
        operation: str,
        name: str = None,
        email: str = None,
        limit: int = 10,
        customer_id: int = None,
        new_email: str = None,
        table_name: str = None,
) -> Any:
    cnxn = get_mysql_conn()
    cur = cnxn.cursor()

    if operation == "create":
        if not name or not email:
            cnxn.close()
            return {"sql": None, "result": "❌ 'name' and 'email' required for create."}

        # NEW LOGIC: Check if customer with this name already exists
        # Search for existing customers with the same first name or full name
        search_name = name.strip()
        
        # Check for exact name matches or first name matches
        cur.execute("""
            SELECT Id, Name, Email FROM Customers 
            WHERE LOWER(Name) = LOWER(%s) 
               OR LOWER(FirstName) = LOWER(%s)
               OR LOWER(Name) LIKE LOWER(%s)
        """, (search_name, search_name, f"%{search_name}%"))
        
        existing_customers = cur.fetchall()
        
        if existing_customers:
            # Filter out customers who already have emails
            customers_without_email = [c for c in existing_customers if not c[2]]  # c[2] is Email
            customers_with_email = [c for c in existing_customers if c[2]]  # c[2] is Email
            
            if len(existing_customers) == 1:
                # Only one customer found
                existing_customer = existing_customers[0]
                if existing_customer[2]:  # Already has email
                    cnxn.close()
                    return {"sql": None, "result": f"ℹ️ Customer '{existing_customer[1]}' already has email '{existing_customer[2]}'. If you want to update it, please specify the full name."}
                else:
                    # Customer exists but no email, update with the email
                    sql_query = "UPDATE Customers SET Email = %s WHERE Id = %s"
                    cur.execute(sql_query, (email, existing_customer[0]))
                    cnxn.commit()
                    cnxn.close()
                    return {"sql": sql_query, "result": f"✅ Email '{email}' added to existing customer '{existing_customer[1]}'."}
            
            elif len(existing_customers) > 1:
                # Multiple customers found - ask for clarification
                customer_list = []
                for c in existing_customers:
                    email_status = f"(has email: {c[2]})" if c[2] else "(no email)"
                    customer_list.append(f"- {c[1]} {email_status}")
                
                customer_details = "\n".join(customer_list)
                cnxn.close()
                return {"sql": None, "result": f"❓ Multiple customers found with name '{search_name}':\n{customer_details}\n\nPlease specify the full name (first and last name) to identify which customer you want to add the email to, or use a different name if you want to create a new customer."}

        # No existing customer found, create new customer
        # Split name into first and last name (simple split)
        name_parts = name.split(' ', 1)
        first_name = name_parts[0]
        last_name = name_parts[1] if len(name_parts) > 1 else ""

        sql_query = "INSERT INTO Customers (FirstName, LastName, Name, Email) VALUES (%s, %s, %s, %s)"
        cur.execute(sql_query, (first_name, last_name, name, email))
        cnxn.commit()
        cnxn.close()
        return {"sql": sql_query, "result": f"✅ New customer '{name}' created with email '{email}'."}
    elif operation == "read":
        # Handle filtering by name if provided
        if name:
            sql_query = """
                        SELECT Id, FirstName, LastName, Name, Email, CreatedAt
                        FROM Customers
                        WHERE LOWER(Name) LIKE LOWER(%s) 
                           OR LOWER(FirstName) LIKE LOWER(%s)
                           OR LOWER(LastName) LIKE LOWER(%s)
                        ORDER BY Id ASC
                        LIMIT %s
                        """
            cur.execute(sql_query, (f"%{name}%", f"%{name}%", f"%{name}%", limit))
        else:
            sql_query = """
                        SELECT Id, FirstName, LastName, Name, Email, CreatedAt
                        FROM Customers
                        ORDER BY Id ASC
                        LIMIT %s
                        """
            cur.execute(sql_query, (limit,))
        
        rows = cur.fetchall()
        result = [
            {
                "Id": r[0],
                "FirstName": r[1],
                "LastName": r[2],
                "Name": r[3],
                "Email": r[4],
                "CreatedAt": r[5].isoformat()
            }
            for r in rows
        ]
        cnxn.close()
        return {"sql": sql_query, "result": result}

    elif operation == "update":
        # Initialize customer_name variable
        customer_name = None
        
        # Enhanced update: resolve customer_id from name if not provided
        if not customer_id and name:
            # Use the original find_customer_by_name function if enhanced version not available
            try:
                customer_info = find_customer_by_name(name)
                if not customer_info["found"]:
                    cnxn.close()
                    return {"sql": None, "result": f"❌ {customer_info['error']}"}
                customer_id = customer_info["id"]
                customer_name = customer_info["name"]
            except Exception as search_error:
                # Fallback to direct database search
                cur.execute("""
                    SELECT Id, Name FROM Customers 
                    WHERE LOWER(Name) = LOWER(%s) 
                       OR LOWER(FirstName) = LOWER(%s)
                       OR LOWER(LastName) = LOWER(%s)
                    LIMIT 1
                """, (name, name, name))
                result = cur.fetchone()
                
                if result:
                    customer_id = result[0]
                    customer_name = result[1]
                else:
                    cnxn.close()
                    return {"sql": None, "result": f"❌ Customer '{name}' not found"}
        
        if not customer_id or not new_email:
            cnxn.close()
            return {"sql": None, "result": "❌ 'customer_id' (or 'name') and 'new_email' required for update."}

        # Check if customer already has this email
        cur.execute("SELECT Name, Email FROM Customers WHERE Id = %s", (customer_id,))
        existing_customer = cur.fetchone()
        
        if not existing_customer:
            cnxn.close()
            return {"sql": None, "result": f"❌ Customer with ID {customer_id} not found."}
        
        # Set customer_name if not already set
        if not customer_name:
            customer_name = existing_customer[0]
        
        if existing_customer[1] == new_email:
            cnxn.close()
            return {"sql": None, "result": f"ℹ️ Customer '{customer_name}' already has email '{new_email}'."}

        sql_query = "UPDATE Customers SET Email = %s WHERE Id = %s"
        cur.execute(sql_query, (new_email, customer_id))
        cnxn.commit()
        cnxn.close()
        
        return {"sql": sql_query, "result": f"✅ Customer '{customer_name}' email updated to '{new_email}'."}

    elif operation == "delete":
        # Initialize customer_name variable
        customer_name = None
        
        # Enhanced delete: resolve customer_id from name if not provided
        if not customer_id and name:
            try:
                customer_info = find_customer_by_name(name)
                if not customer_info["found"]:
                    cnxn.close()
                    return {"sql": None, "result": f"❌ {customer_info['error']}"}
                customer_id = customer_info["id"]
                customer_name = customer_info["name"]
            except Exception as search_error:
                # Fallback to direct database search
                cur.execute("""
                    SELECT Id, Name FROM Customers 
                    WHERE LOWER(Name) = LOWER(%s) 
                       OR LOWER(FirstName) = LOWER(%s)
                       OR LOWER(LastName) = LOWER(%s)
                    LIMIT 1
                """, (name, name, name))
                result = cur.fetchone()
                
                if result:
                    customer_id = result[0]
                    customer_name = result[1]
                else:
                    cnxn.close()
                    return {"sql": None, "result": f"❌ Customer '{name}' not found"}
        elif customer_id:
            # Get customer name for response
            cur.execute("SELECT Name FROM Customers WHERE Id = %s", (customer_id,))
            result = cur.fetchone()
            customer_name = result[0] if result else f"Customer {customer_id}"
        else:
            cnxn.close()
            return {"sql": None, "result": "❌ 'customer_id' or 'name' required for delete."}

        sql_query = "DELETE FROM Customers WHERE Id = %s"
        cur.execute(sql_query, (customer_id,))
        cnxn.commit()
        cnxn.close()
        return {"sql": sql_query, "result": f"✅ Customer '{customer_name}' deleted."}

    elif operation == "describe":
        table = table_name or "Customers"
        sql_query = f"DESCRIBE {table}"
        cur.execute(sql_query)
        rows = cur.fetchall()
        result = [
            {
                "Field": r[0],
                "Type": r[1],
                "Null": r[2],
                "Key": r[3],
                "Default": r[4],
                "Extra": r[5]
            }
            for r in rows
        ]
        cnxn.close()
        return {"sql": sql_query, "result": result}

    else:
        cnxn.close()
        return {"sql": None, "result": f"❌ Unknown operation '{operation}'."}


# ————————————————
# 8. Enhanced PostgreSQL CRUD Tool (Products) with Smart Name Resolution
# ————————————————
@mcp.tool()
async def postgresql_crud(
        operation: str,
        name: str = None,
        price: float = None,
        description: str = None,
        limit: int = 10,
        product_id: int = None,
        new_price: float = None,
        table_name: str = None,
) -> Any:
    cnxn = get_pg_conn()
    cur = cnxn.cursor()

    if operation == "create":
        if not name or price is None:
            cnxn.close()
            return {"sql": None, "result": "❌ 'name' and 'price' required for create."}
        sql_query = "INSERT INTO products (name, price, description) VALUES (%s, %s, %s)"
        cur.execute(sql_query, (name, price, description))
        cnxn.commit()
        result = f"✅ Product '{name}' added with price ${price:.2f}."
        cnxn.close()
        return {"sql": sql_query, "result": result}

    elif operation == "read":
        # Handle filtering by name if provided
        if name:
            sql_query = """
                        SELECT id, name, price, description
                        FROM products
                        WHERE LOWER(name) LIKE LOWER(%s)
                        ORDER BY id ASC
                        LIMIT %s
                        """
            cur.execute(sql_query, (f"%{name}%", limit))
        else:
            sql_query = """
                        SELECT id, name, price, description
                        FROM products
                        ORDER BY id ASC
                        LIMIT %s
                        """
            cur.execute(sql_query, (limit,))
        
        rows = cur.fetchall()
        result = [
            {"id": r[0], "name": r[1], "price": float(r[2]), "description": r[3] or ""}
            for r in rows
        ]
        cnxn.close()
        return {"sql": sql_query, "result": result}

    elif operation == "update":
        # Enhanced update: resolve product_id from name if not provided
        if not product_id and name:
            product_info = find_product_by_name(name)
            if not product_info["found"]:
                cnxn.close()
                return {"sql": None, "result": f"❌ {product_info['error']}"}
            product_id = product_info["id"]
        
        if not product_id or new_price is None:
            cnxn.close()
            return {"sql": None, "result": "❌ 'product_id' (or 'name') and 'new_price' required for update."}
        
        sql_query = "UPDATE products SET price = %s WHERE id = %s"
        cur.execute(sql_query, (new_price, product_id))
        cnxn.commit()
        
        # Get updated product name for response
        cur.execute("SELECT name FROM products WHERE id = %s", (product_id,))
        product_name = cur.fetchone()
        product_name = product_name[0] if product_name else f"Product {product_id}"
        
        cnxn.close()
        return {"sql": sql_query, "result": f"✅ Product '{product_name}' price updated to ${new_price:.2f}."}

    elif operation == "delete":
        # Enhanced delete: resolve product_id from name if not provided
        if not product_id and name:
            product_info = find_product_by_name(name)
            if not product_info["found"]:
                cnxn.close()
                return {"sql": None, "result": f"❌ {product_info['error']}"}
            product_id = product_info["id"]
            product_name = product_info["name"]
        elif product_id:
            # Get product name for response
            cur.execute("SELECT name FROM products WHERE id = %s", (product_id,))
            result = cur.fetchone()
            product_name = result[0] if result else f"Product {product_id}"
        else:
            cnxn.close()
            return {"sql": None, "result": "❌ 'product_id' or 'name' required for delete."}

        sql_query = "DELETE FROM products WHERE id = %s"
        cur.execute(sql_query, (product_id,))
        cnxn.commit()
        cnxn.close()
        return {"sql": sql_query, "result": f"✅ Product '{product_name}' deleted."}

    elif operation == "describe":
        table = table_name or "products"
        sql_query = f"""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_name = %s
                    ORDER BY ordinal_position
                    """
        cur.execute(sql_query, (table,))
        rows = cur.fetchall()
        result = [
            {
                "Column": r[0],
                "Type": r[1],
                "Nullable": r[2],
                "Default": r[3]
            }
            for r in rows
        ]
        cnxn.close()
        return {"sql": sql_query, "result": result}

    else:
        cnxn.close()
        return {"sql": None, "result": f"❌ Unknown operation '{operation}'."}


# ————————————————
# 9. Sales CRUD Tool with Display Formatting Features (Unchanged)
# ————————————————
# Fixed sales_crud function with proper column selection
# Fixed sales_crud function with proper WHERE clause and column selection
# Fixed sales_crud function with proper WHERE clause and column selection
@mcp.tool()
async def sales_crud(
        operation: str,
        customer_id: int = None,
        product_id: int = None,
        quantity: int = 1,
        unit_price: float = None,
        total_amount: float = None,
        sale_id: int = None,
        new_quantity: int = None,
        table_name: str = None,
        display_format: str = None,  # Display formatting parameter
        customer_name: str = None,
        product_name: str = None,
        email: str = None,
        total_price: float = None,
        # Enhanced parameters for column selection and filtering
        columns: str = None,  # Comma-separated list of columns to display
        where_clause: str = None,  # WHERE conditions
        filter_conditions: dict = None,  # Alternative: structured filters
        limit: int = None  # Row limit
) -> Any:
    # For PostgreSQL sales operations (create, update, delete)
    if operation in ["create", "update", "delete"]:
        sales_cnxn = get_pg_sales_conn()
        sales_cur = sales_cnxn.cursor()

        if operation == "create":
            if not customer_id or not product_id:
                sales_cnxn.close()
                return {"sql": None, "result": "❌ 'customer_id' and 'product_id' required for create."}

            # Validate customer exists
            if not validate_customer_exists(customer_id):
                sales_cnxn.close()
                return {"sql": None, "result": f"❌ Customer with ID {customer_id} not found."}

            # Validate product exists and get price
            if not validate_product_exists(product_id):
                sales_cnxn.close()
                return {"sql": None, "result": f"❌ Product with ID {product_id} not found."}

            # Get product price if not provided
            if not unit_price:
                product_details = get_product_details(product_id)
                unit_price = product_details["price"]

            if not total_amount:
                total_amount = unit_price * quantity

            sql_query = """
                        INSERT INTO sales (customer_id, product_id, quantity, unit_price, total_amount)
                        VALUES (%s, %s, %s, %s, %s)
                        """
            sales_cur.execute(sql_query, (customer_id, product_id, quantity, unit_price, total_amount))
            sales_cnxn.commit()

            # Get customer and product names for response
            customer_name = get_customer_name(customer_id)
            product_details = get_product_details(product_id)

            result = f"✅ Sale created: {customer_name} bought {quantity} {product_details['name']}(s) for ${total_amount:.2f}"
            sales_cnxn.close()
            return {"sql": sql_query, "result": result}

        elif operation == "update":
            if not sale_id or new_quantity is None:
                sales_cnxn.close()
                return {"sql": None, "result": "❌ 'sale_id' and 'new_quantity' required for update."}

            # Recalculate total amount
            sql_query = """
                        UPDATE sales
                        SET quantity     = %s,
                            total_amount = unit_price * %s
                        WHERE id = %s
                        """
            sales_cur.execute(sql_query, (new_quantity, new_quantity, sale_id))
            sales_cnxn.commit()
            result = f"✅ Sale id={sale_id} updated to quantity {new_quantity}."
            sales_cnxn.close()
            return {"sql": sql_query, "result": result}

        elif operation == "delete":
            if not sale_id:
                sales_cnxn.close()
                return {"sql": None, "result": "❌ 'sale_id' required for delete."}

            sql_query = "DELETE FROM sales WHERE id = %s"
            sales_cur.execute(sql_query, (sale_id,))
            sales_cnxn.commit()
            result = f"✅ Sale id={sale_id} deleted."
            sales_cnxn.close()
            return {"sql": sql_query, "result": result}

    # Enhanced READ operation with FIXED column selection AND WHERE clause filtering
    elif operation == "read":
        mysql_cnxn = get_mysql_conn()
        mysql_cur = mysql_cnxn.cursor()

        # Fixed column mappings - standardized naming
        available_columns = {
            "sale_id": "s.Id",
            "first_name": "c.FirstName", 
            "last_name": "c.LastName",
            "customer_name": "c.Name",  # Use the Name field which has full name
            "product_name": "p.name",
            "product_description": "p.description",
            "quantity": "s.quantity",
            "unit_price": "s.unit_price",
            "total_price": "s.total_price",
            "amount": "s.total_price",  # Alias for total_price
            "sale_date": "s.sale_date",
            "date": "s.sale_date",  # Alias for sale_date
            "customer_email": "c.Email",
            "email": "c.Email"  # Alias for customer_email
        }

        # FIXED: Process column selection with better parsing
        selected_columns = []
        column_aliases = []
        
        print(f"DEBUG: Raw columns parameter: '{columns}'")
        
        if columns and columns.strip():
            # Clean and split the columns string
            columns_clean = columns.strip()
            
            # Handle different input patterns
            if "," in columns_clean:
                # Comma-separated list
                requested_cols = [col.strip().lower().replace(" ", "_") for col in columns_clean.split(",") if col.strip()]
            else:
                # Space-separated or single column
                requested_cols = [col.strip().lower().replace(" ", "_") for col in columns_clean.split() if col.strip()]
            
            print(f"DEBUG: Requested columns after parsing: {requested_cols}")
            
            # Build SELECT clause based on requested columns
            for col in requested_cols:
                matched = False
                # Try exact match first
                if col in available_columns:
                    selected_columns.append(available_columns[col])
                    column_aliases.append(col)
                    matched = True
                    print(f"DEBUG: Exact match found for '{col}': {available_columns[col]}")
                else:
                    # Try fuzzy matching for common variations
                    for avail_col, db_col in available_columns.items():
                        if (col in avail_col or avail_col in col or 
                            col.replace("_", "") in avail_col.replace("_", "") or
                            avail_col.replace("_", "") in col.replace("_", "")):
                            selected_columns.append(db_col)
                            column_aliases.append(avail_col)
                            matched = True
                            print(f"DEBUG: Fuzzy match found for '{col}' -> '{avail_col}': {db_col}")
                            break
                
                if not matched:
                    print(f"DEBUG: No match found for column '{col}'. Skipping...")

        # If no valid columns found or no columns specified, use default key columns
        if not selected_columns:
            print("DEBUG: Using default key columns")
            selected_columns = [
                "s.Id", "c.Name", "p.name", "s.quantity", "s.unit_price", "s.total_price", "s.sale_date", "c.Email"
            ]
            column_aliases = [
                "sale_id", "customer_name", "product_name", "quantity", "unit_price", "total_price", "sale_date", "email"
            ]

        print(f"DEBUG: Final selected columns: {selected_columns}")
        print(f"DEBUG: Final column aliases: {column_aliases}")

        # Build dynamic SQL query
        select_clause = ", ".join([f"{col} AS {alias}" for col, alias in zip(selected_columns, column_aliases)])
        
        # Base query
        base_sql = f"""
        SELECT  {select_clause}
        FROM    Sales          s
        JOIN    Customers      c ON c.Id = s.customer_id
        JOIN    ProductsCache  p ON p.id = s.product_id
        """
        
        # COMPLETELY REWRITTEN WHERE clause processing
        where_sql = ""
        query_params = []
        
        if where_clause and where_clause.strip():
            print(f"DEBUG: Processing WHERE clause: '{where_clause}'")
            
            import re
            
            # Clean the input
            clause = where_clause.strip().lower()
            
            # Enhanced pattern matching for various query formats
            where_conditions = []
            
            # Pattern 1: "total_price > 50", "total price exceed 50", "total price exceeds $50"
            price_patterns = [
                r'total[_\s]*price[_\s]*(>|>=|exceed[s]?|above|greater\s+than|more\s+than)\s*\$?(\d+(?:\.\d+)?)',
                r'(>|>=|exceed[s]?|above|greater\s+than|more\s+than)\s*\$?(\d+(?:\.\d+)?)\s*total[_\s]*price',
                r'total[_\s]*price[_\s]*(<|<=|below|less\s+than|under)\s*\$?(\d+(?:\.\d+)?)',
                r'total[_\s]*price[_\s]*(=|equals?|is)\s*\$?(\d+(?:\.\d+)?)'
            ]
            
            for pattern in price_patterns:
                match = re.search(pattern, clause)
                if match:
                    if len(match.groups()) == 2:
                        operator_text, value = match.groups()
                        # Map operator text to SQL operator
                        if any(word in operator_text for word in ['exceed', 'above', 'greater', 'more', '>']):
                            operator = '>'
                        elif any(word in operator_text for word in ['below', 'less', 'under', '<']):
                            operator = '<'
                        elif any(word in operator_text for word in ['equal', 'is', '=']):
                            operator = '='
                        else:
                            operator = '>'  # default
                        
                        where_conditions.append(f"s.total_price {operator} %s")
                        query_params.append(float(value))
                        print(f"DEBUG: Found price condition: s.total_price {operator} {value}")
                        break
            
            # Pattern 2: Quantity conditions
            quantity_patterns = [
                r'quantity[_\s]*(>|>=|greater\s+than|more\s+than|above)\s*(\d+)',
                r'quantity[_\s]*(<|<=|less\s+than|below|under)\s*(\d+)',
                r'quantity[_\s]*(=|equals?|is)\s*(\d+)'
            ]
            
            for pattern in quantity_patterns:
                match = re.search(pattern, clause)
                if match:
                    operator_text, value = match.groups()
                    if any(symbol in operator_text for symbol in ['>', 'greater', 'more', 'above']):
                        operator = '>'
                    elif any(symbol in operator_text for symbol in ['<', 'less', 'below', 'under']):
                        operator = '<'
                    else:
                        operator = '='
                    
                    where_conditions.append(f"s.quantity {operator} %s")
                    query_params.append(int(value))
                    print(f"DEBUG: Found quantity condition: s.quantity {operator} {value}")
                    break
            
            # Pattern 3: Customer name conditions
            customer_patterns = [
                r'customer[_\s]*name[_\s]*like[_\s]*["\']([^"\']+)["\']',
                r'customer[_\s]*name[_\s]*=[_\s]*["\']([^"\']+)["\']',
                r'customer[_\s]*=[_\s]*["\']([^"\']+)["\']',
                r'customer[_\s]*name[_\s]*([a-zA-Z\s]+?)(?:\s|$)'
            ]
            
            for pattern in customer_patterns:
                match = re.search(pattern, clause)
                if match:
                    name_value = match.group(1).strip()
                    if 'like' in clause:
                        where_conditions.append("c.Name LIKE %s")
                        query_params.append(f"%{name_value}%")
                    else:
                        where_conditions.append("c.Name = %s")
                        query_params.append(name_value)
                    print(f"DEBUG: Found customer condition: {name_value}")
                    break
            
            # Pattern 4: Product name conditions
            product_patterns = [
                r'product[_\s]*name[_\s]*like[_\s]*["\']([^"\']+)["\']',
                r'product[_\s]*name[_\s]*=[_\s]*["\']([^"\']+)["\']',
                r'product[_\s]*=[_\s]*["\']([^"\']+)["\']'
            ]
            
            for pattern in product_patterns:
                match = re.search(pattern, clause)
                if match:
                    product_value = match.group(1).strip()
                    if 'like' in clause:
                        where_conditions.append("p.name LIKE %s")
                        query_params.append(f"%{product_value}%")
                    else:
                        where_conditions.append("p.name = %s")
                        query_params.append(product_value)
                    print(f"DEBUG: Found product condition: {product_value}")
                    break
            
            # If no specific patterns matched, try a generic approach
            if not where_conditions:
                # Look for any number that might be a price threshold
                number_match = re.search(r'\$?(\d+(?:\.\d+)?)', clause)
                if number_match:
                    value = float(number_match.group(1))
                    # Default to total_price filter if no specific field mentioned
                    if any(word in clause for word in ['exceed', 'above', 'greater', 'more']):
                        where_conditions.append("s.total_price > %s")
                    elif any(word in clause for word in ['below', 'less', 'under']):
                        where_conditions.append("s.total_price < %s")
                    else:
                        where_conditions.append("s.total_price > %s")  # Default assumption
                    
                    query_params.append(value)
                    print(f"DEBUG: Generic number condition: {value}")
            
            # Build the WHERE clause
            if where_conditions:
                where_sql = " WHERE " + " AND ".join(where_conditions)
                print(f"DEBUG: Final WHERE clause: {where_sql}")
                print(f"DEBUG: Query parameters: {query_params}")
        
        # Handle structured filter conditions (alternative to where_clause)
        elif filter_conditions:
            where_conditions = []
            for field, value in filter_conditions.items():
                if field in available_columns:
                    db_field = available_columns[field]
                    if isinstance(value, str):
                        where_conditions.append(f"{db_field} LIKE %s")
                        query_params.append(f"%{value}%")
                    else:
                        where_conditions.append(f"{db_field} = %s")
                        query_params.append(value)
            
            if where_conditions:
                where_sql = " WHERE " + " AND ".join(where_conditions)
        
        # Add ORDER BY and LIMIT
        order_sql = " ORDER BY s.sale_date DESC"
        limit_sql = ""
        if limit:
            limit_sql = f" LIMIT {limit}"
        
        # Complete SQL query
        sql = base_sql + where_sql + order_sql + limit_sql
        
        print(f"DEBUG: Final SQL: {sql}")
        print(f"DEBUG: Final Parameters: {query_params}")

        # Execute query
        try:
            if query_params:
                mysql_cur.execute(sql, query_params)
            else:
                mysql_cur.execute(sql)
            
            rows = mysql_cur.fetchall()
            print(f"DEBUG: Query returned {len(rows)} rows")
        except Exception as e:
            mysql_cnxn.close()
            return {"sql": sql, "result": f"❌ SQL Error: {str(e)}"}
        
        mysql_cnxn.close()

        # Build result with only requested columns
        processed_results = []
        for r in rows:
            row_data = {}
            for i, alias in enumerate(column_aliases):
                if i < len(r):  # Safety check
                    value = r[i]
                    
                    # Apply formatting based on display_format
                    if display_format == "Data Format Conversion":
                        if "date" in alias or "timestamp" in alias:
                            value = value.strftime("%Y-%m-%d %H:%M:%S") if value else "N/A"
                    elif display_format == "Decimal Value Formatting":
                        if "price" in alias or "total" in alias or "amount" in alias:
                            value = f"{float(value):.2f}" if value is not None else "0.00"
                    elif display_format == "Null Value Removal/Handling":
                        if value is None:
                            value = "N/A"
                    
                    row_data[alias] = value
            
            # Handle String Concatenation for specific display format
            if display_format == "String Concatenation":
                if "customer_name" in row_data or ("first_name" in row_data and "last_name" in row_data):
                    if "first_name" in row_data and "last_name" in row_data:
                        row_data["customer_full_name"] = f"{row_data['first_name']} {row_data['last_name']}"
                
                if "product_name" in row_data and "product_description" in row_data:
                    desc = row_data['product_description'] or 'No description'
                    row_data["product_full_description"] = f"{row_data['product_name']} ({desc})"
                
                # Create sale summary if we have the needed fields
                if all(field in row_data for field in ['customer_name', 'quantity', 'product_name', 'total_price']):
                    row_data["sale_summary"] = (
                        f"{row_data['customer_name']} bought {row_data['quantity']} "
                        f"of {row_data['product_name']} for ${float(row_data['total_price']):.2f}"
                    )
            
            # Skip null records if specified
            if display_format == "Null Value Removal/Handling":
                if any(v is None for v in row_data.values()):
                    continue
                    
            processed_results.append(row_data)

        print(f"DEBUG: Processed results count: {len(processed_results)}")
        if processed_results:
            print(f"DEBUG: First result keys: {list(processed_results[0].keys())}")

        return {"sql": sql, "result": processed_results}

    else:
        return {"sql": None, "result": f"❌ Unknown operation '{operation}'."}

# ————————————————
# 10. Main: seed + run server
# ————————————————
if __name__ == "__main__":
    # 1) Create + seed all databases (if needed)
    seed_databases()

    # 2) Launch the MCP server for cloud deployment
    import os

    port = int(os.environ.get("PORT", 8000))
    mcp.run(transport="streamable-http", host="0.0.0.0", port=port)

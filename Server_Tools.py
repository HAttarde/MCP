import os
import pyodbc
import psycopg2
from typing import Any

# MCP server
from fastmcp import FastMCP 
import mysql.connector
from dotenv import load_dotenv
load_dotenv() 

# ————————————————
# 1. SQL Server Configuration
# ————————————————
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DB = os.getenv("MYSQL_DB")

def get_mysql_conn(db: str | None = MYSQL_DB):
    """If db is None we connect to the server only (needed to CREATE DATABASE)."""
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=db,
        ssl_disabled=False,          # Aiven requires TLS; keep this False
        autocommit=True,
    )


# ————————————————
# 2. PostgreSQL Configuration
# ————————————————
def must_get(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Missing required env var {key}")
    return val

PG_HOST = must_get("PG_HOST")
PG_PORT = int(must_get("PG_PORT"))
PG_DB   = os.getenv("PG_DB", "postgres")      # db name can default
PG_USER = must_get("PG_USER")
PG_PASS = must_get("PG_PASSWORD")

def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
        sslmode="require",                    # Supabase enforces TLS
    )

# ————————————————
# 3. Instantiate your MCP server
# ————————————————
mcp = FastMCP("CRUDServer")

# ————————————————
# 4. Synchronous Setup: Create & seed tables
# ————————————————
def seed_databases():
    # ---------- MySQL ----------
    # 1. connect without a default schema
    root_cnx = get_mysql_conn(db=None)
    root_cur = root_cnx.cursor()
    root_cur.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DB}`;")
    root_cur.close()
    root_cnx.close()

    # 2. reconnect *inside* the target DB
    sql_cnx = get_mysql_conn()
    sql_cur = sql_cnx.cursor()

    sql_cur.execute("DROP TABLE IF EXISTS Customers;")
    sql_cur.execute("""
        CREATE TABLE Customers (
            Id        INT AUTO_INCREMENT PRIMARY KEY,
            Name      VARCHAR(100) NOT NULL,
            Email     VARCHAR(100) NOT NULL,
            CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    sql_cur.executemany(
        "INSERT INTO Customers (Name, Email) VALUES (%s, %s)",
        [("Alice", "alice@example.com"),
         ("Bob",   "bob@example.com")]
    )
    sql_cnx.close()

    # PostgreSQL
    pg_cnxn = get_pg_conn()
    pg_cnxn.autocommit = True 
    pg_cur  = pg_cnxn.cursor()
    pg_cur.execute("DROP TABLE IF EXISTS products;")
    pg_cur.execute("""
        CREATE TABLE products (
            id          SERIAL PRIMARY KEY,
            name        TEXT   NOT NULL,
            price       NUMERIC(10,4) NOT NULL,
            description TEXT
        );
    """)
    pg_cur.executemany(
        "INSERT INTO products (name, price, description) VALUES (%s, %s, %s)",
        [("Widget",  9.99,  "A standard widget."),
         ("Gadget", 14.99, "A useful gadget.")]
    )
    pg_cnxn.close()

# ————————————————
# 5. SQL Server CRUD Tool (now with DESCRIBE)
# ————————————————
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
    cnxn = get_mysql_conn()        # already connected to MYSQL_DB
    cur  = cnxn.cursor()

    if operation == "create":
        if not name or not email:
            return {"sql": None, "result": "❌ 'name' and 'email' required for create."}

        sql_query = "INSERT INTO Customers (Name, Email) VALUES (%s, %s)"
        cur.execute(sql_query, (name, email))
        cnxn.commit()
        return {"sql": sql_query, "result": f"✅ Customer '{name}' added."}

    elif operation == "read":
        sql_query = """
            SELECT Id, Name, Email, CreatedAt
            FROM Customers
            ORDER BY Id ASC
        """
        cur.execute(sql_query)
        rows = cur.fetchall()
        result = [
            {"Id": r[0], "Name": r[1], "Email": r[2], "CreatedAt": r[3].isoformat()}
            for r in rows
        ]
        return {"sql": sql_query, "result": result}

    elif operation == "update":
        if not customer_id or not new_email:
            return {"sql": None, "result": "❌ 'customer_id' and 'new_email' required for update."}

        sql_query = "UPDATE Customers SET Email = %s WHERE Id = %s"
        cur.execute(sql_query, (new_email, customer_id))
        cnxn.commit()
        return {"sql": sql_query, "result": f"✅ Customer id={customer_id} updated."}

    elif operation == "delete":
        if not customer_id:
            return {"sql": None, "result": "❌ 'customer_id' required for delete."}

        sql_query = "DELETE FROM Customers WHERE Id = %s"
        cur.execute(sql_query, (customer_id,))
        cnxn.commit()
        return {"sql": sql_query, "result": f"✅ Customer id={customer_id} deleted."}

    elif operation == "describe":
        # Table schema query now includes TABLE_SCHEMA to avoid cross-DB clashes
        if not table_name:
            return {"sql": None, "result": "❌ 'table_name' required for describe."}

        sql_query = """
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """
        cur.execute(sql_query, (MYSQL_DB, table_name))
        rows = cur.fetchall()
        result = [
            {"column": r[0], "type": r[1], "nullable": r[2], "max_length": r[3]}
            for r in rows
        ]
        return {"sql": sql_query, "result": result}

    else:
        return {"sql": None, "result": f"❌ Unknown operation '{operation}'."}

# ————————————————
# 6. PostgreSQL CRUD Tool (now with DESCRIBE)
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
    table_name: str = None,    # Added for DESCRIBE
) -> Any:
    cnxn = get_pg_conn()
    cur  = cnxn.cursor()

    if operation == "create":
        if not name or price is None:
            cnxn.close()
            return {"sql": None, "result": "❌ 'name' and 'price' required for create."}
        sql_query = "INSERT INTO products (name, price, description) VALUES (%s, %s, %s)"
        cur.execute(sql_query, (name, price, description))
        cnxn.commit()
        result = f"✅ Product '{name}' added."
        cnxn.close()
        return {"sql": sql_query, "result": result}

    elif operation == "read":
        sql_query = (
            "SELECT id, name, price, description "
            "FROM products "
            "ORDER BY id ASC"
        )
        cur.execute(sql_query)
        rows = cur.fetchall()
        result = [
            {"id": r[0], "name": r[1], "price": float(r[2]), "description": r[3] or ""}
            for r in rows
        ]
        cnxn.close()
        return {"sql": sql_query, "result": result}

    elif operation == "update":
        if not product_id or new_price is None:
            cnxn.close()
            return {"sql": None, "result": "❌ 'product_id' and 'new_price' required for update."}
        sql_query = "UPDATE products SET price = %s WHERE id = %s"
        cur.execute(sql_query, (new_price, product_id))
        cnxn.commit()
        result = f"✅ Product id={product_id} updated."
        cnxn.close()
        return {"sql": sql_query, "result": result}

    elif operation == "delete":
        if not product_id and not name:
            return {"sql": None,
                "result": "❌ Provide 'product_id' **or** 'name' for delete."}

        if product_id:
            sql_query = "DELETE FROM products WHERE id = %s"
            params = (product_id,)
        else:                      # delete by unique name
            sql_query = "DELETE FROM products WHERE name = %s"
            params = (name,)

        cur.execute(sql_query, params)
        cnxn.commit()
        return {"sql": sql_query, "result": f"✅ Deleted product."}

    elif operation == "describe":
        if not table_name:
            cnxn.close()
            return {"sql": None, "result": "❌ 'table_name' required for describe."}
        sql_query = """
            SELECT column_name, data_type, is_nullable, character_maximum_length
            FROM information_schema.columns
            WHERE table_name = %s
        """
        cur.execute(sql_query, (table_name,))
        rows = cur.fetchall()
        result = [
            {
                "column": r[0],
                "type": r[1],
                "nullable": r[2],
                "max_length": r[3]
            }
            for r in rows
        ]
        cnxn.close()
        return {"sql": sql_query, "result": result}

    else:
        cnxn.close()
        return {"sql": None, "result": f"❌ Unknown operation '{operation}'."}

# ————————————————
# 7. Main: seed + run server
# ————————————————
if __name__ == "__main__":
    # 1) Create + seed both databases
    seed_databases()

    # 2) Launch the MCP server with Streamable HTTP at /streamable-http
    mcp.run(transport="streamable-http", host="0.0.0.0", port = 8000)

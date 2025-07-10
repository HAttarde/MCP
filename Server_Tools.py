import os
import pyodbc
import psycopg2
from typing import Any

# MCP server
from mcp.server.fastmcp import FastMCP

# ————————————————
# 1. SQL Server Configuration
# ————————————————
SQL_DRIVER   = os.getenv("SQL_DRIVER",   "ODBC Driver 17 for SQL Server")
SQL_SERVER   = os.getenv("SQL_SERVER",   "HRUSHIKESH")
SQL_USER     = os.getenv("SQL_USER",     "mcpuser")
SQL_PASSWORD = os.getenv("SQL_PASSWORD", "mcppassword")
SQL_DB       = os.getenv("DB_NAME",      "testdb")

def get_sql_conn(autocommit: bool = True):
    conn_str = (
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
        f"DATABASE={SQL_DB};"
    )
    return pyodbc.connect(conn_str, autocommit=autocommit)

# ————————————————
# 2. PostgreSQL Configuration
# ————————————————
PG_HOST     = os.getenv("PG_HOST",     "localhost")
PG_PORT     = os.getenv("PG_PORT",     "5432")
PG_USER     = os.getenv("PG_USER",     "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "mysecretpassword")
PG_DB       = os.getenv("PG_DB",       "postgres")

def get_pg_conn():
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DB,
    )
    conn.autocommit = True
    return conn

# ————————————————
# 3. Instantiate your MCP server
# ————————————————
mcp = FastMCP("CRUDServer")

# ————————————————
# 4. Synchronous Setup: Create & seed tables
# ————————————————
def seed_databases():
    # SQL Server
    sql_cnxn = get_sql_conn()
    sql_cur  = sql_cnxn.cursor()
    # Create database if missing & use it
    sql_cur.execute(f"IF DB_ID(N'{SQL_DB}') IS NULL CREATE DATABASE [{SQL_DB}];")
    sql_cur.execute(f"USE [{SQL_DB}];")
    # Drop + recreate table
    sql_cur.execute("IF OBJECT_ID(N'dbo.Customers','U') IS NOT NULL DROP TABLE dbo.Customers;")
    sql_cur.execute("""
        CREATE TABLE dbo.Customers (
            Id        INT IDENTITY(1,1) PRIMARY KEY,
            Name      NVARCHAR(100) NOT NULL,
            Email     NVARCHAR(100) NOT NULL,
            CreatedAt DATETIME     DEFAULT GETDATE()
        );
    """)
    # Seed data
    sql_cur.executemany(
        "INSERT INTO dbo.Customers (Name, Email) VALUES (?, ?)",
        [("Alice", "alice@example.com"), ("Bob", "bob@example.com")]
    )
    sql_cnxn.close()

    # PostgreSQL
    pg_cnxn = get_pg_conn()
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
    table_name: str = None,    # Added for DESCRIBE
) -> Any:
    cnxn = get_sql_conn()
    cur  = cnxn.cursor()
    cur.execute(f"USE [{SQL_DB}];")

    if operation == "create":
        if not name or not email:
            cnxn.close()
            return {"sql": None, "result": "❌ 'name' and 'email' required for create."}
        sql_query = "INSERT INTO dbo.Customers (Name, Email) VALUES (?, ?)"
        cur.execute(sql_query, (name, email))
        result = f"✅ Customer '{name}' added."
        cnxn.close()
        return {"sql": sql_query, "result": result}

    elif operation == "read":
        sql_query = (
            "SELECT Id, Name, Email, CreatedAt "
            "FROM dbo.Customers "
            "ORDER BY Id ASC"
        )
        cur.execute(sql_query)
        rows = cur.fetchall()
        result = [
            {"Id": r.Id, "Name": r.Name, "Email": r.Email, "CreatedAt": r.CreatedAt.isoformat()}
            for r in rows
        ]
        cnxn.close()
        return {"sql": sql_query, "result": result}

    elif operation == "update":
        if not customer_id or not new_email:
            cnxn.close()
            return {"sql": None, "result": "❌ 'customer_id' and 'new_email' required for update."}
        sql_query = "UPDATE dbo.Customers SET Email = ? WHERE Id = ?"
        cur.execute(sql_query, (new_email, customer_id))
        result = f"✅ Customer id={customer_id} updated."
        cnxn.close()
        return {"sql": sql_query, "result": result}

    elif operation == "delete":
        if not customer_id:
            cnxn.close()
            return {"sql": None, "result": "❌ 'customer_id' required for delete."}
        sql_query = "DELETE FROM dbo.Customers WHERE Id = ?"
        cur.execute(sql_query, (customer_id,))
        result = f"✅ Customer id={customer_id} deleted."
        cnxn.close()
        return {"sql": sql_query, "result": result}

    elif operation == "describe":
        if not table_name:
            cnxn.close()
            return {"sql": None, "result": "❌ 'table_name' required for describe."}
        sql_query = """
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = ?
        """
        cur.execute(sql_query, (table_name,))
        rows = cur.fetchall()
        result = [
            {
                "column": r.COLUMN_NAME,
                "type": r.DATA_TYPE,
                "nullable": r.IS_NULLABLE,
                "max_length": r.CHARACTER_MAXIMUM_LENGTH,
            }
            for r in rows
        ]
        cnxn.close()
        return {"sql": sql_query, "result": result}

    else:
        cnxn.close()
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
        result = f"✅ Product id={product_id} updated."
        cnxn.close()
        return {"sql": sql_query, "result": result}

    elif operation == "delete":
        if not product_id:
            cnxn.close()
            return {"sql": None, "result": "❌ 'product_id' required for delete."}
        sql_query = "DELETE FROM products WHERE id = %s"
        cur.execute(sql_query, (product_id,))
        result = f"✅ Product id={product_id} deleted."
        cnxn.close()
        return {"sql": sql_query, "result": result}

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
    mcp.run(transport="streamable-http")

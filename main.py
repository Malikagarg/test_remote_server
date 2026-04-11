import aiosqlite
import os
from datetime import datetime, timedelta
from fastmcp import FastMCP
DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(__file__), "expenses.db"))
CATEGORIES_PATH = os.path.join(os.path.dirname(__file__), "categories.json")

# mcp = FastMCP("ExpenseTracker")
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

@asynccontextmanager
async def lifespan(server) -> AsyncIterator[None]:
    await init_db()
    yield

mcp = FastMCP("ExpenseTracker", lifespan=lifespan)


print(f"Database path: {DB_PATH}")


async def init_db():
    try:
        async with aiosqlite.connect(DB_PATH) as c:
            await c.execute("PRAGMA journal_mode=WAL")
            await c.execute("""
                CREATE TABLE IF NOT EXISTS expenses(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    amount REAL NOT NULL,
                    category TEXT NOT NULL,
                    subcategory TEXT DEFAULT '',
                    note TEXT DEFAULT '',
                    source TEXT DEFAULT 'manual'
                )
            """)
            await c.execute("""
                CREATE TABLE IF NOT EXISTS budgets(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    month TEXT NOT NULL,
                    category TEXT NOT NULL,
                    amount REAL NOT NULL,
                    UNIQUE(month, category)
                )
            """)
            await c.execute("""
                CREATE TABLE IF NOT EXISTS recurring_expenses (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    amount REAL NOT NULL,
                    category TEXT NOT NULL,
                    subcategory TEXT DEFAULT '',
                    note TEXT DEFAULT '',
                    start_date TEXT NOT NULL,
                    frequency TEXT NOT NULL,
                    last_generated TEXT
                )
            """)
            await c.execute("CREATE INDEX IF NOT EXISTS idx_expenses_date ON expenses(date)")
            await c.execute("CREATE INDEX IF NOT EXISTS idx_expenses_category ON expenses(category)")
            await c.execute("CREATE INDEX IF NOT EXISTS idx_expenses_source ON expenses(source)")
            # Test write access
            await c.execute(
                "INSERT OR IGNORE INTO expenses(date, amount, category) VALUES ('2000-01-01', 0, '__init__')"
            )
            await c.execute("DELETE FROM expenses WHERE category='__init__'")
            await c.commit()
    except Exception as e:
        print(f"DB init error: {e}")
        raise


@mcp.tool()
async def add_expense(date: str, amount: float, category: str, subcategory: str = "", note: str = ""):
    """Add a new expense entry to the database with validation"""
    
    try:
        amount = float(amount)
    except (ValueError, TypeError):
        return {"error": "Invalid amount. Must be a number"}

    
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        return {"error": "Invalid date format. Use YYYY-MM-DD"}

    if amount < 0:
        return {"error": "Amount must be non-negative"}

    if not category.strip():
        return {"error": "Category cannot be empty"}

    async with aiosqlite.connect(DB_PATH) as c:
        cur = await c.execute(
            "INSERT INTO expenses(date, amount, category, subcategory, note) VALUES (?,?,?,?,?)",
            (date, amount, category, subcategory, note)
        )
        await c.commit()
    return {"status": "ok", "id": cur.lastrowid}


@mcp.tool()
async def list_expenses(start_date: str, last_date: str):
    """List expenses between two dates (inclusive)."""
    async with aiosqlite.connect(DB_PATH) as c:
        c.row_factory = aiosqlite.Row
        cur = await c.execute(
            """
            SELECT id, date, amount, category, subcategory, note
            FROM expenses
            WHERE date BETWEEN ? AND ?
            ORDER BY id ASC
            """,
            (start_date, last_date)
        )
        rows = await cur.fetchall()
        return [dict(row) for row in rows]


@mcp.tool()
async def add_recurring_expense(
    amount: float,
    category: str,
    subcategory: str = "",
    note: str = "",
    start_date: str = None,
    frequency: str = "monthly"
):
    """Add a recurring expense (daily, weekly, monthly, yearly)"""
    start_date = start_date or datetime.today().strftime("%Y-%m-%d")
    async with aiosqlite.connect(DB_PATH) as c:
        cur = await c.execute(
            """
            INSERT INTO recurring_expenses (amount, category, subcategory, note, start_date, frequency)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (amount, category, subcategory, note, start_date, frequency)
        )
        await c.commit()
    return {"status": "ok", "id": cur.lastrowid}


@mcp.tool()
async def generate_due_expenses(date: str = None):
    """Generate due recurring expenses for the given date and tag them as recurring."""
    date = date or datetime.today().strftime("%Y-%m-%d")
    generated = []

    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute("SELECT * FROM recurring_expenses")
        recs = await cur.fetchall()

        for r in recs:
            r = dict(r)
            rec_id    = r["id"]
            amount    = r["amount"]
            category  = r["category"]
            subcategory = r["subcategory"]
            note      = r["note"]
            start_date = r["start_date"]
            freq      = r["frequency"]
            last_gen  = r["last_generated"]

            last_date_str = last_gen or start_date
            last_obj    = datetime.strptime(last_date_str, "%Y-%m-%d")
            current_obj = datetime.strptime(date, "%Y-%m-%d")
            start_obj   = datetime.strptime(start_date, "%Y-%m-%d")

            if current_obj < start_obj:
                continue

            due = False
            if last_gen is None and current_obj >= start_obj:
                due = True
            elif freq == "daily" and (current_obj - last_obj).days >= 1:
                due = True
            elif freq == "weekly" and (current_obj - last_obj).days >= 7:
                due = True
            elif freq == "monthly" and (current_obj.year, current_obj.month) != (last_obj.year, last_obj.month):
                due = True
            elif freq == "yearly" and current_obj.year != last_obj.year:
                due = True

            if due:
                await conn.execute(
                    "INSERT INTO expenses (date, amount, category, subcategory, note, source) VALUES (?,?,?,?,?,'recurring')",
                    (date, amount, category, subcategory, note)
                )
                await conn.execute(
                    "UPDATE recurring_expenses SET last_generated=? WHERE id=?",
                    (date, rec_id)
                )
                generated.append({"id": rec_id, "category": category, "amount": amount, "date": date})

        await conn.commit()

    return {"generated_expenses": generated}


@mcp.tool()
async def edit_expense_by_filter(
    date=None, category=None, note=None,
    new_date=None, new_amount=None, new_category=None,
    new_subcategory=None, new_note=None, confirm=False
):
    """
    Edit expenses based on filters (date, category, note).
    1. Call without confirm → shows matching expenses.
    2. Call with confirm=True → updates them with new values.
    """
    if not (date or category or note):
        return {"error": "Provide at least one filter to select expenses"}

    query = "SELECT id, date, amount, category, subcategory, note FROM expenses WHERE 1=1"
    params = []
    if date:
        query += " AND date = ?"; params.append(date)
    if category:
        query += " AND category = ?"; params.append(category)
    if note:
        query += " AND note LIKE ?"; params.append(f"%{note}%")

    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(query, params)
        results = [dict(r) for r in await cur.fetchall()]

        if not results:
            return {"message": "No matching expenses found"}

        if not confirm:
            return {
                "preview": results,
                "message": "Call again with confirm=True and new_* fields to update these records"
            }

        updates, update_params = [], []
        if new_date      is not None: updates.append("date = ?");        update_params.append(new_date)
        if new_amount    is not None: updates.append("amount = ?");      update_params.append(new_amount)
        if new_category  is not None: updates.append("category = ?");    update_params.append(new_category)
        if new_subcategory is not None: updates.append("subcategory = ?"); update_params.append(new_subcategory)
        if new_note      is not None: updates.append("note = ?");        update_params.append(new_note)

        if not updates:
            return {"message": "No new values provided to update"}

        for expense in results:
            await conn.execute(
                f"UPDATE expenses SET {', '.join(updates)} WHERE id = ?",
                (*update_params, expense["id"])
            )
        await conn.commit()

    return {"status": "updated", "rows_updated": len(results)}


@mcp.tool()
async def spending_insights_with_recurring(month: str):
    """Generate monthly spending insights including recurring expenses."""
    await generate_due_expenses()

    async with aiosqlite.connect(DB_PATH) as conn:
        async def fetchone(sql, params=()):
            cur = await conn.execute(sql, params)
            return await cur.fetchone()

        async def fetchall(sql, params=()):
            cur = await conn.execute(sql, params)
            return await cur.fetchall()

        (total,)  = await fetchone("SELECT COALESCE(SUM(amount),0) FROM expenses WHERE strftime('%Y-%m', date)=?", (month,))
        (count,)  = await fetchone("SELECT COUNT(*) FROM expenses WHERE strftime('%Y-%m', date)=?", (month,))
        avg = total / count if count else 0

        categories = await fetchall("""
            SELECT category, SUM(amount)
            FROM expenses
            WHERE strftime('%Y-%m', date)=?
            GROUP BY category ORDER BY SUM(amount) DESC
        """, (month,))

        highest = await fetchone("""
            SELECT amount, category, note FROM expenses
            WHERE strftime('%Y-%m', date)=?
            ORDER BY amount DESC LIMIT 1
        """, (month,))

        (recurring_total,) = await fetchone("""
            SELECT COALESCE(SUM(amount),0) FROM expenses
            WHERE strftime('%Y-%m', date)=? AND source='recurring'
        """, (month,))

        dt = datetime.strptime(month + "-01", "%Y-%m-%d")
        prev_month = (dt.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
        (prev_total,) = await fetchone(
            "SELECT COALESCE(SUM(amount),0) FROM expenses WHERE strftime('%Y-%m', date)=?",
            (prev_month,)
        )
        trend = ((total - prev_total) / prev_total * 100) if prev_total > 0 else 0

    summary = (
        f"In {month}, you spent ₹{total} across {count} transactions. "
        f"Recurring expenses contributed ₹{recurring_total}. "
        f"Trend vs last month: {round(trend, 2)}%."
    )

    return {
        "month": month,
        "total_spent": total,
        "transactions": count,
        "average_expense": round(avg, 2),
        "trend_percent": round(trend, 2),
        "top_categories": categories[:5],
        "highest_expense": highest,
        "recurring_total": recurring_total,
        "summary": summary,
    }


@mcp.tool()
async def delete_latest_expense():
    """Delete the most recent expense"""
    async with aiosqlite.connect(DB_PATH) as conn:
        cur = await conn.execute("SELECT id FROM expenses ORDER BY id DESC LIMIT 1")
        row = await cur.fetchone()

        if not row:
            return {"error": "No expenses found"}

        await conn.execute("DELETE FROM expenses WHERE id = ?", (row[0],))
        await conn.commit()

    return {"status": "deleted", "deleted_id": row[0]}


@mcp.tool()
async def preview_and_delete(date=None, category=None, note=None, confirm=False):
    """
    Preview matching expenses and optionally delete them.
    1. Call without confirm → shows matching expenses.
    2. Call again with confirm=True → deletes them.
    """
    query = "SELECT id, date, amount, category, subcategory, note FROM expenses WHERE 1=1"
    params = []
    if date:
        query += " AND date = ?"; params.append(date)
    if category:
        query += " AND category = ?"; params.append(category)
    if note:
        query += " AND note LIKE ?"; params.append(f"%{note}%")

    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(query, params)
        results = [dict(r) for r in await cur.fetchall()]

        if not results:
            return {"message": "No matching expenses found"}

        if not confirm:
            return {
                "preview": results,
                "message": "Call again with confirm=True to delete these records"
            }

        delete_query = "DELETE FROM expenses WHERE 1=1"
        if date:     delete_query += " AND date = ?"
        if category: delete_query += " AND category = ?"
        if note:     delete_query += " AND note LIKE ?"

        await conn.execute(delete_query, params)
        await conn.commit()

    return {"status": "deleted", "rows_deleted": len(results)}


@mcp.tool()
async def set_budget(month: str, category: str, amount: float):
    """Set monthly budget for a category"""
    if amount < 0:
        return {"error": "Budget amount cannot be negative"}

    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            """
            INSERT INTO budgets (month, category, amount) VALUES (?, ?, ?)
            ON CONFLICT(month, category) DO UPDATE SET amount=excluded.amount
            """,
            (month, category, amount)
        )
        await conn.commit()
    return {"status": "budget set"}


@mcp.tool()
async def summarize(start_date: str, end_date: str, category: str = None):
    """Summarize expenses by category within an inclusive date range."""
    query = """
        SELECT category, SUM(amount) AS total_amount
        FROM expenses
        WHERE date BETWEEN ? AND ?
    """
    params = [start_date, end_date]

    if category:
        query += " AND category = ?"
        params.append(category)

    query += " GROUP BY category ORDER BY category ASC"

    async with aiosqlite.connect(DB_PATH) as c:
        c.row_factory = aiosqlite.Row
        cur = await c.execute(query, params)
        rows = await cur.fetchall()
        return [dict(row) for row in rows]


@mcp.resource("expense://categories", mime_type="application/json")
async def categories():
    import aiofiles
    async with aiofiles.open(CATEGORIES_PATH, "r", encoding="utf-8") as f:
        return await f.read()


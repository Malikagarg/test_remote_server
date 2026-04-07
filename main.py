import sqlite3
import os
from datetime import datetime, timedelta
from fastmcp import FastMCP

DB_PATH = os.path.join(os.path.dirname(__file__), "expenses.db")
CATEGORIES_PATH = os.path.join(os.path.dirname(__file__), "categories.json")
mcp = FastMCP("ExpenseTracker")


def init_db():
    with sqlite3.connect(DB_PATH) as c:
        c.execute("""
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
        # FIX 1: Added UNIQUE(month, category) so ON CONFLICT works correctly
        c.execute("""
            CREATE TABLE IF NOT EXISTS budgets(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                month TEXT NOT NULL,
                category TEXT NOT NULL,
                amount REAL NOT NULL,
                UNIQUE(month, category)
        )
        """)
        c.execute("""
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
        c.execute("CREATE INDEX IF NOT EXISTS idx_expenses_date ON expenses(date)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_expenses_category ON expenses(category)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_expenses_source ON expenses(source)")


init_db()


@mcp.tool()
def add_expense(date, amount, category, subcategory="", note=""):
    """Add a new expense entry to the database with validation"""
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        return {"error": "Invalid date format. Use YYYY-MM-DD"}

    if amount < 0:
        return {"error": "Amount must be non-negative"}

    if not category.strip():
        return {"error": "Category cannot be empty"}

    with sqlite3.connect(DB_PATH) as c:
        cur = c.execute(
            "INSERT INTO expenses(date, amount, category, subcategory, note) VALUES (?,?,?,?,?)",
            (date, amount, category, subcategory, note)
        )
    return {"status": "ok", "id": cur.lastrowid}


@mcp.tool()
def list_expenses(start_date, last_date):
    """List expenses between two dates (inclusive)."""
    with sqlite3.connect(DB_PATH) as c:
        cur = c.execute(
            """
            SELECT id, date, amount, category, subcategory, note
            FROM expenses
            WHERE date BETWEEN ? AND ?
            ORDER BY id ASC
            """,
            (start_date, last_date)
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


@mcp.tool()
def add_recurring_expense(amount: float, category: str, note: str = "", start_date: str = None, frequency: str = "monthly"):
    """Add a recurring expense (daily, weekly, monthly, yearly)"""
    start_date = start_date or datetime.today().strftime("%Y-%m-%d")
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute("""
            INSERT INTO recurring_expenses (amount, category, note, start_date, frequency)
            VALUES (?, ?, ?, ?, ?)
        """, (amount, category, note, start_date, frequency))
    return {"status": "ok", "id": cur.lastrowid}


@mcp.tool()
def generate_due_expenses(date: str = None):
    """Generate due recurring expenses for the given date and tag them as recurring."""
    # FIX 2: Removed redundant local datetime import (already at top)
    date = date or datetime.today().strftime("%Y-%m-%d")
    generated = []

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM recurring_expenses")
        recs = cur.fetchall()

        for r in recs:
            id, amount, category, subcategory, note, start_date, freq, last_gen = r
            last_date = last_gen or start_date
            last_obj = datetime.strptime(last_date, "%Y-%m-%d")
            current_obj = datetime.strptime(date, "%Y-%m-%d")
            start_obj = datetime.strptime(start_date, "%Y-%m-%d")

            # FIX 3: Skip if current date is before start date
            if current_obj < start_obj:
                continue

            due = False
            # FIX 4: If never generated and current_obj >= start_obj, always trigger on first run
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
                cur.execute("""
                    INSERT INTO expenses (date, amount, category, subcategory, note, source)
                    VALUES (?, ?, ?, ?, ?, 'recurring')
                """, (date, amount, category, subcategory, note))
                cur.execute("UPDATE recurring_expenses SET last_generated=? WHERE id=?", (date, id))
                generated.append({"id": id, "category": category, "amount": amount, "date": date})

        conn.commit()

    return {"generated_expenses": generated}


@mcp.tool()
def edit_expense_by_filter(date=None, category=None, note=None, new_date=None, new_amount=None, new_category=None, new_subcategory=None, new_note=None, confirm=False):
    """
    Edit expenses based on filters (date, category, note).

    Steps:
    1. Call without confirm → shows matching expenses.
    2. Call with confirm=True → updates them with new values.
    """
    if not (date or category or note):
        return {"error": "Provide at least one filter to select expenses"}

    query = "SELECT id, date, amount, category, subcategory, note FROM expenses WHERE 1=1"
    params = []

    if date:
        query += " AND date = ?"
        params.append(date)
    if category:
        query += " AND category = ?"
        params.append(category)
    if note:
        query += " AND note LIKE ?"
        params.append(f"%{note}%")

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(query, params)
        cols = [d[0] for d in cur.description]
        results = [dict(zip(cols, row)) for row in cur.fetchall()]

        if not results:
            return {"message": "No matching expenses found"}

        if not confirm:
            return {
                "preview": results,
                "message": "Call again with confirm=True and new_* fields to update these records"
            }

        updates = []
        update_params = []

        if new_date is not None:
            updates.append("date = ?")
            update_params.append(new_date)
        if new_amount is not None:
            updates.append("amount = ?")
            update_params.append(new_amount)
        if new_category is not None:
            updates.append("category = ?")
            update_params.append(new_category)
        if new_subcategory is not None:
            updates.append("subcategory = ?")
            update_params.append(new_subcategory)
        if new_note is not None:
            updates.append("note = ?")
            update_params.append(new_note)

        if not updates:
            return {"message": "No new values provided to update"}

        for expense in results:
            conn.execute(f"UPDATE expenses SET {', '.join(updates)} WHERE id = ?", (*update_params, expense['id']))
        conn.commit()

    return {
        "status": "updated",
        "rows_updated": len(results)
    }


@mcp.tool()
def spending_insights_with_recurring(month: str):
    """Generate monthly spending insights including recurring expenses."""
    # FIX 2: Removed redundant local datetime import (already at top)
    generate_due_expenses()

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()

        cur.execute("SELECT COALESCE(SUM(amount),0) FROM expenses WHERE strftime('%Y-%m', date)=?", (month,))
        total = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM expenses WHERE strftime('%Y-%m', date)=?", (month,))
        count = cur.fetchone()[0]
        avg = total / count if count else 0

        cur.execute("""
            SELECT category, SUM(amount)
            FROM expenses
            WHERE strftime('%Y-%m', date)=?
            GROUP BY category
            ORDER BY SUM(amount) DESC
        """, (month,))
        categories = cur.fetchall()

        cur.execute("""
            SELECT amount, category, note
            FROM expenses
            WHERE strftime('%Y-%m', date)=?
            ORDER BY amount DESC LIMIT 1
        """, (month,))
        highest = cur.fetchone()

        cur.execute("""
            SELECT COALESCE(SUM(amount),0)
            FROM expenses
            WHERE strftime('%Y-%m', date)=? AND source='recurring'
        """, (month,))
        recurring_total = cur.fetchone()[0]

        dt = datetime.strptime(month + "-01", "%Y-%m-%d")
        prev_month = (dt.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
        cur.execute("SELECT COALESCE(SUM(amount),0) FROM expenses WHERE strftime('%Y-%m', date)=?", (prev_month,))
        prev_total = cur.fetchone()[0]
        trend = ((total - prev_total) / prev_total * 100) if prev_total > 0 else 0

        summary = f"In {month}, you spent ₹{total} across {count} transactions. Recurring expenses contributed ₹{recurring_total}. Trend vs last month: {round(trend, 2)}%."

        return {
            "month": month,
            "total_spent": total,
            "transactions": count,
            "average_expense": round(avg, 2),
            "trend_percent": round(trend, 2),
            "top_categories": categories[:5],
            "highest_expense": highest,
            "recurring_total": recurring_total,
            "summary": summary
        }


@mcp.tool()
def delete_latest_expense():
    """Delete the most recent expense"""
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            "SELECT id FROM expenses ORDER BY id DESC LIMIT 1"
        )
        row = cur.fetchone()

        if not row:
            return {"error": "No expenses found"}

        conn.execute("DELETE FROM expenses WHERE id = ?", (row[0],))

    return {"status": "deleted", "deleted_id": row[0]}


@mcp.tool()
def preview_and_delete(date=None, category=None, note=None, confirm=False):
    """
    Preview matching expenses and optionally delete them.

    Steps:
    1. Call without confirm → shows matching expenses
    2. Call again with confirm=True → deletes them
    """
    query = """
    SELECT id, date, amount, category, subcategory, note
    FROM expenses
    WHERE 1=1
    """
    params = []

    if date:
        query += " AND date = ?"
        params.append(date)
    if category:
        query += " AND category = ?"
        params.append(category)
    if note:
        query += " AND note LIKE ?"
        params.append(f"%{note}%")

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(query, params)
        cols = [d[0] for d in cur.description]
        results = [dict(zip(cols, row)) for row in cur.fetchall()]

        if not results:
            return {"message": "No matching expenses found"}

        if not confirm:
            return {
                "preview": results,
                "message": "Call again with confirm=True to delete these records"
            }

        delete_query = "DELETE FROM expenses WHERE 1=1"
        if date:
            delete_query += " AND date = ?"
        if category:
            delete_query += " AND category = ?"
        if note:
            delete_query += " AND note LIKE ?"

        conn.execute(delete_query, params)

    return {
        "status": "deleted",
        "rows_deleted": len(results)
    }


@mcp.tool()
def set_budget(month: str, category: str, amount: float):
    """Set monthly budget for a category"""
    if amount < 0:
        return {"error": "Budget amount cannot be negative"}

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO budgets (month, category, amount)
            VALUES (?, ?, ?)
            ON CONFLICT(month, category) DO UPDATE SET amount=excluded.amount
            """,
            (month, category, amount)
        )
    return {"status": "budget set"}


@mcp.tool()
def summarize(start_date, end_date, category=None):
    """Summarize expenses by category within an inclusive date range."""
    with sqlite3.connect(DB_PATH) as c:
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

        cur = c.execute(query, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


@mcp.resource("expense://categories", mime_type="application/json")
def categories():
    with open(CATEGORIES_PATH, "r", encoding="utf-8") as f:
        return f.read()


# FIX 5: app = mcp moved before __main__ block so it's always reachable
app = mcp

if __name__ == "__main__":
    mcp.run()
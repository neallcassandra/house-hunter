"""SQLite database for tracking seen properties and preventing duplicate notifications."""

import json
import logging
import sqlite3
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class PropertyDatabase:

    def __init__(self, db_path: str = None):
        if db_path is None:
            project_root = Path(__file__).parent.parent
            data_dir = project_root / "data"
            data_dir.mkdir(exist_ok=True)
            db_path = data_dir / "house_hunter.db"

        self.db_path = str(db_path)
        self.conn = None
        self._connect()
        self._create_tables()

    def _connect(self):
        try:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            logger.info(f"Connected to database: {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def _create_tables(self):
        try:
            cursor = self.conn.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS seen_properties (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    property_id TEXT UNIQUE NOT NULL,
                    address TEXT NOT NULL,
                    city TEXT,
                    state TEXT,
                    zip_code TEXT,
                    price INTEGER,
                    beds INTEGER,
                    baths REAL,
                    sqft INTEGER,
                    year_built INTEGER,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    notified_at TIMESTAMP,
                    review_result TEXT,
                    review_passes BOOLEAN,
                    listing_url TEXT,
                    raw_data TEXT
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS notification_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    property_id TEXT NOT NULL,
                    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    notification_type TEXT,
                    success BOOLEAN,
                    error_message TEXT,
                    FOREIGN KEY (property_id) REFERENCES seen_properties(property_id)
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS price_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    property_id TEXT NOT NULL,
                    price INTEGER NOT NULL,
                    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (property_id) REFERENCES seen_properties(property_id)
                )
            """)

            # Indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_property_id ON seen_properties(property_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_notified_at ON seen_properties(notified_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_city ON seen_properties(city)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_price_history_property ON price_history(property_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_price_history_recorded ON price_history(recorded_at)")

            self.conn.commit()
            logger.info("Database tables created/verified")

        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise

    def is_property_seen(self, property_id: str) -> bool:
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT id FROM seen_properties WHERE property_id = ?", (property_id,))
            return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking if property seen: {e}")
            return False

    def is_property_notified(self, property_id: str) -> bool:
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT notified_at FROM seen_properties WHERE property_id = ? AND notified_at IS NOT NULL",
                (property_id,)
            )
            return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking if property notified: {e}")
            return False

    def track_price_change(self, property_id: str, new_price: int) -> dict[str, Any] | None:
        """Returns price drop info if price went down, None otherwise."""
        try:
            cursor = self.conn.cursor()

            cursor.execute(
                "SELECT price FROM price_history WHERE property_id = ? ORDER BY recorded_at DESC LIMIT 1",
                (property_id,)
            )
            result = cursor.fetchone()

            cursor.execute("INSERT INTO price_history (property_id, price) VALUES (?, ?)", (property_id, new_price))
            self.conn.commit()

            if result:
                old_price = result[0]
                if new_price < old_price:
                    drop_amount = old_price - new_price
                    drop_percent = (drop_amount / old_price) * 100
                    logger.info(f"Price drop detected for {property_id}: ${old_price:,} -> ${new_price:,} (-${drop_amount:,}, -{drop_percent:.1f}%)")
                    return {
                        "old_price": old_price,
                        "new_price": new_price,
                        "drop_amount": drop_amount,
                        "drop_percent": drop_percent
                    }

            return None

        except Exception as e:
            logger.error(f"Error tracking price change: {e}")
            return None

    def mark_property_seen(self, property_data: dict[str, Any], review_result: dict[str, Any] | None = None):
        try:
            cursor = self.conn.cursor()

            price = property_data.get("price")
            if price:
                self.track_price_change(property_data.get("property_id"), price)

            if self.is_property_seen(property_data.get("property_id")):
                cursor.execute(
                    "UPDATE seen_properties SET last_seen = CURRENT_TIMESTAMP, price = ? WHERE property_id = ?",
                    (price, property_data.get("property_id"))
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO seen_properties (
                        property_id, address, city, state, zip_code,
                        price, beds, baths, sqft, year_built,
                        review_result, review_passes, listing_url, raw_data
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        property_data.get("property_id"),
                        property_data.get("address"),
                        property_data.get("city"),
                        property_data.get("state"),
                        property_data.get("zip_code"),
                        property_data.get("price"),
                        property_data.get("beds"),
                        property_data.get("baths"),
                        property_data.get("sqft"),
                        property_data.get("year_built"),
                        json.dumps(review_result) if review_result else None,
                        review_result.get("passes") if review_result else None,
                        property_data.get("listing_url"),
                        json.dumps(property_data.get("raw_data", {}))
                    )
                )

            self.conn.commit()
            logger.info(f"Marked property as seen: {property_data.get('property_id')}")

        except Exception as e:
            logger.error(f"Error marking property as seen: {e}")
            self.conn.rollback()

    def mark_property_notified(self, property_id: str, success: bool = True, error_message: str = None):
        try:
            cursor = self.conn.cursor()

            cursor.execute(
                "UPDATE seen_properties SET notified_at = CURRENT_TIMESTAMP WHERE property_id = ?",
                (property_id,)
            )

            cursor.execute(
                "INSERT INTO notification_history (property_id, notification_type, success, error_message) VALUES (?, ?, ?, ?)",
                (property_id, "telegram", success, error_message)
            )

            self.conn.commit()
            logger.info(f"Marked property as notified: {property_id}")

        except Exception as e:
            logger.error(f"Error marking property as notified: {e}")
            self.conn.rollback()

    def get_recent_properties(self, days: int = 7, only_notified: bool = False) -> list[dict[str, Any]]:
        try:
            cursor = self.conn.cursor()

            query = f"SELECT * FROM seen_properties WHERE first_seen >= datetime('now', '-{days} days')"
            if only_notified:
                query += " AND notified_at IS NOT NULL"
            query += " ORDER BY first_seen DESC"

            cursor.execute(query)
            rows = cursor.fetchall()

            properties = []
            for row in rows:
                prop_dict = dict(row)
                if prop_dict.get("raw_data"):
                    prop_dict["raw_data"] = json.loads(prop_dict["raw_data"])
                if prop_dict.get("review_result"):
                    prop_dict["review_result"] = json.loads(prop_dict["review_result"])
                properties.append(prop_dict)

            return properties

        except Exception as e:
            logger.error(f"Error getting recent properties: {e}")
            return []

    def get_properties_with_price_drops(self, min_drop_percent: float = 2.0) -> list[dict[str, Any]]:
        """Find properties where the latest price is lower than the previous one."""
        try:
            cursor = self.conn.cursor()

            cursor.execute("""
                SELECT
                    sp.*,
                    ph1.price as old_price,
                    ph2.price as new_price,
                    (ph1.price - ph2.price) as drop_amount,
                    ((ph1.price - ph2.price) * 100.0 / ph1.price) as drop_percent,
                    ph2.recorded_at as price_drop_date
                FROM seen_properties sp
                INNER JOIN price_history ph1 ON sp.property_id = ph1.property_id
                INNER JOIN price_history ph2 ON sp.property_id = ph2.property_id
                WHERE ph2.recorded_at > ph1.recorded_at
                  AND ph2.price < ph1.price
                  AND ((ph1.price - ph2.price) * 100.0 / ph1.price) >= ?
                  AND ph2.recorded_at >= datetime('now', '-7 days')
                GROUP BY sp.property_id
                HAVING ph1.recorded_at = (
                    SELECT MAX(recorded_at) FROM price_history
                    WHERE property_id = sp.property_id AND recorded_at < ph2.recorded_at
                )
                AND ph2.recorded_at = (
                    SELECT MAX(recorded_at) FROM price_history
                    WHERE property_id = sp.property_id
                )
                ORDER BY drop_percent DESC
            """, (min_drop_percent,))

            rows = cursor.fetchall()
            properties = []
            for row in rows:
                prop_dict = dict(row)
                if prop_dict.get("raw_data"):
                    prop_dict["raw_data"] = json.loads(prop_dict["raw_data"])
                properties.append(prop_dict)

            return properties

        except Exception as e:
            logger.error(f"Error getting properties with price drops: {e}")
            return []

    def get_days_on_market(self, property_id: str) -> int | None:
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT CAST(JULIANDAY('now') - JULIANDAY(first_seen) AS INTEGER) as days
                FROM seen_properties WHERE property_id = ?
            """, (property_id,))
            result = cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Error calculating days on market: {e}")
            return None

    def get_market_insights(self, property_data: dict[str, Any]) -> dict[str, Any]:
        """Compare a property against city averages (price, $/sqft, days on market)."""
        try:
            cursor = self.conn.cursor()
            city = property_data.get("city")
            price = property_data.get("price")
            sqft = property_data.get("sqft")
            property_id = property_data.get("property_id")

            insights = {}

            if not city:
                return insights

            if property_id:
                days = self.get_days_on_market(property_id)
                if days is not None:
                    insights["days_on_market"] = days

            # Average price in city
            cursor.execute(
                "SELECT AVG(price) as avg_price, COUNT(*) as count FROM seen_properties WHERE city = ? AND price > 0",
                (city,)
            )
            result = cursor.fetchone()
            if result and result[0]:
                avg_price = result[0]
                count = result[1]
                insights["city_avg_price"] = int(avg_price)
                insights["city_property_count"] = count

                if price:
                    diff = price - avg_price
                    diff_percent = (diff / avg_price) * 100
                    insights["price_vs_avg"] = diff
                    insights["price_vs_avg_percent"] = diff_percent

            # Price per sqft
            if sqft and sqft > 0:
                cursor.execute(
                    "SELECT AVG(price * 1.0 / sqft) as avg_price_per_sqft FROM seen_properties WHERE city = ? AND sqft > 0 AND price > 0",
                    (city,)
                )
                result = cursor.fetchone()
                if result and result[0]:
                    avg_price_per_sqft = result[0]
                    property_price_per_sqft = price / sqft if price else 0
                    insights["city_avg_price_per_sqft"] = round(avg_price_per_sqft, 2)
                    insights["property_price_per_sqft"] = round(property_price_per_sqft, 2)

                    if property_price_per_sqft > 0:
                        diff_per_sqft = property_price_per_sqft - avg_price_per_sqft
                        insights["price_per_sqft_vs_avg"] = round(diff_per_sqft, 2)

            # Avg days on market in city
            cursor.execute(
                "SELECT AVG(JULIANDAY('now') - JULIANDAY(first_seen)) as avg_days FROM seen_properties WHERE city = ?",
                (city,)
            )
            result = cursor.fetchone()
            if result and result[0]:
                avg_days_in_city = round(result[0], 1)
                insights["city_avg_days_on_market"] = avg_days_in_city

                if "days_on_market" in insights:
                    property_days = insights["days_on_market"]
                    if property_days < avg_days_in_city * 0.5:
                        insights["staleness_vs_avg"] = "much_fresher"
                    elif property_days < avg_days_in_city:
                        insights["staleness_vs_avg"] = "fresher"
                    elif property_days < avg_days_in_city * 1.5:
                        insights["staleness_vs_avg"] = "average"
                    else:
                        insights["staleness_vs_avg"] = "stale"

            return insights

        except Exception as e:
            logger.error(f"Error getting market insights: {e}")
            return {}

    def get_statistics(self) -> dict[str, Any]:
        try:
            cursor = self.conn.cursor()
            stats = {}

            cursor.execute("SELECT COUNT(*) FROM seen_properties")
            stats["total_properties"] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM seen_properties WHERE notified_at IS NOT NULL")
            stats["properties_notified"] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM seen_properties WHERE review_passes = 1")
            stats["properties_passed"] = cursor.fetchone()[0]

            cursor.execute("SELECT city, COUNT(*) as count FROM seen_properties GROUP BY city ORDER BY count DESC")
            stats["by_city"] = {row[0]: row[1] for row in cursor.fetchall()}

            cursor.execute("SELECT COUNT(*) FROM seen_properties WHERE first_seen >= datetime('now', '-7 days')")
            stats["last_7_days"] = cursor.fetchone()[0]

            price_drops = self.get_properties_with_price_drops(min_drop_percent=1.0)
            stats["price_drops_last_7_days"] = len(price_drops)

            return stats

        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
            return {}

    def cleanup_old_entries(self, days_to_keep: int = 90):
        """Remove old entries that were never notified."""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                f"DELETE FROM seen_properties WHERE first_seen < datetime('now', '-{days_to_keep} days') AND notified_at IS NULL"
            )
            deleted = cursor.rowcount
            self.conn.commit()
            logger.info(f"Cleaned up {deleted} old entries")
            return deleted
        except Exception as e:
            logger.error(f"Error cleaning up old entries: {e}")
            self.conn.rollback()
            return 0

    def close(self):
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

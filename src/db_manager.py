import psycopg2
from psycopg2.extras import DictCursor
import time
import logging

logger = logging.getLogger(__name__)

class PostgreSQLManager:
    def __init__(self, host, port, dbname, user, password, retries=5):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.conn = self._connect_with_retry(retries)

    def _connect_with_retry(self, retries):
        for i in range(retries):
            try:
                conn = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    dbname=self.dbname,
                    user=self.user,
                    password=self.password
                )
                conn.autocommit = True
                logger.info("Successfully connected to PostgreSQL.")
                return conn
            except Exception as e:
                logger.warning(f"Database connection failed. Retrying in {2 ** i} seconds... Error: {e}")
                time.sleep(2 ** i)
        raise ConnectionError("Failed to connect to PostgreSQL after multiple retries.")

    def save_feature(self, entity_id: str, feature_name: str, feature_value: str):
        query = """
            INSERT INTO features (entity_id, feature_name, feature_value, timestamp)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (entity_id, feature_name) 
            DO UPDATE SET feature_value = EXCLUDED.feature_value, timestamp = NOW();
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, (entity_id, feature_name, str(feature_value)))
        except psycopg2.InterfaceError:
            logger.error("Database connection lost. Attempting to reconnect...")
            self.conn = self._connect_with_retry(3)
            with self.conn.cursor() as cur:
                cur.execute(query, (entity_id, feature_name, str(feature_value)))
        except Exception as e:
            logger.error(f"Error saving feature: {e}")

    def get_features(self, entity_id: str) -> dict:
        query = "SELECT feature_name, feature_value FROM features WHERE entity_id = %s;"
        try:
            with self.conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(query, (entity_id,))
                rows = cur.fetchall()
                return {row['feature_name']: row['feature_value'] for row in rows}
        except Exception as e:
            logger.error(f"Error fetching features: {e}")
            return {}

    def get_feature(self, entity_id: str, feature_name: str) -> str | None:
        query = "SELECT feature_value FROM features WHERE entity_id = %s AND feature_name = %s;"
        try:
            with self.conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(query, (entity_id, feature_name))
                row = cur.fetchone()
                return row['feature_value'] if row else None
        except Exception as e:
            logger.error(f"Error fetching feature: {e}")
            return None
    
    def close(self):
        if self.conn:
            self.conn.close()
            logger.info("PostgreSQL connection closed.")

# tests/conftest.py
import pytest
import os
import sys
import mysql.connector
from mysql.connector import Error

@pytest.fixture(scope="session", autouse=True)
def mock_argv():
    """Mock sys.argv to prevent getopt from parsing test runner arguments."""
    original_argv = sys.argv.copy()
    # Set minimal argv to avoid getopt parsing issues during tests
    sys.argv = ['server.py']
    yield
    sys.argv = original_argv

@pytest.fixture(scope="session")
def mysql_connection():
    """Create a test database connection."""
    try:
        connection = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST", "127.0.0.1"),
            user=os.getenv("MYSQL_USER", "root"),
            password=os.getenv("MYSQL_PASSWORD", "testpassword"),
            database=os.getenv("MYSQL_DATABASE", "test_db")
        )
        
        if connection.is_connected():
            # Create a test table
            cursor = connection.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS test_table (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255),
                    value INT
                )
            """)
            connection.commit()
            
            yield connection
            
            # Cleanup
            cursor.execute("DROP TABLE IF EXISTS test_table")
            connection.commit()
            cursor.close()
            connection.close()
            
    except Error as e:
        pytest.fail(f"Failed to connect to MySQL: {e}")

@pytest.fixture(scope="session")
def mysql_cursor(mysql_connection):
    """Create a test cursor."""
    cursor = mysql_connection.cursor()
    yield cursor
    cursor.close()

@pytest.fixture
def mock_argv_with_db_args():
    """Mock sys.argv with database connection arguments for testing."""
    original_argv = sys.argv.copy()
    # Set argv with database arguments
    sys.argv = [
        'server.py',
        '-h', os.getenv("MYSQL_HOST", "127.0.0.1"),
        '-p', os.getenv("MYSQL_PORT", "3306"),
        '-u', os.getenv("MYSQL_USER", "root"),
        '-P', os.getenv("MYSQL_PASSWORD", "testpassword"),
        '-d', os.getenv("MYSQL_DATABASE", "test_db")
    ]
    yield sys.argv
    sys.argv = original_argv
import pytest
import sys
from mysql_mcp_server.server import app, list_tools, list_resources, read_resource, call_tool, get_db_config
from pydantic import AnyUrl

def test_server_initialization():
    """Test that the server initializes correctly."""
    assert app.name == "mysql_mcp_server"

@pytest.mark.asyncio
async def test_list_tools():
    """Test that list_tools returns expected tools."""
    tools = await list_tools()
    assert len(tools) == 1
    assert tools[0].name == "execute_sql"
    assert "query" in tools[0].inputSchema["properties"]

@pytest.mark.asyncio
async def test_call_tool_invalid_name():
    """Test calling a tool with an invalid name."""
    with pytest.raises(ValueError, match="Unknown tool"):
        await call_tool("invalid_tool", {})

@pytest.mark.asyncio
async def test_call_tool_missing_query():
    """Test calling execute_sql without a query."""
    with pytest.raises(ValueError, match="Query is required"):
        await call_tool("execute_sql", {})

# Skip database-dependent tests if no database connection
@pytest.mark.asyncio
@pytest.mark.skipif(
    not all([
        pytest.importorskip("mysql.connector"),
        pytest.importorskip("mysql_mcp_server")
    ]),
    reason="MySQL connection not available"
)
async def test_list_resources():
    """Test listing resources (requires database connection)."""
    try:
        resources = await list_resources()
        assert isinstance(resources, list)
    except ValueError as e:
        if "Missing required database configuration" in str(e):
            pytest.skip("Database configuration not available")
        raise

def test_get_db_config_with_command_line_args(mock_argv_with_db_args):
    """Test that get_db_config works with command line arguments."""
    config = get_db_config()
    
    # Verify that config contains expected values from command line args
    assert config["host"] in ["127.0.0.1", "localhost"]
    assert config["port"] == 3306
    assert config["user"] in ["root", "testuser"]
    assert "password" in config
    assert "database" in config
    assert config["charset"] == "utf8mb4"
    assert config["collation"] == "utf8mb4_unicode_ci"

@pytest.mark.asyncio
async def test_mysql_connection_with_args(mock_argv_with_db_args):
    """Test MySQL connection using command line arguments."""
    try:
        import mysql.connector
        from mysql.connector import Error
        
        config = get_db_config()
        
        # Test the connection with the parsed config
        connection = mysql.connector.connect(**config)
        assert connection.is_connected()
        
        # Test a simple query
        cursor = connection.cursor()
        cursor.execute("SELECT 1 as test")
        result = cursor.fetchone()
        assert result[0] == 1
        
        cursor.close()
        connection.close()
        
    except ImportError:
        pytest.skip("mysql-connector-python not available")
    except Error as e:
        if "Access denied" in str(e) or "Unknown database" in str(e):
            pytest.skip(f"Database not configured for testing: {e}")
        else:
            raise
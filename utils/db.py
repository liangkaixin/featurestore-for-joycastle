# duckdb_helper.py
import duckdb
import os


class DuckDBHelper:
    def __init__(self, db_path: str = ":memory:"):
        """
        初始化 DuckDB 会话
        :param db_path: DuckDB 文件路径（默认为内存数据库）
        """
        self.conn = duckdb.connect(database=db_path)
        self._load_sqlite_extension()

    def _load_sqlite_extension(self):
        """
        安装并加载 SQLite 扩展
        """
        self.conn.execute("INSTALL sqlite;")
        self.conn.execute("LOAD sqlite;")

    def query(self, sql: str, fetch: bool = True):
        """
        执行 SQL 查询
        :param sql: SQL 查询语句
        :param fetch: 是否返回结果
        :return: DataFrame（如果 fetch=True）
        """
        result = self.conn.execute(sql)
        return result.fetchdf() if fetch else None

    def read_sqlite_table(self, sqlite_path: str, table_name: str):
        """
        从 SQLite 数据库中读取表
        :param sqlite_path: SQLite 数据库文件路径
        :param table_name: 要读取的表名
        :return: DataFrame
        """
        sql = f"SELECT * FROM sqlite_scan('{sqlite_path}', '{table_name}')"
        return self.query(sql)

    def register_df(self, df, name: str):
        """
        注册 Pandas DataFrame 为 DuckDB 表
        """
        self.conn.register(name, df)

    def create_table(self, table_name: str, schema: str, if_not_exists: bool = True):
        """
        创建一个新表
        :param table_name: 表名
        :param schema: 字段定义，如 "id INTEGER, name TEXT"
        :param if_not_exists: 如果为 True，则仅当表不存在时创建
        """
        clause = "IF NOT EXISTS" if if_not_exists else ""
        sql = f"CREATE TABLE {clause} {table_name} ({schema});"
        self.conn.execute(sql)

    def close(self):
        """
        关闭连接
        """
        self.conn.close()

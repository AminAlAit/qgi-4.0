"""TODO"""

import logging
import os
import duckdb
import pyarrow.parquet as pq


logging.basicConfig(level=logging.INFO, format = "%(asctime)s - %(message)s")
logger = logging.getLogger()


def read_parquet_as_df(parquet_path: str):
    """TODO"""
    return pq.read_table(parquet_path).to_pandas()


class SDCIQueryManager:
    """TODO"""
    def __init__(self, scdi_finder_output: str, pattern_maker_output: str):
        """Initialize the query manager with the base folder path"""
        self.scdi_finder_output = scdi_finder_output
        self.pattern_maker_output = pattern_maker_output

        # Set memory limit and temp directory size
        self.temp_dir = os.path.join(self.pattern_maker_output, "temp")
        os.makedirs(self.temp_dir, exist_ok=True)

        # Initialize DuckDB connection with temp directory
        self.conn = duckdb.connect()
        self.conn.execute(f"SET temp_directory = '{self.temp_dir}'")
        self.conn.execute("SET memory_limit = '80GB'")
        self.conn.execute("SET max_temp_directory_size = '100GiB'")

    def _find_parquet_files(self):
        """Find all Parquet files in the directory structure"""
        parquet_files = []
        logger.info("Searching for Parquet files...")
        for root, dirs, files in os.walk(self.scdi_finder_output):
            # Skip directories ending with "_shrinked"
            if root.endswith("_shrinked"):
                continue

            for file in files:
                if file.endswith('.parquet'):
                    full_path = os.path.join(root, file)
                    parquet_files.append(full_path)
                    logger.debug(f"Found Parquet file: {full_path}")

        if not parquet_files:
            raise ValueError("No Parquet files found in the specified directory structure")

        logger.info(f"Found {len(parquet_files)} Parquet files")
        return parquet_files

    def _register_parquets_in_chunks(self, chunk_size=100):
        """Register all Parquet files with DuckDB in chunks"""
        parquet_files = self._find_parquet_files()
        total_files = len(parquet_files)
        logger.info(f"Registering Parquet files with DuckDB in chunks of {chunk_size}...")

        for i in range(0, total_files, chunk_size):
            chunk_files = parquet_files[i:i + chunk_size]
            formatted_paths = ", ".join([f"'{path.replace("'", "''")}'" for path in chunk_files])
            logger.info(f"Processing chunk {i//chunk_size + 1} of {total_files // chunk_size + 1}...")

            # Create a temporary table for this chunk
            temp_table_name = f"temp_sdcis_{i//chunk_size}"
            self.conn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
            self.conn.execute(f"""
                CREATE TABLE {temp_table_name} AS
                SELECT * FROM read_parquet([{formatted_paths}])
            """)
            logger.info(f"Loaded {len(chunk_files)} files into {temp_table_name}")

            # Insert into the main table
            if i == 0:
                self.conn.execute(f"CREATE TABLE sdcis AS SELECT * FROM {temp_table_name}")
            else:
                self.conn.execute(f"INSERT INTO sdcis SELECT * FROM {temp_table_name}")
            logger.info(f"Inserted data from {temp_table_name} into sdcis")

            # Drop the temporary table
            self.conn.execute(f"DROP TABLE {temp_table_name}")

        logger.info("Parquet files registered successfully")

    def get_top_sdcis(self, top_n=1):
        """
        Get the top N SDCIs by correlation
        
        Args:
            top_n (int): Number of top SDCIs to return
            
        Returns:
            pandas.DataFrame: DataFrame containing the top N SDCIs
        """
        query = f"""
            SELECT *
            FROM sdcis
            ORDER BY correlation DESC
            LIMIT {top_n}
        """
        logger.info(f"Querying top {top_n} SDCIs...")
        result = self.conn.execute(query)
        df = result.df()
        logger.info(f"Found {len(df)} SDCIs")
        return df

    def execute_query(self, query):
        """
        Execute a custom SQL query against the SDCIs
        
        Args:
            query (str): SQL query to execute
            
        Returns:
            pandas.DataFrame: Results of the query
        """
        logger.info(f"Executing query: {query.split(' ')[0]}...")
        result = self.conn.execute(query)
        return result.df()

    def create_patterns(self, save_as_parquet=True):
        """
        Create patterns by grouping SCDIs and using external storage for intermediate results
        """
        logger.info("Creating patterns by grouping SCDIs...")

        query = """
            CREATE TABLE patterns AS
            SELECT
                country_a_id,
                country_b_id,
                length,
                start_year_a,
                start_year_b,
                AVG(correlation) AS average_correlation,
                COUNT(correlation) AS scdi_count
            FROM sdcis
            GROUP BY
                country_a_id,
                country_b_id,
                length,
                start_year_a,
                start_year_b
        """
        self.conn.execute(query)

        if save_as_parquet:
            # Fetch the patterns data
            logger.info(f"Saving patterns table to {self.pattern_maker_output}...")
            patterns_df = self.conn.execute("SELECT * FROM patterns").df()

            # Save the DataFrame to a Parquet file
            patterns_df.to_parquet(
                self.pattern_maker_output,
                index=False,
                compression="brotli",
                compression_level=11
            )

        logger.info(f"Created and populated the 'patterns' table successfully")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def load_saved_patterns(self, parquet_path):
        """
        Load a previously saved patterns table from disk
        """
        logger.info(f"Loading patterns from {parquet_path}...")
        self.conn.execute(f"""
            CREATE TABLE patterns AS
            SELECT * FROM read_parquet('{parquet_path}')
        """)
        logger.info("Patterns table loaded successfully")


pattern_maker_output_path = r"/workspace/qgi\qgi-4.0\data\pattern_maker_output"
scdi_finder_output_path = r"/workspace/qgi\qgi-4.0\data\scdi_finder_output"


with SDCIQueryManager(scdi_finder_output=scdi_finder_output_path, pattern_maker_output=pattern_maker_output_path) as manager:
    manager._register_parquets_in_chunks()
    # manager.get_top_sdcis(5)
    manager.create_patterns()

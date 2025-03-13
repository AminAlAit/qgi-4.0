# import pandas as pd
# def load_runner(indicators):
#     for indicator in indicators:
#         indicator.data.to_csv(r"/workspaces/qgi-4.0/data/" + indicator.id + ".csv")


import pyarrow.parquet as pq
import os


def load_runner(indicators, config):
    for indicator in indicators:
        output_dir = os.path.join(config["output_path"], indicator.id)
        os.makedirs(output_dir, exist_ok=True)

        # Convert DataFrame to Parquet format
        table = pq.Table.from_pandas(indicator.data)

        # Write Parquet file with partitioning and compression
        pq.write_to_dataset(
            table,
            root_path=output_dir,
            partition_cols=["year"],
            compression="snappy"
        )

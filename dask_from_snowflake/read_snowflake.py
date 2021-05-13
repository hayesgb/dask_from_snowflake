from dask_from_snowflake.snowflake2 import SnowflakeCredentials
import dask.dataframe as dd
from dask import delayed
from dask.distributed import get_client
import snowflake.connector

from .snowflake import SnowflakeCredentials


@delayed
def _get_dataframe(batch, meta: dict):
    try:
        table = batch.create_iter(iter_unit="table")
        df = list(table)[0].to_pandas()
        if meta:
            df = df.astype(meta)
        return df
    except Exception as e:
        pass

def read_snowflake(conn_info: dict,
                   query: str,
                   meta: dict = None,
                   repartition: str = None,
                ):
    """
    Accepts Snowflake connection_info, a Snowflake SQL query string, and optionally metadata as a dictionary
    to handle unexpected datatypes
    
    :param conn_info: dictionary of connection info, some of which may be saved as environmental variables
    :param query: Snowflake SQL query as a string
    :param meta: dictionary of datatype mappings to handle datatype declaration on the fly.  Passing this may reduce
                memory footprint of the dataframe at load time
    :param repartition: If "num_workers", will fetch the number of workers and attempt to partition the data across the # of workers.
        If "100MB", will partition to that size as a recommended option.  These may not fit your use case.
    :returns A lazy Dask Dataframe
    
    """

    sf = SnowflakeCredentials(**conn_info)

    with snowflake.connector.connect(**creds) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            cur.check_can_use_arrow_resultset()
            batches = cur.get_result_batches()

    dfs = []
    for batch in batches:
        if batch.rowcount > 0:
            df = _get_dataframe(batch, meta=meta)
            dfs.append(df)
    ddf = dd.from_delayed(dfs)
    if repartition:
        if repartition == 'num_workers':
            # Fetch the number of available workers and spread the partitions across them
            num_workers = len(get_client().scheduler_info()['workers'])
            ddf = ddf.repartition(npartitions = num_workers)
        elif repartition == "100MB":
            ddf = ddf.repartition(partition_size = "100MB")
        else:
            raise ValueError("Value passed for repartition not supported by read_snowflake!")
    return ddf
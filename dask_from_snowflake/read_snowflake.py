import dask.dataframe as dd
from dask import delayed
import snowflake.connector

from .snowflake import Snowflake


@delayed
def _get_dataframe(batch, meta = None):
    try:
        table = batch.create_iter(iter_unit="table")
        df_ = list(table)[0].to_pandas()
        if meta:
            df_ = df_.astype(meta)
        return df_
    except Exception as e:
        pass

def read_snowflake(conn_info, query, meta = None):

    snowflake = Snowflake(**conn_info)

    with snowflake.connector.connect(**snowflake.connection_info) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            cur.check_can_use_arrow_resultset()
            batches = cur.get_result_batches()

    dfs = []
    for batch in batches:
        if batch.rowcount > 0:
            df = _get_dataframe(batch)
            dfs.append(df)
    ddf = dd.from_delayed(dfs)
    return ddf
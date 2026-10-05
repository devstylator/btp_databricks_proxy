"""
Databricks Delta Sharing Connector - FastAPI app.

Exposes a read endpoint protected by XSUAA for SAP Cloud Integration, with
the Databricks bearer token supplied separately on each request.
"""

from typing import Optional

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel

from app.auth import require_databricks_bearer, require_xsuaa_token
from app.delta_sharing_client import MAX_FETCH_ROWS, ReadError, read_table
from app.query_builder import QueryError, apply_query

app = FastAPI(title="Databricks Delta Sharing Connector", version="1.0.0")


class QueryRequest(BaseModel):
    share: str
    schema_name: str
    table: str
    columns: Optional[str] = "*"
    filters: Optional[str] = None
    top: Optional[int] = 1000


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/databricks/query", dependencies=[Depends(require_xsuaa_token)])
async def query_table(payload: QueryRequest, databricks_bearer: str = Depends(require_databricks_bearer)):
    try:
        df = read_table(payload.share, payload.schema_name, payload.table, databricks_bearer, limit=MAX_FETCH_ROWS)
        result_df = apply_query(df, payload.columns, payload.filters, payload.top)
    except (QueryError, ReadError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - surface as a proxy/upstream error
        raise HTTPException(status_code=502, detail=f"Delta Sharing request failed: {exc}") from exc

    return {
        "columns": list(result_df.columns),
        "rows": result_df.values.tolist(),
    }

from datetime import datetime
from typing import Optional, Dict
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from .models import SnowflakeConfigVersion, SourceRegistry, Running


class SnowflakeDatabase:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.engine = create_engine(self.dsn, future=True)
        self.SessionLocal = sessionmaker(bind=self.engine, expire_on_commit=False)

    def _session(self) -> Session:
        return self.SessionLocal()

    def get_config(self, version_no: Optional[int] = None):
        with self._session() as s:
            stmt = select(SnowflakeConfigVersion)
            if version_no is not None:
                stmt = stmt.where(SnowflakeConfigVersion.version_no == version_no)
            else:
                stmt = stmt.where(SnowflakeConfigVersion.active_flag.is_(True))

            stmt = stmt.order_by(SnowflakeConfigVersion.version_no.desc()).limit(1)
            row = s.execute(stmt).scalar_one_or_none()
            if not row:
                raise ValueError("No active snowflake config")

            return {
                "version_no": row.version_no,
                "timestamp_bits": row.timestamp_bits,
                "datacenter_bits": row.datacenter_bits,
                "worker_bits": row.worker_bits,
                "sequence_bits": row.sequence_bits,
                "epoch_start_ms": int(row.epoch_start.timestamp() * 1000),
            }

    def get_registry_info(self, source_name: str) -> Optional[Dict[str, int]]:
        with self._session() as s:
            stmt = (
                select(SourceRegistry)
                .where(SourceRegistry.source_name == source_name)
                .where(SourceRegistry.active_flag.is_(True))
            )
            row = s.execute(stmt).scalar_one_or_none()
            if not row:
                return None

            return {
                "datacenter_id": row.datacenter_id,
                "worker_id": row.worker_id,
            }

    def get_last_timestamp(self, source_name: str, datacenter_id: int) -> int:
        with self._session() as s:
            stmt = (
                select(SourceRegistry.last_timestamp)
                .where(SourceRegistry.source_name == source_name)
                .where(SourceRegistry.datacenter_id == datacenter_id)
            )
            last = s.execute(stmt).scalar_one_or_none()
            return int(last or 0)

    def update_last_timestamp(self, source_name, datacenter_id, ts, user="system"):
        with self._session() as s:
            stmt = (
                select(SourceRegistry)
                .where(SourceRegistry.source_name == source_name)
                .where(SourceRegistry.datacenter_id == datacenter_id)
            )
            row = s.execute(stmt).scalar_one()
            row.last_timestamp = ts
            row.updatedate = datetime.utcnow()
            row.userupdate = user
            s.commit()

    def log_run(
        self,
        source_name,
        datacenter_id,
        version_no,
        job_name,
        id_start,
        id_end,
        usercreate="system",
    ):
        with self._session() as s:
            stmt = pg_insert(Running).values(
                source_name=source_name,
                datacenter_id=datacenter_id,
                version_no=version_no,
                id_start=id_start,
                id_end=id_end,
                job_name=job_name,
                usercreate=usercreate,
            )

            upsert = stmt.on_conflict_do_update(
                index_elements=[Running.source_name, Running.job_name],
                set_={
                    "id_end": id_end,
                    "updatedate": datetime.utcnow(),
                    "userupdate": usercreate,
                },
            )

            s.execute(upsert)
            s.commit()


def log_running(
    dsn, source_name, version_no, job_name, id_start, id_end, usercreate="system"
):
    db = SnowflakeDatabase(dsn)
    info = db.get_registry_info(source_name)
    if not info:
        raise ValueError(f"Source '{source_name}' not found")

    db.log_run(
        source_name,
        info["datacenter_id"],
        version_no,
        job_name,
        id_start,
        id_end,
        usercreate,
    )

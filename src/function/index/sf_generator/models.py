from datetime import datetime

from sqlalchemy import (
    Column,
    Integer,
    Boolean,
    DateTime,
    String,
    BigInteger,
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# ---------------------------
# system.snowflake_config_version
# ---------------------------


class SnowflakeConfigVersion(Base):
    __tablename__ = "snowflake_config_version"
    __table_args__ = {"schema": "system"}

    version_no = Column(Integer, primary_key=True)
    timestamp_bits = Column(Integer, nullable=False)
    datacenter_bits = Column(Integer, nullable=False)
    worker_bits = Column(Integer, nullable=False)
    sequence_bits = Column(Integer, nullable=False)
    epoch_start = Column(DateTime, nullable=False)
    description = Column(String, nullable=True)
    active_flag = Column(Boolean, default=False)
    createdate = Column(DateTime, default=datetime.utcnow, nullable=False)
    usercreate = Column(String, default="system", nullable=True)
    updatedate = Column(DateTime, nullable=True)
    userupdate = Column(String, nullable=True)


# ---------------------------
# system.source_registry
# ---------------------------


class SourceRegistry(Base):
    __tablename__ = "source_registry"
    __table_args__ = {"schema": "system"}

    # PK = source_name (ตาม DDL)
    source_name = Column(String, primary_key=True)
    datacenter_id = Column(Integer, nullable=False)
    worker_id = Column(Integer, nullable=False)
    last_timestamp = Column(BigInteger, default=0)
    active_flag = Column(Boolean, default=True)

    createdate = Column(DateTime, default=datetime.utcnow, nullable=False)
    usercreate = Column(String, default="system", nullable=True)
    updatedate = Column(DateTime, nullable=True)
    userupdate = Column(String, nullable=True)


# ---------------------------
# system.t_running
# ---------------------------


class Running(Base):
    __tablename__ = "t_running"
    __table_args__ = {"schema": "system"}

    # ใน DDL ไม่มี PK ชัดเจน ใช้ id_running เป็น uuid nullable
    id_running = Column(String, nullable=True)
    source_name = Column(String, primary_key=True)
    job_name = Column(String, primary_key=True)
    datacenter_id = Column(Integer, nullable=False)
    version_no = Column(Integer, nullable=False)
    id_start = Column(String, nullable=True)
    id_end = Column(String, nullable=True)
    job_name = Column(String, nullable=True)

    active_flag = Column(Boolean, default=True)
    createdate = Column(DateTime, default=datetime.utcnow, nullable=False)
    usercreate = Column(String, default="system", nullable=True)
    updatedate = Column(DateTime, nullable=True)
    userupdate = Column(String, nullable=True)

import time
import threading
import numpy as np
import psycopg2
from dataclasses import dataclass
from typing import Optional


# ==========================================================
# CONFIG MODEL
# ==========================================================
@dataclass
class SnowflakeConfig:
    version_no: int
    timestamp_bits: int
    datacenter_bits: int
    worker_bits: int
    sequence_bits: int
    epoch_start_ms: int

    @property
    def max_sequence(self):
        return (1 << self.sequence_bits) - 1

    @property
    def max_worker_id(self):
        return (1 << self.worker_bits) - 1

    @property
    def max_datacenter_id(self):
        return (1 << self.datacenter_bits) - 1


# ==========================================================
# DATABASE HANDLER
# ==========================================================
class SnowflakeDatabase:
    def __init__(self, dsn: str):
        self.dsn = dsn

    def get_config(self, version_no: Optional[int] = None) -> SnowflakeConfig:
        query = """
            SELECT version_no, timestamp_bits, datacenter_bits, worker_bits, sequence_bits, epoch_start
            FROM system.snowflake_config_version
            WHERE (%s IS NOT NULL AND version_no = %s)
               OR (%s IS NULL AND active_flag = TRUE)
            ORDER BY version_no DESC LIMIT 1
        """
        with psycopg2.connect(self.dsn) as conn, conn.cursor() as cur:
            cur.execute(query, (version_no, version_no, version_no))
            row = cur.fetchone()
            if not row:
                raise ValueError("No Snowflake config found")
            version_no, t, d, w, s, epoch = row
            epoch_ms = int(epoch.timestamp() * 1000)
            return SnowflakeConfig(version_no, t, d, w, s, epoch_ms)

    def get_registry_info(self, source_name: str) -> Optional[dict]:
        with psycopg2.connect(self.dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT datacenter_id, worker_id
                FROM system.source_registry
                WHERE source_name=%s AND active_flag=TRUE
                ORDER BY createdate DESC LIMIT 1
            """,
                (source_name,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {"datacenter_id": row[0], "worker_id": row[1]}

    def get_last_timestamp(self, source_name: str, datacenter_id: int) -> int:
        with psycopg2.connect(self.dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(last_timestamp,0)
                FROM system.source_registry
                WHERE source_name=%s AND datacenter_id=%s
            """,
                (source_name, datacenter_id),
            )
            r = cur.fetchone()
            if r:
                return int(r[0])
            return -1

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
        with psycopg2.connect(self.dsn) as conn, conn.cursor() as cur:
            conn.autocommit = True

            # ✅ insert หรือ update ถ้ามี job เดิม
            cur.execute(
                """
                INSERT INTO system.t_running
                    (source_name, datacenter_id, version_no, id_start, id_end, job_name, usercreate)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_name, job_name)
                DO UPDATE SET
                    id_end = EXCLUDED.id_end,
                    updatedate = now(),
                    userupdate = EXCLUDED.usercreate;
                """,
                (
                    source_name,
                    datacenter_id,
                    version_no,
                    id_start,
                    id_end,
                    job_name,
                    usercreate,
                ),
            )

            # ✅ อัปเดต timestamp ล่าสุดใน registry ด้วย
            cur.execute(
                """
                UPDATE system.source_registry
                SET last_timestamp = GREATEST(COALESCE(last_timestamp,0), %s),
                    updatedate = now(),
                    userupdate = %s
                WHERE source_name = %s AND datacenter_id = %s
                """,
                (id_end, usercreate, source_name, datacenter_id),
            )


# ==========================================================
# SNOWFLAKE GENERATOR (STRICT)
# ==========================================================
class SnowflakeGenerator:
    def __init__(self, dsn, source_name, version_no=None, usercreate="system"):
        self.db = SnowflakeDatabase(dsn)
        self.config = self.db.get_config(version_no)

        info = self.db.get_registry_info(source_name)
        if not info:
            raise ValueError(
                f"Source '{source_name}' not found in system.source_registry"
            )

        self.source_name = source_name
        self.datacenter_id = info["datacenter_id"]
        self.worker_id = info["worker_id"]

        self.last_timestamp = self.db.get_last_timestamp(
            source_name, self.datacenter_id
        )
        self.sequence = 0
        self.lock = threading.Lock()

    def _timestamp(self):
        return int(time.time() * 1000)

    def _wait_next(self, last_ts):
        ts = self._timestamp()
        while ts <= last_ts:
            ts = self._timestamp()
        return ts

    def _parse_conn(self):
        from urllib.parse import urlparse

        parsed = urlparse(self.db.dsn)
        return {
            "jdbc_url": f"jdbc:postgresql://{parsed.hostname}:{parsed.port}/{parsed.path.lstrip('/')}",
            "user": parsed.username,
            "password": parsed.password,
            "host": parsed.hostname,
            "port": parsed.port,
            "dbname": parsed.path.lstrip("/"),
        }

    def generate(self):
        with self.lock:
            ts = self._timestamp()
            if ts < self.last_timestamp:
                raise Exception("Clock moved backwards")
            if ts == self.last_timestamp:
                self.sequence = (self.sequence + 1) & self.config.max_sequence
                if self.sequence == 0:
                    ts = self._wait_next(self.last_timestamp)
            else:
                self.sequence = 0
            self.last_timestamp = ts
            elapsed = ts - self.config.epoch_start_ms
            shift_w = self.config.sequence_bits
            shift_d = shift_w + self.config.worker_bits
            shift_t = shift_d + self.config.datacenter_bits

            snowflake_id = (
                (elapsed << shift_t)
                | (self.datacenter_id << shift_d)
                | (self.worker_id << shift_w)
                | self.sequence
            )

            with psycopg2.connect(self.db.dsn) as conn, conn.cursor() as cur:
                conn.autocommit = True
                cur.execute(
                    """
                    UPDATE system.source_registry
                    SET last_timestamp = %s, updatedate = now(), userupdate = %s
                    WHERE source_name = %s AND datacenter_id = %s
                """,
                    (
                        self.last_timestamp,
                        "system",
                        self.source_name,
                        self.datacenter_id,
                    ),
                )

            return snowflake_id

    def bulk_generate_fast(self, count: int):
        ts = int(time.time() * 1000)
        elapsed = ts - self.config.epoch_start_ms
        seq = np.arange(count) & self.config.max_sequence
        ids = (
            (
                elapsed
                << (
                    self.config.datacenter_bits
                    + self.config.worker_bits
                    + self.config.sequence_bits
                )
            )
            | (
                self.datacenter_id
                << (self.config.worker_bits + self.config.sequence_bits)
            )
            | (self.worker_id << self.config.sequence_bits)
            | seq
        )
        return ids.tolist()

    def decode(self, snowflake_id: int):
        s_mask = (1 << self.config.sequence_bits) - 1
        w_mask = (1 << self.config.worker_bits) - 1
        d_mask = (1 << self.config.datacenter_bits) - 1
        seq = snowflake_id & s_mask
        w_id = (snowflake_id >> self.config.sequence_bits) & w_mask
        d_id = (
            snowflake_id >> (self.config.sequence_bits + self.config.worker_bits)
        ) & d_mask
        shift_t = (
            self.config.sequence_bits
            + self.config.worker_bits
            + self.config.datacenter_bits
        )
        elapsed = snowflake_id >> shift_t
        ts_ms = elapsed + self.config.epoch_start_ms
        return {
            "timestamp_ms": ts_ms,
            "datacenter_id": d_id,
            "worker_id": w_id,
            "sequence": seq,
        }

    # def process_source(self, spark, table_name: str, timestamp_col: str = "createdate"):
    #     import io
    #     import psycopg2
    #     from pyspark.sql import functions as F, Window

    #     """
    #     เร็วกว่าเดิม 10–30 เท่า
    #     ใช้ Spark gen sf_id → เขียน temp CSV → COPY เข้า temp table → UPDATE กลับ DB
    #     """
    #     conn = self._parse_conn()

    #     print(f"🚀 Processing {self.source_name} ({table_name})")

    #     # ✅ 1. อ่านเฉพาะข้อมูลที่ sf_id ยังว่าง (pushdown filter)
    #     df = (
    #         spark.read.format("jdbc")
    #         .option("url", conn["jdbc_url"])
    #         .option(
    #             "dbtable",
    #             f"(SELECT url, {timestamp_col} FROM {table_name} WHERE sf_id IS NULL) AS subq",
    #         )
    #         .option("user", conn["user"])
    #         .option("password", conn["password"])
    #         .option("numPartitions", 4)  # อ่านแบบ parallel
    #         .option("partitionColumn", timestamp_col)
    #         .option("lowerBound", "2020-01-01")
    #         .option("upperBound", "2030-01-01")
    #         .load()
    #     )

    #     if df.count() == 0:
    #         print(f"✅ No missing sf_id in {self.source_name}")
    #         return

    #     # ✅ 2. คำนวณ Snowflake ID (ฝั่ง Spark)
    #     df = df.withColumn(
    #         "ts_ms",
    #         (F.col(timestamp_col).cast("timestamp").cast("long") * 1000).cast("long"),
    #     )
    #     window_spec = Window.partitionBy("ts_ms").orderBy(F.col("url"))
    #     df = df.withColumn(
    #         "seq",
    #         (F.row_number().over(window_spec) - 1) & F.lit(self.config.max_sequence),
    #     )
    #     df = df.withColumn(
    #         "elapsed", F.col("ts_ms") - F.lit(self.config.epoch_start_ms)
    #     )

    #     datacenter_shift = self.config.worker_bits + self.config.sequence_bits
    #     timestamp_shift = self.config.datacenter_bits + datacenter_shift

    #     df = df.withColumn(
    #         "sf_id",
    #         (
    #             (F.col("elapsed") << F.lit(timestamp_shift))
    #             | (F.lit(self.datacenter_id) << F.lit(datacenter_shift))
    #             | (F.lit(self.worker_id) << F.lit(self.config.sequence_bits))
    #             | F.col("seq")
    #         ).cast("long"),
    #     )

    #     # ✅ 3. แปลง Spark → Pandas แล้วใช้ COPY
    #     pdf = df.select("url", "sf_id").toPandas()

    #     tmp_csv = io.StringIO()
    #     pdf.to_csv(tmp_csv, sep=";", index=False, header=False)
    #     tmp_csv.seek(0)

    #     # ✅ 4. ใช้ psycopg2 COPY ลง temp table แล้ว update กลับ
    #     with (
    #         psycopg2.connect(
    #             dbname=conn["dbname"],
    #             user=conn["user"],
    #             password=conn["password"],
    #             host=conn["host"],
    #             port=conn["port"],
    #         ) as pg_conn,
    #         pg_conn.cursor() as cur,
    #     ):
    #         pg_conn.autocommit = True

    #         cur.execute("CREATE TEMP TABLE tmp_sfid (url text, sf_id bigint);")
    #         cur.copy_from(tmp_csv, "tmp_sfid", sep=";")

    #         cur.execute(f"""
    #             UPDATE {table_name} AS t
    #             SET sf_id = s.sf_id
    #             FROM tmp_sfid AS s
    #             WHERE t.url = s.url;
    #         """)

    #         print(f"✅ Updated {cur.rowcount} rows for {self.source_name}")

    #     print(f"🎯 Done processing {self.source_name}")


# ==========================================================
# LOG HELPER
# ==========================================================
def log_running(
    dsn, source_name, version_no, job_name, id_start, id_end, usercreate="system"
):
    db = SnowflakeDatabase(dsn)
    info = db.get_registry_info(source_name)
    if not info:
        raise ValueError(f"Source '{source_name}' not found in system.source_registry")
    datacenter_id = info["datacenter_id"]
    db.log_run(
        source_name, datacenter_id, version_no, job_name, id_start, id_end, usercreate
    )

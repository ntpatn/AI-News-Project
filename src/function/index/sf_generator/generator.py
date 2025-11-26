import time
import threading
from typing import Callable, Optional, Dict
import pandas as pd
import numpy as np
from .database import SnowflakeDatabase


class SnowflakeGenerator:
    def __init__(
        self,
        dsn: str,
        source_name: str,
        version_no: Optional[int] = None,
        usercreate: str = "system",
        time_fn: Optional[Callable[[], int]] = None,
        auto_persist: bool = True,
    ):
        self.db = SnowflakeDatabase(dsn)
        cfg = self.db.get_config(version_no)

        self.version_no = cfg["version_no"]
        self.timestamp_bits = cfg["timestamp_bits"]
        self.datacenter_bits = cfg["datacenter_bits"]
        self.worker_bits = cfg["worker_bits"]
        self.sequence_bits = cfg["sequence_bits"]
        self.epoch_start_ms = cfg["epoch_start_ms"]

        info = self.db.get_registry_info(source_name)
        if not info:
            raise ValueError(f"Source '{source_name}' not found")

        self.source_name = source_name
        self.datacenter_id = info["datacenter_id"]
        self.worker_id = info["worker_id"]

        self.last_timestamp = self.db.get_last_timestamp(
            source_name, self.datacenter_id
        )
        self.sequence = 0
        self.lock = threading.Lock()

        self.max_sequence = (1 << self.sequence_bits) - 1
        self.time_fn = time_fn or (lambda: int(time.time() * 1000))
        self.auto_persist = auto_persist
        self.usercreate = usercreate

    def _timestamp(self):
        return self.time_fn()

    def _wait_next(self, last_ts):
        ts = self._timestamp()
        while ts <= last_ts:
            ts = self._timestamp()
        return ts

    def generate(self) -> int:
        with self.lock:
            ts = self._timestamp()

            if ts < self.last_timestamp:
                raise Exception("Clock moved backwards")

            if ts == self.last_timestamp:
                self.sequence = (self.sequence + 1) & self.max_sequence
                if self.sequence == 0:
                    ts = self._wait_next(self.last_timestamp)
            else:
                self.sequence = 0

            self.last_timestamp = ts

            elapsed = ts - self.epoch_start_ms
            shift_w = self.sequence_bits
            shift_d = shift_w + self.worker_bits
            shift_t = shift_d + self.datacenter_bits

            sfid = (
                (elapsed << shift_t)
                | (self.datacenter_id << shift_d)
                | (self.worker_id << shift_w)
                | self.sequence
            )

            if self.auto_persist:
                self.db.update_last_timestamp(
                    self.source_name,
                    self.datacenter_id,
                    self.last_timestamp,
                    self.usercreate,
                )

            return sfid

    def bulk_generate_fast(self, count: int):
        ts = self._timestamp()
        elapsed = ts - self.epoch_start_ms

        seq = np.arange(count) & self.max_sequence

        ids = (
            (elapsed << (self.datacenter_bits + self.worker_bits + self.sequence_bits))
            | (self.datacenter_id << (self.worker_bits + self.sequence_bits))
            | (self.worker_id << self.sequence_bits)
            | seq
        )

        return ids.tolist()

    def decode(self, snowflake_id: int) -> Dict[str, int]:
        s_mask = (1 << self.sequence_bits) - 1
        w_mask = (1 << self.worker_bits) - 1
        d_mask = (1 << self.datacenter_bits) - 1

        seq = snowflake_id & s_mask
        w_id = (snowflake_id >> self.sequence_bits) & w_mask
        d_id = (snowflake_id >> (self.sequence_bits + self.worker_bits)) & d_mask

        shift_t = self.sequence_bits + self.worker_bits + self.datacenter_bits
        elapsed = snowflake_id >> shift_t
        ts_ms = elapsed + self.epoch_start_ms

        return {
            "timestamp_ms": ts_ms,
            "datacenter_id": d_id,
            "worker_id": w_id,
            "sequence": seq,
        }

    def generate_from_timestamp(self, ts_ms: int) -> int:
        elapsed = ts_ms - self.epoch_start_ms
        if ts_ms == self.last_timestamp:
            self.sequence = (self.sequence + 1) & self.max_sequence
            if self.sequence == 0:
                ts_ms = self._wait_next(ts_ms)
        else:
            self.sequence = 0

        self.last_timestamp = ts_ms

        shift_w = self.sequence_bits
        shift_d = shift_w + self.worker_bits
        shift_t = shift_d + self.datacenter_bits

        return (
            (elapsed << shift_t)
            | (self.datacenter_id << shift_d)
            | (self.worker_id << shift_w)
            | self.sequence
        )

    def bulk_generate_from_timestamp(self, timestamps_ms):
        timestamps_ms = np.array(timestamps_ms)
        elapsed = timestamps_ms - self.epoch_start_ms
        _, seq = np.unique(timestamps_ms, return_inverse=True)

        seq = seq & self.max_sequence

        shift_w = self.sequence_bits
        shift_d = shift_w + self.worker_bits
        shift_t = shift_d + self.datacenter_bits

        ids = (
            (elapsed << shift_t)
            | (self.datacenter_id << shift_d)
            | (self.worker_id << shift_w)
            | seq
        )

        return ids.tolist()

    def assign_ids_from_dataframe(
        self,
        df: pd.DataFrame,
        ts_col: str = "createdate",
        sf_col: str = "sf_id",
    ) -> pd.DataFrame:
        mask = df[sf_col].isna() | (df[sf_col] == "")

        if not mask.any():
            return df

        ts_series = df.loc[mask, ts_col]

        if not pd.api.types.is_datetime64_any_dtype(ts_series):
            ts_series = pd.to_datetime(ts_series, errors="raise")

        ts_ms = (ts_series.astype("int64") // 10**6).astype("int64")

        seq = ts_ms.groupby(ts_ms).cumcount().astype("int64")
        shift_w = self.sequence_bits
        shift_d = shift_w + self.worker_bits
        shift_t = shift_d + self.datacenter_bits

        elapsed = (ts_ms - self.epoch_start_ms).astype("int64")
        ids = (
            np.left_shift(elapsed, shift_t)
            | np.left_shift(self.datacenter_id, shift_d)
            | np.left_shift(self.worker_id, shift_w)
            | (seq.to_numpy(dtype="int64") & self.max_sequence)
        ).astype("int64")
        df.loc[mask, sf_col] = ids

        return df

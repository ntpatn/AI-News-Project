import os
import re
import time
import tempfile
import logging
from dataclasses import dataclass, asdict
from typing import Dict, Optional
from airflow.exceptions import AirflowException
from minio import Minio
from minio.error import S3Error
# import boto3

log = logging.getLogger(__name__)


@dataclass
class Artifact:
    """Metadata wrapper for artifact storage (local or remote)."""

    location_type: str
    path: Optional[str] = None
    bucket: Optional[str] = None
    object_name: Optional[str] = None

    def to_dict(self) -> Dict[str, Optional[str]]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Optional[str]]) -> "Artifact":
        return cls(**data)


class ArtifactStorageManager:
    def __init__(
        self, artifact_prefix: Optional[str] = None, backend: Optional[str] = None
    ) -> None:
        env_backend = os.getenv("ARTIFACT_STORAGE_BACKEND") or os.getenv(
            "ARTIFACT_STORAGE_MODE", "local"
        )
        self.backend = (backend or env_backend).lower().strip()

        # Validate backend early
        supported_backends = {"local", "minio"}
        if self.backend not in supported_backends:
            raise AirflowException(
                f"Unsupported ARTIFACT_STORAGE_BACKEND '{self.backend}', "
                f"supported: {sorted(supported_backends)}"
            )

        self.bucket = os.getenv("ARTIFACT_STORAGE_BUCKET", "airflow-artifacts")
        self._artifact_prefix = (
            artifact_prefix or os.getenv("ARTIFACT_STORAGE_PREFIX", "artifacts")
        ).strip("/")

        self._minio_client: Optional[Minio] = None
        self._minio_bucket_checked = False
        # self._s3_client = None

    def _with_retry(self, func, *args, retries=3, delay=1.5, **kwargs):
        """Generic retry wrapper for MinIO operations."""
        last_exc = None
        for attempt in range(1, retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exc = e
                log.warning(
                    "Artifact operation failed (attempt %s/%s): %s",
                    attempt,
                    retries,
                    e,
                )
                if attempt < retries:
                    time.sleep(delay)
        raise last_exc

    # -----------------------------
    # Publish
    # -----------------------------
    def publish(self, local_path: str, category: str) -> Artifact:
        """Publish a local file to MinIO or return direct local path."""

        # Validation
        if not local_path or not os.path.isfile(local_path):
            raise AirflowException(f"Local file does not exist: {local_path}")

        if not category or not category.strip():
            raise AirflowException("Category is required for publishing artifact")

        if self.backend == "local":
            log.info("Artifact stored locally: %s", local_path)
            return Artifact(location_type="local", path=local_path)

        # Build object path
        object_name = self._build_object_name(local_path, category)

        if self.backend == "minio":
            self._upload_minio(local_path, object_name)
        # elif self.backend == "s3":
        #     self._upload_s3(local_path, object_name)
        else:
            raise AirflowException(
                f"Unsupported ARTIFACT_STORAGE_BACKEND '{self.backend}'"
            )

        return Artifact(
            location_type="remote",
            bucket=self.bucket,
            object_name=object_name,
        )

    # -----------------------------
    # Fetch
    # -----------------------------
    def fetch(self, artifact: Artifact, suffix: str = "") -> str:
        """Download remote artifact to temp file or return local path."""

        if artifact.location_type == "local":
            if not artifact.path:
                raise AirflowException("Local artifact missing path")
            return artifact.path

        if artifact.location_type != "remote":
            raise AirflowException(
                f"Unknown artifact location_type: {artifact.location_type}"
            )

        if not artifact.object_name:
            raise AirflowException("Remote artifact missing object name")

        temp_file = tempfile.NamedTemporaryFile(
            prefix=f"{self._artifact_prefix}_dl_",
            suffix=suffix,
            delete=False,
            dir="/tmp",
        )
        temp_file.close()

        if self.backend == "minio":
            self._download_minio(artifact, temp_file.name)
        # elif self.backend == "s3":
        #     self._download_s3(artifact, temp_file.name)
        else:
            raise AirflowException(
                f"Unsupported ARTIFACT_STORAGE_BACKEND '{self.backend}'"
            )

        return temp_file.name

    # -----------------------------
    # Cleanup
    # -----------------------------
    def cleanup(self, path: Optional[str]) -> None:
        """Delete temp file quietly."""
        if not path:
            return

        try:
            if os.path.exists(path):
                os.remove(path)
                log.debug("Cleaned up temp artifact file: %s", path)
        except Exception as e:
            log.warning("Failed to cleanup temp file %s: %s", path, e)

    # -----------------------------
    # MinIO Upload
    # -----------------------------
    def _upload_minio(self, local_path: str, object_name: str) -> None:
        client = self._get_minio_client()
        self._ensure_minio_bucket(client)

        log.info(
            "Uploading artifact to MinIO bucket=%s, object=%s", self.bucket, object_name
        )

        try:
            self._with_retry(
                client.fput_object,
                self.bucket,
                object_name,
                local_path,
                retries=3,
                delay=2.0,
            )
        except Exception as e:
            log.exception("Failed to upload artifact to MinIO")
            raise AirflowException(f"MinIO upload failed: {e}") from e

    # -----------------------------
    # MinIO Download
    # -----------------------------
    def _download_minio(self, artifact: Artifact, dest_path: str) -> None:
        client = self._get_minio_client()
        bucket = artifact.bucket or self.bucket

        log.info(
            "Downloading artifact from MinIO bucket=%s, object=%s",
            bucket,
            artifact.object_name,
        )

        try:
            self._with_retry(
                client.fget_object,
                bucket,
                artifact.object_name,
                dest_path,
                retries=3,
                delay=2.0,
            )
        except Exception as e:
            log.exception("Failed to download artifact from MinIO")
            raise AirflowException(f"MinIO download failed: {e}") from e

    # -----------------------------
    # MinIO Client
    # -----------------------------
    def _get_minio_client(self) -> Minio:
        """Lazy-init MinIO client."""
        if not Minio:
            raise AirflowException("minio package is required for MinIO backend")

        if self._minio_client:
            return self._minio_client

        endpoint = os.getenv("MINIO_ENDPOINT")
        access_key = os.getenv("MINIO_ACCESS_KEY")
        secret_key = os.getenv("MINIO_SECRET_KEY")
        secure = os.getenv("MINIO_SECURE", "false").lower() == "true"

        if not all([endpoint, access_key, secret_key]):
            raise AirflowException(
                "MINIO_ENDPOINT, MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set for MinIO backend"
            )

        self._minio_client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )
        return self._minio_client

    # -----------------------------
    # MinIO Ensure Bucket
    # -----------------------------
    def _ensure_minio_bucket(self, client: Minio) -> None:
        """Check/create bucket (race-condition safe)."""
        if self._minio_bucket_checked:
            return

        try:
            if not client.bucket_exists(self.bucket):
                client.make_bucket(self.bucket)
        except S3Error as e:
            if e.code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                log.exception("Failed to ensure MinIO bucket=%s", self.bucket)
                raise
        finally:
            self._minio_bucket_checked = True

    # -----------------------------
    # Build object path
    # -----------------------------
    def _build_object_name(self, local_path: str, category: str) -> str:
        file_name = os.path.basename(local_path)

        # sanitize category
        sanitized_category = re.sub(r"[^a-zA-Z0-9_\-/]", "_", category.strip("/"))

        return f"{self._artifact_prefix}/{sanitized_category}/{file_name}"

    # def _upload_s3(self, local_path: str, object_name: str) -> None:
    #     client = self._get_s3_client()
    #     client.upload_file(local_path, self.bucket, object_name)

    # def _download_s3(self, artifact: Artifact, dest_path: str) -> None:
    #     client = self._get_s3_client()
    #     client.download_file(artifact.bucket, artifact.object_name, dest_path)

    # def _get_s3_client(self):
    #     if not boto3:
    #         raise AirflowException("boto3 package is required for S3 backend")

    #     if self._s3_client:
    #         return self._s3_client

    #     aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    #     aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    #     aws_session_token = os.getenv("AWS_SESSION_TOKEN")
    #     aws_region = os.getenv("AWS_REGION")

    #     session = boto3.session.Session(
    #         aws_access_key_id=aws_access_key_id,
    #         aws_secret_access_key=aws_secret_access_key,
    #         aws_session_token=aws_session_token,
    #         region_name=aws_region,
    #     )
    #     self._s3_client = session.client("s3")
    #     return self._s3_client

from minio import Minio

client = Minio(
    "localhost:9002", access_key="minio", secret_key="minio12345", secure=False
)
# client.remove_bucket("test")
client.fput_object(
    "mlflow",
    "myfile.csv",  # object name (ชื่อไฟล์ใน bucket)
    "C:/Users/Lenovo/Desktop/AI-News-Project/data/interim/data_news_segmentation/description_not_null/newsseg_desc_not_null_v20250811_144642.csv",  # path ไฟล์ในเครื่อง
)
print("Upload done")
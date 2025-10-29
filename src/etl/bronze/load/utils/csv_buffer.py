import io

def prepare_csv_buffer(csv_text: str, delimiter: str = ";"):
    """Clean BOM, validate header, and return buffer + header"""
    text = csv_text.lstrip("\ufeff")
    csv_buffer = io.StringIO(text)

    line0 = csv_buffer.readline().rstrip("\n\r")
    header = [h.strip() for h in line0.split(delimiter)] if line0 else []

    if not header or len(header) != len(set(h.lower() for h in header)):
        raise ValueError("Invalid CSV header")

    csv_buffer.seek(0)
    return csv_buffer, header

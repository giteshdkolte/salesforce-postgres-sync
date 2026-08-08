# =============================
# RECURSIVE CLEANER + FLATTENER
# =============================
def clean_record(record):
    """Recursively remove 'attributes' keys from nested dicts."""
    if isinstance(record, dict):
        return {k: clean_record(v) for k, v in record.items() if k != 'attributes'}
    elif isinstance(record, list):
        return [clean_record(item) for item in record]
    return record

def flatten_record(record, parent_key='', sep='_'):
    """Flatten nested dicts: {'RecordType': {'Name': 'X'}} → {'RecordType_Name': 'X'}"""
    items = {}
    for k, v in record.items():
        # Handling fields to not get duplicate in postgres
        k_lower = k.lower()
        new_key = f"{parent_key}{sep}{k_lower}" if parent_key else k_lower
        if isinstance(v, dict):
            items.update(flatten_record(v, new_key, sep=sep))
        elif isinstance(v, bool):   # Handling boolean
            items[new_key] = str(v).lower()  # False→'false', True→'true'
        else:
            items[new_key] = v
    return items

def process_records(records: list) -> list:
    cleaned   = [clean_record(r) for r in records]
    flattened = [flatten_record(r) for r in cleaned]
    return flattened
# media_schema.py
from datetime import datetime

try:
    from pymongo import IndexModel
except ImportError:
    IndexModel = None

MEDIA_CONVERSION_SCHEMA = {
    'validator': {
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['user_id', 'timestamp'],
            'properties': {
                'user_id': {
                    'bsonType': 'int',
                    'description': 'Telegram user ID'
                },
                'file_name': {
                    'bsonType': 'string',
                    'description': 'Original file name'
                },
                'file_type': {
                    'bsonType': 'string',
                    'enum': ['video', 'audio', 'document', 'image'],
                    'description': 'File type'
                },
                'file_size': {
                    'bsonType': 'int',
                    'minimum': 0,
                    'description': 'File size in bytes'
                },
                'action': {
                    'bsonType': 'string',
                    'enum': ['upload', 'convert', 'compress', 'merge', 'trim', 'extract', 'optimize', 'info', 'screenshot'],
                    'description': 'Action performed'
                },
                'source_format': {
                    'bsonType': 'string',
                    'description': 'Original file format'
                },
                'target_format': {
                    'bsonType': 'string',
                    'description': 'Target file format'
                },
                'parameters': {
                    'bsonType': 'object',
                    'description': 'Conversion parameters'
                },
                'success': {
                    'bsonType': 'bool',
                    'description': 'Operation success status'
                },
                'error_message': {
                    'bsonType': 'string',
                    'description': 'Error message if failed'
                },
                'processing_time': {
                    'bsonType': 'int',
                    'minimum': 0,
                    'description': 'Processing time in seconds'
                },
                'output_size': {
                    'bsonType': 'int',
                    'minimum': 0,
                    'description': 'Output file size in bytes'
                },
                'timestamp': {
                    'bsonType': 'date',
                    'description': 'Operation timestamp'
                }
            }
        }
    },
    'indexes': [
        IndexModel([('user_id', 1), ('timestamp', -1)]),
        IndexModel([('action', 1)]),
        IndexModel([('success', 1)]),
        IndexModel([('timestamp', -1)], expireAfterSeconds=30*24*60*60)  # Auto-delete after 30 days
    ] if IndexModel else []
}
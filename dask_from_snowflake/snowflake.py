
import os


class Snowflake:
    def __init__(self,
                 user,
                 role,
                 account,
                 warehouse,
                 database,
                 schema,
                 password = None,
                 private_key_file = None,
                 private_key_file_passphrase = None,
                 
    ):
    
        self.user = user or os.getenv("SNOWFLAKE_USER")
        self.role = role or os.getenv("SNOWFLAKE_ROLE")
        self.account = account or os.getenv("SNOWFLAKE_ACCOUNT")
        self.warehouse = warehouse or os.getenv("SNOWFLAKE_WAREHOUSE")
        self.database = database or os.getenv("SNOWFLAKE_DATABASE")
        self.schema = schema or os.getenv("SNOWFLAKE_SCHEMA")
        self.password = password or os.getenv("SNOWFLAKE_USER_PASSWORD")
        self.private_key_file = private_key_file or os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE")
        self.private_key_file_passphrase = private_key_file_passphrase or os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PASSPHRASE")
        self.make_connection_info()
    
    def make_connection_info(self):
    
        if all(i is not None for i in [self.private_key_file, self.private_key_file_passphrase]):
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives.asymmetric import rsa
            from cryptography.hazmat.primitives.asymmetric import dsa
            from cryptography.hazmat.primitives import serialization
            with open(self.private_key_file, "rb") as key:
                p_key = serialization.load_pem_private_key(
                    key.read(),
                    self.private_key_file_passphrase.encode(),
                    backend=default_backend(),
                )
            pbk = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
            
            self.connection_info={
                "user": self.user,
                "role": self.role,
                "account": self.account,
                "warehouse": self.warehouse,
                "database": self.database,
                "schema": self.schema,
                "private_key": pbk,
        } 

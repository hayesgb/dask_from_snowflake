
import os


class SnowflakeCredentials:
    def __init__(self,
                 user = None,
                 role = None,
                 account = None,
                 warehouse = None,
                 database = None,
                 schema = None,
                 password = None,
                 private_key_file = None,
                 private_key_passphrase = None,
                 
    ):
    
        self.user = user or os.getenv("SNOWFLAKE_USER")
        self.role = role or os.getenv("SNOWFLAKE_ROLE")
        self.account = account or os.getenv("SNOWFLAKE_ACCOUNT")
        self.warehouse = warehouse or os.getenv("SNOWFLAKE_WAREHOUSE")
        self.database = database or os.getenv("SNOWFLAKE_DATABASE")
        self.schema = schema or os.getenv("SNOWFLAKE_SCHEMA")
        self.password = password or os.getenv("SNOWFLAKE_USER_PASSWORD")
        self.private_key_file = private_key_file or os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE")
        self.private_key_passphrase = private_key_passphrase or os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
        self.connection_info = {
                "user": self.user,
                "role": self.role,
                "account": self.account,
                "warehouse": self.warehouse,
                "database": self.database,
                "schema": self.schema,            
        }
        self.get_credential()
    
    def get_credential(self):
    
        if all(i is not None for i in [self.private_key_file, self.private_key_passphrase]):
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives.asymmetric import rsa
            from cryptography.hazmat.primitives.asymmetric import dsa
            from cryptography.hazmat.primitives import serialization
            with open(self.private_key_file, "rb") as key:
                p_key = serialization.load_pem_private_key(
                    key.read(),
                    self.private_key_passphrase.encode(),
                    backend=default_backend(),
                )
            pbk = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
            
            self.connection_info["private_key"] = pbk

        elif self.password is not None:
            self.connection_info['password'] = self.password

        else:
            raise ValueError("No authentication information provided!")


from collections.abc import Mapping, MutableMapping
import os


class SnowflakeCredentials(MutableMapping):
    def __init__(self, **kwargs):
        self._items = kwargs
        self._user = kwargs.get('user', None) or os.getenv("SNOWFLAKE_USER")
        self._role = kwargs.get('role', None) or os.getenv("SNOWFLAKE_ROLE")
        self._account = kwargs.get('account', None) or os.getenv("SNOWFLAKE_ACCOUNT")
        self._warehouse = kwargs.get('warehouse', None) or os.getenv("SNOWFLAKE_WAREHOUSE")
        self._database = kwargs.get('database', None) or os.getenv("SNOWFLAKE_DATABASE")
        self._schema = kwargs.get('schema', None) or os.getenv("SNOWFLAKE_SCHEMA")
        self._password = kwargs.get('password', None) or os.getenv("SNOWFLAKE_USER_PASSWORD")
        self._private_key_file = kwargs.get('private_key_file', None) or os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE")
        self._private_key_passphrase = kwargs.get('private_key_passphrase', None) or os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
        if self._password is None:
            self.get_credential()

    def __getitem__(self, key):
        return self._items[key]
    
    def __iter__(self):
        for k, v in self._items.items():
            yield k, v
    
    def __len__(self):
        return len(self._items)

    def __delitem__(self, key):
        try:
            del self._items[key]
        except KeyError:
            raise KeyError("Key not found!")

    def __setitem__(self, key, value):
        self._items[key] = value
    
    def keys(self):
        return self._items.keys()

    def values(self):
        return self._items.values()

    def items(self):
        return self._items.items()
    
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
            
            self._items['private_key'] = pbk

        else:
            raise ValueError("No authentication information provided!")
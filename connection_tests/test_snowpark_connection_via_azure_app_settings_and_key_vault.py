
# Test connection to Snowpark leveraging Azure app settings (i.e. environment variables) and a private key stored as a secret in Azure Key Vault

## Import required function
from ..interworks_snowpark_python.snowpark_session_builder import build_snowpark_session_using_stored_private_key_in_azure_secrets_vault as build_snowpark_session

'''
## Optional section to set specific environment variables temporarily
import os

os.environ[AZURE_KEY_VAULT_NAME] = "<name-of-key-vault>"
os.environ[SNOWFLAKE_ACCOUNT] = "<account>[.<region>][.<cloud provider>]"
os.environ[SNOWFLAKE_USER] = "<username>"
os.environ[SNOWFLAKE_DEFAULT_ROLE] = "<default role>" ## Enter "None" if not required
os.environ[SNOWFLAKE_DEFAULT_WAREHOUSE] = "<default warehouse>" ## Enter "None" if not required
os.environ[SNOWFLAKE_DEFAULT_DATABASE] = "<default database>" ## Enter "None" if not required
os.environ[SNOWFLAKE_DEFAULT_SCHEMA] = "<default schema>" ## Enter "None" if not required
'''

## Generate Snowpark session
snowpark_session = build_snowpark_session()

## Simple commands to test the connection by listing the databases in the environment
df_test = snowpark_session.sql('SHOW DATABASES')
df_test.show()

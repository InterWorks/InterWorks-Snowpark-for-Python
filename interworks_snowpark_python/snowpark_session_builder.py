
# Build Snowpark sessions for Snowflake

## Import session module
from snowflake.snowpark import Session

## Define function to build a Snowpark session
def build_snowpark_session_from_connection_parameters(
    snowflake_connection_parameters: dict
  ) :

  ### Create session to connect to Snowflake
  snowpark_session = Session.builder.configs(snowflake_connection_parameters).create()

  return snowpark_session

## Define function to build a Snowpark session
## leveraging a locally-stored JSON file
## containing Snowflake connection parameters
def build_snowpark_session_via_parameters_json(
    snowflake_connection_parameters_json_filepath: str = 'snowflake_connection_parameters.json'
  ):

  ### Import required module
  from .leverage_snowflake_connection_parameters_dictionary import import_snowflake_connection_parameters_from_local_json

  ### Generate Snowflake connection parameters from the relevant source
  snowflake_connection_parameters = import_snowflake_connection_parameters_from_local_json(snowflake_connection_parameters_json_filepath, private_key_output_format = 'snowpark')

  ### Create session to connect to Snowflake
  snowpark_session = build_snowpark_session_from_connection_parameters(snowflake_connection_parameters)

  return snowpark_session

## Define function to build a Snowpark session
## leveraging a streamlit secrets file
## containing Snowflake connection parameters.
## Only works if streamlit package is installed
def build_snowpark_session_via_streamlit_secrets():
  
  ### Import required module
  from .leverage_snowflake_connection_parameters_dictionary import import_snowflake_connection_parameters_from_streamlit_secrets

  ### Generate Snowflake connection parameters from the relevant source
  snowflake_connection_parameters = import_snowflake_connection_parameters_from_streamlit_secrets(private_key_output_format = 'snowpark')

  ### Create session to connect to Snowflake
  snowpark_session = build_snowpark_session_from_connection_parameters(snowflake_connection_parameters)

  return snowpark_session

## Define function to build a Snowpark session
## leveraging environment variables
## containing Snowflake connection parameters
def build_snowpark_session_via_environment_variables():
  
  ### Import required module
  from .leverage_snowflake_connection_parameters_dictionary import import_snowflake_connection_parameters_from_environment_variables

  ### Generate Snowflake connection parameters from the relevant source
  snowflake_connection_parameters = import_snowflake_connection_parameters_from_environment_variables(private_key_output_format = 'snowpark')

  ### Create session to connect to Snowflake
  snowpark_session = build_snowpark_session_from_connection_parameters(snowflake_connection_parameters)

  return snowpark_session

## Define function to build a Snowpark session
## leveraging an inbound dictionary argument
## containing Snowflake connection parameters
def build_snowpark_session_via_parameters_object(
    imported_connection_parameters: dict
  ):

  '''
  imported_connection_parameters is expected in the following format:
  {
    "account": "<account>[.<region>][.<cloud provider>]",
    "user": "<username>",
    "default_role" : "<default role>", // Enter "None" if not required
    "default_warehouse" : "<default warehouse>", // Enter "None" if not required
    "default_database" : "<default database>", // Enter "None" if not required
    "default_schema" : "<default schema>", // Enter "None" if not required
    "private_key_path" : "path\\to\\private\\key", // Enter "None" if not required, in which case private key plain text or password will be used
    "private_key_plain_text" : "-----BEGIN PRIVATE KEY-----\nprivate\nkey\nas\nplain\ntext\n-----END PRIVATE KEY-----", // Not best practice but may be required in some cases. Ignored if private key path is provided
    "private_key_passphrase" : "<passphrase>", // Enter "None" if not required
    "password" : "<password>" // Enter "None" if not required, ignored if private key path or private key plain text is provided
  }
  '''

  ### Import required module
  from .leverage_snowflake_connection_parameters_dictionary import retrieve_snowflake_connection_parameters

  ### Generate Snowflake connection parameters from the relevant source
  snowflake_connection_parameters = retrieve_snowflake_connection_parameters(imported_connection_parameters, private_key_output_format = 'snowpark')

  ### Create session to connect to Snowflake
  snowpark_session = build_snowpark_session_from_connection_parameters(snowflake_connection_parameters)

  return snowpark_session

## Define function to build a Snowpark session
## leveraging environment variables
## containing Snowflake connection parameters,
## with the private key stored in 
## an Azure secrets vault.
## Only works if corresponding Azure packages are installed
def build_snowpark_session_using_stored_private_key_in_azure_secrets_vault(
      key_vault_uri: str = None
    , snowflake_user: str = None
    , snowflake_account: str = None
    , snowflake_default_role: str = None
    , snowflake_default_warehouse: str = None
    , snowflake_default_database: str = None
    , snowflake_default_schema: str = None
  ) :
  
  ### Import required modules
  from .azure_secrets_vault_functions import retrieve_secret_from_azure_secrets
  from .leverage_snowflake_connection_parameters_dictionary import retrieve_snowflake_connection_parameters
  import os

  ### Retrieve any missing inputs from environment variables
  if snowflake_user is None or len(snowflake_user) == 0:
    snowflake_user = os.getenv("SNOWFLAKE_USER")
  if snowflake_account is None or len(snowflake_account) == 0:
    snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT")
  if snowflake_default_role is None or len(snowflake_default_role) == 0:
    snowflake_default_role = os.getenv("SNOWFLAKE_DEFAULT_ROLE")
  if snowflake_default_warehouse is None or len(snowflake_default_warehouse) == 0:
    snowflake_default_warehouse = os.getenv("SNOWFLAKE_DEFAULT_WAREHOUSE")
  if snowflake_default_database is None or len(snowflake_default_database) == 0:
    snowflake_default_database = os.getenv("SNOWFLAKE_DEFAULT_DATABASE")
  if snowflake_default_schema is None or len(snowflake_default_schema) == 0:
    snowflake_default_schema = os.getenv("SNOWFLAKE_DEFAULT_SCHEMA")
  
  ### Create name of desired secret
  private_key_secret_name = f"{snowflake_user}__private_key"

  ### Retrieve private key for user from Azure Secrets Vault
  private_key = retrieve_secret_from_azure_secrets(secret_name=private_key_secret_name, key_vault_uri=key_vault_uri)

  ### Define imported connection parameters using environment variables
  ### whilst targeting specific account
  imported_connection_parameters = {
      "account" : snowflake_account
    , "user" : snowflake_user
    , "default_role" : snowflake_default_role
    , "default_warehouse" : snowflake_default_warehouse
    , "default_database" : snowflake_default_database
    , "default_schema" : snowflake_default_schema
    , "private_key_plain_text" : private_key
  }

  ### Populate snowflake_connection_parameters from imported_connection_parameters
  snowflake_connection_parameters = retrieve_snowflake_connection_parameters(imported_connection_parameters, private_key_output_format = 'snowpark')

  ### Create session to connect to Snowflake
  snowpark_session = build_snowpark_session_from_connection_parameters(snowflake_connection_parameters)

  return snowpark_session
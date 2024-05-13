
# Test connection to Snowpipe leveraging Azure app settings (i.e. environment variables) and a private key stored as a secret in Azure Key Vault

## Import required function
from ..interworks_snowpark_python.snowpipe_ingest_manager_builder import build_snowpark_session_using_stored_private_key_in_azure_secrets_vault as build_snowpipe_ingest_manager

'''
## Optional section to set specific environment variables temporarily
import os

os.environ["AZURE_KEY_VAULT_NAME"] = "<name-of-key-vault>"
os.environ["SNOWFLAKE_ACCOUNT"] = "<account>[.<region>][.<cloud provider>]"
os.environ["SNOWFLAKE_USER"] = "<username>"
os.environ["SNOWFLAKE_DEFAULT_ROLE"] = "<default role>" ## Enter "None" if not required
os.environ["SNOWFLAKE_DEFAULT_WAREHOUSE"] = "<default warehouse>" ## Enter "None" if not required
os.environ["SNOWFLAKE_DEFAULT_DATABASE"] = "<default database>" ## Enter "None" if not required
os.environ["SNOWFLAKE_DEFAULT_SCHEMA"] = "<default schema>" ## Enter "None" if not required
'''

## If desired, populate a target pipe name to test the connection to
target_pipe_name = None

## Generate Snowpark session
snowpipe_ingest_manager = build_snowpipe_ingest_manager(target_pipe_name=target_pipe_name)

## Simple commands to test the connection by listing the databases in the environment
try :
  snowpipe_response = snowpipe_ingest_manager.ingest_files([])
  assert(snowpipe_response['responseCode'] == 'SUCCESS')

  print('Test successful')
except Exception as e:
  if 'Message: Specified object does not exist or not authorized. Pipe not found' in e.message :
    ## In this case, our test is successful as we have authenticated.
    ## This has failed anyway because we have entered a None pipe,
    ## which is our only option if this is to be generic
    print('Authentication successful, however the pipe itself was not found')
  else :
    print('Error identified:')
    print(e)
    
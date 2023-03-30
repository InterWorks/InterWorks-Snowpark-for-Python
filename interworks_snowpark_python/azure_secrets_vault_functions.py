
# Import the required modules for Azure
from azure.keyvault.secrets import SecretClient
from azure.identity import ManagedIdentityCredential

# Import other required packages
import os

# Retrieve key vault URI
def retrieve_key_vault_uri() :
  key_vault_name = os.getenv("AZURE_KEY_VAULT_NAME")
  key_vault_uri = f"https://{key_vault_name}.vault.azure.net"

  return key_vault_uri

# Define function to leverage managed identity
# to retrieve secret client
def retrieve_secret_client() :
  
  managed_identity_credential = ManagedIdentityCredential()

  key_vault_uri = retrieve_key_vault_uri()

  secret_client = SecretClient(vault_url=key_vault_uri, credential=managed_identity_credential)

  return secret_client

# Define function to convert the given
# secret name to one that is compliant
# with Azure Secrets naming conventions
def protect_secret_name(
      secret_name: str
  ) :

  ### Replace underscores with hyphens, as underscores
  ### are expected in Snowflake service account usernames
  protected_secret_name = secret_name.replace("_", "-")

  return protected_secret_name

# Define function to retrieve secret
# from the secrets vault
def retrieve_secret_from_azure_secrets(
      secret_name: str
  ) :
    
  ### Leverage managed identity to retrieve secrets client
  secret_client = retrieve_secret_client()
  
  ### Convert key pair name to one that is compliant
  ### with Azure Secrets naming conventions
  protected_secret_name = protect_secret_name(secret_name)

  ### Retrieve the key pair from the vault
  secret_value = secret_client.get_secret(protected_secret_name).value
  
  return secret_value

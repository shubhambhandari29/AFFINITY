import requests
from azure.identity import ManagedIdentityCredential

# Managed Identity credential
credential = ManagedIdentityCredential()

# Get access token for Microsoft Graph
token = credential.get_token("https://graph.microsoft.com/.default").token
print("TOKEN >>>", token)
user_id = "sh1bhandari@hanover.com"

endpoint = f"https://graph.microsoft.com/v1.0/users/{user_id}/transitiveMemberOf/microsoft.graph.group?$select=id,displayName"

headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

response = requests.get(endpoint, headers=headers)

print(response.status_code)
print(response.json())

import requests


def getAccessToken(tenant_id: str, client_id: str, client_secret: str) -> str:
    """Get Access Using Service Principal Authentication and Graph API"""

    token_url = f"https://login.microsoftonline.com{tenant_id}/oauth2/v2.0/token"

    token_data = {
        'grant_type': 'client_credentials', 'client_id': client_id, 'client_secret': client_secret, 'scope': 'https://graph.microsoft.com/.default'
    }

    response = requests.post(token_url, headers=token_data)
    response.raise_for_status()

    return response.json()['access_token']


def siteAndListIds() -> dict:

    access_token = getAccessToken()

    headers = {
        'Authorization': f"Bearer {access_token}",
        'Accept': 'application/json'
    }

    rootSiteUrl = f"https://graph.microsoft.com/v1.0/sites/{rootUrl}:/teams/{siteName}"

    response = requests.get(
        rootSiteUrl,
        headers=headers
    )

    response.raise_for_status()

    site_data = response.json()

    return {
        site_id: site_data,
        list_id: "List Id"
    }

import requests
from utils.getCredentials import getAccessToken
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession as spark


def GetSharepointListData(tenant_id: str, client_id: str, client_secret: str, site_id: str, list_id: str) -> DataFrame:

    access_token = getAccessToken(
        tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?expand=fields"
    headers = {
        'Authorization': f"Bearer {access_token}",
        'Content-type': 'application/json'
    }

    all_data = []

    try:
        while url:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            batch_items = [
                item['fields'] for item in data.get('value', []) if item.get('fields')
            ]
            all_data.extend(batch_items)
            url = url.get('@odata.nextLink')

        print(f'Total Records Extracted for List: {len(all_data)}')

        df = spark.createDataFrame(all_data)

        return df

    except requests.exceptions.RequestException as e:
        print(f"Request Error: {e}")
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 0


if __name__ == '__main__':
    GetSharepointListData('123a','123a','123a','123a','123a')
import requests
import os
import random
import pandas as pd
from azure.storage.blob import BlobServiceClient

URL = os.getenv('WEBHOOK')
STORAGEACCOUNTURL= os.getenv('STORAGEURL')
STORAGEACCOUNTKEY= os.getenv('STORAGEKEY')
CONTAINERNAME= os.getenv('CONTAINER')
WEBHOOK = os.getenv('WEBHOOK')    

class MusicData:
    def __init__(self, blob_client, df, topic) -> None:
        self.blob_client = blob_client
        self.df = df
        self.topic = topic

    @classmethod
    def blob_to_df(cls, blobname):
        blob_service_client_instance = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)
        blob_client = blob_service_client_instance.get_blob_client(CONTAINERNAME, blobname, snapshot=None)
        blob_data = blob_client.download_blob().content_as_text()
        data = [x.replace('\r','').split(',',maxsplit=2) for x in blob_data.split('\n')[1:] if x not in ['', ',,']]
        df = pd.DataFrame(data)
        df.columns = ['selected', 'no', 'theme']
        blobdf = df.astype({'no': 'int32'})
        return cls(
            blob_client=blob_client,
            df=blobdf,
            topic=random.randint(1,113)
        )

    def select_theme(self):        
        while True:
            if self.df[self.df.no == self.topic]['selected'].values[0] == 'X':
                self.topic = random.randint(1,113)
            else:
                mypayload = {"text":f'THIS WEEK\'S TOPIC:\n`{self.df[self.df.no == self.topic]["theme"].values[0]}`'}
                self.df.at[self.topic-1, 'selected'] = 'X'
                requests.post(WEBHOOK,json=mypayload)
                break

    def upload_csv(self):
        output = self.df.to_csv(sep=',',header=['Selected', 'No.', 'Theme'],index=False)
        self.blob_client.upload_blob(output, blob_type="BlockBlob", overwrite=True)

import requests
import re


def get_release(self):

    """
    return the most updated version
    """
    
    #client = requests.Session()
    #self.client
    response = self.client.head(self.SRC_URLS[0])
    text = response.headers["Content-Disposition"]
    date = re.findall(r'\d{4}-\d\d-\d\d',text)

    return date[0]


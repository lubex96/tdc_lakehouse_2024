from urllib.parse import quote, urlparse
import requests
import json

class HdlfsResponse:

    def __init__(self, response) -> None:
        self.statusCode = response.status_code
        self.success = self.__isSuccessful(response)
        self.body = self.__getPrettyJsonFromResponseBody(response)

    def __isSuccessful(self, response):
        return response.status_code >= 200 and response.status_code < 300
    
    def __getPrettyJsonFromResponseBody(self, response):
        jsonPayload = json.loads(response.text)
        return json.dumps(jsonPayload, indent=4)

class HdlfsClient:

    HDLF_CATALOG_API_BASE_PATH = "/catalog/v2"
    HDLF_CATALOG_API_SHARES_BASE_PATH = HDLF_CATALOG_API_BASE_PATH + "/shares"

    def __init__(self, hdlfEndpoint, clientCertPath, clientKeyPath) -> None:
        self.hdlfEndpoint = hdlfEndpoint if hdlfEndpoint.startswith("https://") else f"https://{hdlfEndpoint}"
        self.clientCert = (clientCertPath, clientKeyPath)
        self.fileContainer = self.__extractFileContainer(self.hdlfEndpoint)

    def listShares(self):
        requestPath = self.HDLF_CATALOG_API_SHARES_BASE_PATH
        requestUrl = self.hdlfEndpoint + requestPath
        requestHeaders = self.__getDefaultHeaders()
        response = requests.get(requestUrl, headers=requestHeaders, cert=self.clientCert)

        return HdlfsResponse(response)

    def createShare(self, shareName):
        encodedShareName = quote(shareName)
        requestPath = f"{self.HDLF_CATALOG_API_SHARES_BASE_PATH}/{encodedShareName}"
        requestUrl = self.hdlfEndpoint + requestPath
        requestHeaders = self.__getDefaultHeaders()
        response = requests.put(requestUrl, headers=requestHeaders, cert=self.clientCert)

        return HdlfsResponse(response)

    def createShareTable(self, shareName, shareSchemaName, shareTableName, deltaTableLocation):
        encodedShareName = quote(shareName)
        encodedSchemaName = quote(shareSchemaName)
        encodedTableName = quote(shareTableName)
        requestPath = f"{self.HDLF_CATALOG_API_SHARES_BASE_PATH}/{encodedShareName}/schemas/{encodedSchemaName}/tables/{encodedTableName}"
        requestUrl = self.hdlfEndpoint + requestPath
        requestHeaders = self.__getDefaultHeaders()
        requestBody = { "location": deltaTableLocation }
        response = requests.put(requestUrl, headers=requestHeaders, json=requestBody, cert=self.clientCert)

        return HdlfsResponse(response)

    def listShareTables(self, shareName, shareSchemaName):
        encodedShareName = quote(shareName)
        encodedSchemaName = quote(shareSchemaName)
        requestPath = f"{self.HDLF_CATALOG_API_SHARES_BASE_PATH}/{encodedShareName}/schemas/{encodedSchemaName}/tables"
        requestUrl = self.hdlfEndpoint + requestPath
        requestHeaders = self.__getDefaultHeaders()
        response = requests.get(requestUrl, headers=requestHeaders, cert=self.clientCert)

        return HdlfsResponse(response)

    def __extractFileContainer(self, hdlfEndpoint):
        parsedUrl = urlparse(hdlfEndpoint)
        host = parsedUrl.hostname

        return host.split('.')[0]
    
    def __getDefaultHeaders(self):
        return {
            "X-SAP-FileContainer": self.fileContainer
        }

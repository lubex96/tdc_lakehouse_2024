import jwt, json, argparse
from urllib.parse import urlparse
from datetime import datetime, timedelta
from cryptography import x509

def getX5c(certData):
    str = certData.decode("UTF-8")
    withoutHeaderFooter = str.replace("-----BEGIN CERTIFICATE-----\n", "").replace("\n-----END CERTIFICATE-----", "")
    return "".join(withoutHeaderFooter.splitlines())

def generateEntitlements(authorizedShares):
    entitlement = {
        "resources": [ f"share:{share}" for share in authorizedShares ],
        "privileges": [ "browse", "open" ]
    }
    entitlements = [ entitlement ]

    return json.dumps(entitlements)

def extractHostFromUrl(url):
    parsedUrl = urlparse(url)
    return parsedUrl.hostname if len(parsedUrl.scheme) > 0 else url

def generateJwt(audience, clientCertPath, privateKeyPath, authorizedShares, expirationHours):
    with open(clientCertPath, "rb") as keyFile:
        clientCertData = keyFile.read()
        clientCert = x509.load_pem_x509_certificate(clientCertData)

    with open(privateKeyPath, "rb") as keyFile:
        privateKeyData = keyFile.read()

    now = datetime.now()
    nowEpoch = int(now.timestamp())
    expirationTime = now + timedelta(hours=expirationHours)
    expirationTimeEpoch = int(expirationTime.timestamp())
    clientCertDataStr = getX5c(clientCertData)
    clientCertIssuerSubjectDN = clientCert.subject.rfc4514_string()
    clientCertCommonName = clientCert.subject.get_attributes_for_oid(x509.NameOID.COMMON_NAME)[0].value

    headers = {
        "x5c": [ clientCertDataStr ]
    }

    payload = {
        "aud": audience,
        "exp": expirationTimeEpoch,
        "iat": nowEpoch,
        "nbf": nowEpoch,
        "iss": clientCertIssuerSubjectDN,
        "sub": clientCertCommonName,
        "com.sap.bds/entitlements": generateEntitlements(authorizedShares)
    }

    expirationTimeStr = expirationTime.isoformat(timespec='milliseconds')

    return jwt.encode(payload, headers=headers, key=privateKeyData, algorithm="RS256"), expirationTimeStr

def generateDeltaSharingProfile(hdlfEndpoint, bearerToken, expirationTime):
    hdlfSharingEndpoint = hdlfEndpoint.replace(".files.", ".sharing.")

    return {
        "endpoint": f"https://{hdlfSharingEndpoint}",
        "bearerToken": bearerToken,
        "shareCredentialsVersion": 1,
        "expirationTime": expirationTime
    }

def saveDeltaSharingProfileToFile(deltaSharingProfile, outputFilePath):
    print(f"Saving Delta Sharing profile file to [{outputFilePath}]")

    with open(outputFilePath, "w") as outFile:
        json.dump(deltaSharingProfile, outFile, indent=4)

def main():
    parser = argparse.ArgumentParser(description="Generate Delta Sharing profile for HDL Files Delta Sharing")
    
    parser.add_argument('--hdlfEndpoint', type=str, required=True, help="HANA DataLake Files endpoint (without the URL scheme).")
    parser.add_argument('--clientCertPath', type=str, required=True, help="Path to the client certificate.")
    parser.add_argument('--clientKeyPath', type=str, required=True, help="Path to the client key.")
    parser.add_argument('--authorizedShares', type=str, nargs='+', required=True, help="List of authorized shares (space-separated).")
    parser.add_argument('--outputFilePath', type=str, required=True, help="Path to the output Delta Sharing profile file.")
    parser.add_argument('--expirationTime', type=int, default=1, help="Expiration time in hours (default: 1 hour).")
    
    args = parser.parse_args()
    hdlfFilesEndpoint = extractHostFromUrl(args.hdlfEndpoint)
    outputFilePath = args.outputFilePath

    jwt, expTime = generateJwt(hdlfFilesEndpoint, args.clientCertPath, args.clientKeyPath, args.authorizedShares, args.expirationTime)
    deltaSharingProfile = generateDeltaSharingProfile(hdlfFilesEndpoint, jwt, expTime)
    saveDeltaSharingProfileToFile(deltaSharingProfile, outputFilePath)

if __name__ == '__main__':
    main()

import argparse
from src.HdlfsClient import HdlfsClient

def fail(message, response):
    print(f"[ERROR] {message}")
    print("Response: ")
    print(f"  - Status: {response.statusCode}")
    print(f"  - Payload:")
    print(response.body)
    exit(1)

def main():
    parser = argparse.ArgumentParser(description="Share a Delta table using HDL Files Delta Sharing")

    parser.add_argument('--hdlfEndpoint', type=str, required=True, help='HANA DataLake Files endpoint')
    parser.add_argument('--clientCertPath', type=str, required=True, help='Path to the client certificate')
    parser.add_argument('--clientKeyPath', type=str, required=True, help='Path to the client key')
    parser.add_argument('--shareName', type=str, required=True, help='Name of the Share')
    parser.add_argument('--shareSchemaName', type=str, required=True, help='Name of the Schema in the Share')
    parser.add_argument('--shareTableName', type=str, required=True, help='Name of the Table in the Share Schema')
    parser.add_argument('--deltaTableLocation', type=str, required=True, help='Location of the Delta table in the HDLF container')

    args = parser.parse_args()

    print(f"[INFO] Initializing HdlfsClient")
    hdlfsClient = HdlfsClient(args.hdlfEndpoint, args.clientCertPath, args.clientKeyPath)

    print(f"[INFO] Creating Share(name={args.shareName})")
    createShareResponse = hdlfsClient.createShare(args.shareName)

    if createShareResponse.success:
        print(f"[INFO] Share(name={args.shareName}) successfully created")
    else:
        fail(f"Failed to create Share(name={args.shareName})", createShareResponse)

    print(f"[INFO] Creating ShareTable(name={args.shareTableName}, schema={args.shareSchemaName}, share={args.shareName})")
    createShareTableResponse = hdlfsClient.createShareTable(args.shareName, args.shareSchemaName, args.shareTableName, args.deltaTableLocation)

    if createShareTableResponse.success:
        print(f"[INFO] ShareTable(name={args.shareTableName}, schema={args.shareSchemaName}, share={args.shareName}) successfully created")
    else:
        fail(f"Failed to create ShareTable(name={args.shareTableName}, schema={args.shareSchemaName}, share={args.shareName})", createShareTableResponse)

if __name__ == '__main__':
    main()

from coinbase.rest import RESTClient
import json

def save_json(data, filename):
    """
    Saves data as a JSON file.
    """
    try:
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
        print(f"✅ Data saved in {filename}")
        
    except Exception as e:
        print(f"⚠️ Error saving file: {e}")

def init_config():
    api_key = "organizations/dc0f55cc-f894-4f10-befa-a0b9e94fa294/apiKeys/084c7500-6472-4600-be95-6189b52ea4d7"
    api_secret = "-----BEGIN EC PRIVATE KEY-----\nMHcCAQEEID9LJOhj6+HjzxJcOAMFgUg8AwLHs43cEvdW5A0da+ivoAoGCCqGSM49\nAwEHoUQDQgAEmIiBIsQ6pXNbqJkWXvIn4TL2u5dufTL43n/dLGjK9uEiBgzOnu2k\nVoS/QoCfWyb1U9aUl8VMfZjJa23rjVKxoA==\n-----END EC PRIVATE KEY-----\n"
    return RESTClient(api_key=api_key, api_secret=api_secret)

    

if __name__ == "__main__":
    print("🔄 Init_config...")
    client = init_config()

    print("🔄 Retrieving products...")
    products = client.get('/api/v3/brokerage/products')
    save_json(products, "coinbase/resources/products.json")

    print("✅ Process complete!")




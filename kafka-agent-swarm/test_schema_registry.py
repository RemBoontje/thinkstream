import os
from dotenv import load_dotenv
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema

load_dotenv()

def test_registry():
    print("📜 Testing Schema Registry Connection...")
    
    # 1. Configuration
    conf = {
        'url': os.getenv("SCHEMA_REGISTRY_URL"),
        'basic.auth.user.info': f"{os.getenv('SCHEMA_REGISTRY_USER')}:{os.getenv('SCHEMA_REGISTRY_PASSWORD')}"
    }

    try:
        client = SchemaRegistryClient(conf)
        
        # 2. Test: List subjects (See what's already there)
        subjects = client.get_subjects()
        print(f"✅ Connection Successful! Found {len(subjects)} subjects.")
        
        # 3. Test: Register a dummy schema
        schema_str = """
        {
            "type": "record",
            "name": "ConnectionTest",
            "fields": [
                {"name": "status", "type": "string"}
            ]
        }
        """
        schema = Schema(schema_str, schema_type="AVRO")
        schema_id = client.register_schema("test-connection-subject", schema)
        print(f"✅ Successfully registered schema ID: {schema_id}")
        
    except Exception as e:
        print(f"❌ Registry Failed: {e}")
        print("\nTIP: Check if your SCHEMA_REGISTRY_URL starts with https:// and has the correct port.")

if __name__ == "__main__":
    test_registry()
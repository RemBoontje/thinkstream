import uuid
from utils import get_producer

# The topic where all agents will talk
TOPIC = "agent-workflow-state"

def send_job(query):
    print(f"🚀 Connecting to Relay Core...")
    producer = get_producer()
    
    job_id = str(uuid.uuid4())
    
    # Matches the Avro Schema exactly
    job_state = {
        "job_id": job_id,
        "current_stage": "STARTED",
        "user_query": query,
        "research_facts": None,
        "final_draft": None,
        "error_log": None
    }
    
    print(f"📦 Sending Job {job_id}: '{query}'")
    
    # Produce!
    # Note: We use job_id as the key to ensure order (same ID = same partition)
    producer.produce(topic=TOPIC, key=job_id, value=job_state)
    producer.flush()
    print(f"✅ Job sent successfully!")

if __name__ == "__main__":
    send_job("Explain how Fiber Optic cables work")
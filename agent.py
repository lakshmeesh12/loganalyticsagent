import os
import time
import logging
from openai import OpenAI
from dotenv import load_dotenv

# Load .env file
load_dotenv()
OPENAI_API_KEY = os.getenv("OPEN_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("OPEN_API_KEY not set in environment")

client = OpenAI(api_key=OPENAI_API_KEY)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("AGENT")


ERROR_LOG = "snowflake_errors.log"
last_position = 0


def analyze_error(message: str) -> str:
    """
    Use OpenAI's GPT to analyze the error message and provide remediation steps.
    """
    prompt = (
        "You are a CloudWatch error analysis assistant. "
        "Given the following error log entry, analyze the root cause and provide clear, actionable remediation steps:\n\n"
        f"{message}\n\n"
    )
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=500,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"OpenAI API error: {e}")
        return f"Failed to analyze error: {e}"


def agent_loop():
    global last_position
    logger.info("Starting AI agent for error analysis...")

    if not os.path.exists(ERROR_LOG):
        open(ERROR_LOG, 'a').close()

    try:
        while True:
            with open(ERROR_LOG, 'r', encoding='utf-8') as f:
                f.seek(last_position)
                new_entries = f.read()
                last_position = f.tell()

            if new_entries:
                errors = new_entries.strip().split('-' * 60)
                for err in errors:
                    err = err.strip()
                    if not err:
                        continue
                    logger.info("New error detected, sending to AI agent...")
                    remediation = analyze_error(err)
                    with open("remediation.log", 'a', encoding='utf-8') as rem_file:
                        rem_file.write(f"Error:\n{err}\nRemediation:\n{remediation}\n{'='*60}\n")
                    logger.info("Remediation written to remediation.log")

                    print(f"\nRemediation for error:\n{remediation}\n{'='*60}", flush=True)

            time.sleep(10)
    except KeyboardInterrupt:
        logger.info("Agent stopped by user")


if __name__ == "__main__":
    agent_loop()

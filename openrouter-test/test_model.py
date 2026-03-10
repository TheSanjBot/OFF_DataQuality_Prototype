import os
from openai import OpenAI

client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=os.getenv("OPENROUTER_API_KEY"),
)

response = client.chat.completions.create(
    model="arcee-ai/trinity-large-preview:free",
    messages=[
        {"role": "user", "content": "Explain machine learning simply"}
    ],
)

print(response.choices[0].message.content)
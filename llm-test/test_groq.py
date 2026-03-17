import os

from openai import OpenAI


def main() -> None:
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY is not set. Set it in your shell before running.")

    client = OpenAI(
        api_key=api_key,
        base_url="https://api.groq.com/openai/v1",
    )

    response = client.chat.completions.create(
        model="openai/gpt-oss-120b",
        messages=[{"role": "user", "content": "Explain quantum tunnelling in one paragraph"}],
        temperature=0.7,
        max_tokens=500,
    )
    print(response.choices[0].message.content)


if __name__ == "__main__":
    main()

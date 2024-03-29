import openai
import os
import pandas as pd
import time

# Obtén tu API key de OpenAI
api_key = os.getenv("CHAT_GPT")
openai.api_key = api_key

def get_completion(prompt):
    messages = [{"role": "user", "content": prompt}]
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=messages,
        temperature=0,
    )
    return response.choices[0].message["content"]
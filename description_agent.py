import requests
import json
import re

OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "mistral"
NOW_YEAR = "2025"

def create_short_description(title, subtitle, author, description, publish_year):

    try:

        if not is_valid_description(description):
            return None

        prompt = make_prompt(title, subtitle, author, description, publish_year)
        
        print(prompt)

        response = requests.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "num_gpu_layers": 32
                }
            },
            timeout=300
        )
        result = response.json().get("response", "").strip()

        # Check if it's too long
        if len(result) > 800:
            print(f"⚠️ Output too long ({len(result)} chars). Sending for shortening.")
            short_prompt = make_shorten_prompt(result)
            response = requests.post(
                OLLAMA_URL,
                json={
                    "model": OLLAMA_MODEL,
                    "prompt": short_prompt,
                    "stream": False,
                    "options": {
                        "num_gpu_layers": 32
                    }
                },
                timeout=300
            )
            
        result = response.json().get("response", "").strip()

        return result

    except Exception as e:
        print(f"❌ Failed to process: {e}")


# Helper to detect junk descriptions
def is_valid_description(desc):
    if not desc:
        return False
    desc = desc.strip()
    if len(desc) < 30 or len(desc.split()) < 5:
        return False
    return True

# Helper to sanitize Redis key
def sanitize_key(s):
    return re.sub(r'[^a-zA-Z0-9_-]', '_', s.strip())[:100]

# Helper to generate prompt
#def make_prompt(title, subtitle, author, original_desc):
#    return (
#        f"Rewrite the following book description to be compelling and under 500 characters. "
#
#        f"Title: {title}\n"
#        f"Subtitle: {subtitle}\n"
#        f"Author: {author}\n\n"
#        f"Original Description:\n{original_desc}"
#)

def make_prompt(title, subtitle, author, original_desc, publish_year):

    try:
        pub_year = int(publish_year)
    except:
        pub_year = None

    age_note = ""
    if pub_year and pub_year < int(NOW_YEAR) - 10:
        age_note = (
            f"The current year is {NOW_YEAR}. This book was originally published in {pub_year}, "
            f"so avoid presenting it as a contemporary or current work. Do not imply that it addresses modern events or issues. "
            f"However avoid explicitly mentioning the publish date."
        )


    return (
        f"Rewrite the following book description to make it compelling and under 500 characters. "
        f"Focus on engaging the reader, not listing metadata. Avoid any mention of edition info or sales terms."
        f"Write in a natural, engaging tone and focus on captivating the reader. "
        f"Do NOT use emoji, symbols, or any decorative characters. "
        f"Do **not** include any labels like 'Title:', 'Author:', or metadata."
        f"{age_note}"
        f"Just return the rewritten description only, in plain text.\n\n"
        f"Title: {title}\n"
        f"Subtitle: {subtitle}\n"
        f"Author: {author}\n\n"
        f"Original Description:\n{original_desc}\n\n"
        f"Rewritten Description:"
    )

def make_shorten_prompt(text):
    return (
        f"Revise the following text to be under 600 characters while preserving tone and clarity. "
        f"Avoid sales phrases and edition info:\n\n{text}"
    )



   

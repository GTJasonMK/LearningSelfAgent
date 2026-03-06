import asyncio
from backend.src.services.llm.llm_client import LLMClient

async def main():
    client = LLMClient(
        provider='deepseek',
        api_key='sk-3879ded22e48455d85a38d74e8fbab65',
        base_url='https://api.deepseek.com/v1',
        default_model='deepseek-chat',
        strict_mode=True,
    )
    text, tokens = await client.complete_prompt(
        prompt='只回复 OK',
        model='deepseek-chat',
        parameters={'temperature': 0, 'max_tokens': 8},
        timeout=20,
    )
    print({'ok': bool(str(text).strip()), 'preview': str(text).strip()[:80], 'tokens': tokens})

asyncio.run(main())

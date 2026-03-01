import sqlite3, time
from backend.src.services.llm.llm_client import call_llm, resolve_default_model, resolve_default_provider
conn = sqlite3.connect('backend/data/agent.db')
row = conn.execute('select prompt from llm_records where id=8').fetchone()
conn.close()
prompt = row[0] if row and row[0] else ''
print('prompt_len', len(prompt))
provider = resolve_default_provider(); model = resolve_default_model()
print('provider', provider, 'model', model)
start = time.time()
try:
    content, tokens = call_llm(prompt, model, {'temperature':0.2}, provider=provider)
    print('elapsed', time.time()-start)
    print('content_len', len(content or ''))
    print('tokens', tokens)
except Exception as exc:
    print('ERR', type(exc).__name__, str(exc))
    print('elapsed', time.time()-start)

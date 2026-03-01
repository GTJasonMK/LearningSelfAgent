from backend.src.services.llm.llm_client import call_llm, resolve_default_model, resolve_default_provider
import time
prompt = 'reply OK only'
model = resolve_default_model()
provider = resolve_default_provider()
print('provider', provider, 'model', model)
start = time.time()
try:
    content, tokens = call_llm(prompt, model, {'temperature': 0.0}, provider=provider)
    print('elapsed', time.time() - start)
    print('content', content[:120])
    print('tokens', tokens)
except Exception as exc:
    print('ERR', type(exc).__name__, str(exc), 'elapsed', time.time() - start)

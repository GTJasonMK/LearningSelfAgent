export function shouldFinalizePendingFinal(options = {}) {
  const evalFinal = options?.evalFinal === true;
  if (evalFinal) return true;

  const terminal = options?.terminal === true;
  if (!terminal) return false;

  // 没有评估记录时，不应继续卡在“评估中”门控，终态后可直接收敛最终输出。
  const hasReview = options?.hasReview === true;
  if (!hasReview) return true;

  const ageMs = Number(options?.ageMs);
  const timeoutMs = Number(options?.timeoutMs);
  if (!Number.isFinite(ageMs) || !Number.isFinite(timeoutMs) || timeoutMs < 0) {
    return false;
  }

  return ageMs > timeoutMs;
}

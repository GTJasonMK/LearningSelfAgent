// 桌宠图片加载兜底：
// - 避免单一 src 失效时整块空白；
// - 在悬浮窗与世界页复用同一套候选顺序。

export const PET_IMAGE_CANDIDATES = [
  "../../public/image/leim.png",
  "../../public/image/荭乐Q版.png",
  "../../public/image/Gemini_Generated_Image_85c1if85c1if85c1.png"
];

function normalizeCandidatePath(rawPath) {
  return String(rawPath || "").trim();
}

function normalizeCurrentSrc(imgEl) {
  if (!imgEl) return "";
  const attrSrc = String(imgEl.getAttribute("src") || "").trim();
  if (attrSrc) return attrSrc;
  return String(imgEl.src || "").trim();
}

function setVisibleStyle(imgEl) {
  if (!imgEl) return;
  imgEl.style.display = "block";
  imgEl.style.visibility = "visible";
  imgEl.style.opacity = "1";
}

export function bindPetImageFallback(imgEl, candidates = PET_IMAGE_CANDIDATES) {
  if (!imgEl) return;

  const candidateList = Array.from(
    new Set((Array.isArray(candidates) ? candidates : []).map(normalizeCandidatePath).filter(Boolean))
  );
  if (!candidateList.length) {
    setVisibleStyle(imgEl);
    return;
  }

  setVisibleStyle(imgEl);

  let index = candidateList.findIndex((path) => path === normalizeCurrentSrc(imgEl));
  if (index < 0) {
    index = 0;
    imgEl.setAttribute("src", candidateList[0]);
  }

  function tryNextCandidate() {
    index += 1;
    if (index >= candidateList.length) {
      // 全部失败时仍保留可见占位，避免“完全消失”。
      setVisibleStyle(imgEl);
      imgEl.setAttribute("alt", "pet-image-load-failed");
      return;
    }
    imgEl.setAttribute("src", candidateList[index]);
  }

  imgEl.addEventListener("error", tryNextCandidate);
  imgEl.addEventListener("load", () => {
    setVisibleStyle(imgEl);
  });
}


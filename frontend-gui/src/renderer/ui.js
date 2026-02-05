import { INPUT_ATTRS, UI_TEXT, UI_THEME } from "./constants.js";

export function applyText(root = document) {
  root.querySelectorAll("[data-i18n]").forEach((el) => {
    const key = el.dataset.i18n;
    if (UI_TEXT[key]) {
      el.textContent = UI_TEXT[key];
    }
  });

  root.querySelectorAll("[data-placeholder]").forEach((el) => {
    const key = el.dataset.placeholder;
    if (UI_TEXT[key]) {
      el.setAttribute("placeholder", UI_TEXT[key]);
    }
  });

  root.querySelectorAll("[data-title]").forEach((el) => {
    const key = el.dataset.title;
    if (UI_TEXT[key]) {
      el.setAttribute("title", UI_TEXT[key]);
    }
  });

  root.querySelectorAll("[data-attrs]").forEach((el) => {
    const key = el.dataset.attrs;
    const attrs = INPUT_ATTRS[key];
    if (!attrs) return;
    Object.entries(attrs).forEach(([attr, value]) => {
      el.setAttribute(attr, String(value));
    });
  });

  Object.entries(UI_THEME).forEach(([key, value]) => {
    document.documentElement.style.setProperty(`--${key.toLowerCase()}`, value);
  });
}

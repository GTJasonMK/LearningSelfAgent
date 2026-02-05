import { applyText } from "./ui.js";
import { UI_TEXT } from "./constants.js";

document.title = UI_TEXT.ENTRY_TITLE;
applyText();

const panelBtn = document.getElementById("open-panel");
const petBtn = document.getElementById("open-pet");

if (panelBtn) {
  panelBtn.addEventListener("click", () => {
    window.location.href = "panel.html";
  });
}

if (petBtn) {
  petBtn.addEventListener("click", () => {
    window.open("pet.html", "_blank");
  });
}

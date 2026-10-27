(() => {
  "use strict";
  const STORAGE_KEY = "scamGuiTheme";
  const root = document.documentElement;
  let activeViewTransition = null;
  let fallbackTimer = null;

  function applyTheme(mode) {
    const selectedMode = mode === "dark" ? "dark" : "light";
    root.dataset.theme = selectedMode;
    document.body.classList.toggle("theme-dark", selectedMode === "dark");
    root.classList.remove("theme-dark-preload");
    try { localStorage.setItem(STORAGE_KEY, selectedMode); } catch (_error) {}

    const select = document.getElementById("themeModeSelect");
    if (select) select.value = selectedMode;
    const toggle = document.getElementById("themeModeToggle");
    if (toggle) {
      toggle.innerHTML = selectedMode === "dark"
        ? '<i class="fas fa-sun"></i>'
        : '<i class="fas fa-moon"></i>';
      toggle.title = selectedMode === "dark" ? "Switch to light mode" : "Switch to dark mode";
      toggle.setAttribute("aria-pressed", selectedMode === "dark" ? "true" : "false");
    }
    if (document.body.classList.contains("page-insights") &&
        typeof window._repaintInsightsChartsForTheme === "function") {
      window.setTimeout(window._repaintInsightsChartsForTheme, 40);
    }
  }

  function setGuiTheme(mode, animate = true) {
    const selectedMode = mode === "dark" ? "dark" : "light";
    const reduceMotion = window.matchMedia &&
      window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    if (!animate || reduceMotion) {
      if (fallbackTimer) window.clearTimeout(fallbackTimer);
      root.classList.remove("theme-switching");
      applyTheme(selectedMode);
      return;
    }
    if (typeof document.startViewTransition === "function") {
      if (activeViewTransition) activeViewTransition.skipTransition();
      try {
        const transition = document.startViewTransition(() => applyTheme(selectedMode));
        activeViewTransition = transition;
        transition.finished.then(
          () => { if (activeViewTransition === transition) activeViewTransition = null; },
          () => { if (activeViewTransition === transition) activeViewTransition = null; },
        );
      } catch (_error) { applyTheme(selectedMode); }
      return;
    }
    if (fallbackTimer) window.clearTimeout(fallbackTimer);
    root.classList.add("theme-switching");
    applyTheme(selectedMode);
    fallbackTimer = window.setTimeout(() => {
      root.classList.remove("theme-switching");
      fallbackTimer = null;
    }, 180);
  }

  function toggleGuiTheme() {
    const currentMode = root.dataset.theme ||
      (document.body.classList.contains("theme-dark") ? "dark" : "light");
    setGuiTheme(currentMode === "dark" ? "light" : "dark");
  }

  function initGuiTheme() {
    let mode = document.body.classList.contains("page-insights") ? "dark" : "light";
    try { mode = localStorage.getItem(STORAGE_KEY) || mode; } catch (_error) {}
    setGuiTheme(mode, false);
  }

  window.setGuiTheme = setGuiTheme;
  window.toggleGuiTheme = toggleGuiTheme;
  initGuiTheme();
})();

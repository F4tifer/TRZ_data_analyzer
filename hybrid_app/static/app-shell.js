(function () {
  function applyInitialTheme() {
    const saved = localStorage.getItem("theme");
    const preferredDark =
      window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
    const theme = saved || (preferredDark ? "dark" : "light");
    document.documentElement.setAttribute("data-theme", theme);
  }

  function initThemeToggle() {
    const toggle = document.getElementById("theme-toggle");
    if (!toggle) return;

    function syncLabel() {
      const current = document.documentElement.getAttribute("data-theme") || "light";
      toggle.textContent = current === "dark" ? "Light mode" : "Dark mode";
    }

    toggle.addEventListener("click", function () {
      const html = document.documentElement;
      const current = html.getAttribute("data-theme") || "light";
      const next = current === "dark" ? "light" : "dark";
      html.setAttribute("data-theme", next);
      localStorage.setItem("theme", next);
      document.dispatchEvent(new CustomEvent("app:theme-changed", { detail: { theme: next } }));
      syncLabel();
    });

    syncLabel();
  }

  function initToasts() {
    const host = document.getElementById("app-toasts");
    if (!host) return;

    window.showToast = function (message, level) {
      const item = document.createElement("div");
      item.className = "callout panel " + (level === "error" ? "callout-error" : "");
      item.style.margin = "0 0 8px 0";
      item.textContent = message || "Done.";
      host.appendChild(item);
      setTimeout(function () {
        item.remove();
      }, 2600);
    };

    document.body.addEventListener("htmx:responseError", function () {
      window.showToast("Request failed. Check error details below.", "error");
    });
    document.body.addEventListener("toast-success", function (event) {
      const message = event && event.detail && event.detail.message;
      window.showToast(message || "Changes were applied successfully.", "success");
    });
    document.body.addEventListener("toast-info", function (event) {
      const message = event && event.detail && event.detail.message;
      window.showToast(message || "Action finished. Review next step hints.", "info");
    });
  }

  function initGlobalLoading() {
    const overlay = document.getElementById("global-loading");
    const text = document.getElementById("global-loading-text");
    if (!overlay || !text) return;

    let pending = 0;
    let hideTimer = null;

    function skipGlobalLoading(elt) {
      try {
        return !!(elt && elt.closest && elt.closest("[data-no-loading='true']"));
      } catch (_error) {
        return false;
      }
    }

    function setOverlayVisible(on) {
      overlay.classList.toggle("is-visible", on);
      overlay.setAttribute("aria-busy", on ? "true" : "false");
      overlay.setAttribute("aria-hidden", on ? "false" : "true");
    }

    function show(msg) {
      if (hideTimer) {
        clearTimeout(hideTimer);
        hideTimer = null;
      }
      text.textContent = msg || "Loading data...";
      setOverlayVisible(true);
    }

    function hideSoon() {
      if (hideTimer) clearTimeout(hideTimer);
      hideTimer = setTimeout(function () {
        setOverlayVisible(false);
      }, 180);
    }

    document.body.addEventListener("htmx:beforeRequest", function (evt) {
      const elt = evt.detail && evt.detail.elt;
      const requestConfig = evt.detail && evt.detail.requestConfig;
      const triggeringEvent = requestConfig && requestConfig.triggeringEvent;
      const eventType = triggeringEvent && typeof triggeringEvent.type === "string"
        ? triggeringEvent.type
        : "";
      const isUserAction = eventType === "click" || eventType === "submit";
      if (skipGlobalLoading(elt)) {
        if (evt.detail) evt.detail._skipGlobalLoading = true;
        return;
      }
      // Keep background HTMX refreshes silent; show overlay only for user-initiated actions.
      if (!isUserAction) {
        if (evt.detail) evt.detail._skipGlobalLoading = true;
        return;
      }
      pending += 1;
      show("Loading data...");
    });

    function finishHtmx(evt) {
      const d = evt && evt.detail;
      if (d && (d._skipGlobalLoading || skipGlobalLoading(d.elt))) return;
      pending = Math.max(0, pending - 1);
      if (pending === 0) hideSoon();
    }

    document.body.addEventListener("htmx:afterRequest", finishHtmx);
    document.body.addEventListener("htmx:responseError", finishHtmx);
    document.body.addEventListener("htmx:sendError", finishHtmx);
    document.body.addEventListener("htmx:timeout", finishHtmx);
    document.body.addEventListener("htmx:abort", finishHtmx);

    document.addEventListener(
      "submit",
      function (evt) {
        const form = evt.target;
        if (!(form instanceof HTMLFormElement)) return;
        if (form.matches("[data-no-loading='true']")) return;
        const isHtmx =
          form.hasAttribute("hx-get") ||
          form.hasAttribute("hx-post") ||
          form.hasAttribute("hx-put") ||
          form.hasAttribute("hx-delete");
        if (isHtmx) return;
        show("Loading data...");
      },
      true
    );

    document.addEventListener(
      "click",
      function (evt) {
        if (evt.defaultPrevented) return;
        if (evt.button !== 0) return;
        if (evt.metaKey || evt.ctrlKey || evt.shiftKey || evt.altKey) return;
        const t = evt.target;
        const a = t && t.closest ? t.closest("a[href]") : null;
        if (!a || !(a instanceof HTMLAnchorElement)) return;
        if (skipGlobalLoading(a)) return;
        if (a.hasAttribute("download")) return;
        const tgt = (a.getAttribute("target") || "").trim().toLowerCase();
        if (tgt && tgt !== "_self") return;
        const hrefAttr = (a.getAttribute("href") || "").trim();
        if (!hrefAttr || hrefAttr === "#") return;
        if (/^javascript:/i.test(hrefAttr)) return;
        let url;
        try {
          url = new URL(a.href);
        } catch (_error) {
          return;
        }
        if (url.protocol === "mailto:" || url.protocol === "tel:") return;
        if (url.origin !== window.location.origin) return;
        if (/\/export\/?$/.test(url.pathname)) return;
        const cur = new URL(window.location.href);
        if (url.pathname === cur.pathname && url.search === cur.search && url.hash !== cur.hash) {
          return;
        }
        show("Loading data...");
      },
      true
    );

    window.addEventListener("pageshow", function () {
      pending = 0;
      if (hideTimer) {
        clearTimeout(hideTimer);
        hideTimer = null;
      }
      setOverlayVisible(false);
    });
  }

  function initRunErrorDialog() {
    const dlg = document.getElementById("run-error-dialog");
    const pre = document.getElementById("run-error-dialog-pre");
    const closeBtn = document.getElementById("run-error-dialog-close");
    if (!dlg || !pre) return;

    function openFromButton(btn) {
      const raw = btn.getAttribute("data-error");
      let text = "";
      if (raw != null && raw !== "") {
        try {
          text = JSON.parse(raw);
        } catch (_error) {
          text = raw;
        }
      }
      if (typeof text !== "string") text = String(text || "");
      if (!text.trim()) text = "No error text is stored in the database.";
      pre.textContent = text;
      if (typeof dlg.showModal === "function") dlg.showModal();
    }

    document.body.addEventListener("click", function (e) {
      const btn = e.target && e.target.closest && e.target.closest(".run-fail-open-btn");
      if (!btn) return;
      e.preventDefault();
      openFromButton(btn);
    });

    if (closeBtn) {
      closeBtn.addEventListener("click", function () {
        dlg.close();
      });
    }
    dlg.addEventListener("click", function (e) {
      if (e.target === dlg) dlg.close();
    });
  }

  function initAppInfoDialog() {
    const openBtn = document.getElementById("app-info-open");
    const closeBtn = document.getElementById("app-info-close");
    const dialog = document.getElementById("app-info-dialog");
    if (!openBtn || !dialog) return;

    openBtn.addEventListener("click", function () {
      if (typeof dialog.showModal === "function") dialog.showModal();
    });
    if (closeBtn) {
      closeBtn.addEventListener("click", function () {
        dialog.close();
      });
    }
    dialog.addEventListener("click", function (e) {
      if (e.target === dialog) dialog.close();
    });
  }

  applyInitialTheme();
  document.addEventListener("DOMContentLoaded", function () {
    initThemeToggle();
    initToasts();
    initGlobalLoading();
    initRunErrorDialog();
    initAppInfoDialog();
  });
})();

/**
 * Fullscreen for Plotly chart panels (data-chart-panel).
 * Re-initializes after HTMX swaps.
 * Plotly charts in #analysis-results: data comes from #run-detail-charts-json (HTMX does not run inline scripts in swapped HTML reliably).
 */
(function () {
  function isDarkTheme() {
    return (document.documentElement.getAttribute("data-theme") || "light") === "dark";
  }

  function themedPlotLayout(layout) {
    var rootStyles = getComputedStyle(document.documentElement);
    var fg = (rootStyles.getPropertyValue("--gh-fg-default") || "").trim() || "#1f2328";
    var bg = (rootStyles.getPropertyValue("--gh-bg-canvas") || "").trim() || "#ffffff";
    var plotBg = (rootStyles.getPropertyValue("--gh-bg-subtle") || "").trim() || "#f6f8fa";
    var grid = (rootStyles.getPropertyValue("--gh-border-default") || "").trim() || "#d0d7de";
    var merged = Object.assign({}, layout || {});
    merged.template = isDarkTheme() ? "plotly_dark" : "plotly_white";
    merged.paper_bgcolor = bg;
    merged.plot_bgcolor = plotBg;
    merged.font = Object.assign({}, merged.font || {}, { color: fg });
    merged.xaxis = Object.assign({}, merged.xaxis || {}, { gridcolor: grid, zerolinecolor: grid });
    merged.yaxis = Object.assign({}, merged.yaxis || {}, { gridcolor: grid, zerolinecolor: grid });
    return merged;
  }

  function applyThemeToRenderedPlots(scope) {
    if (!window.Plotly) return;
    (scope || document).querySelectorAll(".chart-plot-area").forEach(function (el) {
      if (!el || !el._fullLayout) return;
      try {
        Plotly.relayout(el, themedPlotLayout({}));
      } catch (_error) {
        /* ignore */
      }
    });
  }

  function setChartFallbackState(root, state) {
    var fallback = root && root.querySelector ? root.querySelector("#chart-render-fallback") : null;
    if (!fallback) return;
    var loadingItems = fallback.querySelectorAll(".chart-fallback-loading");
    var errorItems = fallback.querySelectorAll(".chart-fallback-error");

    if (state === "hidden") {
      fallback.hidden = true;
      fallback.classList.remove("is-loading", "is-error");
      loadingItems.forEach(function (el) {
        el.hidden = true;
      });
      errorItems.forEach(function (el) {
        el.hidden = true;
      });
      return;
    }

    fallback.hidden = false;
    fallback.classList.toggle("is-loading", state === "loading");
    fallback.classList.toggle("is-error", state === "error");
    loadingItems.forEach(function (el) {
      el.hidden = state !== "loading";
    });
    errorItems.forEach(function (el) {
      el.hidden = state !== "error";
    });
  }

  function figureFromPayload(raw) {
    if (raw == null) return null;
    if (typeof raw === "string") {
      try {
        return JSON.parse(raw);
      } catch (e) {
        return null;
      }
    }
    return raw;
  }

  /**
   * @param {HTMLElement} root — typically #analysis-results after HTMX swap
   */
  function renderRunDetailCharts(root) {
    if (!window.Plotly || !root) return;
    setChartFallbackState(root, "loading");
    var ta = root.querySelector("#run-detail-charts-json");
    if (!ta || !ta.value) {
      setChartFallbackState(root, "error");
      return;
    }
    var payload;
    try {
      payload = JSON.parse(ta.value);
    } catch (e) {
      console.error("run-detail-charts-json parse failed", e);
      setChartFallbackState(root, "error");
      return;
    }
    var figPareto = figureFromPayload(payload.pareto);
    var figPie = figureFromPayload(payload.pie);
    var elP = document.getElementById("pareto-chart");
    var elPie = document.getElementById("pie-chart");
    try {
      if (figPareto && elP) {
        Plotly.purge(elP);
        Plotly.newPlot(elP, figPareto.data, themedPlotLayout(figPareto.layout), { displaylogo: false });
      }
      if (figPie && elPie) {
        Plotly.purge(elPie);
        Plotly.newPlot(elPie, figPie.data, themedPlotLayout(figPie.layout), { displaylogo: false });
      }
    } catch (e) {
      console.error("Plotly render failed", e);
      setChartFallbackState(root, "error");
      return;
    }
    setChartFallbackState(root, "hidden");
    if (window.initChartFullscreenPanels) {
      window.initChartFullscreenPanels();
    }
  }

  window.renderRunDetailCharts = renderRunDetailCharts;

  function onAnalysisResultsReady(evt) {
    var t = evt.detail && evt.detail.target;
    if (!t || t.id !== "analysis-results") return;
    requestAnimationFrame(function () {
      renderRunDetailCharts(t);
    });
  }

  /* afterSettle = DOM ready after swap (more reliable than inline script in fragment) */
  document.body.addEventListener("htmx:afterSettle", onAnalysisResultsReady);

  function resizePlotsIn(panel) {
    if (!window.Plotly) return;
    panel.querySelectorAll(".chart-plot-area").forEach(function (el) {
      if (el._fullLayout) {
        try {
          Plotly.relayout(el, { autosize: true, width: null, height: null });
          Plotly.Plots.resize(el);
        } catch (e) {
          /* ignore */
        }
      }
    });
  }

  function bindPanel(panel) {
    if (panel.dataset.chartFsBound) return;
    panel.dataset.chartFsBound = "1";
    const btn = panel.querySelector(".chart-fullscreen-btn");
    if (!btn) return;
    btn.addEventListener("click", function (ev) {
      ev.preventDefault();
      ev.stopPropagation();
      if (document.fullscreenElement === panel) {
        document.exitFullscreen();
      } else {
        panel.requestFullscreen().catch(function () {
          if (window.showToast) window.showToast("Fullscreen is not available in this browser context.", "error");
        });
      }
    });
  }

  function init() {
    document.querySelectorAll("[data-chart-panel]").forEach(bindPanel);
  }

  window.initChartFullscreenPanels = init;

  document.addEventListener("DOMContentLoaded", init);
  document.body.addEventListener("htmx:afterSwap", function () {
    setTimeout(init, 0);
  });
  document.body.addEventListener("htmx:afterSettle", function () {
    setTimeout(init, 0);
  });

  document.addEventListener("fullscreenchange", function () {
    var fs = document.fullscreenElement;
    document.querySelectorAll("[data-chart-panel] .chart-fullscreen-btn").forEach(function (btn) {
      btn.textContent = "⛶";
      btn.setAttribute("aria-label", "Fullscreen");
      btn.setAttribute("title", "Fullscreen");
    });
    if (fs && fs.matches && fs.matches("[data-chart-panel]")) {
      var activeBtn = fs.querySelector(".chart-fullscreen-btn");
      if (activeBtn) {
        activeBtn.textContent = "×";
        activeBtn.setAttribute("aria-label", "Exit fullscreen");
        activeBtn.setAttribute("title", "Exit fullscreen");
      }
      requestAnimationFrame(function () {
        resizePlotsIn(fs);
      });
    } else {
      // After exit fullscreen, run resize on next frames to ensure final layout dimensions are settled.
      requestAnimationFrame(function () {
        requestAnimationFrame(function () {
          document.querySelectorAll("[data-chart-panel]").forEach(resizePlotsIn);
        });
      });
    }
  });

  window.addEventListener("resize", function () {
    if (document.fullscreenElement && document.fullscreenElement.matches("[data-chart-panel]")) {
      resizePlotsIn(document.fullscreenElement);
    }
  });

  document.addEventListener("app:theme-changed", function () {
    applyThemeToRenderedPlots(document);
  });
})();

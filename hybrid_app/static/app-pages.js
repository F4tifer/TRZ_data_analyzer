(function () {
  function isDarkTheme() {
    return (document.documentElement.getAttribute("data-theme") || "light") === "dark";
  }

  function themedPlotLayout(layout) {
    const rootStyles = getComputedStyle(document.documentElement);
    const fg = (rootStyles.getPropertyValue("--gh-fg-default") || "").trim() || "#1f2328";
    const bg = (rootStyles.getPropertyValue("--gh-bg-canvas") || "").trim() || "#ffffff";
    const plotBg = (rootStyles.getPropertyValue("--gh-bg-subtle") || "").trim() || "#f6f8fa";
    const grid = (rootStyles.getPropertyValue("--gh-border-default") || "").trim() || "#d0d7de";
    const merged = Object.assign({}, layout || {});
    merged.template = isDarkTheme() ? "plotly_dark" : "plotly_white";
    merged.paper_bgcolor = bg;
    merged.plot_bgcolor = plotBg;
    merged.font = Object.assign({}, merged.font || {}, { color: fg });
    merged.xaxis = Object.assign({}, merged.xaxis || {}, { gridcolor: grid, zerolinecolor: grid });
    merged.yaxis = Object.assign({}, merged.yaxis || {}, { gridcolor: grid, zerolinecolor: grid });
    return merged;
  }

  function refreshRenderedPlotThemes(scope) {
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

  function initDashboardPage() {
    if (window.__dashboardPageInit) return;
    const filterForm = document.getElementById("runs-filter");
    const pageInput = document.getElementById("page-input");
    const hiddenInput = document.getElementById("multi-run-ids-hidden");
    const countEl = document.getElementById("multi-run-selection-count");
    const exportBtn = document.getElementById("multi-sn-export-btn");
    const analysisBtn = document.getElementById("multi-analysis-open-btn");
    const snInput = document.getElementById("multi-sn-input");
    const snForm = document.getElementById("multi-sn-form");
    if (!filterForm || !pageInput || !hiddenInput || !countEl || !snForm) return;
    window.__dashboardPageInit = true;

    const STORAGE_KEY = "dashboard-multi-run-ids";
    filterForm.addEventListener("change", function () {
      pageInput.value = "1";
    });

    function loadSelected() {
      try {
        const raw = localStorage.getItem(STORAGE_KEY) || "[]";
        const arr = JSON.parse(raw);
        return Array.isArray(arr) ? arr.map(String) : [];
      } catch (_error) {
        return [];
      }
    }

    function saveSelected(ids) {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(ids));
    }

    function getSelection() {
      return hiddenInput.value
        ? hiddenInput.value
            .split(",")
            .map(function (x) {
              return x.trim();
            })
            .filter(Boolean)
        : [];
    }

    function setSelection(ids) {
      const unique = Array.from(new Set(ids.filter(Boolean)));
      hiddenInput.value = unique.join(",");
      countEl.textContent = "Selected runs: " + unique.length;
      document.querySelectorAll('input[data-role="multi-run"]').forEach(function (cb) {
        cb.checked = unique.indexOf(cb.value) >= 0;
      });
      const allVisible = document.querySelectorAll('input[data-role="multi-run"]');
      const allChecked =
        allVisible.length > 0 &&
        Array.from(allVisible).every(function (cb) {
          return cb.checked;
        });
      const selectAll = document.getElementById("multi-run-select-all");
      if (selectAll) selectAll.checked = allChecked;
      saveSelected(unique);
    }

    document.body.addEventListener("change", function (e) {
      const target = e.target;
      if (!target) return;
      if (target.matches('input[data-role="multi-run"]')) {
        const ids = new Set(getSelection());
        if (target.checked) ids.add(target.value);
        else ids.delete(target.value);
        setSelection(Array.from(ids));
      } else if (target.id === "multi-run-select-all") {
        const ids = new Set(getSelection());
        document.querySelectorAll('input[data-role="multi-run"]').forEach(function (cb) {
          cb.checked = target.checked;
          if (target.checked) ids.add(cb.value);
          else ids.delete(cb.value);
        });
        setSelection(Array.from(ids));
      }
    });

    document.body.addEventListener("htmx:afterSwap", function (evt) {
      const t = evt && evt.target;
      if (t && t.id === "runs-table") {
        setSelection(loadSelected());
      }
    });

    snForm.addEventListener("submit", function (evt) {
      const ids = getSelection();
      evt.target
        .querySelectorAll('input[name="run_ids"][data-generated="1"]')
        .forEach(function (el) {
          el.remove();
        });
      ids.forEach(function (id) {
        const input = document.createElement("input");
        input.type = "hidden";
        input.name = "run_ids";
        input.setAttribute("data-generated", "1");
        input.value = id;
        evt.target.appendChild(input);
      });
    });

    if (exportBtn) {
      exportBtn.addEventListener("click", function () {
        const ids = getSelection();
        const sn = (snInput && snInput.value ? snInput.value : "").trim();
        if (!ids.length || !sn) return;
        const p = new URLSearchParams();
        ids.forEach(function (id) {
          p.append("run_ids", id);
        });
        p.set("sn", sn);
        window.location.href = "/runs/multi-sn-export?" + p.toString();
      });
    }
    if (analysisBtn) {
      analysisBtn.addEventListener("click", function () {
        const ids = getSelection();
        if (!ids.length) return;
        const p = new URLSearchParams();
        ids.forEach(function (id) {
          p.append("run_ids", id);
        });
        window.location.href = "/runs/multi-analysis?" + p.toString();
      });
    }

    document.addEventListener("visibilitychange", function () {
      if (document.visibilityState !== "visible" || !window.htmx) return;
      const table = document.getElementById("runs-table");
      const kpis = document.getElementById("dashboard-kpis");
      if (table) htmx.trigger(table, "refresh");
      if (kpis) htmx.trigger(kpis, "refresh");
    });

    setSelection(loadSelected());
  }

  function initRunStartPage() {
    if (window.__runStartPageInit) return;
    const select = document.getElementById("data-source");
    const form = document.getElementById("start-analysis");
    if (!select || !form) return;
    window.__runStartPageInit = true;

    const filesBlock = document.getElementById("files-only");
    const mysqlBlock = document.getElementById("mysql-only");
    const uploadBlock = document.getElementById("upload-only");
    const sourceSetupSection = document.getElementById("source-setup-section");
    const pathInput = document.getElementById("path-input");
    const uploadArchiveInput = document.getElementById("upload-archive-input");
    const btnPickFolder = document.getElementById("btn-pick-folder");
    const btnPastePath = document.getElementById("btn-paste-path");
    const step2Title = document.getElementById("step2-title");
    const pathFolderInput = document.getElementById("path-folder-input");
    const btnDb = document.getElementById("btn-db-login");
    const dialog = document.getElementById("db-auth-dialog");
    const dbUser = document.getElementById("db-user-input");
    const dbPass = document.getElementById("db-pass-input");
    const dbUrl = document.getElementById("db-url-input");
    const dbProfile = document.getElementById("db-profile-select");
    const hiddenProfile = document.getElementById("mysql-db-profile-hidden");
    const hiddenBaseUrl = document.getElementById("mysql-db-base-url-hidden");

    function syncMysqlHiddenFromDialog() {
      if (dbProfile && hiddenProfile) hiddenProfile.value = dbProfile.value;
      if (dbUrl && hiddenBaseUrl) hiddenBaseUrl.value = dbUrl.value.trim() || hiddenBaseUrl.value;
    }
    function applyDbProfileUrl() {
      if (!dbUrl || !dbProfile) return;
      dbUrl.value =
        dbProfile.value === "manufacturing"
          ? "https://manufacturing-db.corp.sldev.cz/t3w1"
          : "https://pegatron-db.corp.sldev.cz/";
      syncMysqlHiddenFromDialog();
    }
    function setMode() {
      const files = select.value === "files";
      const mysql = select.value === "mysql";
      const upload = select.value === "upload";
      if (sourceSetupSection) sourceSetupSection.hidden = !(files || mysql || upload);
      filesBlock.hidden = !files;
      if (uploadBlock) uploadBlock.hidden = !upload;
      if (mysqlBlock) mysqlBlock.hidden = !mysql;
      pathInput.disabled = !files;
      if (btnPickFolder) btnPickFolder.disabled = !files;
      if (btnPastePath) btnPastePath.disabled = !files;
      if (mysql) {
        if (step2Title) step2Title.textContent = "Step 2 - Source setup (MySQL)";
        pathInput.removeAttribute("required");
        if (uploadArchiveInput) uploadArchiveInput.removeAttribute("required");
        btnDb.hidden = false;
        if (uploadArchiveInput) uploadArchiveInput.disabled = true;
      } else if (upload) {
        if (step2Title) step2Title.textContent = "Step 2 - Source setup (Upload)";
        pathInput.removeAttribute("required");
        if (uploadArchiveInput) uploadArchiveInput.setAttribute("required", "required");
        btnDb.hidden = true;
        if (uploadArchiveInput) uploadArchiveInput.disabled = false;
        if (dialog && dialog.open) dialog.close();
      } else {
        if (files && step2Title) step2Title.textContent = "Step 2 - Source setup (Files)";
        if (files) pathInput.setAttribute("required", "required");
        else pathInput.removeAttribute("required");
        if (uploadArchiveInput) uploadArchiveInput.removeAttribute("required");
        btnDb.hidden = true;
        if (uploadArchiveInput) uploadArchiveInput.disabled = true;
        if (dialog && dialog.open) dialog.close();
      }
      [dbUser, dbPass, dbUrl, dbProfile].forEach(function (el) {
        if (!el) return;
        el.disabled = !mysql;
      });
    }

    if (dbProfile) dbProfile.addEventListener("change", applyDbProfileUrl);
    if (dbUrl) dbUrl.addEventListener("input", syncMysqlHiddenFromDialog);
    select.addEventListener("change", function () {
      setMode();
      if (select.value === "mysql") {
        syncMysqlHiddenFromDialog();
        if (dialog && typeof dialog.showModal === "function") dialog.showModal();
      }
    });
    if (btnDb) {
      btnDb.addEventListener("click", function () {
        syncMysqlHiddenFromDialog();
        if (dialog && typeof dialog.showModal === "function") dialog.showModal();
      });
    }

    const dbCancel = document.getElementById("db-dialog-cancel");
    const dbOk = document.getElementById("db-dialog-ok");
    if (dbCancel) dbCancel.addEventListener("click", function () { if (dialog) dialog.close(); });
    if (dbOk) dbOk.addEventListener("click", function () { syncMysqlHiddenFromDialog(); if (dialog) dialog.close(); });

    form.addEventListener("submit", function (e) {
      if (select.value === "mysql") {
        syncMysqlHiddenFromDialog();
        const u = dbUser && dbUser.value ? dbUser.value.trim() : "";
        const p = dbPass && dbPass.value ? dbPass.value : "";
        if (!u || !p) {
          e.preventDefault();
          if (dialog && typeof dialog.showModal === "function") dialog.showModal();
        }
      } else if (select.value === "upload") {
        const hasFile = uploadArchiveInput && uploadArchiveInput.files && uploadArchiveInput.files.length > 0;
        if (!hasFile) {
          e.preventDefault();
          if (uploadArchiveInput) uploadArchiveInput.focus();
        }
      }
    });

    if (btnPickFolder) {
      btnPickFolder.addEventListener("click", async function () {
        if (select.value === "mysql") return;
        if (window.showDirectoryPicker) {
          try {
            const handle = await window.showDirectoryPicker({ mode: "read" });
            if (handle && handle.name && !pathInput.value.trim()) pathInput.value = handle.name;
          } catch (_error) {}
        }
        if (pathFolderInput) pathFolderInput.click();
      });
    }
    if (pathFolderInput) {
      pathFolderInput.addEventListener("change", function (e) {
        const files = e && e.target ? e.target.files : null;
        const first = files && files.length ? files[0] : null;
        if (!first || !pathInput) return;
        if (typeof first.path === "string" && first.path) {
          const normalized = String(first.path).replace(/\\\\/g, "/");
          const slash = normalized.lastIndexOf("/");
          if (slash > 0) pathInput.value = normalized.slice(0, slash);
        } else if (first.webkitRelativePath) {
          const rootFolder = first.webkitRelativePath.split("/")[0] || "";
          if (rootFolder && !pathInput.value.trim()) pathInput.value = rootFolder;
        }
        pathInput.focus();
      });
    }
    if (btnPastePath) {
      btnPastePath.addEventListener("click", async function () {
        if (select.value === "mysql" || !pathInput) return;
        try {
          const text = await navigator.clipboard.readText();
          const path = (text || "").trim();
          if (path) pathInput.value = path;
        } catch (_error) {}
        pathInput.focus();
      });
    }

    setMode();
    syncMysqlHiddenFromDialog();
  }

  function initRunAnalysisPage() {
    if (window.__runAnalysisPageInit) return;
    const root = document.getElementById("analysis-results");
    if (!root) return;
    window.__runAnalysisPageInit = true;

    function activate(name) {
      document.querySelectorAll("[data-extra-tab]").forEach(function (tab) {
        tab.classList.toggle("is-active", tab.getAttribute("data-extra-tab") === name);
      });
      document.querySelectorAll("[data-extra-panel]").forEach(function (panel) {
        panel.hidden = panel.getAttribute("data-extra-panel") !== name;
      });
      localStorage.setItem("analysis-extra-tab", name);
    }

    if (window.renderRunDetailCharts) {
      window.renderRunDetailCharts(root);
    }
    document.querySelectorAll("[data-extra-tab]").forEach(function (tab) {
      tab.addEventListener("click", function () {
        activate(tab.getAttribute("data-extra-tab"));
      });
    });
    activate(localStorage.getItem("analysis-extra-tab") === "correlation" ? "correlation" : "chart-type");
  }

  function initLimitsAutoReload() {
    if (window.__limitsAutoReloadBound) return;
    window.__limitsAutoReloadBound = true;
    document.body.addEventListener("limits-saved", function () {
      window.location.reload();
    });
  }

  function prettyTestName(raw) {
    const val = String(raw || "").trim();
    if (!val) return "";
    const parts = val.split("_").filter(Boolean);
    if (!parts.length) return val;
    const head = parts[0].charAt(0).toUpperCase() + parts[0].slice(1);
    if (parts.length === 1) return head;
    if (parts.length === 2) return head + " " + parts[1];
    return head + " " + parts[1] + " - " + parts.slice(2).join(" ");
  }

  function initRunDetailPage() {
    if (window.__runDetailPageInit) return;
    const form = document.getElementById("detail-filter");
    const tabs = Array.from(document.querySelectorAll("[data-detail-tab]"));
    const panels = Array.from(document.querySelectorAll("[data-detail-panel]"));
    if (!form && !tabs.length) return;
    window.__runDetailPageInit = true;
    document.documentElement.setAttribute("data-density", "compact");

    if (tabs.length && panels.length) {
      function activate(name) {
        tabs.forEach(function (tab) {
          tab.classList.toggle("is-active", tab.getAttribute("data-detail-tab") === name);
        });
        panels.forEach(function (panel) {
          panel.hidden = panel.getAttribute("data-detail-panel") !== name;
        });
        localStorage.setItem("run-detail-tab", name);
      }
      tabs.forEach(function (tab) {
        tab.addEventListener("click", function () {
          activate(tab.getAttribute("data-detail-tab"));
        });
      });
      activate(localStorage.getItem("run-detail-tab") === "sn" ? "sn" : "analysis");
    }

    if (!form) return;
    const runId = form.getAttribute("data-run-id") || "";
    if (!runId) return;

    function syncExportHref() {
      const exportBtn = document.getElementById("export-csv-btn");
      const kindSel = document.getElementById("export-kind-sel");
      const statusSel = document.getElementById("export-status-sel");
      if (!exportBtn || !kindSel || !statusSel) return;
      const p = new URLSearchParams(new FormData(form));
      p.set("mode", "selection");
      p.set("export_type", kindSel.value);
      p.set("status_filter", statusSel.value);
      exportBtn.dataset.exportUrl = "/runs/" + runId + "/export?" + p.toString();
    }

    function bindFallbackSearch() {
      form.querySelectorAll(".filter-checkbox-label[data-test-raw]").forEach(function (el) {
        const raw = el.getAttribute("data-test-raw") || "";
        el.textContent = prettyTestName(raw);
      });
      const input = document.getElementById("tests-search-input-fallback");
      if (!input) return;
      input.addEventListener("input", function () {
        const q = String(input.value || "").toLowerCase().trim();
        form.querySelectorAll('input[name="tests"][type="checkbox"]').forEach(function (cb) {
          const row = cb.closest(".filter-checkbox-row");
          if (!row) return;
          const raw = cb.value || "";
          const pretty = prettyTestName(raw);
          row.hidden = !!q && raw.toLowerCase().indexOf(q) < 0 && pretty.toLowerCase().indexOf(q) < 0;
        });
      });
      input.dispatchEvent(new Event("input"));
    }

    function bindFacetsMode() {
      const facetsEl = document.getElementById("selection-facets-json");
      if (!facetsEl) return false;
      let raw;
      try {
        raw = JSON.parse(facetsEl.textContent || "{}");
      } catch (_error) {
        return false;
      }
      const testsByKind = raw.testsByKind || {};
      const stationsByKindTest = raw.stationsByKindTest || {};
      const allStations = raw.allStations || [];
      const testsPanel = document.getElementById("tests-panel");
      const stationsPanel = document.getElementById("stations-panel");
      if (!testsPanel || !stationsPanel) return false;

      function appendCheckbox(container, name, value, checked, labelText) {
        const lab = document.createElement("label");
        lab.className = "filter-checkbox-row";
        const cb = document.createElement("input");
        cb.type = "checkbox";
        cb.name = name;
        cb.value = value;
        cb.checked = checked;
        const span = document.createElement("span");
        span.className = "filter-checkbox-label";
        span.textContent = labelText;
        lab.appendChild(cb);
        lab.appendChild(span);
        container.appendChild(lab);
      }

      function getCheckedKinds() {
        return Array.from(form.querySelectorAll('input[data-role="kind"]:checked')).map(function (cb) {
          return cb.value;
        });
      }
      function getCheckedTests() {
        return Array.from(form.querySelectorAll('input[name="tests"]:checked')).map(function (cb) {
          return cb.value;
        });
      }
      function testsForKinds(kinds) {
        const set = new Set();
        kinds.forEach(function (k) {
          (testsByKind[k] || []).forEach(function (t) {
            set.add(t);
          });
        });
        return Array.from(set).sort();
      }
      function stationsForSelection(kinds, selectedTests) {
        const set = new Set();
        kinds.forEach(function (k) {
          selectedTests.forEach(function (t) {
            const arr = (stationsByKindTest[k] && stationsByKindTest[k][t]) || [];
            arr.forEach(function (s) {
              set.add(s);
            });
          });
        });
        return Array.from(set).sort();
      }
      function renderTestsPanel() {
        const kinds = getCheckedKinds();
        const tests = testsForKinds(kinds);
        const searchInput = document.getElementById("tests-search-input");
        const q = searchInput ? String(searchInput.value || "").toLowerCase().trim() : "";
        const prev = new Set(getCheckedTests());
        testsPanel.innerHTML = "";
        if (!tests.length) {
          testsPanel.innerHTML = '<p class="muted" style="margin:0">Select tester type in the first column.</p>';
          return;
        }
        const filtered = q
          ? tests.filter(function (t) {
              return t.toLowerCase().indexOf(q) >= 0 || prettyTestName(t).toLowerCase().indexOf(q) >= 0;
            })
          : tests;
        if (!filtered.length) {
          testsPanel.innerHTML = '<p class="muted" style="margin:0">No tests match the search.</p>';
          return;
        }
        filtered.forEach(function (t) {
          appendCheckbox(testsPanel, "tests", t, prev.has(t), prettyTestName(t));
        });
      }
      function renderStationsPanel() {
        const kinds = getCheckedKinds();
        const selectedTests = getCheckedTests();
        const noTestsSelected = selectedTests.length === 0;
        const stations = noTestsSelected ? allStations.slice() : stationsForSelection(kinds, selectedTests);
        const prev = new Set(
          Array.from(form.querySelectorAll('input[name="stations"]:checked')).map(function (cb) {
            return cb.value;
          })
        );
        stationsPanel.innerHTML = "";
        if (!stations.length) {
          stationsPanel.innerHTML =
            '<p class="muted" style="margin:0">' +
            (noTestsSelected ? "No stations were found in data." : "No stations are available for this test selection.") +
            "</p>";
          return;
        }
        if (!prev.size && !noTestsSelected) {
          stations.forEach(function (s) {
            prev.add(s);
          });
        }
        stations.forEach(function (s) {
          appendCheckbox(stationsPanel, "stations", s, prev.has(s), s);
        });
      }

      const testsSearchInput = document.getElementById("tests-search-input");
      if (testsSearchInput) {
        testsSearchInput.addEventListener("input", function () {
          renderTestsPanel();
          syncExportHref();
        });
      }
      form.addEventListener("change", function (e) {
        const t = e.target;
        if (t && t.getAttribute && t.getAttribute("data-role") === "kind") {
          renderTestsPanel();
          renderStationsPanel();
        } else if (t && t.name === "tests") {
          renderStationsPanel();
        }
        syncExportHref();
      });
      form.addEventListener("click", function (e) {
        const btn = e.target.closest("[data-action]");
        if (!btn || !form.contains(btn)) return;
        e.preventDefault();
        const action = btn.getAttribute("data-action");
        if (action === "kinds-all" || action === "kinds-none") {
          form.querySelectorAll('input[data-role="kind"]').forEach(function (cb) {
            cb.checked = action === "kinds-all";
          });
          renderTestsPanel();
          renderStationsPanel();
        } else if (action === "tests-all" || action === "tests-none") {
          form.querySelectorAll('input[name="tests"][type="checkbox"]').forEach(function (cb) {
            cb.checked = action === "tests-all";
          });
          renderStationsPanel();
        } else if (action === "stations-all" || action === "stations-none") {
          form.querySelectorAll('input[name="stations"][type="checkbox"]').forEach(function (cb) {
            cb.checked = action === "stations-all";
          });
        }
        syncExportHref();
      });

      renderTestsPanel();
      renderStationsPanel();
      return true;
    }

    bindFallbackSearch();
    bindFacetsMode();
    form.addEventListener("change", syncExportHref);
    form.addEventListener("click", function (e) {
      const btn = e.target.closest("[data-action]");
      if (!btn || !form.contains(btn)) return;
      syncExportHref();
    });
    const exportBtn = document.getElementById("export-csv-btn");
    if (exportBtn) {
      exportBtn.addEventListener("click", function () {
        const url = exportBtn.dataset.exportUrl;
        if (url) window.location.href = url;
      });
    }
    form.addEventListener("submit", function (e) {
      e.preventDefault();
      const params = new URLSearchParams(new FormData(form));
      window.location.href = "/runs/" + runId + "/analysis?" + params.toString();
    });
    syncExportHref();
  }

  function initMultiRunAnalysisPage() {
    if (window.__multiRunAnalysisInit) return;
    const form = document.getElementById("multi-analysis-filter");
    if (!form) return;
    window.__multiRunAnalysisInit = true;

    const search = document.getElementById("multi-tests-search");
    const panel = document.getElementById("multi-tests-panel");
    if (search && panel) {
      search.addEventListener("input", function () {
        const q = (search.value || "").toLowerCase().trim();
        panel.querySelectorAll(".filter-checkbox-row").forEach(function (row) {
          const raw = row.getAttribute("data-test-row") || "";
          row.hidden = !!q && raw.toLowerCase().indexOf(q) < 0;
        });
      });
    }

    function bindAllNone(btnId, selector, checked) {
      const b = document.getElementById(btnId);
      if (!b) return;
      b.addEventListener("click", function (e) {
        e.preventDefault();
        document.querySelectorAll(selector).forEach(function (cb) {
          const row = cb.closest(".filter-checkbox-row");
          if (!row || !row.hidden) cb.checked = checked;
        });
      });
    }
    bindAllNone("multi-tests-all", '#multi-tests-panel input[name="tests"]', true);
    bindAllNone("multi-tests-none", '#multi-tests-panel input[name="tests"]', false);
    bindAllNone("multi-stations-all", '#multi-stations-panel input[name="stations"]', true);
    bindAllNone("multi-stations-none", '#multi-stations-panel input[name="stations"]', false);

    document.body.addEventListener("click", function (e) {
      const link = e.target.closest("#multi-analysis-export-link");
      if (!link) return;
      e.preventDefault();
      const params = new URLSearchParams(new FormData(form));
      window.location.href = "/runs/multi-analysis-export?" + params.toString();
    });
  }

  function initMysqlSqlPage() {
    if (window.__mysqlSqlInit) return;
    const b = document.getElementById("copy-mysql-sql");
    const pre = document.getElementById("mysql-sql-pre");
    if (!b || !pre) return;
    window.__mysqlSqlInit = true;
    b.addEventListener("click", function () {
      navigator.clipboard.writeText(pre.textContent || "").then(function () {
        b.textContent = "Copied";
        setTimeout(function () {
          b.textContent = "Copy";
        }, 1600);
      });
    });
  }

  function initDrilldownBackButtons() {
    if (window.__drilldownBackBound) return;
    window.__drilldownBackBound = true;
    document.body.addEventListener("click", function (e) {
      const btn = e.target.closest(".js-drilldown-back");
      if (!btn) return;
      e.preventDefault();
      const targetSel = btn.getAttribute("data-target");
      if (!targetSel) return;
      const target = document.querySelector(targetSel);
      if (!target) return;
      const raw = btn.getAttribute("data-empty-text");
      if (raw !== null && String(raw).trim() === "") {
        target.innerHTML = "";
        return;
      }
      const text = raw && String(raw).trim() ? raw : "Select KPI bucket to view details.";
      target.innerHTML = '<p class="muted">' + text + "</p>";
    });
  }

  function renderExtraCharts(container) {
    if (!window.Plotly || !container) return;
    container.querySelectorAll(".js-plotly-json").forEach(function (el) {
      if (el.dataset.rendered === "1") return;
      const targetId = el.getAttribute("data-target-id");
      const target = targetId ? document.getElementById(targetId) : null;
      if (!target) return;
      try {
        const fig = JSON.parse(el.textContent || "{}");
        Plotly.newPlot(target, fig.data, themedPlotLayout(fig.layout), { displaylogo: false });
        el.dataset.rendered = "1";
      } catch (error) {
        console.error("Plotly render failed", error);
      }
    });
    if (window.initChartFullscreenPanels) window.initChartFullscreenPanels();
  }

  document.addEventListener("DOMContentLoaded", function () {
    initDashboardPage();
    initRunStartPage();
    initRunAnalysisPage();
    initRunDetailPage();
    initMultiRunAnalysisPage();
    initMysqlSqlPage();
    initDrilldownBackButtons();
    initLimitsAutoReload();
    renderExtraCharts(document);
  });

  document.body.addEventListener("htmx:afterSwap", function (evt) {
    const t = evt && evt.target ? evt.target : document;
    renderExtraCharts(t);
  });

  document.addEventListener("app:theme-changed", function () {
    refreshRenderedPlotThemes(document);
  });
})();

# Project Memory — Scam Intelligence GUI

## Table Component Convention
- All data tables use `.table-container` or `.table-wrap`.
- Shared styles live in `static/gui-base.css` (linked AFTER each template's inline `<style>`).
- ID columns should carry `data-col="id"` on both `<th>` and `<td>` for consistent styling via `[data-col="id"]`.
- Table container styling: `border-radius: 12px`, `border: 1px solid #e2e8f0`, `box-shadow: 0 1px 3px rgba(15,23,42,0.08)`, `margin: 6px 12px`.
- ID column styling: `min-width: 120px`, `padding-left: 16px`, `overflow: visible`, `white-space: nowrap`.

## Known Layout Details
- `.container` has `overflow: hidden` and fixed header space via `--fixed-header-space`.
- Table containers must NOT use negative margins if rounded corners need to be visible (parent overflow clips them).
- Filters and pagination use `margin: 0 -15px` for full-bleed; tables are inset cards.

## Night Mode Centralization
- Canonical dark theme lives in `static/gui-base.css` under "DARK THEME (canonical QC-Review palette)" — uses `!important` on every rule so it overrides the page-specific inline `body.theme-dark` blocks in each template.
- Palette mirrors the QC Review (`qc_gui.html`) page: body `#0F172A`, cards/headers `#1E293B`, table rows `#1E293B`/`#223047`/`#2C3E50`, `th` `#273449`, filter badges `rgba(22,163,74,.14)` bg + `#BBF7D0` text, links `#38BDF8`, icons `#60A5FA`, primary text `#F8FAFC`.
- Cache-buster on `/static/gui-base.css?v=YYYYMMDD-token` is bumped whenever the centralized dark theme changes so browsers pick up the new rules. Current value: `20260923-visibility4`.
- All non-login templates already link `gui-base.css`; the file is loaded AFTER each template's inline `<style>` so cascading order + `!important` gives the centralized rules the final word.
- Filter-section numeric values are recolored via `body.theme-dark .filters-header span` (green `#BBF7D0`); inline-styled colored badges are caught by `[style*="background:#f0fff4"]` etc.
- New dark-theme block in gui-base.css (lines ~894-1014) covers inline pill colors that survived the original pass: Social Media Department (`#e8f5e9`), Number Type count badges (`#e3f2fd` Postpaid, `#fff3e0` Disposable), Recharge Date pills (`#fff1f2` Expired / `#fff7ed` ≤3 days / `#ecfdf5` healthy), Website Directory Origin (`#eaf4fd`) and Automated (`#fde8e8` No / `#f5f5f5` other), all 6 GUI Management `.tag-*` classes + `.secret-value` for Password / AML Password masked spans, and `.chip` for the GUI Management Allowed Departments pills (slate translucent + `#E2E8F0` text).
- When you spot a missed inline color, add a `body.theme-dark [style*="background:#xxxxxx"]` block here rather than touching the template — keeps the fix centralized and prevents future drift.

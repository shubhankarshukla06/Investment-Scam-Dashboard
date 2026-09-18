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

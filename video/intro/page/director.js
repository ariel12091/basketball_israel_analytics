// Injected into every recorded page (page.addInitScript). Draws the visible
// cursor and the highlight ring, and finds DataTables cells by row/column text.
(() => {
  const AMBER = '#e8a435';
  const CURSOR_SVG = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><path d="M3 2l17 10.5-7.4 1.6L9 21z" fill="#fff" stroke="#14100C" stroke-width="1.6" stroke-linejoin="round"/></svg>';

  function node(id, style) {
    let e = document.getElementById(id);
    if (!e) {
      e = document.createElement('div');
      e.id = id;
      Object.assign(e.style, { position: 'fixed', pointerEvents: 'none', zIndex: '2147483647', boxSizing: 'border-box' }, style);
      document.documentElement.appendChild(e);
    }
    return e;
  }
  const cursor = () => node('__dir_cursor', {
    left: '0', top: '0', width: '30px', height: '30px',
    transform: 'translate(800px, 450px)',
    transition: 'transform 600ms cubic-bezier(.4,0,.2,1)',
    background: `url("data:image/svg+xml;utf8,${encodeURIComponent(CURSOR_SVG)}") no-repeat 0 0 / contain`,
    filter: 'drop-shadow(0 2px 4px rgba(0,0,0,.6))',
  });
  const ring = () => node('__dir_ring', {
    border: `4px solid ${AMBER}`, borderRadius: '10px', opacity: '0',
    boxShadow: '0 0 0 9999px rgba(0,0,0,.18), 0 0 24px rgba(232,164,53,.55)',
    transition: 'opacity 250ms ease',
  });

  const norm = (s) => String(s ?? '').replace(/\s+/g, ' ').trim().toUpperCase();

  function dtApi(sel) {
    const host = document.querySelector(sel);
    const $ = window.jQuery;
    if (!host || !$ || !$.fn.dataTable) return null;
    const tbl = host.matches('table') ? host : host.querySelector('table.dataTable');
    return tbl && $.fn.dataTable.isDataTable(tbl) ? $(tbl).DataTable() : null;
  }

  function find(target) {
    const spec = target.cell ?? target.header;
    if (!spec) return null;
    const api = dtApi(spec.table);
    if (!api) return null;
    let col = -1;
    api.columns().every(function (i) {
      if (col < 0 && this.visible() && norm(this.header()?.textContent).includes(norm(spec.col))) col = i;
    });
    if (col < 0) return null;
    if (target.header) return api.column(col).header();
    let row = -1;
    api.rows({ page: 'current' }).every(function (i) {
      if (row < 0 && norm(this.node()?.textContent).includes(norm(spec.row))) row = i;
    });
    return row < 0 ? null : api.cell(row, col).node();
  }

  window.__dir = {
    cursorTo(x, y, ms = 600) {
      const c = cursor();
      c.style.transitionDuration = `${ms}ms`;
      c.style.transform = `translate(${x - 4}px, ${y - 2}px)`;
    },
    ring(r, pad = 6) {
      Object.assign(ring().style, {
        left: `${r.x - pad}px`, top: `${r.y - pad}px`,
        width: `${r.width + 2 * pad}px`, height: `${r.height + 2 * pad}px`, opacity: '1',
      });
    },
    clearRing() { ring().style.opacity = '0'; },
    tag(target, id) {
      const el = find(target);
      if (!el) return false;
      el.setAttribute('data-dir-target', id);
      return true;
    },
    busy() {
      // Outputs on hidden tabs stay .recalculating while suspended; only a
      // visible one means the page the viewer sees is still loading.
      if (document.documentElement.classList.contains('shiny-busy')) return true;
      return [...document.querySelectorAll('.recalculating')].some((e) => e.getClientRects().length > 0);
    },
  };
})();

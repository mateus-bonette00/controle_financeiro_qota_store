# app.py
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from html import escape
from pathlib import Path
import base64, os, sqlite3, pandas as pd, streamlit as st, altair as alt, re

# ===== SP-API (Amazon) =====
from sp_api.api import Sellers, Orders, Inventories, CatalogItems, Finances
from sp_api.base import Marketplaces, SellingApiException

# ----------------------------------
# Config
# ----------------------------------
st.set_page_config(page_title="Controle Financeiro Qota Store", layout="wide")

DB_PATH = os.getenv("DB_PATH", "finance.db")
PRIMARY = "#2F529E"
ACCENT  = "#FE0000"
WHITE   = "#FFFFFF"
POSITIVE = "#00FF00"

# Paleta para os cards KPI
GREEN = "#2ECC71"  # receita / stroke
RED   = "#E74C3C"  # despesa / stroke
BLUE  = "#3498DB"  # resultado / stroke

# ----------------------------------
# DB helpers
# ----------------------------------
@st.cache_resource
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def iso8601_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).strftime('%Y-%m-%dT%H:%M:%SZ')

def ensure_table(sql_create: str):
    conn = get_conn()
    conn.execute(sql_create)
    conn.commit()

def table_has_column(table: str, col: str) -> bool:
    cur = get_conn().execute(f"PRAGMA table_info({table});")
    return col in [r[1] for r in cur.fetchall()]

def add_column_if_missing(table: str, col: str, decl: str):
    if not table_has_column(table, col):
        get_conn().execute(f'ALTER TABLE {table} ADD COLUMN "{col}" {decl};').connection.commit()

def get_columns(table: str):
    cur = get_conn().execute(f"PRAGMA table_info({table});")
    return [r[1] for r in cur.fetchall()]

def df_sql(sql: str, params: tuple | None = None) -> pd.DataFrame:
    return pd.read_sql_query(sql, get_conn(), params=params)

def add_row(table: str, row: dict):
    cols = ",".join(row.keys()); qmarks = ",".join(["?"] * len(row))
    try:
        get_conn().execute(f"INSERT INTO {table} ({cols}) VALUES ({qmarks});", tuple(row.values())).connection.commit()
    except sqlite3.IntegrityError:
        if table == "amazon_receitas" and "produto_id" in row:
            safe = dict(row); safe["produto_id"] = None
            cols = ",".join(safe.keys()); qmarks = ",".join(["?"] * len(safe))
            get_conn().execute(f"INSERT INTO {table} ({cols}) VALUES ({qmarks});", tuple(safe.values())).connection.commit()
            return
        raise

def delete_row(table: str, id_: int):
    get_conn().execute(f"DELETE FROM {table} WHERE id = ?;", (id_,)).connection.commit()

# ----------------------------------
# Utils de formatação
# ----------------------------------
def money_brl(x):
    try: v = Decimal(str(x))
    except Exception: return "R$ 0,00"
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def money_usd(x):
    try: v = Decimal(str(x))
    except Exception: return "$ 0.00"
    return f"$ {v:,.2f}"

# ====== Mês nomeado (pt-BR) ======
MESES_PT = {1:"janeiro",2:"fevereiro",3:"março",4:"abril",5:"maio",6:"junho",7:"julho",8:"agosto",9:"setembro",10:"outubro",11:"novembro",12:"dezembro"}

def month_label(yyyy_mm: str | None) -> str:
    if not yyyy_mm: return ""
    try:
        y, m = yyyy_mm.split("-"); mi = int(m); nome = MESES_PT.get(mi, m)
        return f"{nome} ({y})"
    except Exception:
        return yyyy_mm

# ----------------------------------
# Navegação por query (exclusões)
# ----------------------------------
def handle_query_deletions():
    try: q = dict(st.query_params)
    except Exception: q = st.experimental_get_query_params()
    changed = False
    mapping = {"del_gasto":"gastos","del_inv":"investimentos","del_rec":"receitas","del_saldo":"amazon_saldos",
               "del_pc":"produtos_compra","del_ar":"amazon_receitas","del_prod":"produtos","del_settle":"amazon_settlements"}
    for param, table in mapping.items():
        if param in q:
            try: delete_row(table, int(q.get(param)))
            except Exception: pass
            changed = True
    if changed:
        try: st.query_params.clear()
        except Exception: st.experimental_set_query_params()
        st.rerun()

# ----------------------------------
# UI helpers (design)
# ----------------------------------
def render_logo_centered(path: str, width: int = 220):
    file = Path(path)
    if file.exists():
        b64 = base64.b64encode(file.read_bytes()).decode("utf-8")
        st.markdown(
            f"""<div style="text-align:center; margin:8px 0 0;">
                   <img src="data:image/png;base64,{b64}" style="width:{width}px; filter: drop-shadow(0 12px 24px rgba(0,0,0,.35));">
                </div>""",
            unsafe_allow_html=True,
        )

def inject_global_css():
    st.markdown(
        f"""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@400;600;700;800;900&display=swap');
        html, body, .stApp, [class^="block-container"] {{ font-family: "Poppins", system-ui, -apple-system, Segoe UI, Roboto, sans-serif !important; font-weight: 800; }}
        .stApp {{
            background:
                radial-gradient(1200px 800px at 10% -10%, rgba(46, 82, 158, 0.25), transparent 60%),
                radial-gradient(1200px 800px at 90% -10%, rgba(254, 0, 0, 0.12), transparent 60%),
                linear-gradient(180deg, #0a122b 0%, #0d1735 40%, #0a122b 100%);
        }}
        .stMainBlockContainer {{ padding-top: 12px; }}
        h1, h2, h3, h4, h5, h6, .stMarkdown label {{ font-family: 'Poppins', sans-serif !important; font-weight: 900 !important;
          color: #1a6bc6 !important; text-shadow: 5px 5px 15px rgba(128,128,128,.25);
          filter: saturate(1.25) contrast(3.45);}}
        .stMarkdown a {{ color: {ACCENT} !important; }}

        .stTextInput > div > div input, .stNumberInput input, .stTextArea textarea, .stDateInput input {{
            background: rgba(255,255,255,0.06); border: 1px solid rgba(255,255,255,.12); color: {WHITE};
            border-radius: 12px; box-shadow: 0 8px 22px rgba(0,0,0,.25) inset; font-weight: 600;
        }}
        .stSelectbox > div > div, .stMultiSelect > div > div {{ background: rgba(255,255,255,0.06); border: 1px solid rgba(255,255,255,.12); border-radius: 12px; }}
        .stCheckbox, .stRadio, .stDateInput label, .stNumberInput label, .stTextInput label {{ color: {WHITE} !important; }}

        .st-emotion-cache-1anq8dj {{
            display: inline-flex; align-items: center; justify-content: center;
            background: linear-gradient(135deg, #FE0000, #b30000);
            color: #FFFFFF; border: none; border-radius: 12px; padding: 10px 16px; font-weight: 800; letter-spacing: .3px;
            box-shadow: 0 10px 24px rgba(254, 0, 0, .35);
            transition: transform .06s ease, box-shadow .12s ease, filter .12s ease, outline-color .12s ease;
            cursor: pointer; outline: 2px solid rgba(255,255,255,0.0); outline-offset: 2px;
        }}
        .st-emotion-cache-1anq8dj:hover {{ filter: brightness(1.06); transform: translateY(-1px); box-shadow: 0 14px 34px rgba(254,0,0,.45); }}

        .stButton > button {{
            background: linear-gradient(135deg, {ACCENT}, #b30000); color: {WHITE}; border: none; border-radius: 12px;
            padding: 10px 16px; font-weight: 800; letter-spacing:.3px; box-shadow: 0 10px 24px rgba(254,0,0,.35);
            transition: transform .06s ease, box-shadow .12s ease, filter .12s ease, outline-color .12s ease;
            cursor: pointer; outline: 2px solid rgba(255,255,255,0.0); outline-offset: 2px;
        }}
        .stButton > button:hover {{ filter: brightness(1.06); transform: translateY(-1px); box-shadow: 0 14px 34px rgba(254,0,0,.45); }}

        table.fin {{
            width:100%; border-collapse:separate; border-spacing:0; font-size:14px; color:{WHITE};
            background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,.06); border-radius: 14px;
            box-shadow: 0 16px 40px rgba(0,0,0,.45); overflow: hidden; margin-top: 28px !important;
        }}
        table.fin thead th {{
            background: linear-gradient(135deg, {PRIMARY}, #1c2f6a); color:#fff; text-align:left; padding:10px 12px; position:sticky; top:0; z-index:1;
            border-top:1px solid rgba(255,255,255,.08);
        }}
        table.fin td {{ padding:10px 12px; border-top:1px solid rgba(255,255,255,.06); }}
        table.fin td:last-child, table.fin th:last-child {{ text-align:center; width:120px; }}
        a.trash {{
            text-decoration:none; padding:6px 12px; border-radius:10px; display:inline-block;
            border:1px solid rgba(255,255,255,0.18); color:#fff !important; background: linear-gradient(135deg, #23386e, #17264f);
            box-shadow: 0 10px 22px rgba(0,0,0,.35); font-weight: 800;
        }}
        a.trash:hover {{ background: linear-gradient(135deg, {ACCENT}, #b30000); border-color: rgba(255,255,255,.3); color:#fff; }}

        .stTabs [role="tablist"] {{ justify-content: center; gap: 18px; border-bottom: 0; margin-top: 6px; }}
        .stTabs [role="tab"] {{
            position: relative; padding: 16px 22px 16px 48px !important; border-radius: 14px 14px 0 0 !important;
            border: 1px solid rgba(255,255,255,.18) !important; border-bottom: 3px solid transparent !important;
            background: rgba(255,255,255,0.06) !important; color: #FFFFFF !important; backdrop-filter: blur(6px);
            box-shadow: 0 10px 24px rgba(0,0,0,.35); font-weight: 800;
        }}
        .stTabs [role="tab"][aria-selected="true"] {{
            background: linear-gradient(135deg, {PRIMARY}, #1a2d66) !important; border-color: rgba(255,255,255,.28) !important;
            border-bottom-color: {PRIMARY} !important; box-shadow: 0 2px 0 0 {PRIMARY} inset, 0 14px 36px rgba(0,0,0,.45) !important;
        }}
        .stTabs [role="tab"]::before {{
            content: ""; position: absolute; left: 16px; top: 50%; transform: translateY(-50%);
            width: 20px; height: 20px; opacity:.95; background-repeat:no-repeat; background-size:20px 20px;
            filter: drop-shadow(0 2px 4px rgba(0,0,0,.35));
        }}
        .stTabs [role="tab"]:nth-child(1)::before {{ background-image: url("data:image/svg+xml;utf8,<svg viewBox='0 0 24 24' xmlns='http://www.w3.org/2000/svg'><path fill='%23FFFFFF' d='M12 3l9 8h-3v9h-5v-6H11v6H6v-9H3l9-8z'/></svg>"); }}
        .stTabs [role="tab"]:nth-child(2)::before {{ background-image: url("data:image/svg+xml;utf8,<svg viewBox='0 0 24 24' xmlns='http://www.w3.org/2000/svg'><path fill='%23FFFFFF' d='M21 8l-9-5-9 5v8l9 5 9-5V8zm-9 11l-7-3.89V9.47L12 13l7-3.53v5.64L12 19z'/></svg>"); }}
        .stTabs [role="tab"]:nth-child(3)::before {{ background-image: url("data:image/svg+xml;utf8,<svg viewBox='0 0 24 24' xmlns='http://www.w3.org/2000/svg'><path fill='%23FFFFFF' d='M21 7H3V5h14a2 2 0 012 2zm0 2v8a2 2 0 01-2 2H3a2 2 0 01-2-2v-4z M17 12a2 2 0 100 4h3v-4h-3z'/></svg>"); }}
        .stTabs [role="tab"]:nth-child(4)::before {{ background-image: url("data:image/svg+xml;utf8,<svg viewBox='0 0 24 24' xmlns='http://www.w3.org/2000/svg'><path fill='%23FFFFFF' d='M3 3h2v18H3V3zm4 10h2v8H7v-8zm4-6h2v14h-2V7zm4 4h2v10h-2V11zm4-6h2v16h-2V5z'/></svg>"); }}
        .stTabs [role="tab"]:nth-child(5)::before {{ background-image: url("data:image/svg+xml;utf8,<svg viewBox='0 0 24 24' xmlns='http://www.w3.org/2000/svg'><path fill='%23FFFFFF' d='M12 3L2 9v2h20V9L12 3zM4 13h16v6H4v-6zm-2 8h20v2H2v-2z'/></svg>"); }}
        .stTabs [role="tab"]:nth-child(6)::before {{ background-image: url("data:image/svg+xml;utf8,<svg viewBox='0 0 24 24' xmlns='http://www.w3.org/2000/svg'><path fill='%23FFFFFF' d='M9 2h6a2 2 0 012 2h1a2 2 0 012 2v14a2 2 0 01-2 2H6a2 2 0 01-2-2v-4z M9 4v2h6V4H9z'/></svg>"); }}

        /* ===== KPI Cards – com stroke 1px e variações ===== */
        .kpi-row {{
          display:grid; grid-template-columns: repeat(3, minmax(260px,1fr));
          gap: 14px; margin: 8px 0 18px;
        }}
        .kpi {{
          position: relative;
          display:flex; align-items:center; gap:14px;
          padding:16px 18px; border-radius:16px;
          border:1px solid rgba(255,255,255,.08);
          background:#0f1c3f;
          box-shadow: 0 10px 26px rgba(0,0,0,.35);
        }}
        .kpi .ico {{
          width:44px; height:44px; border-radius:12px;
          display:grid; place-items:center;
          background: rgba(255,255,255,.08);
          border: 1px solid currentColor;
          overflow:hidden;
        }}
        .kpi .ico img, .kpi .ico svg {{ width:22px; height:22px; display:block; }}
        .kpi .lbl {{ font-size:12px; opacity:.9; text-transform:uppercase; font-weight:800 }}
        .kpi .val {{ font-size:22px; font-weight:900; line-height:1.15; }}
        .kpi .line-small {{ font-size:14px; font-weight:800; opacity:.9 }}

        .kpi.receita {{ border-color: {GREEN};
          background: linear-gradient(180deg, rgba(46,204,113,.12), rgba(46,204,113,.06));
          box-shadow: 0 2px 0 0 {GREEN} inset, 0 10px 26px rgba(0,0,0,.35);
          color: {GREEN}; }}
        .kpi.receita .ico {{ background: rgba(46,204,113,.18); }}

        .kpi.despesa {{ border-color: {RED};
          background: linear-gradient(180deg, rgba(231,76,60,.12), rgba(231,76,60,.06));
          box-shadow: 0 2px 0 0 {RED} inset, 0 10px 26px rgba(0,0,0,.35);
          color: {RED}; }}
        .kpi.despesa .ico {{ background: rgba(231,76,60,.18); }}

        .kpi.result {{ border-color: {BLUE};
          background: linear-gradient(180deg, rgba(52,152,219,.12), rgba(52,152,219,.06));
          box-shadow: 0 2px 0 0 {BLUE} inset, 0 10px 26px rgba(0,0,0,.35);
          color: {BLUE}; }}
        .kpi.result .ico {{ background: rgba(52,152,219,.18); }}
        </style>
        """, unsafe_allow_html=True
    )

def metric_duo_cards(section_title: str, brl: float, usd: float, month: str | None = None):
    suffix = f" — {month_label(month)}" if month else ""
    st.markdown(
        f"""
        <div class="metric-duo" style="display:grid; grid-template-columns: repeat(2, minmax(260px, 1fr)); gap: 16px; margin: 8px 0 4px;">
            <div class="metric-card brl" style="background: linear-gradient(145deg, #233a74, #1a2b57); color: {WHITE}; border: 1px solid rgba(255,255,255,.10);
                        border-radius: 18px; padding: 18px 20px; box-shadow: 0 18px 46px rgba(0,0,0,.55), 0 2px 0 {PRIMARY} inset;">
                <div class="title" style="font-size: 12px; letter-spacing:.45px; text-transform: uppercase; opacity: .85; font-weight: 800;">{escape(section_title + suffix)} — BRL</div>
                <div class="value" style="font-size: 28px; font-weight: 900; margin-top: 6px;">{escape(money_brl(brl))}</div>
            </div>
            <div class="metric-card usd" style="background: linear-gradient(145deg, #233a74, #1a2b57); color: {WHITE}; border: 1px solid rgba(255,255,255,.10);
                        border-radius: 18px; padding: 18px 20px; box-shadow: 0 18px 46px rgba(0,0,0,.55), 0 2px 0 {PRIMARY} inset;">
                <div class="title" style="font-size: 12px; letter-spacing:.45px; text-transform: uppercase; opacity: .85; font-weight: 800;">{escape(section_title + suffix)} — USD</div>
                <div class="value" style="font-size: 28px; font-weight: 900; margin-top: 6px;">{escape(money_usd(usd))}</div>
            </div>
        </div>
        """, unsafe_allow_html=True
    )

def footer_total_badge(title: str, brl: float, usd: float, margin_top: int = 28):
    st.markdown(
        f"""
        <div style="width:100%; display:flex; justify-content:flex-start; margin:{margin_top}px 0 8px;">
          <div class="total-badge" style="max-width: 840px; background: linear-gradient(135deg, #12224d, #0f1c3f); color:#fff;
                        padding:20px 28px; border-radius:18px; font-weight:800; font-size:18px; line-height:1.2;
                        border:1px solid rgba(255,255,255,.10); box-shadow: 0 22px 60px rgba(0,0,0,.60), 0 0 0 1px rgba(255,255,255,.04) inset;">
            <span>{escape(title)}</span>
            <span style="margin-left:14px; opacity:.98;">• BRL: {escape(money_brl(brl))} • USD: {escape(money_usd(usd))}</span>
          </div>
        </div>
        """, unsafe_allow_html=True
    )

def df_to_clean_html(df: pd.DataFrame, del_param: str, anchor: str) -> str:
    if "Data" in df: df["Data"] = pd.to_datetime(df["Data"]).dt.strftime("%d/%m/%Y")
    for col in df.columns:
        if col in {"ID", "Valor (BRL)", "Valor (USD)", "Lucro (USD)", "Subtotal (USD)", "Total (USD)", "Margem %"}: continue
        df[col] = df[col].astype(str).map(escape)
    df["Ações"] = df["ID"].map(lambda i: f'<a class="trash" href="?{del_param}={int(i)}#{anchor}" title="Excluir">Excluir</a>')
    return df.to_html(index=False, escape=False, border=0, classes=["fin"])

# ===== KPI (novo) =====
from base64 import b64encode
def _svg_data_uri(path: str | None) -> str | None:
    if not path: return None
    try:
        with open(path, "rb") as f:
            return "data:image/svg+xml;base64," + b64encode(f.read()).decode("utf-8")
    except Exception:
        return None

ICON_RECEITA_PATH = "assets/triangle-up.svg"
ICON_DESPESA_PATH = "assets/triangle-down.svg"
ICON_RESULT_PATH  = "assets/coins.svg"

def render_total_kpi_cards(usd_receitas: float, brl_receitas: float,
                           usd_despesas: float, brl_despesas: float, title_suffix="(TOTAL)"):
    usd_result = usd_receitas - usd_despesas
    brl_result = brl_receitas - brl_despesas

    num_color_usd = GREEN if usd_result > 0 else (RED if usd_result < 0 else WHITE)
    num_color_brl = GREEN if brl_result > 0 else (RED if brl_result < 0 else WHITE)

    uri_up = _svg_data_uri(ICON_RECEITA_PATH) or \
        "data:image/svg+xml;utf8," + "<svg viewBox='0 0 24 24' xmlns='http://www.w3.org/2000/svg'><path fill='%232ECC71' d='M12 4l8 14H4z'/></svg>"
    uri_down = _svg_data_uri(ICON_DESPESA_PATH) or \
        "data:image/svg+xml;utf8," + "<svg viewBox='0 0 24 24' xmlns='http://www.w3.org/2000/svg'><path fill='%23E74C3C' d='M12 20L4 6h16z'/></svg>"
    uri_coin = _svg_data_uri(ICON_RESULT_PATH) or \
        "data:image/svg+xml;utf8," + "<svg viewBox='0 0 24 24' xmlns='http://www.w3.org/2000/svg'><circle cx='12' cy='12' r='9' fill='%233498DB'/><text x='12' y='16' text-anchor='middle' font-family='Arial' font-size='12' fill='white'>$</text></svg>"

    html = f"""
    <div class="kpi-row">
      <div class="kpi receita">
        <div class="ico"><img src="{uri_up}" alt="receita"></div>
        <div>
          <div class="lbl">Receitas {escape(title_suffix)}</div>
          <div class="val">USD: {escape(money_usd(usd_receitas))}</div>
          <div class="line-small">BRL: {escape(money_brl(brl_receitas))}</div>
        </div>
      </div>
      <div class="kpi despesa">
        <div class="ico"><img src="{uri_down}" alt="despesa"></div>
        <div>
          <div class="lbl">Despesas {escape(title_suffix)}</div>
          <div class="val">USD: {escape(money_usd(usd_despesas))}</div>
          <div class="line-small">BRL: {escape(money_brl(brl_despesas))}</div>
        </div>
      </div>
      <div class="kpi result">
        <div class="ico"><img src="{uri_coin}" alt="resultado"></div>
        <div>
          <div class="lbl">Resultado {escape(title_suffix)}</div>
          <div class="val" style="color:{num_color_usd}">USD: {escape(money_usd(usd_result))}</div>
          <div class="line-small" style="color:{num_color_brl}">BRL: {escape(money_brl(brl_result))}</div>
        </div>
      </div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)

def render_single_kpi(kind: str, label: str, usd_value: float, brl_value: float, center: bool = False):
    """Card único no mesmo estilo dos KPIs de receita/despesa/resultado.
       center=True centraliza o card na página."""
    assert kind in {"receita","despesa","result"}
    # define ícone conforme o tipo
    if kind == "receita":
        icon = _svg_data_uri(ICON_RECEITA_PATH) or "data:image/svg+xml;utf8," + "<svg viewBox='0 0 24 24' xmlns='http://www.w3.org/2000/svg'><path fill='%232ECC71' d='M12 4l8 14H4z'/></svg>"
    elif kind == "despesa":
        icon = _svg_data_uri(ICON_DESPESA_PATH) or "data:image/svg+xml;utf8," + "<svg viewBox='0 0 24 24' xmlns='http://www.w3.org/2000/svg'><path fill='%23E74C3C' d='M12 20L4 6h16z'/></svg>"
    else:
        icon = _svg_data_uri(ICON_RESULT_PATH) or "data:image/svg+xml;utf8," + "<svg viewBox='0 0 24 24' xmlns='http://www.w3.org/2000/svg'><circle cx='12' cy='12' r='9' fill='%233498DB'/><text x='12' y='16' text-anchor='middle' font-family='Arial' font-size='12' fill='white'>$</text></svg>"

    html = f"""
    <div class="kpi-row" style="grid-template-columns: minmax(260px, 520px);">
      <div class="kpi {kind}">
        <div class="ico"><img src="{icon}" alt=""></div>
        <div>
          <div class="lbl">{escape(label)}</div>
          <div class="val">USD: {escape(money_usd(usd_value))}</div>
          <div class="line-small">BRL: {escape(money_brl(brl_value))}</div>
        </div>
      </div>
    </div>
    """
    if center:
        html = f'<div style="display:flex; justify-content:center;">{html}</div>'
    st.markdown(html, unsafe_allow_html=True)

# ----------------------------------
# Datas em produtos
# ----------------------------------
def produtos_date_sql_expr() -> str:
    cols = set(get_columns("produtos"))
    has_amz = "data_amz" in cols; has_add = "data_add" in cols
    if has_amz and has_add: return "COALESCE(data_amz, data_add)"
    if has_amz: return "data_amz"
    if has_add: return "data_add"
    add_column_if_missing("produtos", "data_add", "TEXT"); return "data_add"

def produtos_date_insert_map(d: date) -> dict:
    ds = d.strftime("%Y-%m-%d"); cols = set(get_columns("produtos")); out = {}
    if "data_amz" in cols: out["data_amz"] = ds
    if "data_add" in cols: out["data_add"] = ds
    if not out:
        add_column_if_missing("produtos", "data_add", "TEXT"); out["data_add"] = ds
    return out

# ----------------------------------
# Filtros de mês/ano
# ----------------------------------
def get_all_months() -> list[str]:
    meses = set()
    for tbl in ["gastos","investimentos","receitas","produtos_compra","amazon_receitas","amazon_saldos","amazon_settlements"]:
        try:
            df = df_sql(f"SELECT DISTINCT strftime('%Y-%m', date(data)) AS m FROM {tbl};"); meses |= set(df["m"].dropna().tolist())
        except Exception: pass
    try:
        expr = produtos_date_sql_expr(); df = df_sql(f"SELECT DISTINCT strftime('%Y-%m', date({expr})) AS m FROM produtos;")
        meses |= set(df["m"].dropna().tolist())
    except Exception: pass
    return sorted([m for m in meses if m])

def apply_month_filter(df: pd.DataFrame, month: str, col: str = "data") -> pd.DataFrame:
    if not month or df.empty or col not in df.columns: return df
    return df[df[col].astype(str).str.startswith(month)].copy()

# ----------------------------------
# DB boot / migrations
# ----------------------------------
def init_db():
    ensure_table("""
        CREATE TABLE IF NOT EXISTS gastos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            categoria TEXT NOT NULL,
            descricao TEXT,
            valor_brl REAL NOT NULL DEFAULT 0,
            valor_usd REAL NOT NULL DEFAULT 0,
            metodo TEXT,
            conta TEXT,
            quem TEXT
        );
    """)
    ensure_table("""
        CREATE TABLE IF NOT EXISTS investimentos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            valor_brl REAL NOT NULL DEFAULT 0,
            valor_usd REAL NOT NULL DEFAULT 0,
            metodo TEXT,
            conta TEXT,
            quem TEXT
        );
    """)
    ensure_table("""
        CREATE TABLE IF NOT EXISTS receitas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            origem TEXT NOT NULL DEFAULT 'FBA',
            descricao TEXT,
            valor_brl REAL NOT NULL DEFAULT 0,
            valor_usd REAL NOT NULL DEFAULT 0,
            metodo TEXT,
            conta TEXT,
            quem TEXT,
            bruto REAL NOT NULL DEFAULT 0,
            cogs REAL NOT NULL DEFAULT 0,
            taxas_amz REAL NOT NULL DEFAULT 0,
            ads REAL NOT NULL DEFAULT 0,
            frete REAL NOT NULL DEFAULT 0,
            descontos REAL NOT NULL DEFAULT 0,
            lucro REAL NOT NULL DEFAULT 0
        );
    """)
    ensure_table("""
        CREATE TABLE IF NOT EXISTS produtos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_add TEXT NOT NULL,
            nome TEXT NOT NULL,
            sku TEXT,
            upc TEXT,
            asin TEXT,
            estoque INTEGER NOT NULL DEFAULT 0,
            custo_base REAL NOT NULL DEFAULT 0,
            freight REAL NOT NULL DEFAULT 0,
            tax REAL NOT NULL DEFAULT 0,
            quantidade INTEGER NOT NULL DEFAULT 0,
            prep REAL NOT NULL DEFAULT 2,
            sold_for REAL NOT NULL DEFAULT 0,
            amazon_fees REAL NOT NULL DEFAULT 0,
            link_amazon TEXT,
            link_fornecedor TEXT,
            data_amz TEXT
        );
    """)
    for col, decl in [
        ("sku","TEXT"), ("upc","TEXT"), ("asin","TEXT"),
        ("custo_base","REAL NOT NULL DEFAULT 0"), ("freight","REAL NOT NULL DEFAULT 0"),
        ("tax","REAL NOT NULL DEFAULT 0"), ("quantidade","INTEGER NOT NULL DEFAULT 0"),
        ("prep","REAL NOT NULL DEFAULT 2"), ("sold_for","REAL NOT NULL DEFAULT 0"),
        ("amazon_fees","REAL NOT NULL DEFAULT 0"), ("link_amazon","TEXT"), ("link_fornecedor","TEXT"),
        ("data_amz","TEXT")
    ]:
        add_column_if_missing("produtos", col, decl)

    ensure_table("""
        CREATE TABLE IF NOT EXISTS amazon_receitas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            produto_id INTEGER,
            quantidade INTEGER NOT NULL DEFAULT 0,
            valor_usd REAL NOT NULL DEFAULT 0,
            quem TEXT,
            obs TEXT,
            produto TEXT,
            sku TEXT,
            FOREIGN KEY(produto_id) REFERENCES produtos(id) ON DELETE SET NULL
        );
    """)
    for col, decl in [("produto_id","INTEGER"), ("produto","TEXT"), ("sku","TEXT"), ("valor_usd","REAL NOT NULL DEFAULT 0")]:
        add_column_if_missing("amazon_receitas", col, decl)
    try:
        conn = get_conn()
        conn.execute("""
            UPDATE amazon_receitas
               SET produto_id = (
                   SELECT p.id FROM produtos p
                    WHERE COALESCE(p.sku,'') <> '' AND p.sku = amazon_receitas.sku
               )
             WHERE (produto_id IS NULL OR produto_id = 0)
               AND COALESCE(sku,'') <> '';
        """); conn.commit()
    except Exception: pass

    ensure_table("""
        CREATE TABLE IF NOT EXISTS amazon_saldos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            disponivel REAL NOT NULL DEFAULT 0,
            pendente REAL NOT NULL DEFAULT 0,
            moeda TEXT NOT NULL DEFAULT 'USD'
        );
    """)
    ensure_table("""
        CREATE TABLE IF NOT EXISTS amazon_settlements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            amount_usd REAL NOT NULL DEFAULT 0,
            group_id TEXT,
            desc TEXT
        );
    """)

# ----------------------------------
# Métricas de produto
# ----------------------------------
def price_to_buy_eff(row) -> float:
    base = float(row.get("custo_base", 0) or 0); tax = float(row.get("tax", 0) or 0)
    freight = float(row.get("freight", 0) or 0); qty = float(row.get("quantidade", 0) or 0)
    rateio = (tax + freight) / qty if qty > 0 else 0.0; return base + rateio

def gross_profit_unit(row) -> float:
    sold_for = float(row.get("sold_for", 0) or 0); amz = float(row.get("amazon_fees", 0) or 0)
    prep = float(row.get("prep", 0) or 0); p2b = price_to_buy_eff(row)
    return sold_for - amz - prep - p2b

def gross_roi(row) -> float:
    p2b = price_to_buy_eff(row); return (gross_profit_unit(row) / p2b) if p2b > 0 else 0.0

def margin_pct(row) -> float:
    sold_for = float(row.get("sold_for", 0) or 0); gp = gross_profit_unit(row)
    return (gp / sold_for) if sold_for > 0 else 0.0

def _norm(x: str) -> str:
    if x is None: return ""
    x = str(x).strip().lower(); x = re.sub(r"[\s\-._]+", "", x); return x

def _match_prod_for_receipt(row, by_id, by_sku, by_upc, by_asin, by_name):
    pid = row.get("produto_id")
    if pd.notna(pid):
        try:
            prod = by_id.get(int(pid)); 
            if prod: return prod
        except Exception:
            pass
    sku = _norm(row.get("sku"))
    if sku and sku in by_sku: return by_sku[sku]
    upc = _norm(row.get("upc") if "upc" in row else None)
    if upc and upc in by_upc: return by_upc[upc]
    asin = _norm(row.get("asin") if "asin" in row else None)
    if asin and asin in by_asin: return by_asin[asin]
    name = _norm(row.get("produto") if "produto" in row else None)
    if name and name in by_name: return by_name[name]
    return None

# ----------------------------------
# Boot
# ----------------------------------
init_db(); handle_query_deletions(); inject_global_css()

# ----------------------------------
# Header + Tabs
# ----------------------------------
render_logo_centered("logo-qota-storee-semfundo.png", width=295)
st.markdown("<h1 style='text-align:center; font-weight:900; letter-spacing:.3px; margin-top:4px;'>Controle Financeiro Qota Store</h1>", unsafe_allow_html=True)

# ===== Filtro: Mês e Ano separados =====
all_months = get_all_months()
all_years = sorted({ int(m.split("-")[0]) for m in all_months }) or [date.today().year]
month_names = {i: MESES_PT[i] for i in range(1,13)}
col_f1, col_f2 = st.columns([3,1.2], gap="small")
with col_f1:
    m_default = st.session_state.get("g_mes_m", date.today().month)
    month_idx = st.selectbox("Mês", options=list(range(1,13)),
                             index=(m_default-1) if 1 <= m_default <= 12 else (date.today().month-1),
                             format_func=lambda i: month_names[i].capitalize(), key="g_mes_m")
with col_f2:
    y_default = st.session_state.get("g_mes_y", date.today().year)
    year = st.selectbox("Ano", options=all_years, index=all_years.index(y_default) if y_default in all_years else len(all_years)-1, key="g_mes_y")

g_mes = f"{year}-{month_idx:02d}"
st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Principal", "Receitas (FBA)", "Fluxo de Caixa", "Gráficos", "Saldos (Amazon)", "Produtos (SKU Planner)"
])

contas = ["Nubank", "Nomad", "Wise", "Mercury Bank", "WesternUnion"]
pessoas = ["Bonette", "Daniel"]

# ============================
# TAB 1 - PRINCIPAL
# ============================
with tab1:

    df_g_k  = apply_month_filter(df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM gastos;"), g_mes)
    df_i_k  = apply_month_filter(df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM investimentos;"), g_mes)

    df_ar_k = apply_month_filter(df_sql("SELECT date(data) as data, valor_usd FROM amazon_receitas;"), g_mes)
    df_rc_k = apply_month_filter(df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM receitas;"), g_mes)

    rec_usd_mes = float((df_ar_k["valor_usd"].sum() if not df_ar_k.empty else 0.0) +
                        (df_rc_k["valor_usd"].sum() if not df_rc_k.empty else 0.0))
    rec_brl_mes = float(df_rc_k["valor_brl"].sum() if not df_rc_k.empty else 0.0)

    desp_usd_mes = float((df_g_k["valor_usd"].sum() if not df_g_k.empty else 0.0) +
                        (df_i_k["valor_usd"].sum() if not df_i_k.empty else 0.0))
    desp_brl_mes = float((df_g_k["valor_brl"].sum() if not df_g_k.empty else 0.0) +
                        (df_i_k["valor_brl"].sum() if not df_i_k.empty else 0.0))

    render_total_kpi_cards(
        usd_receitas=rec_usd_mes,
        brl_receitas=rec_brl_mes,
        usd_despesas=desp_usd_mes,
        brl_despesas=desp_brl_mes,
        title_suffix="(MÊS)"
    )

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Gastos")
        with st.form("form_gasto"):
            data_gasto = st.date_input("Data do gasto", value=date.today(), format="DD/MM/YYYY")
            categoria = st.selectbox("Categoria", ["Compra de Produto","Mensalidade/Assinatura","Contabilidade/Legal","Taxas/Impostos","Frete/Logística","Outros"])
            desc = st.text_input("Descrição do gasto")
            val_brl = st.number_input("Valor em BRL", min_value=0.0, step=0.01, format="%.2f")
            val_usd = st.number_input("Valor em USD", min_value=0.0, step=0.01, format="%.2f")
            metodo = st.selectbox("Método de pagamento", ["Pix","Cartão de Crédito","Boleto","Transferência","Dinheiro"])
            conta = st.selectbox("Conta/Banco", contas)
            quem = st.selectbox("Quem pagou", pessoas)
            if st.form_submit_button("Adicionar gasto"):
                add_row("gastos", dict(data=data_gasto.strftime("%Y-%m-%d"), categoria=categoria, descricao=desc,
                                       valor_brl=val_brl, valor_usd=val_usd, metodo=metodo, conta=conta, quem=quem))
                st.rerun()

    with col2:
        st.subheader("Investimentos")
        with st.form("form_invest"):
            data_inv = st.date_input("Data do investimento", value=date.today(), format="DD/MM/YYYY")
            inv_brl = st.number_input("Valor em BRL", min_value=0.0, step=0.01, format="%.2f")
            inv_usd = st.number_input("Valor em USD", min_value=0.0, step=0.01, format="%.2f")
            metodo_i = st.selectbox("Método de pagamento", ["Pix","Cartão de Crédito","Boleto","Transferência","Dinheiro"])
            conta_i = st.selectbox("Conta/Banco", contas)
            quem_i = st.selectbox("Quem investiu/pagou", pessoas)
            if st.form_submit_button("Adicionar investimento"):
                add_row("investimentos", dict(data=data_inv.strftime("%Y-%m-%d"), valor_brl=inv_brl, valor_usd=inv_usd,
                                              metodo=metodo_i, conta=conta_i, quem=quem_i))
                st.rerun()

    left, right = st.columns(2)
    with left:
        st.markdown("### Gastos cadastrados")
        df_g_all = df_sql("""SELECT id, data, categoria, descricao, valor_brl, valor_usd, metodo, conta, quem
                             FROM gastos ORDER BY date(data) DESC, id DESC;""")
        df_g = apply_month_filter(df_g_all, g_mes)
        tot_g_brl = float(df_g["valor_brl"].sum()) if not df_g.empty else 0.0
        tot_g_usd = float(df_g["valor_usd"].sum()) if not df_g.empty else 0.0
        metric_duo_cards("Totais de Gastos", tot_g_brl, tot_g_usd, month=(g_mes or None))

        if not df_g.empty:
            df_view = pd.DataFrame({
                "ID": df_g["id"].astype(int),
                "Data": df_g["data"], "Categoria": df_g["categoria"].fillna(""),
                "Descrição": df_g["descricao"].fillna(""),
                "Valor (BRL)": df_g["valor_brl"].map(money_brl),
                "Valor (USD)": df_g["valor_usd"].map(money_usd),
                "Método": df_g["metodo"].fillna(""), "Quem pagou": df_g["quem"].fillna(""),
            })
            st.markdown(df_to_clean_html(df_view, "del_gasto", "tbl_gastos"), unsafe_allow_html=True)
        else:
            st.info("Sem gastos no filtro atual.")

    with right:
        st.markdown("### Investimentos cadastrados")
        df_i_all = df_sql("""SELECT id, data, valor_brl, valor_usd, metodo, conta, quem
                             FROM investimentos ORDER BY date(data) DESC, id DESC;""")
        df_i = apply_month_filter(df_i_all, g_mes)
        tot_i_brl = float(df_i["valor_brl"].sum()) if not df_i.empty else 0.0
        tot_i_usd = float(df_i["valor_usd"].sum()) if not df_i.empty else 0.0
        metric_duo_cards("Totais de Investimentos", tot_i_brl, tot_i_usd, month=(g_mes or None))

        if not df_i.empty:
            df_view_i = pd.DataFrame({
                "ID": df_i["id"].astype(int), "Data": df_i["data"],
                "Valor (BRL)": df_i["valor_brl"].map(money_brl),
                "Valor (USD)": df_i["valor_usd"].map(money_usd),
                "Método": df_i["metodo"].fillna(""), "Quem investiu/pagou": df_i["quem"].fillna(""),
            })
            st.markdown(df_to_clean_html(df_view_i, "del_inv", "tbl_invest"), unsafe_allow_html=True)
        else:
            st.info("Sem investimentos no filtro atual.")

        # === KPI de Investimentos (TOTAL – todos os meses), estilo "despesa"
        total_inv_brl_all = float(df_i_all["valor_brl"].sum()) if not df_i_all.empty else 0.0
        total_inv_usd_all = float(df_i_all["valor_usd"].sum()) if not df_i_all.empty else 0.0
        render_single_kpi(
            kind="despesa",
            label="Investimentos — Total de TODOS os meses",
            usd_value=total_inv_usd_all,
            brl_value=total_inv_brl_all
        )

    df_rec_all = df_sql("SELECT valor_brl, valor_usd FROM receitas;")
    df_ar_all  = df_sql("SELECT valor_usd FROM amazon_receitas;")

    total_receita_usd_all = float((df_rec_all["valor_usd"].sum() if not df_rec_all.empty else 0.0) +
                                  (df_ar_all["valor_usd"].sum() if not df_ar_all.empty else 0.0))
    total_receita_brl_all = float(df_rec_all["valor_brl"].sum() if not df_rec_all.empty else 0.0)

    total_desp_brl_all = float((df_g_all["valor_brl"].sum() if not df_g_all.empty else 0.0) +
                               (df_i_all["valor_brl"].sum() if not df_i_all.empty else 0.0))
    total_desp_usd_all = float((df_g_all["valor_usd"].sum() if not df_g_all.empty else 0.0) +
                               (df_i_all["valor_usd"].sum() if not df_i_all.empty else 0.0))

    st.markdown("---")
    render_total_kpi_cards(
        usd_receitas=total_receita_usd_all, brl_receitas=total_receita_brl_all,
        usd_despesas=total_desp_usd_all, brl_despesas=total_desp_brl_all,
        title_suffix="(TOTAL — todos os meses)"
    )
    
# ============================
# TAB 2 - RECEITAS (FBA)
# ============================
with tab2:
    st.subheader("Produtos Vendidos (Receitas FBA)")

    def _secret(k: str, default: str = "") -> str:
        try: return st.secrets[k]
        except Exception: return os.environ.get(k, default)

    SPAPI_CREDS = dict(
        refresh_token=_secret("SPAPI_REFRESH_TOKEN"),
        lwa_app_id=_secret("LWA_CLIENT_ID"),
        lwa_client_secret=_secret("LWA_CLIENT_SECRET"),
        aws_access_key=_secret("AWS_ACCESS_KEY_ID"),
        aws_secret_key=_secret("AWS_SECRET_ACCESS_KEY"),
    )

    with st.expander("Testar conexão com a Amazon (clique para abrir)"):
        c1, c2 = st.columns(2)
        if c1.button("Testar Sellers.get_marketplace_participations"):
            try:
                sellers = Sellers(marketplace=Marketplaces.US, credentials=SPAPI_CREDS)
                resp = sellers.get_marketplace_participations()
                st.json(resp.payload); st.success("Sellers OK ✅")
            except SellingApiException as e:
                st.error(f"Erro Sellers: {e}")

        if c2.button("Listar Orders (últimos 3 dias)"):
            try:
                after = iso8601_z(datetime.now(timezone.utc) - timedelta(days=3))
                orders_api = Orders(marketplace=Marketplaces.US, credentials=SPAPI_CREDS)
                r = orders_api.get_orders(CreatedAfter=after, OrderStatuses=["Unshipped","Shipped","Pending"])
                st.json(r.payload); st.success("Orders OK ✅ (JSON acima)")
            except SellingApiException as e:
                st.error(f"Erro Orders: {e}")

    def sync_orders_to_db(days=7):
        orders_api = Orders(marketplace=Marketplaces.US, credentials=SPAPI_CREDS)
        after = iso8601_z(datetime.now(timezone.utc) - timedelta(days=days))
        res = orders_api.get_orders(CreatedAfter=after, OrderStatuses=["Unshipped","Shipped","Pending","Canceled","ShippedPartial","Unfulfillable","Unconfirmed"])
        orders = res.payload.get("Orders", [])
        if not orders: return 0, pd.DataFrame(), pd.DataFrame()

        dfp = df_sql("SELECT id, sku FROM produtos;")
        sku_to_pid = { str(s or "").strip(): int(i) for i, s in zip(dfp["id"], dfp["sku"]) }

        inseridos = 0; all_items = []
        for o in orders:
            oid = o.get("AmazonOrderId")
            try:
                items = orders_api.get_order_items(oid).payload.get("OrderItems", [])
            except SellingApiException as e:
                st.warning(f"Falha ao pegar itens do pedido {oid}: {e}"); continue
            if not items: continue

            dfi = pd.DataFrame([{
                "AmazonOrderId": oid, "SellerSKU": it.get("SellerSKU"), "Title": it.get("Title"),
                "Qty": int(it.get("QuantityOrdered") or 0),
                "ItemPrice": float((it.get("ItemPrice") or {}).get("Amount") or 0.0),
                "Currency": (it.get("ItemPrice") or {}).get("CurrencyCode"),
            } for it in items])

            all_items.append(dfi)
            grp = dfi.groupby(["SellerSKU","Currency"], dropna=False).agg(Qtd=("Qty","sum"), ValorUnit=("ItemPrice","mean")).reset_index()

            for _, row in grp.iterrows():
                sku = str(row["SellerSKU"] or "").strip(); pid = sku_to_pid.get(sku)
                qtd = int(row["Qtd"] or 0); valor_total = float(row["ValorUnit"] or 0.0) * qtd

                add_row("amazon_receitas", dict(
                    data=date.today().strftime("%Y-%m-%d"),
                    produto_id=pid, quantidade=qtd, valor_usd=valor_total,
                    quem="SP-API", obs=f"Sync {oid}", sku=sku
                ))
                if pid and qtd:
                    get_conn().execute("UPDATE produtos SET estoque = MAX(0, estoque - ?) WHERE id = ?;", (qtd, pid)).connection.commit()
                inseridos += 1

        df_orders = pd.DataFrame([{
            "AmazonOrderId": o.get("AmazonOrderId"),
            "PurchaseDate": o.get("PurchaseDate"),
            "OrderStatus": o.get("OrderStatus"),
            "FulfillmentChannel": o.get("FulfillmentChannel"),
            "MarketplaceId": o.get("MarketplaceId"),
            "OrderTotal": (o.get("OrderTotal") or {}).get("Amount"),
            "Currency": (o.get("OrderTotal") or {}).get("CurrencyCode"),
        } for o in orders])

        df_items = pd.concat(all_items, ignore_index=True) if all_items else pd.DataFrame()
        return inseridos, df_orders, df_items

    def enrich_name_by_asin(asin: str) -> str:
        if not asin: return ""
        try:
            cat = CatalogItems(marketplace=Marketplaces.US, credentials=SPAPI_CREDS)
            ci = cat.get_catalog_item(marketplaceIds=[Marketplaces.US.marketplace_id], asin=asin).payload
            attr = ci.get("attributes") or {}
            for key in ["item_name","productTitle","title"]:
                val = attr.get(key)
                if isinstance(val, list) and val: return str(val[0])
                if isinstance(val, str): return val
            return (ci.get("summaries") or [{}])[0].get("itemName") or ""
        except Exception:
            return ""

    def sync_fba_inventory():
        inv = Inventories(marketplace=Marketplaces.US, credentials=SPAPI_CREDS)
        token = None; total_updates = 0; created = 0; rows = []
        while True:
            try:
                resp = inv.get_inventory_summary_marketplace(details=True, marketplaceIds=[Marketplaces.US.marketplace_id], nextToken=token)
                payload = resp.payload or {}
            except SellingApiException as e:
                st.error(f"Erro Inventories: {e}"); break

            summaries = payload.get("inventorySummaries") or payload.get("InventorySummaries") or []
            for s in summaries:
                sku = (s.get("sellerSku") or s.get("SellerSku") or "").strip()
                asin = (s.get("asin") or s.get("ASIN") or "").strip()
                qty = int(s.get("totalSupplyQuantity") or s.get("TotalSupplyQuantity") or 0)
                rows.append({"sku": sku, "asin": asin, "estoque": qty})

                dfp = df_sql("SELECT id FROM produtos WHERE sku = ?;", (sku,))
                if not dfp.empty:
                    pid = int(dfp.iloc[0]["id"])
                    get_conn().execute("UPDATE produtos SET estoque = ? WHERE id = ?;", (qty, pid)).connection.commit()
                    total_updates += 1
                else:
                    nome = enrich_name_by_asin(asin) or f"Produto {sku or asin}"
                    today = date.today().strftime("%Y-%m-%d")
                    add_row("produtos", dict(
                        data_add=today, data_amz=today, nome=nome, sku=sku, upc="", asin=asin,
                        estoque=qty, custo_base=0.0, freight=0.0, tax=0.0, quantidade=0, prep=2.0,
                        sold_for=0.0, amazon_fees=0.0, link_amazon="", link_fornecedor=""
                    ))
                    created += 1

            token = payload.get("nextToken") or payload.get("NextToken")
            if not token: break

        df_out = pd.DataFrame(rows)
        return total_updates, created, df_out

    def sync_finances_settlements(days=60):
        fin = Finances(marketplace=Marketplaces.US, credentials=SPAPI_CREDS)
        after = iso8601_z(datetime.now(timezone.utc) - timedelta(days=days))
        try:
            groups = fin.list_financial_event_groups(FinancialEventGroupStartedAfter=after).payload
        except SellingApiException as e:
            st.error(f"Erro Finances.groups: {e}"); return 0, pd.DataFrame()

        groups_list = groups.get("FinancialEventGroupList", []) or []
        inserted = 0; out = []
        for g in groups_list:
            gid = g.get("FinancialEventGroupId"); posted = g.get("ProcessingStatus")
            transfer = float((g.get("OriginalTotal") or {}).get("CurrencyAmount") or 0.0)
            currency = (g.get("OriginalTotal") or {}).get("CurrencyCode") or "USD"
            settled_at = (g.get("FundTransferDate") or g.get("FinancialEventGroupStart") or "")[:10] or date.today().strftime("%Y-%m-%d")
            desc = f"Settlement {posted} ({currency})"

            add_row("amazon_settlements", dict(data=settled_at, amount_usd=transfer if currency == "USD" else transfer,
                                               group_id=gid, desc=desc))
            inserted += 1
            out.append({"group_id": gid, "data": settled_at, "amount": transfer, "currency": currency, "status": posted})
        return inserted, pd.DataFrame(out)

    st.markdown("#### Integração SP-API — Pedidos")
    if st.button("Sincronizar pedidos (últimos 7 dias)"):
        with st.spinner("Sincronizando com a Amazon..."):
            inseridos, df_o, df_i = sync_orders_to_db(days=7)
        if inseridos == 0: st.info("Nenhum pedido encontrado no período.")
        else: st.success(f"Inserções em amazon_receitas: {inseridos}")
        if not df_o.empty: st.write("Pedidos:"); st.dataframe(df_o, use_container_width=True, hide_index=True)
        if not df_i.empty: st.write("Itens dos pedidos:"); st.dataframe(df_i, use_container_width=True, hide_index=True)
        st.rerun()

    st.markdown("#### Integração SP-API — Inventário FBA")
    c_inv1, c_inv2 = st.columns(2)
    if c_inv1.button("Sincronizar inventário (FBA) — atualizar estoque e criar SKUs faltantes"):
        with st.spinner("Lendo inventário FBA..."):
            upd, created, df_inv = sync_fba_inventory()
        st.success(f"Estoque atualizado para {upd} SKU(s). Produtos criados: {created}.")
        if not df_inv.empty: st.dataframe(df_inv, use_container_width=True, hide_index=True)

    if c_inv2.button("Sincronizar settlements (últimos 60 dias) — Finances"):
        with st.spinner("Buscando settlements..."):
            ins, df_set = sync_finances_settlements(days=60)
        if ins == 0: st.info("Nenhum settlement encontrado no período.")
        else:
            st.success(f"Settlements inseridos: {ins}")
            if not df_set.empty: st.dataframe(df_set, use_container_width=True, hide_index=True)
        st.rerun()

    # ---- restante da aba (listagens)
    date_expr = produtos_date_sql_expr()
    dfp_all = df_sql(f"""
        SELECT id, {date_expr} AS data_add, nome, sku, upc, asin, estoque,
               custo_base, freight, tax, quantidade, prep, sold_for, amazon_fees,
               link_amazon, link_fornecedor
        FROM produtos
        ORDER BY date({date_expr}) DESC, id DESC;
    """)
    dfp = apply_month_filter(dfp_all, g_mes, col="data_add") if g_mes else dfp_all

    prod_options = []
    if not dfp.empty:
        for _, r in dfp.iterrows():
            label = f"{str(r.get('sku') or '').strip()} | {str(r.get('upc') or '').strip()} | {r['nome']}".strip(" |")
            prod_options.append((label, int(r["id"]), str(r.get("sku") or "")))

    with st.form("form_amz_receitas"):
        data_ar = st.date_input("Data do crédito", value=st.session_state.get("ar_data", date.today()),
                                format="DD/MM/YYYY", key="ar_data")

        # ====== Seleção do produto
        if prod_options:
            labels = [x[0] for x in prod_options]
            sel_label = st.selectbox("Produto vendido (SKU | UPC | Nome)", labels, index=0, key="ar_sel_label")
            pid = [x for x in prod_options if x[0]==sel_label][0][1]
            sel_sku = [x for x in prod_options if x[0]==sel_label][0][2]
            # Busca o sold_for do produto selecionado
            prod_row = dfp_all[dfp_all["id"] == pid].head(1)
            sold_for_default = float(prod_row["sold_for"].iloc[0] if not prod_row.empty else 0.0)

            # Inicializa/atualiza valor default do campo "valor recebido por unidade"
            if "ar_last_pid" not in st.session_state:
                st.session_state["ar_last_pid"] = pid
                st.session_state["ar_val"] = sold_for_default
            else:
                if st.session_state["ar_last_pid"] != pid:
                    st.session_state["ar_last_pid"] = pid
                    st.session_state["ar_val"] = sold_for_default
        else:
            st.warning("Cadastre produtos na aba **Produtos (SKU Planner)** para selecionar aqui.")
            pid, sel_sku = None, ""
            if "ar_val" not in st.session_state: st.session_state["ar_val"] = 0.0

        qty_ar = st.number_input("Quantidade vendida", min_value=1, step=1, value=1, key="ar_qty")

        # ===== Campo auto-preenchido com sold_for do produto selecionado
        val_ar = st.number_input(
            "Valor recebido (USD) dentro da Amazon (por unidade)",
            min_value=0.0, step=0.01, format="%.2f",
            key="ar_val", value=st.session_state.get("ar_val", 0.0)
        )

        quem_ar = st.selectbox("Quem lançou", pessoas, key="ar_quem")
        obs_ar = st.text_input("Observação (opcional)", key="ar_obs")

        if st.form_submit_button("Adicionar recebimento (Amazon)"):
            add_row("amazon_receitas", dict(
                data=data_ar.strftime("%Y-%m-%d"),
                produto_id=pid, quantidade=int(qty_ar), valor_usd=val_ar*int(qty_ar),
                quem=quem_ar, obs=obs_ar.strip(), sku=sel_sku
            ))
            if pid:
                get_conn().execute("UPDATE produtos SET estoque = MAX(0, estoque - ?) WHERE id = ?;", (int(qty_ar), int(pid))).connection.commit()
            st.success("Recebimento adicionado e estoque atualizado.")
            st.rerun()

    dr_all = df_sql("""SELECT id, data, produto_id, quantidade, valor_usd, quem, obs, sku, produto
                       FROM amazon_receitas ORDER BY date(data) DESC, id DESC;""")
    dr = apply_month_filter(dr_all, g_mes) if g_mes else dr_all

    tot_qty = int(dr["quantidade"].sum()) if not dr.empty else 0
    tot_val = float(dr["valor_usd"].sum()) if not dr.empty else 0.0
    titulo_vendido = "Vendido no período" + (f" — {month_label(g_mes)}" if g_mes else "")
    st.markdown(
        f"""<div class="metric-card center" style="max-width:1080px;margin:12px auto;padding:22px 26px;background:linear-gradient(145deg,#233a74,#1a2b57);color:#fff;border:1px solid rgba(255,255,255,.10);border-radius:18px;box-shadow:0 18px 46px rgba(0,0,0,.55),0 2px 0 {PRIMARY} inset;">
                <div class="title" style="font-size:12px;letter-spacing:.45px;text-transform:uppercase;opacity:.85;font-weight:800;">{escape(titulo_vendido)}</div>
                <div class="value" style="font-size:28px;font-weight:900;margin-top:6px;">
                    Quantidade: {tot_qty} • Valor: {escape(money_usd(tot_val))}
                </div>
            </div>""", unsafe_allow_html=True
    )

    if not dr.empty:
        prods = dfp_all[["id","nome","sku","upc","asin"]].rename(columns={"id":"pid"})
        dr["produto_id"] = pd.to_numeric(dr["produto_id"], errors="coerce").astype("Int64")
        prods["pid"] = pd.to_numeric(prods["pid"], errors="coerce").astype("Int64")
        dshow = dr.merge(prods, left_on="produto_id", right_on="pid", how="left")
        dshow["sku"] = dshow["sku_x"].fillna(dshow["sku_y"]).fillna("")
        df_ar_view = pd.DataFrame({
            "ID": dshow["id"].astype(int),
            "Data": dshow["data"],
            "Produto": dshow["nome"].fillna(""),
            "SKU": dshow["sku"].fillna(""),
            "Qtd": dshow["quantidade"].astype(int),
            "Valor (USD)": dshow["valor_usd"].map(money_usd),
            "Quem": dshow["quem"].fillna(""),
        })
        st.markdown(df_to_clean_html(df_ar_view, "del_ar", "tbl_ar"), unsafe_allow_html=True)
    else:
        st.info("Sem recebidos no filtro atual.")

# ============================
# TAB 3 - FLUXO DE CAIXA
# ============================
with tab3:
    st.subheader("Fluxo de Caixa — Resumo Mensal e Total Geral (Receitas em USD)")

    df_g = df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM gastos;")
    df_i = df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM investimentos;")
    df_r = df_sql("SELECT date(data) as data, valor_usd FROM amazon_receitas;")
    df_r["valor_brl"] = 0.0; df_r["lucro"] = 0.0

    if g_mes:
        df_g = apply_month_filter(df_g, g_mes); df_i = apply_month_filter(df_i, g_mes); df_r = apply_month_filter(df_r, g_mes)

    def monthly(df, label):
        if df.empty: return pd.DataFrame(columns=["mes","tipo","brl","usd"])
        t = df.copy(); t["mes"] = pd.to_datetime(t["data"]).dt.to_period("M").astype(str)
        g = t.groupby("mes")[["valor_brl","valor_usd"]].sum().reset_index().rename(columns={"valor_brl":"brl","valor_usd":"usd"})
        g["tipo"] = label; return g

    m_g = monthly(df_g, "Despesas (Gastos)")
    m_i = monthly(df_i, "Despesas (Invest.)")
    m_r = monthly(df_r, "Receitas (Amazon)")

    all_brl = pd.concat([m_g[["mes","tipo","brl"]], m_i[["mes","tipo","brl"]], m_r[["mes","tipo","brl"]]], ignore_index=True)
    all_usd = pd.concat([m_g[["mes","tipo","usd"]], m_i[["mes","tipo","usd"]], m_r[["mes","tipo","usd"]]], ignore_index=True)

    if all_brl.empty and all_usd.empty:
        st.info("Sem dados suficientes.")
    else:
        p_brl = all_brl.pivot_table(index="mes", columns="tipo", values="brl", aggfunc="sum", fill_value=0).reset_index()
        p_usd = all_usd.pivot_table(index="mes", columns="tipo", values="usd", aggfunc="sum", fill_value=0).reset_index()
        for c in ["Despesas (Gastos)","Despesas (Invest.)","Receitas (Amazon)"]:
            if c not in p_brl: p_brl[c] = 0.0
            if c not in p_usd: p_usd[c] = 0.0
        p_brl["Resultado"] = p_brl["Receitas (Amazon)"] - (p_brl["Despesas (Gastos)"] + p_brl["Despesas (Invest.)"])
        p_usd["Resultado"] = p_usd["Receitas (Amazon)"] - (p_usd["Despesas (Gastos)"] + p_usd["Despesas (Invest.)"])

        c1, c2 = st.columns(2)
        with c1:
            st.markdown("#### BRL — por mês" + (f" (filtro: {month_label(g_mes)})" if g_mes else ""))
            dfv = p_brl.copy()
            for col in ["Receitas (Amazon)","Despesas (Gastos)","Despesas (Invest.)","Resultado"]:
                dfv[col] = dfv[col].map(money_brl)
            st.dataframe(dfv.rename(columns={"mes":"Mês"}), use_container_width=True, hide_index=True)
        with c2:
            st.markdown("#### USD — por mês" + (f" (filtro: {month_label(g_mes)})" if g_mes else ""))
            dfv = p_usd.copy()
            for col in ["Receitas (Amazon)","Despesas (Gastos)","Despesas (Invest.)","Resultado"]:
                dfv[col] = dfv[col].map(money_usd)
            st.dataframe(dfv.rename(columns={"mes":"Mês"}), use_container_width=True, hide_index=True)

        # ==== Totais em TODOS os meses (USD e BRL) para montar os 3 KPIs como na segunda imagem
        p_usd_all = pd.concat([
            monthly(df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM gastos;"),"Despesas (Gastos)"),
            monthly(df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM investimentos;"),"Despesas (Invest.)"),
            monthly(df_sql("SELECT date(data) as data, valor_usd, 0 as valor_brl FROM amazon_receitas;"),"Receitas (Amazon)")
        ], ignore_index=True).pivot_table(index="mes", columns="tipo", values="usd", aggfunc="sum", fill_value=0).reset_index()
        for c in ["Despesas (Gastos)","Despesas (Invest.)","Receitas (Amazon)"]:
            if c not in p_usd_all: p_usd_all[c] = 0.0
        p_usd_all["Resultado"] = p_usd_all["Receitas (Amazon)"] - (p_usd_all["Despesas (Gastos)"] + p_usd_all["Despesas (Invest.)"])

        p_brl_all = pd.concat([
            monthly(df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM gastos;"),"Despesas (Gastos)"),
            monthly(df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM investimentos;"),"Despesas (Invest.)"),
            monthly(df_sql("SELECT date(data) as data, 0 as valor_usd, 0 as valor_brl FROM amazon_receitas;"),"Receitas (Amazon)")
        ], ignore_index=True).pivot_table(index="mes", columns="tipo", values="brl", aggfunc="sum", fill_value=0).reset_index()
        for c in ["Despesas (Gastos)","Despesas (Invest.)","Receitas (Amazon)"]:
            if c not in p_brl_all: p_brl_all[c] = 0.0
        p_brl_all["Resultado"] = p_brl_all["Receitas (Amazon)"] - (p_brl_all["Despesas (Gastos)"] + p_brl_all["Despesas (Invest.)"])

        tot_receita_usd = float(p_usd_all["Receitas (Amazon)"].sum())
        tot_desp_usd    = float(p_usd_all["Despesas (Gastos)"].sum() + p_usd_all["Despesas (Invest.)"].sum())
        tot_receita_brl = float(p_brl_all["Receitas (Amazon)"].sum())
        tot_desp_brl    = float(p_brl_all["Despesas (Gastos)"].sum() + p_brl_all["Despesas (Invest.)"].sum())

        st.markdown("### Totais Gerais — soma de todos os meses")
        render_total_kpi_cards(
            usd_receitas=tot_receita_usd,
            brl_receitas=tot_receita_brl,
            usd_despesas=tot_desp_usd,
            brl_despesas=tot_desp_brl,
            title_suffix="(TOTAL — soma de todos os meses)"
        )

# ============================
# TAB 4 - GRÁFICOS
# ============================
with tab4:
    st.subheader("Gráficos Mensais (USD como principal)")

    # --- coleta (aplica o mesmo filtro do mês selecionado)
    df_g = df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM gastos;")
    df_i = df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM investimentos;")
    df_r = df_sql("SELECT date(data) as data, valor_usd FROM amazon_receitas;")
    df_r["valor_brl"] = 0.0  # só para manter colunas compatíveis

    if g_mes:
        df_g = apply_month_filter(df_g, g_mes)
        df_i = apply_month_filter(df_i, g_mes)
        df_r = apply_month_filter(df_r, g_mes)

    # --- agrega por mês
    def monthly_sum(df, label):
        if df.empty:
            return pd.DataFrame(columns=["mes", "tipo", "USD"])
        d = df.copy()
        d["mes"] = pd.to_datetime(d["data"]).dt.to_period("M").astype(str)
        g = (
            d.groupby("mes")[["valor_usd"]]
            .sum()
            .reset_index()
            .rename(columns={"valor_usd": "USD"})
        )
        g["tipo"] = label
        return g[["mes", "tipo", "USD"]]

    agg = pd.concat(
        [
            monthly_sum(df_r, "Receitas (Amazon)"),
            monthly_sum(df_g, "Despesas (Gastos)"),
            monthly_sum(df_i, "Despesas (Invest.)"),
        ],
        ignore_index=True,
    )

    if agg.empty:
        st.info("Cadastre dados para visualizar os gráficos.")
    else:
        # --- prepara pivot e métricas auxiliares
        pivot = (
            agg.pivot_table(
                index="mes", columns="tipo", values="USD", aggfunc="sum", fill_value=0
            )
            .reset_index()
        )
        for c in ["Despesas (Gastos)", "Despesas (Invest.)", "Receitas (Amazon)"]:
            if c not in pivot:
                pivot[c] = 0.0
        pivot["Despesas Totais"] = pivot["Despesas (Gastos)"] + pivot["Despesas (Invest.)"]
        pivot["Resultado"] = pivot["Receitas (Amazon)"] - pivot["Despesas Totais"]

        # --- cores (linha forte + área translúcida)
        RECEITA_LINE = "#2ecc71"            # verde (linha)
        RECEITA_FILL = "rgba(46,204,113,.18)"
        DESPESA_LINE = "#ff6b6b"            # vermelho (linha)
        DESPESA_FILL = "rgba(255,107,107,.18)"

        # --- dados no formato longo somente para Receita x Despesa
        df_long = pivot.melt(
            id_vars="mes",
            value_vars=["Receitas (Amazon)", "Despesas Totais"],
            var_name="Serie",
            value_name="Valor",
        )

        def serie_layer(name: str, line_color: str, fill_color: str):
            base = alt.Chart(df_long).transform_filter(alt.datum.Serie == name)

            # área translúcida
            area = base.mark_area(
                interpolate="monotone",
            ).encode(
                x=alt.X("mes:N", title="Mês", sort=alt.SortField("mes", order="ascending")),
                y=alt.Y("Valor:Q", title="USD", stack=None),
                tooltip=[
                    alt.Tooltip("mes:N", title="Mês"),
                    alt.Tooltip("Valor:Q", title="USD", format=",.2f"),
                ],
                color=alt.value(fill_color),  # define o fill com alpha
            )

            # linha destacada
            line = base.mark_line(
                interpolate="monotone",
                strokeWidth=3.5,
            ).encode(
                x=alt.X("mes:N", sort=alt.SortField("mes", order="ascending")),
                y=alt.Y("Valor:Q", stack=None),
                color=alt.value(line_color),
            )

            # pontos na linha
            pts = base.mark_point(size=70, filled=True).encode(
                x="mes:N",
                y="Valor:Q",
                color=alt.value(line_color),
            )

            return area + line + pts

        receita_layer = serie_layer("Receitas (Amazon)", RECEITA_LINE, RECEITA_FILL)
        despesa_layer = serie_layer("Despesas Totais", DESPESA_LINE, DESPESA_FILL)

        line_chart = (
            alt.layer(receita_layer, despesa_layer)
            .resolve_scale(color="independent")
            .properties(height=320)
            .configure_view(strokeWidth=0)
        )

        # --- barras do Resultado (cores condicionais e cantos arredondados)
        bars = (
            alt.Chart(pivot)
            .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
            .encode(
                x=alt.X("mes:N", title="Mês", sort=alt.SortField("mes", order="ascending")),
                y=alt.Y("Resultado:Q", title="Resultado (USD)"),
                color=alt.condition(
                    alt.datum.Resultado >= 0, alt.value(RECEITA_LINE), alt.value(DESPESA_LINE)
                ),
                tooltip=[
                    alt.Tooltip("mes:N", title="Mês"),
                    alt.Tooltip("Resultado:Q", title="USD", format=",.2f"),
                ],
            )
            .properties(height=320)
            .configure_view(strokeWidth=0)
        )

        # --- render
        c1, c2 = st.columns(2)
        with c1:
            st.markdown(
                "#### Receitas x Despesas (USD)"
                + (f" — {month_label(g_mes)}" if g_mes else "")
            )
            st.altair_chart(line_chart, use_container_width=True)

        with c2:
            st.markdown(
                "#### Resultado por mês (USD)"
                + (f" — {month_label(g_mes)}" if g_mes else "")
            )
            st.altair_chart(bars, use_container_width=True)


# ============================
# TAB 5 - SALDOS (AMAZON)
# ============================
with tab5:
    st.subheader("Saldos — Amazon Seller (USD)")

    with st.form("form_saldos"):
        data_s = st.date_input("Data do snapshot", value=date.today(), format="DD/MM/YYYY")
        disp = st.number_input("Disponível para saque (USD)", 0.0, step=0.01, format="%.2f")
        pend = st.number_input("Pendente (USD)", 0.0, step=0.01, format="%.2f")
        moeda = st.selectbox("Moeda", ["USD","BRL","EUR"], index=0)
        if st.form_submit_button("Salvar snapshot"):
            add_row("amazon_saldos", dict(data=data_s.strftime("%Y-%m-%d"), disponivel=disp, pendente=pend, moeda=moeda))
            st.rerun()

    df_s = df_sql("""SELECT id, data, disponivel, pendente, moeda
                     FROM amazon_saldos
                     ORDER BY date(data) DESC, id DESC;""")
    if not df_s.empty:
        last = df_s.iloc[0]
        card = (f"Disponível: {money_usd(last['disponivel'])} · Pendente: {money_usd(last['pendente'])}") \
               if last["moeda"] == "USD" else (f"Disponível: {money_brl(last['disponivel'])} · Pendente: {money_brl(last['pendente'])}")
        st.markdown(
            f"""<div class="metric-card center" style="max-width:760px;background:linear-gradient(145deg,#233a74,#1a2b57);color:#fff;border:1px solid rgba(255,255,255,.10);border-radius:18px;padding:22px 26px;box-shadow:0 18px 46px rgba(0,0,0,.55),0 2px 0 {PRIMARY} inset;">
                    <div class="title" style="font-size:12px;letter-spacing:.45px;text-transform:uppercase;opacity:.85;font-weight:800;">Último snapshot ({last['data']} — {last['moeda']})</div>
                    <div class="value" style="font-size:28px;font-weight:900;margin-top:6px;">{card}</div>
                </div>""", unsafe_allow_html=True
        )

        df_view_s = pd.DataFrame({
            "ID": df_s["id"].astype(int), "Data": df_s["data"],
            "Disponível": df_s.apply(lambda r: money_usd(r["disponivel"]) if r["moeda"]=="USD" else money_brl(r["disponivel"]), axis=1),
            "Pendente":  df_s.apply(lambda r: money_usd(r["pendente"])  if r["moeda"]=="USD" else money_brl(r["pendente"]),  axis=1),
            "Moeda": df_s["moeda"],
        })
        st.markdown(df_to_clean_html(df_view_s, "del_saldo", "tbl_saldo"), unsafe_allow_html=True)
    else:
        st.info("Sem snapshots cadastrados.")

    st.markdown("### Settlements (Finances) — transferências da Amazon para você")
    df_set = df_sql("""SELECT id, data, amount_usd, group_id, desc
                       FROM amazon_settlements
                       ORDER BY date(data) DESC, id DESC;""")
    if not df_set.empty:
        view_set = pd.DataFrame({
            "ID": df_set["id"].astype(int), "Data": df_set["data"],
            "Valor (USD)": df_set["amount_usd"].map(money_usd),
            "Group": df_set["group_id"].fillna(""), "Descrição": df_set["desc"].fillna(""),
        })
        st.markdown(df_to_clean_html(view_set, "del_settle", "tbl_settle"), unsafe_allow_html=True)
        total_set = float(df_set["amount_usd"].sum())
        st.markdown(
            f"""<div class="metric-card center" style="max-width:760px; margin-top:14px;background:linear-gradient(145deg,#233a74,#1a2b57);color:#fff;border:1px solid rgba(255,255,255,.10);border-radius:18px;padding:22px 26px;box-shadow:0 18px 46px rgba(0,0,0,.55),0 2px 0 {PRIMARY} inset;">
                    <div class="title">Total recebido (settlements)</div>
                    <div class="value">{escape(money_usd(total_set))}</div>
                </div>""", unsafe_allow_html=True
        )
    else:
        st.info("Sem settlements sincronizados. Use o botão na aba 'Receitas (FBA)' para puxar os últimos 60 dias.")

# ============================
# TAB 6 - PRODUTOS (SKU PLANNER)
# ============================
with tab6:
    st.subheader("Cadastro e métricas por Produto (FBA)")

    with st.form("form_produto"):
        c1, c2 = st.columns([2, 1])
        with c1:
            data_add_dt = st.date_input("Data adicionada na Amazon", value=date.today(), format="DD/MM/YYYY")
            nome = st.text_input("Nome do produto *", placeholder="Ex.: Carrinho")
            sku = st.text_input("SKU", placeholder="Ex.: ABC-123")
            upc = st.text_input("UPC")
            asin = st.text_input("ASIN")
            link_amz = st.text_input("Link do produto na Amazon")
            link_for = st.text_input("Link do fornecedor")
        with c2:
            estoque = st.number_input("Estoque", min_value=0, step=1, value=0)
            quantidade = st.number_input("Quantidade comprada (para rateio)", min_value=0, step=1, value=0)
            custo_base = st.number_input("Custo unitário base (USD)", min_value=0.0, step=0.01, format="%.2f")
            freight = st.number_input("Frete do lote (USD)", min_value=0.0, step=0.01, format="%.2f")
            tax = st.number_input("TAX do lote (USD)", min_value=0.0, step=0.01, format="%.2f")
            prep = st.number_input("PREP (USD) por unidade", min_value=0.0, step=0.01, value=2.0, format="%.2f")
            sold_for = st.number_input("Sold for (USD)", min_value=0.0, step=0.01, format="%.2f")
            amazon_fees = st.number_input("Amazon Fees (USD)", min_value=0.0, step=0.01, format="%.2f")

        if st.form_submit_button("Salvar produto"):
            if not nome.strip():
                st.warning("Informe o nome do produto.")
            else:
                date_map = produtos_date_insert_map(data_add_dt)
                row = dict(
                    **date_map,
                    nome=nome.strip(), sku=sku.strip(), upc=upc.strip(), asin=asin.strip(),
                    estoque=int(estoque), custo_base=custo_base, freight=freight, tax=tax,
                    quantidade=int(quantidade), prep=prep, sold_for=sold_for, amazon_fees=amazon_fees,
                    link_amazon=link_amz.strip(), link_fornecedor=link_for.strip()
                )
                add_row("produtos", row)
                st.success("Produto salvo!")
                st.rerun()

    # ---- Lista + métricas dos produtos
    date_expr = produtos_date_sql_expr()
    dfp_all = df_sql(f"""
        SELECT id, {date_expr} AS data_add, nome, sku, upc, asin, estoque,
               custo_base, freight, tax, quantidade, prep, sold_for, amazon_fees,
               link_amazon, link_fornecedor
        FROM produtos
        ORDER BY date({date_expr}) DESC, id DESC;
    """)
    dfp = apply_month_filter(dfp_all, g_mes, col="data_add") if g_mes else dfp_all

    if not dfp.empty:
        # métricas por produto
        dfv = dfp.copy()
        dfv["p2b"] = dfv.apply(price_to_buy_eff, axis=1)
        dfv["gross_profit"] = dfv.apply(gross_profit_unit, axis=1)
        dfv["roi"] = dfv.apply(gross_roi, axis=1)
        dfv["margin"] = dfv.apply(margin_pct, axis=1)

        # tabela
        view = pd.DataFrame({
            "ID": dfv["id"].astype(int), "Data": dfv["data_add"], "Nome": dfv["nome"],
            "SKU": dfv["sku"].fillna(""), "UPC": dfv["upc"].fillna(""), "ASIN": dfv["asin"].fillna(""),
            "Estoque": dfv["estoque"].astype(int),
            "Price to Buy": dfv["p2b"].map(money_usd), "Amazon Fees": dfv["amazon_fees"].map(money_usd),
            "PREP": dfv["prep"].map(money_usd), "Sold for": dfv["sold_for"].map(money_usd),
            "Gross Profit": dfv["gross_profit"].map(money_usd),
            "Gross ROI": (dfv["roi"]*100).map(lambda x: f"{x:.2f}%"),
            "Margem %": (dfv["margin"]*100).map(lambda x: f"{x:.2f}%"),
            "Amazon": dfv["link_amazon"].fillna(""), "Fornecedor": dfv["link_fornecedor"].fillna(""),
        })
        st.markdown(df_to_clean_html(view, "del_prod", "tbl_prod"), unsafe_allow_html=True)

        # ===== Receitas FBA para calcular lucro realizado
        dr_all = df_sql("""
            SELECT id, data, produto_id, quantidade, valor_usd, sku, produto
            FROM amazon_receitas
            ORDER BY date(data) DESC, id DESC;
        """)
        dr_mes = apply_month_filter(dr_all, g_mes) if g_mes else dr_all

        # ---- helpers de matching e soma do lucro
        def _maps_from_products(df_products: pd.DataFrame):
            by_id  = df_products.set_index("id").to_dict("index")
            by_sku = { _norm(s): r for s, r in df_products.set_index("sku").to_dict("index").items()  if s and str(s).strip() }
            by_upc = { _norm(s): r for s, r in df_products.set_index("upc").to_dict("index").items()  if s and str(s).strip() }
            by_asin= { _norm(s): r for s, r in df_products.set_index("asin").to_dict("index").items() if s and str(s).strip() }
            by_name= { _norm(s): r for s, r in df_products.set_index("nome").to_dict("index").items() if s and str(s).strip() }
            return by_id, by_sku, by_upc, by_asin, by_name

        def _sum_profit(receipts_df: pd.DataFrame, prod_df: pd.DataFrame) -> float:
            if receipts_df.empty or prod_df.empty:
                return 0.0
            by_id, by_sku, by_upc, by_asin, by_name = _maps_from_products(prod_df)
            r = receipts_df.copy()
            r["produto_id"] = pd.to_numeric(r["produto_id"], errors="coerce").astype("Int64")
            total = 0.0
            for _, row in r.iterrows():
                prod = _match_prod_for_receipt(row, by_id, by_sku, by_upc, by_asin, by_name)
                if not prod:
                    continue
                gp_u = gross_profit_unit(prod)
                try:
                    q = int(row.get("quantidade", 0) or 0)
                except Exception:
                    q = 0
                total += gp_u * q
            return float(total)

        # 1) Lucro do período selecionado (usa produtos filtrados)
        lucro_periodo = _sum_profit(dr_mes, dfv)

        # 2) Lucro TOTAL (todos os meses/anos)
        lucro_total = _sum_profit(dr_all, dfp_all)

        # ==== Cards no estilo KPI verde, centralizados
        label_lucro = "Lucro realizado no período" + (f" — {month_label(g_mes)}" if g_mes else "")
        render_single_kpi(kind="receita", label=label_lucro, usd_value=lucro_periodo, brl_value=0.0, center=True)

        # Card extra: Lucro TOTAL (todos os meses/anos)
        render_single_kpi(kind="receita", label="Lucro TOTAL — soma de todos os meses", usd_value=lucro_total, brl_value=0.0, center=True)

    else:
        st.info("Cadastre produtos para ver as métricas.")

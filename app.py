# app.py
from datetime import date
from decimal import Decimal
from html import escape
from pathlib import Path
import base64
import os
import sqlite3
import pandas as pd
import streamlit as st
import altair as alt
import re

# ----------------------------------
# Config
# ----------------------------------
st.set_page_config(page_title="Controle Financeiro Qota Store", layout="wide")

DB_PATH = os.getenv("DB_PATH", "finance.db")
PRIMARY = "#2F529E"      # azul principal
ACCENT  = "#FE0000"      # vermelho
WHITE   = "#FFFFFF"

# ----------------------------------
# DB helpers
# ----------------------------------
@st.cache_resource
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

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
    cols = ",".join(row.keys())
    qmarks = ",".join(["?"] * len(row))
    get_conn().execute(f"INSERT INTO {table} ({cols}) VALUES ({qmarks});", tuple(row.values())).connection.commit()

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

# ----------------------------------
# Navegação por query (exclusões)
# ----------------------------------
def handle_query_deletions():
    try: q = dict(st.query_params)
    except Exception: q = st.experimental_get_query_params()
    changed = False
    mapping = {
        "del_gasto": "gastos",
        "del_inv": "investimentos",
        "del_rec": "receitas",
        "del_saldo": "amazon_saldos",
        "del_pc": "produtos_compra",
        "del_ar": "amazon_receitas",
        "del_prod": "produtos",
    }
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
        .stApp {{
            background:
                radial-gradient(1200px 800px at 10% -10%, rgba(46, 82, 158, 0.25), transparent 60%),
                radial-gradient(1200px 800px at 90% -10%, rgba(254, 0, 0, 0.12), transparent 60%),
                linear-gradient(180deg, #0a122b 0%, #0d1735 40%, #0a122b 100%);
        }}
        .stMainBlockContainer {{ padding-top: 12px; }}
        h1, h2, h3, h4, h5, h6, .stMarkdown p, label, span {{ color: {WHITE} !important; }}
        .stMarkdown a {{ color: {ACCENT} !important; }}

        .stTextInput > div > div input,
        .stNumberInput input,
        .stTextArea textarea,
        .stDateInput input {{
            background: rgba(255,255,255,0.06);
            border: 1px solid rgba(255,255,255,.12);
            color: {WHITE};
            border-radius: 12px;
            box-shadow: 0 8px 22px rgba(0,0,0,.25) inset;
        }}
        .stSelectbox > div > div,
        .stMultiSelect > div > div {{
            background: rgba(255,255,255,0.06);
            border: 1px solid rgba(255,255,255,.12);
            border-radius: 12px;
        }}
        .stCheckbox, .stRadio, .stDateInput label, .stNumberInput label, .stTextInput label {{
            color: {WHITE} !important;
        }}

        .stButton > button {{
            background: linear-gradient(135deg, {ACCENT}, #b30000);
            color: {WHITE}; border: none; border-radius: 12px;
            padding: 10px 16px; font-weight: 800; letter-spacing:.3px;
            box-shadow: 0 10px 24px rgba(254,0,0,.35);
        }}
        .stButton > button:hover {{
            filter: brightness(1.05);
            transform: translateY(-1px);
            box-shadow: 0 14px 34px rgba(254,0,0,.45);
        }}

        table.fin {{
            width:100%; border-collapse:separate; border-spacing:0; font-size:14px; color:{WHITE};
            background: rgba(255,255,255,0.03);
            border: 1px solid rgba(255,255,255,.06);
            border-radius: 14px;
            box-shadow: 0 16px 40px rgba(0,0,0,.45);
            overflow: hidden;
        }}
        table.fin thead th {{
            background: linear-gradient(135deg, {PRIMARY}, #1c2f6a);
            color:#fff; text-align:left; padding:10px 12px; position:sticky; top:0; z-index:1;
            border-top:1px solid rgba(255,255,255,.08);
        }}
        table.fin td {{ padding:10px 12px; border-top:1px solid rgba(255,255,255,.06); }}
        table.fin td:last-child, table.fin th:last-child {{ text-align:center; width:120px; }}

        a.trash {{
            text-decoration:none; padding:6px 12px; border-radius:10px; display:inline-block;
            border:1px solid rgba(255,255,255,0.18); color:#fff;
            background: linear-gradient(135deg, #23386e, #17264f);
            box-shadow: 0 10px 22px rgba(0,0,0,.35);
            font-weight: 800;
        }}
        a.trash:hover {{ background: linear-gradient(135deg, {ACCENT}, #b30000); border-color: rgba(255,255,255,.3); }}

        .stTabs [role="tablist"] {{ justify-content: center; gap: 18px; border-bottom: 0; margin-top: 6px; }}
        .stTabs [role="tab"] {{
            position: relative; padding: 16px 22px 16px 48px !important;
            border-radius: 14px 14px 0 0 !important;
            border: 1px solid rgba(255,255,255,.18) !important;
            border-bottom: 3px solid transparent !important;
            background: rgba(255,255,255,0.06) !important;
            color: #FFFFFF !important; backdrop-filter: blur(6px);
            box-shadow: 0 10px 24px rgba(0,0,0,.35);
        }}
        .stTabs [role="tab"]:hover {{ background: rgba(255,255,255,0.1) !important; }}
        .stTabs [role="tab"][aria-selected="true"] {{
            background: linear-gradient(135deg, {PRIMARY}, #1a2d66) !important;
            border-color: rgba(255,255,255,.28) !important;
            border-bottom-color: {PRIMARY} !important;
            box-shadow: 0 2px 0 0 {PRIMARY} inset, 0 14px 36px rgba(0,0,0,.45) !important;
        }}
        .stTabs [role="tab"]::before {{
            content: ""; position: absolute; left: 16px; top: 50%; transform: translateY(-50%);
            width: 20px; height: 20px; opacity:.95; background-repeat:no-repeat; background-size:20px 20px;
            filter: drop-shadow(0 2px 4px rgba(0,0,0,.35));
        }}
        .stTabs [role="tab"]:nth-child(1)::before {{
            background-image: url("data:image/svg+xml;utf8,<svg viewBox='0 0 24 24' xmlns='http://www.w3.org/2000/svg'><path fill='%23FFFFFF' d='M12 3l9 8h-3v9h-5v-6H11v6H6v-9H3l9-8z'/></svg>");
        }}
        .stTabs [role="tab"]:nth-child(2)::before {{
            background-image: url(\"data:image/svg+xml;utf8,<svg viewBox='0 0 24 24' xmlns='http://www.w3.org/2000/svg'><path fill='%23FFFFFF' d='M21 8l-9-5-9 5v8l9 5 9-5V8zm-9 11l-7-3.89V9.47L12 13l7-3.53v5.64L12 19z'/></svg>\");
        }}
        .stTabs [role="tab"]:nth-child(3)::before {{
            background-image: url(\"data:image/svg+xml;utf8,<svg viewBox='0 0 24 24' xmlns='http://www.w3.org/2000/svg'><path fill='%23FFFFFF' d='M21 7H3V5h14a2 2 0 012 2zm0 2v8a2 2 0 01-2 2H3a2 2 0 01-2-2V9h20zm-4 3a2 2 0 100 4h3v-4h-3z'/></svg>\");
        }}
        .stTabs [role="tab"]:nth-child(4)::before {{
            background-image: url(\"data:image/svg+xml;utf8,<svg viewBox='0 0 24 24' xmlns='http://www.w3.org/2000/svg'><path fill='%23FFFFFF' d='M3 3h2v18H3V3zm4 10h2v8H7v-8zm4-6h2v14h-2V7zm4 4h2v10h-2V11zm4-6h2v16h-2V5z'/></svg>\");
        }}
        .stTabs [role="tab"]:nth-child(5)::before {{
            background-image: url(\"data:image/svg+xml;utf8,<svg viewBox='0 0 24 24' xmlns='http://www.w3.org/2000/svg'><path fill='%23FFFFFF' d='M12 3L2 9v2h20V9L12 3zM4 13h16v6H4v-6zm-2 8h20v2H2v-2z'/></svg>\");
        }}
        .stTabs [role="tab"]:nth-child(6)::before {{
            background-image: url(\"data:image/svg+xml;utf8,<svg viewBox='0 0 24 24' xmlns='http://www.w3.org/2000/svg'><path fill='%23FFFFFF' d='M9 2h6a2 2 0 012 2h1a2 2 0 012 2v14a2 2 0 01-2 2H6a2 2 0 01-2-2V6a2 2 0 012-2h1a2 2 0 012-2zm0 2v2h6V4H9z'/></svg>\");
        }}

        .metric-duo {{
            display: grid; grid-template-columns: repeat(2, minmax(260px, 1fr)); gap: 16px;
            margin: 8px 0 4px;
        }}
        .metric-card {{
            background: linear-gradient(145deg, #233a74, #1a2b57);
            color: {WHITE};
            border: 1px solid rgba(255,255,255,.10);
            border-radius: 18px;
            padding: 18px 20px;
            box-shadow: 0 18px 46px rgba(0,0,0,.55), 0 2px 0 {PRIMARY} inset;
        }}
        .metric-card .title {{
            font-size: 12px; letter-spacing:.45px; text-transform: uppercase; opacity: .85; font-weight: 800;
        }}
        .metric-card .value {{
            font-size: 28px; font-weight: 900; margin-top: 6px;
        }}
        .metric-card.brl::after, .metric-card.usd::after {{
            content: ""; display:block; height:4px; width: 64px; margin-top: 10px;
            border-radius: 999px; background: {ACCENT};
            box-shadow: 0 6px 18px rgba(254,0,0,.55);
        }}

        .metric-card.center {{
            max-width: 1080px;
            margin: 12px auto;
            padding: 22px 26px;
        }}
        .metric-card.center .title {{ font-size: 14px; letter-spacing:.6px; }}
        .metric-card.center .value {{ font-size: 36px; line-height: 1.15; }}
        .metric-card.center p,
        .metric-card.center div:not(.title):not(.value) {{ font-size: 16px; }}

        .total-badge {{
            max-width: 840px;
            background: linear-gradient(135deg, #12224d, #0f1c3f);
            color:#fff; padding:20px 28px; border-radius:18px;
            font-weight:800; font-size:18px; line-height:1.2;
            border:1px solid rgba(255,255,255,.10);
            box-shadow: 0 22px 60px rgba(0,0,0,.60), 0 0 0 1px rgba(255,255,255,.04) inset;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )

def metric_duo_cards(section_title: str, brl: float, usd: float):
    st.markdown(
        f"""
        <div class="metric-duo">
            <div class="metric-card brl">
                <div class="title">{escape(section_title)} — BRL</div>
                <div class="value">{escape(money_brl(brl))}</div>
            </div>
            <div class="metric-card usd">
                <div class="title">{escape(section_title)} — USD</div>
                <div class="value">{escape(money_usd(usd))}</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

def footer_total_badge(title: str, brl: float, usd: float, margin_top: int = 28):
    st.markdown(
        f"""
        <div style="width:100%; display:flex; justify-content:flex-start; margin:{margin_top}px 0 8px;">
          <div class="total-badge">
            <span>{escape(title)}</span>
            <span style="margin-left:14px; opacity:.98;">• BRL: {escape(money_brl(brl))} • USD: {escape(money_usd(usd))}</span>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

def df_to_clean_html(df: pd.DataFrame, del_param: str, anchor: str) -> str:
    if "Data" in df: df["Data"] = pd.to_datetime(df["Data"]).dt.strftime("%d/%m/%Y")
    for col in df.columns:
        if col in {"ID", "Valor (BRL)", "Valor (USD)", "Lucro (USD)", "Subtotal (USD)", "Total (USD)", "Margem %"}:
            continue
        df[col] = df[col].astype(str).map(escape)
    df["Ações"] = df["ID"].map(lambda i: f'<a class="trash" href="?{del_param}={int(i)}#{anchor}" title="Excluir">Excluir</a>')
    return df.to_html(index=False, escape=False, border=0, classes=["fin"])

# ----------------------------------
# Compatibilidade com data_add / data_amz
# ----------------------------------
def produtos_date_sql_expr() -> str:
    cols = set(get_columns("produtos"))
    has_amz = "data_amz" in cols
    has_add = "data_add" in cols
    if has_amz and has_add:
        return "COALESCE(data_amz, data_add)"
    if has_amz:
        return "data_amz"
    if has_add:
        return "data_add"
    add_column_if_missing("produtos", "data_add", "TEXT")
    return "data_add"

def produtos_date_insert_map(d: date) -> dict:
    ds = d.strftime("%Y-%m-%d")
    cols = set(get_columns("produtos"))
    out = {}
    if "data_amz" in cols:
        out["data_amz"] = ds
    if "data_add" in cols:
        out["data_add"] = ds
    if not out:
        add_column_if_missing("produtos", "data_add", "TEXT")
        out["data_add"] = ds
    return out

# ----------------------------------
# Filtros de mês
# ----------------------------------
def get_all_months() -> list[str]:
    meses = set()
    for tbl in ["gastos", "investimentos", "receitas", "produtos_compra", "amazon_receitas", "amazon_saldos"]:
        try:
            df = df_sql(f"SELECT DISTINCT strftime('%Y-%m', date(data)) AS m FROM {tbl};")
            meses |= set(df["m"].dropna().tolist())
        except Exception:
            pass
    try:
        expr = produtos_date_sql_expr()
        df = df_sql(f"SELECT DISTINCT strftime('%Y-%m', date({expr})) AS m FROM produtos;")
        meses |= set(df["m"].dropna().tolist())
    except Exception:
        pass
    return sorted([m for m in meses if m])

def apply_month_filter(df: pd.DataFrame, month: str, col: str = "data") -> pd.DataFrame:
    if not month or df.empty or col not in df.columns:
        return df
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
            link_fornecedor TEXT
        );
    """)
    for col, decl in [
        ("sku","TEXT"), ("upc","TEXT"), ("asin","TEXT"),
        ("custo_base","REAL NOT NULL DEFAULT 0"), ("freight","REAL NOT NULL DEFAULT 0"),
        ("tax","REAL NOT NULL DEFAULT 0"), ("quantidade","INTEGER NOT NULL DEFAULT 0"),
        ("prep","REAL NOT NULL DEFAULT 2"), ("sold_for","REAL NOT NULL DEFAULT 0"),
        ("amazon_fees","REAL NOT NULL DEFAULT 0"), ("link_amazon","TEXT"), ("link_fornecedor","TEXT"),
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
    for col, decl in [
        ("produto_id","INTEGER"), ("produto","TEXT"), ("sku","TEXT"),
        ("valor_usd","REAL NOT NULL DEFAULT 0")
    ]:
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
        """)
        conn.commit()
    except Exception:
        pass

    ensure_table("""
        CREATE TABLE IF NOT EXISTS amazon_saldos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            disponivel REAL NOT NULL DEFAULT 0,
            pendente REAL NOT NULL DEFAULT 0,
            moeda TEXT NOT NULL DEFAULT 'USD'
        );
    """)

# ----------------------------------
# Métricas de produto
# ----------------------------------
def price_to_buy_eff(row) -> float:
    base = float(row.get("custo_base", 0) or 0)
    tax = float(row.get("tax", 0) or 0)
    freight = float(row.get("freight", 0) or 0)
    qty = float(row.get("quantidade", 0) or 0)
    rateio = (tax + freight) / qty if qty > 0 else 0.0
    return base + rateio

def gross_profit_unit(row) -> float:
    sold_for = float(row.get("sold_for", 0) or 0)
    amz = float(row.get("amazon_fees", 0) or 0)
    prep = float(row.get("prep", 0) or 0)
    p2b = price_to_buy_eff(row)
    return sold_for - amz - prep - p2b

def gross_roi(row) -> float:
    p2b = price_to_buy_eff(row)
    return (gross_profit_unit(row) / p2b) if p2b > 0 else 0.0

def margin_pct(row) -> float:
    sold_for = float(row.get("sold_for", 0) or 0)
    gp = gross_profit_unit(row)
    return (gp / sold_for) if sold_for > 0 else 0.0

# ===== normalização e casamento robusto =====
def _norm(x: str) -> str:
    if x is None:
        return ""
    x = str(x)
    x = x.strip().lower()
    x = re.sub(r"[\s\-._]+", "", x)  # remove espaços, hífens, pontos, _
    return x

def _match_prod_for_receipt(row, by_id, by_sku, by_upc, by_asin, by_name):
    # 1) por id
    pid = row.get("produto_id")
    if pd.notna(pid):
        prod = by_id.get(int(pid))
        if prod: 
            return prod
    # 2) por SKU
    sku = _norm(row.get("sku"))
    if sku and sku in by_sku:
        return by_sku[sku]
    # 3) por UPC
    upc = _norm(row.get("upc") if "upc" in row else None)
    if upc and upc in by_upc:
        return by_upc[upc]
    # 4) por ASIN
    asin = _norm(row.get("asin") if "asin" in row else None)
    if asin and asin in by_asin:
        return by_asin[asin]
    # 5) por nome (último recurso)
    name = _norm(row.get("produto") if "produto" in row else None)
    if name and name in by_name:
        return by_name[name]
    return None

# ----------------------------------
# Boot
# ----------------------------------
init_db()
handle_query_deletions()
inject_global_css()

# ----------------------------------
# Header + Tabs
# ----------------------------------
render_logo_centered("logo-qota-storee-semfundo.png", width=260)
st.markdown(
    "<h1 style='text-align:center; font-weight:900; letter-spacing:.3px; margin-top:4px;'>Controle Financeiro Qota Store</h1>",
    unsafe_allow_html=True,
)

# Filtro de mês global
global_meses = get_all_months()
g_default = st.session_state.get("g_mes", "")
g_idx = ([""] + global_meses).index(g_default) if g_default in ([""] + global_meses) else 0
g_mes = st.selectbox(
    "Filtro de mês (global) — YYYY-MM",
    options=[""] + global_meses,
    index=g_idx,
    help="Em branco = todos os meses. Afeta Principal, Receitas (FBA), Fluxo de Caixa, Gráficos e Produtos.",
    key="g_mes",
)
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
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Gastos")
        with st.form("form_gasto"):
            data_gasto = st.date_input("Data do gasto", value=date.today(), format="DD/MM/YYYY")
            categoria = st.selectbox("Categoria",
                                     ["Compra de Produto","Mensalidade/Assinatura","Contabilidade/Legal",
                                      "Taxas/Impostos","Frete/Logística","Outros"])
            desc = st.text_input("Descrição do gasto")
            val_brl = st.number_input("Valor em BRL", min_value=0.0, step=0.01, format="%.2f")
            val_usd = st.number_input("Valor em USD", min_value=0.0, step=0.01, format="%.2f")
            metodo = st.selectbox("Método de pagamento",
                                  ["Pix","Cartão de Crédito","Boleto","Transferência","Dinheiro"])
            conta = st.selectbox("Conta/Banco", contas)
            quem = st.selectbox("Quem pagou", pessoas)
            if st.form_submit_button("Adicionar gasto"):
                add_row("gastos", dict(
                    data=data_gasto.strftime("%Y-%m-%d"), categoria=categoria, descricao=desc,
                    valor_brl=val_brl, valor_usd=val_usd, metodo=metodo, conta=conta, quem=quem
                ))
                st.rerun()

    with col2:
        st.subheader("Investimentos")
        with st.form("form_invest"):
            data_inv = st.date_input("Data do investimento", value=date.today(), format="DD/MM/YYYY")
            inv_brl = st.number_input("Valor em BRL", min_value=0.0, step=0.01, format="%.2f")
            inv_usd = st.number_input("Valor em USD", min_value=0.0, step=0.01, format="%.2f")
            metodo_i = st.selectbox("Método de pagamento",
                                    ["Pix","Cartão de Crédito","Boleto","Transferência","Dinheiro"])
            conta_i = st.selectbox("Conta/Banco", contas)
            quem_i = st.selectbox("Quem investiu/pagou", pessoas)
            if st.form_submit_button("Adicionar investimento"):
                add_row("investimentos", dict(
                    data=data_inv.strftime("%Y-%m-%d"), valor_brl=inv_brl, valor_usd=inv_usd,
                    metodo=metodo_i, conta=conta_i, quem=quem_i
                ))
                st.rerun()

    left, right = st.columns(2)
    with left:
        st.markdown("### Gastos cadastrados")
        df_g_all = df_sql("""SELECT id, data, categoria, descricao, valor_brl, valor_usd, metodo, conta, quem
                             FROM gastos ORDER BY date(data) DESC, id DESC;""")
        df_g = apply_month_filter(df_g_all, g_mes)
        tot_g_brl = float(df_g["valor_brl"].sum()) if not df_g.empty else 0.0
        tot_g_usd = float(df_g["valor_usd"].sum()) if not df_g.empty else 0.0
        metric_duo_cards("Totais de Gastos (mês filtrado)" if g_mes else "Totais de Gastos", tot_g_brl, tot_g_usd)

        if not df_g.empty:
            df_view = pd.DataFrame({
                "ID": df_g["id"].astype(int),
                "Data": df_g["data"],
                "Categoria": df_g["categoria"].fillna(""),
                "Descrição": df_g["descricao"].fillna(""),
                "Valor (BRL)": df_g["valor_brl"].map(money_brl),
                "Valor (USD)": df_g["valor_usd"].map(money_usd),
                "Método": df_g["metodo"].fillna(""),
                "Quem pagou": df_g["quem"].fillna(""),
            })
            st.markdown(df_to_clean_html(df_view, "del_gasto", "tbl_gastos"), unsafe_allow_html=True)
        else:
            st.info("Sem gastos no filtro atual.")
        footer_total_badge(
            "Gastos — Total de TODOS os meses",
            float(df_g_all["valor_brl"].sum()) if not df_g_all.empty else 0.0,
            float(df_g_all["valor_usd"].sum()) if not df_g_all.empty else 0.0,
            margin_top=24
        )

    with right:
        st.markdown("### Investimentos cadastrados")
        df_i_all = df_sql("""SELECT id, data, valor_brl, valor_usd, metodo, conta, quem
                             FROM investimentos ORDER BY date(data) DESC, id DESC;""")
        df_i = apply_month_filter(df_i_all, g_mes)
        tot_i_brl = float(df_i["valor_brl"].sum()) if not df_i.empty else 0.0
        tot_i_usd = float(df_i["valor_usd"].sum()) if not df_i.empty else 0.0
        metric_duo_cards("Totais de Investimentos (mês filtrado)" if g_mes else "Totais de Investimentos",
                         tot_i_brl, tot_i_usd)

        if not df_i.empty:
            df_view_i = pd.DataFrame({
                "ID": df_i["id"].astype(int),
                "Data": df_i["data"],
                "Valor (BRL)": df_i["valor_brl"].map(money_brl),
                "Valor (USD)": df_i["valor_usd"].map(money_usd),
                "Método": df_i["metodo"].fillna(""),
                "Quem investiu/pagou": df_i["quem"].fillna(""),
            })
            st.markdown(df_to_clean_html(df_view_i, "del_inv", "tbl_invest"), unsafe_allow_html=True)
        else:
            st.info("Sem investimentos no filtro atual.")
        footer_total_badge(
            "Investimentos — Total de TODOS os meses",
            float(df_i_all["valor_brl"].sum()) if not df_i_all.empty else 0.0,
            float(df_i_all["valor_usd"].sum()) if not df_i_all.empty else 0.0,
            margin_top=24
        )

# ============================
# TAB 2 - RECEITAS (FBA)
# ============================
with tab2:
    st.subheader("Produtos Vendidos (Receitas FBA)")

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
        if prod_options:
            labels = [x[0] for x in prod_options]
            sel = st.selectbox("Produto vendido (SKU | UPC | Nome)", labels, index=0)
            pid = [x for x in prod_options if x[0]==sel][0][1]
            sel_sku = [x for x in prod_options if x[0]==sel][0][2]
        else:
            st.warning("Cadastre produtos na aba **Produtos (SKU Planner)** para selecionar aqui.")
            pid, sel_sku = None, ""

        qty_ar = st.number_input("Quantidade vendida", min_value=1, step=1, value=1, key="ar_qty")
        val_ar = st.number_input("Valor recebido (USD) dentro da Amazon (por unidade)", min_value=0.0, step=0.01,
                                 format="%.2f", key="ar_val")
        quem_ar = st.selectbox("Quem lançou", pessoas, key="ar_quem")
        obs_ar = st.text_input("Observação (opcional)", key="ar_obs")

        if st.form_submit_button("Adicionar recebimento (Amazon)"):
            if pid is None:
                st.error("Selecione um produto.")
            else:
                add_row("amazon_receitas", dict(
                    data=data_ar.strftime("%Y-%m-%d"), produto_id=pid, quantidade=int(qty_ar),
                    valor_usd=val_ar*int(qty_ar), quem=quem_ar, obs=obs_ar.strip(), sku=sel_sku
                ))
                get_conn().execute(
                    "UPDATE produtos SET estoque = MAX(0, estoque - ?) WHERE id = ?;", (int(qty_ar), pid)
                ).connection.commit()
                st.success("Recebimento adicionado e estoque atualizado.")
                st.rerun()

    dr_all = df_sql("""SELECT id, data, produto_id, quantidade, valor_usd, quem, obs, sku, produto
                       FROM amazon_receitas ORDER BY date(data) DESC, id DESC;""")
    dr = apply_month_filter(dr_all, g_mes) if g_mes else dr_all

    tot_qty = int(dr["quantidade"].sum()) if not dr.empty else 0
    tot_val = float(dr["valor_usd"].sum()) if not dr.empty else 0.0
    st.markdown(
        f"""<div class="metric-card center">
                <div class="title">Vendido no período</div>
                <div class="value">
                    Quantidade: {tot_qty} • Valor: {escape(money_usd(tot_val))}
                </div>
            </div>""",
        unsafe_allow_html=True
    )

    if not dr.empty:
        prods = dfp_all[["id","nome","sku","upc","asin"]].rename(columns={"id":"pid"})
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
    df_r["valor_brl"] = 0.0
    df_r["lucro"] = 0.0

    df_g_f = apply_month_filter(df_g, g_mes) if g_mes else df_g
    df_i_f = apply_month_filter(df_i, g_mes) if g_mes else df_i
    df_r_f = apply_month_filter(df_r, g_mes) if g_mes else df_r

    def monthly(df, kind):
        if df.empty:
            return pd.DataFrame(columns=["mes","tipo","brl","usd"])
        t = df.copy()
        t["mes"] = pd.to_datetime(t["data"]).dt.to_period("M").astype(str)
        g = t.groupby("mes")[["valor_brl","valor_usd"]].sum().reset_index().rename(columns={"valor_brl":"brl","valor_usd":"usd"})
        g["tipo"]=kind
        return g

    m_g = monthly(df_g_f,"Despesas (Gastos)")
    m_i = monthly(df_i_f,"Despesas (Invest.)")
    m_r = monthly(df_r_f,"Receitas (Amazon)")

    all_brl = pd.concat([m_g[["mes","tipo","brl"]], m_i[["mes","tipo","brl"]], m_r[["mes","tipo","brl"]]], ignore_index=True)
    all_usd = pd.concat([m_g[["mes","tipo","usd"]], m_i[["mes","tipo","usd"]], m_r[["mes","tipo","usd"]]], ignore_index=True)

    if all_usd.empty and all_brl.empty:
        st.info("Sem dados suficientes.")
    else:
        p_brl = all_brl.pivot_table(index="mes", columns="tipo", values="brl", aggfunc="sum", fill_value=0).reset_index()
        p_usd = all_usd.pivot_table(index="mes", columns="tipo", values="usd", aggfunc="sum", fill_value=0).reset_index()
        for c in ["Despesas (Gastos)","Despesas (Invest.)","Receitas (Amazon)"]:
            if c not in p_brl: p_brl[c]=0.0
            if c not in p_usd: p_usd[c]=0.0
        p_brl["Resultado"]=p_brl["Receitas (Amazon)"]-(p_brl["Despesas (Gastos)"]+p_brl["Despesas (Invest.)"])
        p_usd["Resultado"]=p_usd["Receitas (Amazon)"]-(p_usd["Despesas (Gastos)"]+p_usd["Despesas (Invest.)"])

        c1,c2 = st.columns(2)
        with c1:
            st.markdown("#### BRL — por mês" + (f" (filtro: {g_mes})" if g_mes else ""))
            dfv = p_brl.copy()
            for col in ["Receitas (Amazon)","Despesas (Gastos)","Despesas (Invest.)","Resultado"]:
                dfv[col]=dfv[col].map(money_brl)
            st.dataframe(dfv.rename(columns={"mes":"Mês"}), use_container_width=True, hide_index=True)
        with c2:
            st.markdown("#### USD — por mês" + (f" (filtro: {g_mes})" if g_mes else ""))
            dfv = p_usd.copy()
            for col in ["Receitas (Amazon)","Despesas (Gastos)","Despesas (Invest.)","Resultado"]:
                dfv[col]=dfv[col].map(money_usd)
            st.dataframe(dfv.rename(columns={"mes":"Mês"}), use_container_width=True, hide_index=True)

        p_usd_all = pd.concat([
            monthly(df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM gastos;"),"Despesas (Gastos)"),
            monthly(df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM investimentos;"),"Despesas (Invest.)"),
            monthly(df_sql("SELECT date(data) as data, valor_usd, 0 as valor_brl FROM amazon_receitas;"),"Receitas (Amazon)")
        ], ignore_index=True).pivot_table(index="mes", columns="tipo", values="usd", aggfunc="sum", fill_value=0).reset_index()

        for c in ["Despesas (Gastos)","Despesas (Invest.)","Receitas (Amazon)"]:
            if c not in p_usd_all: p_usd_all[c]=0.0
        p_usd_all["Resultado"] = p_usd_all["Receitas (Amazon)"] - (p_usd_all["Despesas (Gastos)"] + p_usd_all["Despesas (Invest.)"])
        tot_receita_usd=float(p_usd_all["Receitas (Amazon)"].sum())
        tot_desp_usd=float(p_usd_all["Despesas (Gastos)"].sum()+p_usd_all["Despesas (Invest.)"].sum())
        tot_result_usd=float(p_usd_all["Resultado"].sum())
        st.markdown("### Totais Gerais (USD) — soma de todos os meses")
        st.markdown(
            f"""<div class="metric-card center" style="margin:12px 0 18px;">
                    <div class="title">Resumo</div>
                    <div class="value">
                        Receitas: {escape(money_usd(tot_receita_usd))} •
                        Despesas: {escape(money_usd(tot_desp_usd))} •
                        <span style="color:{ACCENT}">Resultado: {escape(money_usd(tot_result_usd))}</span>
                    </div>
                </div>""",
            unsafe_allow_html=True
        )

# ============================
# TAB 4 - GRÁFICOS
# ============================
with tab4:
    st.subheader("Gráficos Mensais (USD como principal)")

    df_g = df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM gastos;")
    df_i = df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM investimentos;")
    df_r = df_sql("SELECT date(data) as data, valor_usd FROM amazon_receitas;")
    df_r["valor_brl"] = 0.0
    df_r["lucro"] = 0.0

    if g_mes:
        df_g = apply_month_filter(df_g, g_mes)
        df_i = apply_month_filter(df_i, g_mes)
        df_r = apply_month_filter(df_r, g_mes)

    def monthly_sum(df, label):
        if df.empty:
            return pd.DataFrame(columns=["mes","tipo","BRL","USD","Lucro"])
        d = df.copy()
        d["mes"] = pd.to_datetime(d["data"]).dt.to_period("M").astype(str)
        g = d.groupby("mes")[["valor_brl","valor_usd"]].sum().reset_index() \
             .rename(columns={"valor_brl":"BRL","valor_usd":"USD"})
        g["tipo"] = label
        g["Lucro"] = 0.0
        return g[["mes","tipo","BRL","USD","Lucro"]]

    agg = pd.concat([
        monthly_sum(df_r, "Receitas (Amazon)"),
        monthly_sum(df_g, "Despesas (Gastos)"),
        monthly_sum(df_i, "Despesas (Invest.)")
    ], ignore_index=True)

    if agg.empty:
        st.info("Cadastre dados para visualizar os gráficos.")
    else:
        st.markdown("#### USD" + (f" (filtro: {g_mes})" if g_mes else ""))
        usd = agg[["mes","tipo","USD"]].rename(columns={"USD":"valor"})
        barsu = alt.Chart(usd).mark_bar().encode(
            x=alt.X("mes:N", sort=alt.SortField("mes", order="ascending"), title="Mês"),
            y=alt.Y("valor:Q", title="Valor (USD)"),
            color="tipo:N",
            tooltip=["mes","tipo","valor"]
        )
        resu = usd.pivot_table(index="mes", columns="tipo", values="valor",
                               aggfunc="sum", fill_value=0).reset_index()
        for c in ["Despesas (Gastos)","Despesas (Invest.)","Receitas (Amazon)"]:
            if c not in resu:
                resu[c] = 0.0
        resu["Resultado"] = resu["Receitas (Amazon)"] - (resu["Despesas (Gastos)"] + resu["Despesas (Invest.)"])
        lineu = alt.Chart(resu).mark_line(point=True).encode(
            x="mes:N",
            y=alt.Y("Resultado:Q", title="Resultado (USD)"),
            tooltip=["mes","Resultado"]
        )
        st.altair_chart(barsu + lineu, use_container_width=True)

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
            add_row("amazon_saldos", dict(
                data=data_s.strftime("%Y-%m-%d"),
                disponivel=disp, pendente=pend, moeda=moeda
            ))
            st.rerun()

    df_s = df_sql("""SELECT id, data, disponivel, pendente, moeda
                     FROM amazon_saldos
                     ORDER BY date(data) DESC, id DESC;""")
    if not df_s.empty:
        last = df_s.iloc[0]
        card = (f"Disponível: {money_usd(last['disponivel'])} · Pendente: {money_usd(last['pendente'])}") \
               if last["moeda"] == "USD" \
               else (f"Disponível: {money_brl(last['disponivel'])} · Pendente: {money_brl(last['pendente'])}")
        st.markdown(
            f"""<div class="metric-card center" style="max-width:760px;">
                    <div class="title">Último snapshot ({last['data']} — {last['moeda']})</div>
                    <div class="value">{card}</div>
                </div>""",
            unsafe_allow_html=True
        )

        df_view_s = pd.DataFrame({
            "ID": df_s["id"].astype(int),
            "Data": df_s["data"],
            "Disponível": df_s.apply(lambda r: money_usd(r["disponivel"]) if r["moeda"]=="USD" else money_brl(r["disponivel"]), axis=1),
            "Pendente":  df_s.apply(lambda r: money_usd(r["pendente"])  if r["moeda"]=="USD" else money_brl(r["pendente"]),  axis=1),
            "Moeda": df_s["moeda"],
        })
        st.markdown(df_to_clean_html(df_view_s, "del_saldo", "tbl_saldo"), unsafe_allow_html=True)
    else:
        st.info("Sem snapshots cadastrados.")

# ============================
# TAB 6 - PRODUTOS (SKU PLANNER)
# ============================
with tab6:
    st.subheader("Cadastro e métricas por Produto (FBA)")

    # -------- Form de produto
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
                # Preenche ambas colunas de data se existirem, evitando NOT NULL em bases antigas.
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

    # -------- Lista de produtos + métricas
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
        # ---- métricas unitárias
        dfv = dfp.copy()
        dfv["p2b"] = dfv.apply(price_to_buy_eff, axis=1)
        dfv["gross_profit"] = dfv.apply(gross_profit_unit, axis=1)
        dfv["roi"] = dfv.apply(gross_roi, axis=1)
        dfv["margin"] = dfv.apply(margin_pct, axis=1)

        view = pd.DataFrame({
            "ID": dfv["id"].astype(int),
            "Data": dfv["data_add"],
            "Nome": dfv["nome"],
            "SKU": dfv["sku"].fillna(""),
            "UPC": dfv["upc"].fillna(""),
            "ASIN": dfv["asin"].fillna(""),
            "Estoque": dfv["estoque"].astype(int),
            "Price to Buy": dfv["p2b"].map(money_usd),
            "Amazon Fees": dfv["amazon_fees"].map(money_usd),
            "PREP": dfv["prep"].map(money_usd),
            "Sold for": dfv["sold_for"].map(money_usd),
            "Gross Profit": dfv["gross_profit"].map(money_usd),
            "Gross ROI": (dfv["roi"]*100).map(lambda x: f"{x:.2f}%"),
            "Margem %": (dfv["margin"]*100).map(lambda x: f"{x:.2f}%"),
            "Amazon": dfv["link_amazon"].fillna(""),
            "Fornecedor": dfv["link_fornecedor"].fillna(""),
        })
        st.markdown(df_to_clean_html(view, "del_prod", "tbl_prod"), unsafe_allow_html=True)

        # ---- lucro realizado no período, combinando vendas amazon_receitas
        dr = df_sql("""SELECT id, data, produto_id, quantidade, valor_usd, sku, produto
                       FROM amazon_receitas
                       ORDER BY date(data) DESC, id DESC;""")
        dr = apply_month_filter(dr, g_mes) if g_mes else dr

        total_lucro = 0.0
        if not dr.empty:
            # dicionários de produto para casamento robusto
            by_id = dfv.set_index("id").to_dict("index")
            by_sku = { _norm(s): r for s, r in dfv.set_index("sku").to_dict("index").items() if s and str(s).strip() }
            by_upc = { _norm(s): r for s, r in dfv.set_index("upc").to_dict("index").items() if s and str(s).strip() }
            by_asin= { _norm(s): r for s, r in dfv.set_index("asin").to_dict("index").items() if s and str(s).strip() }
            by_name= { _norm(s): r for s, r in dfv.set_index("nome").to_dict("index").items() if s and str(s).strip() }

            for _, row in dr.iterrows():
                prod = _match_prod_for_receipt(row, by_id, by_sku, by_upc, by_asin, by_name)
                if not prod:
                    continue
                gp_u = gross_profit_unit(prod)
                try:
                    q = int(row.get("quantidade", 0) or 0)
                except Exception:
                    q = 0
                total_lucro += gp_u * q

        # ---- card centralizado de lucro realizado
        st.markdown(
            f"""<div class="metric-card center" style="max-width: 1200px;">
                    <div class="title">Lucro realizado no período selecionado</div>
                    <div class="value">{escape(money_usd(total_lucro))}</div>
                    <div style="margin-top:10px; font-weight:700;">Como calculamos?</div>
                    <div>Lucro = Σ (Gross Profit por unidade × quantidade vendida).</div>
                    <div>Gross Profit = Sold for – Amazon Fees – PREP – (Price to Buy + (TAX + Frete) ÷ quantidade).</div>
                </div>""",
            unsafe_allow_html=True
        )
    else:
        st.info("Cadastre produtos para ver as métricas.")

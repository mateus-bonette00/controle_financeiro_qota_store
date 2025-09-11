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

# ----------------------------------
# Config
# ----------------------------------
st.set_page_config(page_title="Controle Financeiro Qota Store", layout="wide")
DB_PATH = os.getenv("DB_PATH", "finance.db")
PRIMARY = "#0053b0"

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

def rebuild_receitas_without_real():
    cols = get_columns("receitas")
    if "REAL" in cols:
        conn = get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS receitas_tmp (
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
        keep = ["id","data","origem","descricao","valor_brl","valor_usd","metodo","conta","quem",
                "bruto","cogs","taxas_amz","ads","frete","descontos","lucro"]
        sel = ",".join([c for c in keep if c in cols])
        conn.execute(f"INSERT INTO receitas_tmp ({sel}) SELECT {sel} FROM receitas;")
        conn.execute("DROP TABLE receitas;")
        conn.execute("ALTER TABLE receitas_tmp RENAME TO receitas;")
        conn.commit()

def init_db():
    # básicos
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
            quem TEXT
        );
    """)
    for c in ["bruto","cogs","taxas_amz","ads","frete","descontos","lucro"]:
        add_column_if_missing("receitas", c, "REAL NOT NULL DEFAULT 0")

    # produtos/estoque
    ensure_table("""
        CREATE TABLE IF NOT EXISTS produtos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_add TEXT NOT NULL,
            nome TEXT NOT NULL,
            sku TEXT,
            upc TEXT,
            asin TEXT,
            estoque INTEGER NOT NULL DEFAULT 0,
            price_to_buy REAL NOT NULL DEFAULT 0,
            freight REAL NOT NULL DEFAULT 0,
            tax REAL NOT NULL DEFAULT 0,
            prep REAL NOT NULL DEFAULT 2,
            sold_for REAL NOT NULL DEFAULT 0,
            amazon_fees REAL NOT NULL DEFAULT 0,
            link_amazon TEXT,
            link_fornecedor TEXT
        );
    """)
    # migração defensiva
    add_column_if_missing("produtos", "data_add", "TEXT NOT NULL DEFAULT '1970-01-01'")
    add_column_if_missing("produtos", "nome", "TEXT")
    add_column_if_missing("produtos", "sku", "TEXT")
    add_column_if_missing("produtos", "upc", "TEXT")
    add_column_if_missing("produtos", "asin", "TEXT")
    add_column_if_missing("produtos", "estoque", "INTEGER NOT NULL DEFAULT 0")
    add_column_if_missing("produtos", "price_to_buy", "REAL NOT NULL DEFAULT 0")
    add_column_if_missing("produtos", "freight", "REAL NOT NULL DEFAULT 0")
    add_column_if_missing("produtos", "tax", "REAL NOT NULL DEFAULT 0")
    add_column_if_missing("produtos", "prep", "REAL NOT NULL DEFAULT 2")
    add_column_if_missing("produtos", "sold_for", "REAL NOT NULL DEFAULT 0")
    add_column_if_missing("produtos", "amazon_fees", "REAL NOT NULL DEFAULT 0")
    add_column_if_missing("produtos", "link_amazon", "TEXT")
    add_column_if_missing("produtos", "link_fornecedor", "TEXT")

    # recebidos amazon
    ensure_table("""
        CREATE TABLE IF NOT EXISTS amazon_receitas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            produto_id INTEGER,
            quantidade INTEGER NOT NULL DEFAULT 0,
            valor_usd REAL NOT NULL DEFAULT 0,
            quem TEXT,
            obs TEXT,
            FOREIGN KEY(produto_id) REFERENCES produtos(id) ON DELETE SET NULL
        );
    """)
    add_column_if_missing("amazon_receitas", "produto", "TEXT")
    add_column_if_missing("amazon_receitas", "sku", "TEXT")

    # saldos
    ensure_table("""
        CREATE TABLE IF NOT EXISTS amazon_saldos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            disponivel REAL NOT NULL DEFAULT 0,
            pendente REAL NOT NULL DEFAULT 0,
            moeda TEXT NOT NULL DEFAULT 'USD'
        );
    """)

    rebuild_receitas_without_real()

def add_row(table: str, row: dict):
    cols = ",".join(row.keys())
    qmarks = ",".join(["?"] * len(row))
    get_conn().execute(f"INSERT INTO {table} ({cols}) VALUES ({qmarks});", tuple(row.values())).connection.commit()

def delete_row(table: str, id_: int):
    get_conn().execute(f"DELETE FROM {table} WHERE id = ?;", (id_,)).connection.commit()

def df_sql(sql: str) -> pd.DataFrame:
    return pd.read_sql_query(sql, get_conn())

# ----------------------------------
# Utils
# ----------------------------------
def money_brl(x):
    try: v = Decimal(str(x))
    except Exception: return "R$ 0,00"
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def money_usd(x):
    try: v = Decimal(str(x))
    except Exception: return "$ 0.00"
    return f"$ {v:,.2f}"

def handle_query_deletions():
    try:
        q = dict(st.query_params)
    except Exception:
        q = st.experimental_get_query_params()
    changed = False
    mapping = {
        "del_gasto": "gastos",
        "del_inv": "investimentos",
        "del_rec": "receitas",
        "del_saldo": "amazon_saldos",
        "del_pc": "produtos",
        "del_ar": "amazon_receitas",
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

def totals_card(title: str, brl: float, usd: float):
    st.markdown(
        f"""
        <div style="margin: 15px 0; padding:16px; background:{PRIMARY};
                    border-radius:14px; color:white; display:flex;
                    flex-direction:column; align-items:flex-start;">
            <div style="font-size:20px; font-weight:600; margin-bottom:8px;">{escape(title)}</div>
            <div style="font-size:26px; font-weight:800; margin-bottom:6px;">Total BRL: {escape(money_brl(brl))}</div>
            <div style="font-size:26px; font-weight:800;">Total USD: {escape(money_usd(usd))}</div>
        </div>
        """, unsafe_allow_html=True,
    )

def primary_card_centered(html_inside: str, margin_top_px: int = 36):
    """Card central azul (sem tags 'soltas')."""
    st.markdown(
        f"""
        <div style="width:100%; display:flex; justify-content:center; margin:{margin_top_px}px 0 8px;">
          <div style="max-width:1100px; background:{PRIMARY}; color:#fff; padding:18px 24px;
                      border-radius:16px; font-weight:700; font-size:18px; line-height:1.35;
                      text-align:center; box-shadow:0 6px 20px rgba(0,0,0,.18);">
            {html_inside}
          </div>
        </div>
        """, unsafe_allow_html=True,
    )

def summary_card_usd(title: str, receita: float, despesa: float, resultado: float):
    st.markdown(
        f"""
        <div style="margin: 10px 0; padding:16px; background:{PRIMARY};
                    border-radius:14px; color:white;">
            <div style="font-size:18px; font-weight:700; margin-bottom:8px;">{escape(title)}</div>
            <div style="display:flex; gap:18px; flex-wrap:wrap;">
                <span><b>Receitas (USD):</b> {escape(money_usd(receita))}</span>
                <span><b>Despesas (USD):</b> {escape(money_usd(despesa))}</span>
                <span><b>Resultado (USD):</b> {escape(money_usd(resultado))}</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

def footer_total_badge(title: str, brl: float, usd: float, margin_top: int = 32):
    st.markdown(
        f"""
        <div style="width:100%; display:flex; justify-content:center; margin:{margin_top}px 0 8px;">
          <div style="background:{PRIMARY}; color:#fff; padding:16px 24px; border-radius:16px;
                      font-weight:800; font-size:18px; line-height:1.15; box-shadow:0 6px 20px rgba(0,0,0,.15);
                      letter-spacing:.2px; text-align:center;">
            <span>{escape(title)}</span>
            <span style="margin-left:10px;">— BRL: {escape(money_brl(brl))} · USD: {escape(money_usd(usd))}</span>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

def df_to_clean_html(df: pd.DataFrame, del_param: str, anchor: str) -> str:
    if "Data" in df:
        df["Data"] = pd.to_datetime(df["Data"]).dt.strftime("%d/%m/%Y")
    for col in df.columns:
        if col in {"ID", "Valor (BRL)", "Valor (USD)", "Lucro (USD)", "Subtotal (USD)", "Total (USD)", "Margem %"}:
            continue
        df[col] = df[col].astype(str).map(escape)
    df["Ações"] = df["ID"].map(lambda i: f'<a class="trash" href="?{del_param}={int(i)}#{anchor}" title="Excluir">🗑️</a>')
    return df.to_html(index=False, escape=False, border=0, classes=["fin"])

def inject_table_css():
    st.markdown(
        f"""
        <style>
        table.fin {{ width:100%; border-collapse:collapse; font-size:14px; }}
        table.fin thead th {{ background:{PRIMARY}; color:#fff; text-align:left; padding:8px 10px; }}
        table.fin td {{ border:1px solid rgba(255,255,255,0.08); padding:8px 10px; }}
        table.fin td:last-child, table.fin th:last-child {{ text-align:center; width:72px; }}
        a.trash {{ text-decoration:none; padding:6px 10px; border-radius:8px; display:inline-block;
                   border:1px solid rgba(255,255,255,0.15); color:#fff; background:transparent; }}
        a.trash:hover {{ background:{PRIMARY}; border-color:{PRIMARY}; }}
        </style>
        """, unsafe_allow_html=True,
    )

def render_logo_centered(path: str, width: int = 220):
    file = Path(path)
    if file.exists():
        b64 = base64.b64encode(file.read_bytes()).decode("utf-8")
        st.markdown(
            f"""<div style="text-align:center; margin:10px 0 6px;">
                   <img src="data:image/png;base64,{b64}" style="width:{width}px;">
                </div>""",
            unsafe_allow_html=True,
        )

def style_tabs_center_big():
    st.markdown(
        f"""
        <style>
        .stTabs [role="tablist"] {{
            justify-content: center; gap: 48px; border-bottom: 0; margin-top: 6px;
        }}
        .stTabs [role="tab"] {{
            padding: 18px 30px !important;
            border-radius: 14px 14px 0 0 !important;
            border: 1px solid white !important;
            border-bottom: 3px solid transparent !important;
            background: rgba(255,255,255,0.04) !important;
            color: #FFFFFF !important;
        }}
        .stTabs [role="tab"] span, .stTabs [role="tab"] p, .stTabs [role="tab"] div {{
            font-size: 20px !important; font-weight: 800 !important; line-height: 1.15 !important;
            color: #FFFFFF !important; margin: 0 !important;
        }}
        .stTabs [role="tab"]:hover {{ background: rgba(255,255,255,0.08) !important; }}
        .stTabs [role="tab"][aria-selected="true"] {{
            background: {PRIMARY} !important; border-color: white !important;
            border-bottom-color: {PRIMARY} !important; box-shadow: 0 2px 0 0 {PRIMARY} inset !important;
        }}
        .stTabs [role="tab"][aria-selected="true"] span, .stTabs [role="tab"][aria-selected="true"] p,
        .stTabs [role="tab"][aria-selected="true"] div {{ color: #FFFFFF !important; font-weight: 900 !important; }}
        .stTabs [role="tab"] a, .stTabs [role="tab"] svg {{ color: #FFFFFF !important; fill:#FFFFFF !important; }}
        </style>
        """,
        unsafe_allow_html=True,
    )

def get_all_months() -> list[str]:
    meses = set()
    for tbl in ["gastos", "investimentos", "receitas", "produtos", "amazon_receitas", "amazon_saldos"]:
        try:
            # produtos usa data_add
            if tbl == "produtos":
                df = df_sql("SELECT DISTINCT strftime('%Y-%m', date(data_add)) AS m FROM produtos;")
            else:
                df = df_sql(f"SELECT DISTINCT strftime('%Y-%m', date(data)) AS m FROM {tbl};")
            meses |= set(df["m"].dropna().tolist())
        except Exception:
            pass
    meses = sorted([m for m in meses if m])
    return meses

def apply_month_filter(df: pd.DataFrame, month: str) -> pd.DataFrame:
    if not month or df.empty:
        return df
    col = None
    if "data" in df.columns: col = "data"
    if "data_add" in df.columns: col = "data_add"
    if not col: return df
    return df[df[col].astype(str).str.startswith(month)].copy()

def apply_reset_if_needed(flag_key: str, defaults: dict):
    if st.session_state.get(flag_key):
        for k, v in defaults.items():
            st.session_state[k] = v
        st.session_state[flag_key] = False

# ---- cálculos produto
def per_unit_buy_effective(row) -> float:
    qtd = max(int(row.get("estoque", 0)), 1)
    return float(row.get("price_to_buy", 0)) + (float(row.get("tax", 0)) + float(row.get("freight", 0))) / qtd

def gross_profit_unit(row) -> float:
    buy_eff = per_unit_buy_effective(row)
    return float(row.get("sold_for", 0)) - float(row.get("amazon_fees", 0)) - float(row.get("prep", 0)) - buy_eff

def gross_roi(row) -> float:
    buy_eff = per_unit_buy_effective(row)
    return (gross_profit_unit(row) / buy_eff) if buy_eff > 0 else 0.0

def margin_pct(row) -> float:
    sf = float(row.get("sold_for", 0))
    return (gross_profit_unit(row) / sf) if sf > 0 else 0.0

# ----------------------------------
# Boot
# ----------------------------------
init_db()
handle_query_deletions()
inject_table_css()

# ----------------------------------
# Header + Navbar
# ----------------------------------
render_logo_centered("logo-qota-storee-semfundo.png", width=260)
st.markdown("<h1 style='text-align:center;'>Controle Financeiro Qota Store</h1>", unsafe_allow_html=True)

# ------- Filtro de mês GLOBAL -------
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

style_tabs_center_big()
st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "🏠 Principal", "📦 Receitas (FBA)", "📊 Fluxo de Caixa", "📈 Gráficos", "🏦 Saldos (Amazon)", "📋 Produtos (SKU Planner)"
])

contas = ["Nubank", "Nomad", "Wise", "Mercury Bank", "WesternUnion"]
pessoas = ["Bonette", "Daniel"]

# ============================
# TAB 1 - PRINCIPAL
# ============================
with tab1:
    col1, col2 = st.columns(2)

    # --- GASTOS
    gastos_defaults = {
        "g_data": date.today(),
        "g_categoria": "Compra de Produto",
        "g_desc": "",
        "g_val_brl": 0.0,
        "g_val_usd": 0.0,
        "g_metodo": "Pix",
        "g_conta": contas[0],
        "g_quem": pessoas[0],
    }
    apply_reset_if_needed("g_reset", gastos_defaults)

    with col1:
        st.subheader("Gastos")
        with st.form("form_gasto"):
            data_gasto = st.date_input("Data do gasto", value=st.session_state.get("g_data", date.today()),
                                       format="DD/MM/YYYY", key="g_data")
            categoria = st.selectbox("Categoria",
                                     ["Compra de Produto","Mensalidade/Assinatura","Contabilidade/Legal",
                                      "Taxas/Impostos","Frete/Logística","Outros"],
                                     key="g_categoria")
            desc = st.text_input("Descrição do gasto", key="g_desc")
            val_brl = st.number_input("Valor em BRL", min_value=0.0, step=0.01, format="%.2f", key="g_val_brl")
            val_usd = st.number_input("Valor em USD", min_value=0.0, step=0.01, format="%.2f", key="g_val_usd")
            metodo = st.selectbox("Método de pagamento",
                                  ["Pix","Cartão de Crédito","Boleto","Transferência","Dinheiro"],
                                  key="g_metodo")
            conta = st.selectbox("Conta/Banco", contas, key="g_conta")
            quem = st.selectbox("Quem pagou", pessoas, key="g_quem")
            if st.form_submit_button("Adicionar gasto"):
                add_row("gastos", dict(
                    data=data_gasto.strftime("%Y-%m-%d"), categoria=categoria, descricao=desc,
                    valor_brl=val_brl, valor_usd=val_usd, metodo=metodo, conta=conta, quem=quem
                ))
                st.session_state["g_reset"] = True
                st.rerun()

    # --- INVESTIMENTOS
    invest_defaults = {
        "i_data": date.today(),
        "i_brl": 0.0,
        "i_usd": 0.0,
        "i_metodo": "Pix",
        "i_conta": contas[0],
        "i_quem": pessoas[0],
    }
    apply_reset_if_needed("i_reset", invest_defaults)

    with col2:
        st.subheader("Investimentos")
        with st.form("form_invest"):
            data_inv = st.date_input("Data do investimento", value=st.session_state.get("i_data", date.today()),
                                     format="DD/MM/YYYY", key="i_data")
            inv_brl = st.number_input("Valor em BRL", min_value=0.0, step=0.01, format="%.2f", key="i_brl")
            inv_usd = st.number_input("Valor em USD", min_value=0.0, step=0.01, format="%.2f", key="i_usd")
            metodo_i = st.selectbox("Método de pagamento",
                                    ["Pix","Cartão de Crédito","Boleto","Transferência","Dinheiro"],
                                    key="i_metodo")
            conta_i = st.selectbox("Conta/Banco", contas, key="i_conta")
            quem_i = st.selectbox("Quem investiu/pagou", pessoas, key="i_quem")
            if st.form_submit_button("Adicionar investimento"):
                add_row("investimentos", dict(
                    data=data_inv.strftime("%Y-%m-%d"), valor_brl=inv_brl, valor_usd=inv_usd,
                    metodo=metodo_i, conta=conta_i, quem=quem_i
                ))
                st.session_state["i_reset"] = True
                st.rerun()

    # Listas + totais
    left, right = st.columns(2)
    with left:
        st.markdown("### Gastos cadastrados")
        df_g_all = df_sql("""SELECT id, data, categoria, descricao, valor_brl, valor_usd, metodo, conta, quem
                             FROM gastos ORDER BY date(data) DESC, id DESC;""")
        df_g = apply_month_filter(df_g_all, g_mes)
        tot_g_brl = float(df_g["valor_brl"].sum()) if not df_g.empty else 0.0
        tot_g_usd = float(df_g["valor_usd"].sum()) if not df_g.empty else 0.0
        totals_card("Totais de Gastos (mês filtrado)" if g_mes else "Totais de Gastos", tot_g_brl, tot_g_usd)
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
        totals_card("Totais de Investimentos (mês filtrado)" if g_mes else "Totais de Investimentos",
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
# TAB 2 - RECEITAS (FBA) — apenas VENDAS
# ============================
with tab2:
    st.subheader("Dinheiro recebido dentro da Amazon (USD)")

    # produtos para o dropdown
    df_prod_all = df_sql("""
        SELECT id, data_add, nome, sku, upc, asin, estoque,
               price_to_buy, freight, tax, prep, sold_for, amazon_fees,
               link_amazon, link_fornecedor
        FROM produtos
        ORDER BY date(data_add) DESC, id DESC;
    """)
    if df_prod_all.empty:
        st.info("Cadastre produtos na aba **Produtos (SKU Planner)** para registrar vendas.")
    else:
        def label_row(r):
            parts = [str(r["sku"] or "").strip(), str(r["upc"] or "").strip(), str(r["nome"] or "").strip()]
            label = " | ".join([p for p in parts if p])
            return f'{label}'

        labels = {int(r["id"]): label_row(r) for _, r in df_prod_all.iterrows()}

        with st.form("form_amz_receitas_only"):
            data_ar = st.date_input("Data do crédito", value=st.session_state.get("ar_data", date.today()),
                                    format="DD/MM/YYYY", key="ar_data")
            pid = st.selectbox("Produto vendido (SKU | UPC | Nome)", options=list(labels.keys()),
                               format_func=lambda x: labels.get(int(x), str(x)))
            qty_ar = st.number_input("Quantidade vendida", min_value=1, step=1,
                                     value=max(int(st.session_state.get("ar_qty", 1)), 1), key="ar_qty")
            val_ar = st.number_input("Valor recebido (USD) dentro da Amazon", min_value=0.0, step=0.01,
                                     format="%.2f", key="ar_val")
            quem_ar = st.selectbox("Quem lançou", pessoas, key="ar_quem")
            obs_ar = st.text_input("Observação (opcional)", key="ar_obs")

            submitted = st.form_submit_button("Adicionar recebimento (Amazon)")
            if submitted:
                add_row("amazon_receitas", dict(
                    data=data_ar.strftime("%Y-%m-%d"),
                    produto_id=int(pid),
                    quantidade=int(qty_ar),
                    valor_usd=float(val_ar),
                    quem=quem_ar,
                    obs=obs_ar.strip(),
                    produto=labels[int(pid)],
                    sku=df_prod_all.set_index("id").loc[int(pid)]["sku"]
                        if int(pid) in df_prod_all.set_index("id").index else ""
                ))
                # baixa estoque
                conn = get_conn()
                conn.execute("""
                    UPDATE produtos
                       SET estoque = CASE WHEN estoque - ? < 0 THEN 0 ELSE estoque - ? END
                     WHERE id=?;
                """, (int(qty_ar), int(qty_ar), int(pid)))
                conn.commit()
                st.success("Recebimento registrado e estoque atualizado.")
                st.rerun()

        # listagem
        df_ar_all = df_sql("""SELECT id, data, produto_id, quantidade, valor_usd, quem, obs
                              FROM amazon_receitas ORDER BY date(data) DESC, id DESC;""")
        df_ar = apply_month_filter(df_ar_all.copy(), g_mes) if g_mes else df_ar_all.copy()
        tot_ar_usd = float(df_ar["valor_usd"].sum()) if not df_ar.empty else 0.0
        tot_ar_qty = int(df_ar["quantidade"].sum()) if not df_ar.empty else 0

        # >>> NOVO: banner centralizado com total vendido (USD) e quantidade
        primary_card_centered(
            f"<div style='font-size:20px; font-weight:800;'>Total vendido (USD): {escape(money_usd(tot_ar_usd))}</div>"
            f"<div style='margin-top:4px;'>Quantidade vendida: <b>{tot_ar_qty}</b></div>",
            margin_top_px=4
        )

        if not df_ar.empty:
            df_prod = df_prod_all.set_index("id") if not df_prod_all.empty else pd.DataFrame()

            def getp(pid, col, default=""):
                try: return df_prod.loc[int(pid)][col]
                except Exception: return default

            df_ar_view = pd.DataFrame({
                "ID": df_ar["id"].astype(int),
                "Data": df_ar["data"],
                "Produto": df_ar["produto_id"].map(lambda x: getp(x, "nome","")),
                "SKU": df_ar["produto_id"].map(lambda x: getp(x, "sku","")),
                "Qtd": df_ar["quantidade"].astype(int),
                "Valor recebido (USD)": df_ar["valor_usd"].map(money_usd),
                "Quem": df_ar["quem"].fillna(""),
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
    df_r = df_sql("SELECT date(data) as data, 0 as valor_brl, bruto as valor_usd, lucro FROM receitas;")

    df_g_f = apply_month_filter(df_g, g_mes) if g_mes else df_g
    df_i_f = apply_month_filter(df_i, g_mes) if g_mes else df_i
    df_r_f = apply_month_filter(df_r, g_mes) if g_mes else df_r

    def monthly(df, kind, include_usd=True):
        if df.empty:
            x = pd.DataFrame(columns=["mes","tipo","brl","usd"])
            if "lucro" in df.columns: x["lucro"]=[]
            return x
        t = df.copy()
        t["mes"] = pd.to_datetime(t["data"]).dt.to_period("M").astype(str)
        agg = {"valor_brl":"sum", "valor_usd":"sum"} if include_usd else {"valor_brl":"sum"}
        g = t.groupby("mes").agg(agg)
        if "lucro" in t.columns: g["lucro"] = t.groupby("mes")["lucro"].sum()
        g = g.reset_index().rename(columns={"valor_brl":"brl","valor_usd":"usd"})
        g["tipo"]=kind
        return g

    m_g = monthly(df_g_f,"Despesas (Gastos)")
    m_i = monthly(df_i_f,"Despesas (Invest.)")
    m_r = monthly(df_r_f,"Receitas (FBA)")

    all_brl = pd.concat([m_g[["mes","tipo","brl"]], m_i[["mes","tipo","brl"]], m_r[["mes","tipo","brl"]]], ignore_index=True)
    all_usd = pd.concat([m_g[["mes","tipo","usd"]], m_i[["mes","tipo","usd"]], m_r[["mes","tipo","usd"]]], ignore_index=True)

    if all_usd.empty and all_brl.empty:
        st.info("Sem dados suficientes.")
    else:
        p_brl = all_brl.pivot_table(index="mes", columns="tipo", values="brl", aggfunc="sum", fill_value=0).reset_index()
        p_usd = all_usd.pivot_table(index="mes", columns="tipo", values="usd", aggfunc="sum", fill_value=0).reset_index()
        for c in ["Despesas (Gastos)","Despesas (Invest.)","Receitas (FBA)"]:
            if c not in p_brl: p_brl[c]=0.0
            if c not in p_usd: p_usd[c]=0.0
        p_brl["Resultado"]=p_brl["Receitas (FBA)"]-(p_brl["Despesas (Gastos)"]+p_brl["Despesas (Invest.)"])
        p_usd["Resultado"]=p_usd["Receitas (FBA)"]-(p_usd["Despesas (Gastos)"]+p_usd["Despesas (Invest.)"])

        c1,c2 = st.columns(2)
        with c1:
            st.markdown("#### BRL — por mês" + (f" (filtro: {g_mes})" if g_mes else ""))
            dfv = p_brl.copy()
            for col in ["Receitas (FBA)","Despesas (Gastos)","Despesas (Invest.)","Resultado"]:
                dfv[col]=dfv[col].map(money_brl)
            st.dataframe(dfv.rename(columns={"mes":"Mês"}), use_container_width=True, hide_index=True)
        with c2:
            st.markdown("#### USD — por mês" + (f" (filtro: {g_mes})" if g_mes else ""))
            dfv = p_usd.copy()
            for col in ["Receitas (FBA)","Despesas (Gastos)","Despesas (Invest.)","Resultado"]:
                dfv[col]=dfv[col].map(money_usd)
            st.dataframe(dfv.rename(columns={"mes":"Mês"}), use_container_width=True, hide_index=True)

        # Totais gerais
        p_usd_all = pd.concat([
            monthly(df_g,"Despesas (Gastos)")[["mes","tipo","usd"]],
            monthly(df_i,"Despesas (Invest.)")[["mes","tipo","usd"]],
            monthly(df_r,"Receitas (FBA)")[["mes","tipo","usd"]],
        ], ignore_index=True).pivot_table(index="mes", columns="tipo", values="usd", aggfunc="sum", fill_value=0).reset_index()
        for c in ["Despesas (Gastos)","Despesas (Invest.)","Receitas (FBA)"]:
            if c not in p_usd_all: p_usd_all[c]=0.0
        p_usd_all["Resultado"] = p_usd_all["Receitas (FBA)"] - (p_usd_all["Despesas (Gastos)"] + p_usd_all["Despesas (Invest.)"])
        tot_receita_usd=float(p_usd_all["Receitas (FBA)"].sum())
        tot_desp_usd=float(p_usd_all["Despesas (Gastos)"].sum()+p_usd_all["Despesas (Invest.)"].sum())
        tot_result_usd=float(p_usd_all["Resultado"].sum())
        st.markdown("### Totais Gerais (USD) — soma de todos os meses")
        summary_card_usd("Totais gerais (USD)", tot_receita_usd, tot_desp_usd, tot_result_usd)

# ============================
# TAB 4 - GRÁFICOS
# ============================
with tab4:
    st.subheader("Gráficos Mensais (USD como principal)")

    df_g = df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM gastos;")
    df_i = df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM investimentos;")
    df_r = df_sql("SELECT date(data) as data, 0 as valor_brl, bruto as valor_usd, lucro FROM receitas;")

    if g_mes:
        df_g = apply_month_filter(df_g, g_mes)
        df_i = apply_month_filter(df_i, g_mes)
        df_r = apply_month_filter(df_r, g_mes)

    def monthly_sum(df, label):
        if df.empty:
            return pd.DataFrame(columns=["mes","tipo","BRL","USD","Lucro"])
        d=df.copy(); d["mes"]=pd.to_datetime(d["data"]).dt.to_period("M").astype(str)
        g=d.groupby("mes")[["valor_brl","valor_usd"]].sum().reset_index().rename(columns={"valor_brl":"BRL","valor_usd":"USD"})
        g["tipo"]=label
        if "lucro" in d.columns:
            l=d.groupby("mes")["lucro"].sum().reset_index().rename(columns={"lucro":"Lucro"})
            g=g.merge(l,on="mes",how="left").fillna(0.0)
        else:
            g["Lucro"]=0.0
        return g[["mes","tipo","BRL","USD","Lucro"]]

    agg=pd.concat([monthly_sum(df_r,"Receitas (FBA)"),
                   monthly_sum(df_g,"Despesas (Gastos)"),
                   monthly_sum(df_i,"Despesas (Invest.)")],ignore_index=True)

    if agg.empty:
        st.info("Cadastre dados para visualizar os gráficos.")
    else:
        st.markdown("#### USD" + (f" (filtro: {g_mes})" if g_mes else ""))
        usd = agg[["mes","tipo","USD"]].rename(columns={"USD":"valor"})
        barsu=alt.Chart(usd).mark_bar().encode(
            x=alt.X("mes:N",sort=alt.SortField("mes",order="ascending"),title="Mês"),
            y=alt.Y("valor:Q",title="Valor (USD)"), color="tipo:N", tooltip=["mes","tipo","valor"]
        )
        resu=usd.pivot_table(index="mes",columns="tipo",values="valor",aggfunc="sum",fill_value=0).reset_index()
        for c in ["Despesas (Gastos)","Despesas (Invest.)","Receitas (FBA)"]:
            if c not in resu: resu[c]=0.0
        resu["Resultado"]=resu["Receitas (FBA)"]-(resu["Despesas (Gastos)"]+resu["Despesas (Invest.)"])
        lineu=alt.Chart(resu).mark_line(point=True).encode(
            x="mes:N", y=alt.Y("Resultado:Q",title="Resultado (USD)"), tooltip=["mes","Resultado"]
        )
        st.altair_chart(barsu+lineu, use_container_width=True)

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

    df_s = df_sql("SELECT id, data, disponivel, pendente, moeda FROM amazon_saldos ORDER BY date(data) DESC, id DESC;")
    if not df_s.empty:
        last = df_s.iloc[0]
        card = (f"Disponível: {money_usd(last['disponivel'])} · Pendente: {money_usd(last['pendente'])}") if last["moeda"]=="USD" \
               else (f"Disponível: {money_brl(last['disponivel'])} · Pendente: {money_brl(last['pendente'])}")
        st.markdown(f"**Último snapshot ({last['data']} - {last['moeda']}):** {card}")

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
    st.subheader("Cadastro e Controle de Produtos (Estoque)")

    # defaults
    p_defaults = {
        "p_data": date.today(), "p_nome":"", "p_sku":"", "p_upc":"", "p_asin":"",
        "p_estoque":0, "p_buy":0.0, "p_frete":0.0, "p_tax":0.0, "p_prep":2.0,
        "p_soldfor":0.0, "p_amzfees":0.0, "p_link_amz":"", "p_link_forn":""
    }
    apply_reset_if_needed("p_reset", p_defaults)

    with st.form("form_produto"):
        c1,c2 = st.columns([2,1])
        with c1:
            data_p = st.date_input("Data adicionada na Amazon", value=st.session_state.get("p_data", date.today()),
                                   format="DD/MM/YYYY", key="p_data")
            nome = st.text_input("Nome", value=st.session_state.get("p_nome",""), key="p_nome")
            sku = st.text_input("SKU", value=st.session_state.get("p_sku",""), key="p_sku")
            upc = st.text_input("UPC", value=st.session_state.get("p_upc",""), key="p_upc")
            asin = st.text_input("ASIN", value=st.session_state.get("p_asin",""), key="p_asin")
            link_amz = st.text_input("Link do produto na Amazon", value=st.session_state.get("p_link_amz",""), key="p_link_amz")
            link_forn = st.text_input("Link do fornecedor", value=st.session_state.get("p_link_forn",""), key="p_link_forn")
        with c2:
            estoque = st.number_input("Estoque (qtd)", min_value=0, step=1, value=int(st.session_state.get("p_estoque",0)), key="p_estoque")
            buy = st.number_input("Price to Buy (USD, unitário)", min_value=0.0, step=0.01, format="%.2f", key="p_buy")
            frete = st.number_input("Frete total do lote (USD)", min_value=0.0, step=0.01, format="%.2f", key="p_frete")
            tax = st.number_input("TAX do lote (USD)", min_value=0.0, step=0.01, format="%.2f", key="p_tax")
            prep = st.number_input("PREP (USD)", min_value=0.0, step=0.01, format="%.2f", value=2.0, key="p_prep")
            soldfor = st.number_input("Sold for (USD)", min_value=0.0, step=0.01, format="%.2f", key="p_soldfor")
            amzfees = st.number_input("Amazon Fees (USD, unitário)", min_value=0.0, step=0.01, format="%.2f", key="p_amzfees")

        if st.form_submit_button("Salvar produto"):
            if nome.strip():
                add_row("produtos", dict(
                    data_add=data_p.strftime("%Y-%m-%d"), nome=nome.strip(), sku=sku.strip(), upc=upc.strip(), asin=asin.strip(),
                    estoque=int(estoque), price_to_buy=buy, freight=frete, tax=tax, prep=prep, sold_for=soldfor,
                    amazon_fees=amzfees, link_amazon=link_amz.strip(), link_fornecedor=link_forn.strip()
                ))
                st.session_state["p_reset"] = True
                st.success("Produto salvo!")
                st.rerun()
            else:
                st.warning("Informe o nome do produto.")

    # listar produtos
    dfp_all = df_sql("""
        SELECT id, data_add, nome, sku, upc, asin, estoque,
               price_to_buy, freight, tax, prep, sold_for, amazon_fees,
               link_amazon, link_fornecedor
        FROM produtos ORDER BY date(data_add) DESC, id DESC;
    """)
    dfp = apply_month_filter(dfp_all.copy(), g_mes) if g_mes else dfp_all.copy()

    if not dfp.empty:
        calc = dfp.to_dict("records")
        rows=[]
        for r in calc:
            buy_eff = per_unit_buy_effective(r)
            gp = gross_profit_unit(r)
            roi = gross_roi(r)
            mrg = margin_pct(r)
            rows.append({
                "ID": int(r["id"]),
                "Data": r["data_add"],
                "Nome": r["nome"],
                "SKU": r["sku"],
                "UPC": r["upc"],
                "ASIN": r["asin"],
                "Estoque": int(r["estoque"]),
                "Price to Buy": money_usd(r["price_to_buy"]),
                "Amazon Fees": money_usd(r["amazon_fees"]),
                "PREP": money_usd(r["prep"]),
                "Sold for": money_usd(r["sold_for"]),
                "Gross Profit": money_usd(gp),
                "Gross ROI": f"{roi*100:.2f}%",
                "Margem %": f"{mrg*100:.2f}%",
                "Amazon": f'<a href="{escape(str(r["link_amazon"] or ""))}" target="_blank">Link</a>' if r["link_amazon"] else "",
                "Fornecedor": f'<a href="{escape(str(r["link_fornecedor"] or ""))}" target="_blank">Link</a>' if r["link_fornecedor"] else "",
            })
        df_view = pd.DataFrame(rows)
        st.markdown(df_to_clean_html(df_view, "del_pc", "tbl_prod"), unsafe_allow_html=True)
    else:
        st.info("Sem produtos no filtro atual.")

    # ----- CARD de lucro realizado no período (sem tag solta)
    dr = df_sql("""SELECT ar.id, ar.data, ar.produto_id, ar.quantidade
                   FROM amazon_receitas ar
                   ORDER BY date(ar.data) DESC, ar.id DESC;""")
    dr = apply_month_filter(dr, g_mes) if g_mes else dr

    total_lucro = 0.0
    if not dr.empty and not dfp_all.empty:
        prods = dfp_all.set_index("id").to_dict("index")
        for _, row in dr.iterrows():
            prod = prods.get(int(row["produto_id"]))
            if not prod: continue
            gp_u = gross_profit_unit(prod)
            qtd = int(row["quantidade"])
            total_lucro += gp_u * qtd

        explic_html = (
            f"<div style='font-size:20px; font-weight:800;'>Lucro realizado no período selecionado: "
            f"{escape(money_usd(total_lucro))}</div>"
            "<div style='margin-top:6px; font-weight:600;'>Como calculamos?</div>"
            "<div>Lucro = Σ (Gross Profit por unidade × quantidade vendida).</div>"
            "<div>Onde: Gross Profit = Sold for – Amazon Fees – PREP – "
            "(Price to Buy + (TAX + Frete) ÷ quantidade).</div>"
        )
        primary_card_centered(explic_html, margin_top_px=24)

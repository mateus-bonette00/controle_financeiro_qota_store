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

def df_sql(sql: str, params: tuple | None = None) -> pd.DataFrame:
    return pd.read_sql_query(sql, get_conn(), params=params)

def add_row(table: str, row: dict):
    cols = ",".join(row.keys())
    qmarks = ",".join(["?"] * len(row))
    get_conn().execute(f"INSERT INTO {table} ({cols}) VALUES ({qmarks});", tuple(row.values())).connection.commit()

def delete_row(table: str, id_: int):
    get_conn().execute(f"DELETE FROM {table} WHERE id = ?;", (id_,)).connection.commit()

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

def center_primary_badge(html_inner: str, margin_top_px: int = 12):
    st.markdown(
        f"""
        <div style="width:100%; display:flex; justify-content:center; margin:{margin_top_px}px 0;">
          <div style="max-width:1000px; background:{PRIMARY}; color:#fff; padding:18px 24px;
                      border-radius:16px; font-weight:700; text-align:center; box-shadow:0 6px 20px rgba(0,0,0,.15);">
            {html_inner}
          </div>
        </div>
        """,
        unsafe_allow_html=True
    )

def df_to_clean_html(df: pd.DataFrame, del_param: str, anchor: str) -> str:
    if "Data" in df: df["Data"] = pd.to_datetime(df["Data"]).dt.strftime("%d/%m/%Y")
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
    for tbl in ["gastos", "investimentos", "receitas", "produtos_compra", "amazon_receitas", "amazon_saldos", "produtos"]:
        try:
            df = df_sql(f"SELECT DISTINCT strftime('%Y-%m', date(data)) AS m FROM {tbl};")
            meses |= set(df["m"].dropna().tolist())
        except Exception:
            pass
        # produtos tem data_add
        try:
            df = df_sql(f"SELECT DISTINCT strftime('%Y-%m', date(data_add)) AS m FROM produtos;")
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
    # gastos
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
    # investimentos
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
    # receitas FBA (mantida para compatibilidade — não usamos mais UI de repasse)
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

    # produtos (SKU planner)
    ensure_table("""
        CREATE TABLE IF NOT EXISTS produtos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_add TEXT NOT NULL,
            nome TEXT NOT NULL,
            sku TEXT,
            upc TEXT,
            asin TEXT,
            estoque INTEGER NOT NULL DEFAULT 0,
            custo_base REAL NOT NULL DEFAULT 0,   -- preço unitário base
            freight REAL NOT NULL DEFAULT 0,      -- frete total do lote
            tax REAL NOT NULL DEFAULT 0,          -- taxa total do lote
            quantidade INTEGER NOT NULL DEFAULT 0,-- quantidade do lote (para rateio)
            prep REAL NOT NULL DEFAULT 2,         -- prep center unitário
            sold_for REAL NOT NULL DEFAULT 0,     -- preço de venda
            amazon_fees REAL NOT NULL DEFAULT 0,  -- taxas amazon por unidade
            link_amazon TEXT,
            link_fornecedor TEXT
        );
    """)
    # migrações produtos (bases antigas)
    for col, decl in [
        ("data_add", "TEXT"), ("sku","TEXT"), ("upc","TEXT"), ("asin","TEXT"),
        ("custo_base","REAL NOT NULL DEFAULT 0"), ("freight","REAL NOT NULL DEFAULT 0"),
        ("tax","REAL NOT NULL DEFAULT 0"), ("quantidade","INTEGER NOT NULL DEFAULT 0"),
        ("prep","REAL NOT NULL DEFAULT 2"), ("sold_for","REAL NOT NULL DEFAULT 0"),
        ("amazon_fees","REAL NOT NULL DEFAULT 0"), ("link_amazon","TEXT"), ("link_fornecedor","TEXT"),
    ]:
        add_column_if_missing("produtos", col, decl)

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
            produto TEXT,
            sku TEXT,
            FOREIGN KEY(produto_id) REFERENCES produtos(id) ON DELETE SET NULL
        );
    """)
    # migrações + backfill produto_id por sku
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

    # saldos amazon
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
# Price to Buy efetivo e métricas
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

style_tabs_center_big()
st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "🏠 Principal", "📦 Receitas (FBA)", "📊 Fluxo de Caixa", "📈 Gráficos", "🏦 Saldos (Amazon)", "📋 Produtos (SKU Planner)"
])

contas = ["Nubank", "Nomad", "Wise", "Mercury Bank", "WesternUnion"]
pessoas = ["Bonette", "Daniel"]

# ============================
# TAB 1 - PRINCIPAL (igual ao seu, sem mudanças de regra)
# ============================
with tab1:
    col1, col2 = st.columns(2)

    # --- GASTOS
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

    # --- INVESTIMENTOS
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
# TAB 2 - RECEITAS (FBA) — ⚠️ somente Amazon Recebidos
# ============================
with tab2:
    st.subheader("Dinheiro recebido dentro da Amazon (USD)")

    # Produtos para dropdown
    dfp_all = df_sql("""
        SELECT id, data_add, nome, sku, upc, asin, estoque,
               custo_base, freight, tax, quantidade, prep, sold_for, amazon_fees,
               link_amazon, link_fornecedor
        FROM produtos
        ORDER BY date(data_add) DESC, id DESC;
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
            default_idx = 0
            sel = st.selectbox("Produto vendido (SKU | UPC | Nome)", labels, index=default_idx)
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

        submit = st.form_submit_button("Adicionar recebimento (Amazon)")
        if submit:
            if pid is None:
                st.error("Selecione um produto.")
            else:
                add_row("amazon_receitas", dict(
                    data=data_ar.strftime("%Y-%m-%d"), produto_id=pid, quantidade=int(qty_ar),
                    valor_usd=val_ar*int(qty_ar), quem=quem_ar, obs=obs_ar.strip(),
                    sku=sel_sku
                ))
                # baixa estoque
                get_conn().execute("UPDATE produtos SET estoque = MAX(0, estoque - ?) WHERE id = ?;", (int(qty_ar), pid)).connection.commit()
                st.success("Recebimento adicionado e estoque atualizado.")
                st.rerun()

    # Tabela recebidos
    dr_all = df_sql("""SELECT id, data, produto_id, quantidade, valor_usd, quem, obs, sku
                       FROM amazon_receitas ORDER BY date(data) DESC, id DESC;""")
    dr = apply_month_filter(dr_all, g_mes) if g_mes else dr_all

    # badge total vendido
    tot_qty = int(dr["quantidade"].sum()) if not dr.empty else 0
    tot_val = float(dr["valor_usd"].sum()) if not dr.empty else 0.0
    center_primary_badge(
        f"<div style='font-size:18px;'>Vendido no período — "
        f"<b>Quantidade:</b> {tot_qty} · <b>Valor:</b> {escape(money_usd(tot_val))}</div>",
        margin_top_px=10
    )

    if not dr.empty:
        # join com produtos p/ exibir nome/sku
        prods = dfp_all[["id","nome","sku"]].rename(columns={"id":"pid"})
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
# TAB 3 - FLUXO DE CAIXA (mantido)
# ============================
with tab3:
    st.subheader("Fluxo de Caixa — Resumo Mensal e Total Geral (Receitas em USD)")

    df_g = df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM gastos;")
    df_i = df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM investimentos;")
    # receitas: usamos apenas amazon_receitas (valor_usd)
    df_r = df_sql("SELECT date(data) as data, valor_usd FROM amazon_receitas;").rename(columns={"valor_usd":"valor_usd"})
    df_r["valor_brl"] = 0.0
    df_r["lucro"] = 0.0  # não somamos lucro aqui, só o caixa

    df_g_f = apply_month_filter(df_g, g_mes) if g_mes else df_g
    df_i_f = apply_month_filter(df_i, g_mes) if g_mes else df_i
    df_r_f = apply_month_filter(df_r, g_mes) if g_mes else df_r

    def monthly(df, kind):
        if df.empty:
            x = pd.DataFrame(columns=["mes","tipo","brl","usd"])
            return x
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

        # Totais gerais (todos os meses)
        p_usd_all = pd.concat([monthly(df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM gastos;"),"Despesas (Gastos)"),
                               monthly(df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM investimentos;"),"Despesas (Invest.)"),
                               monthly(df_sql("SELECT date(data) as data, valor_usd, 0 as valor_brl FROM amazon_receitas;"),"Receitas (Amazon)")], ignore_index=True)\
                        .pivot_table(index="mes", columns="tipo", values="usd", aggfunc="sum", fill_value=0).reset_index()
        for c in ["Despesas (Gastos)","Despesas (Invest.)","Receitas (Amazon)"]:
            if c not in p_usd_all: p_usd_all[c]=0.0
        p_usd_all["Resultado"] = p_usd_all["Receitas (Amazon)"] - (p_usd_all["Despesas (Gastos)"] + p_usd_all["Despesas (Invest.)"])
        tot_receita_usd=float(p_usd_all["Receitas (Amazon)"].sum())
        tot_desp_usd=float(p_usd_all["Despesas (Gastos)"].sum()+p_usd_all["Despesas (Invest.)"].sum())
        tot_result_usd=float(p_usd_all["Resultado"].sum())
        st.markdown("### Totais Gerais (USD) — soma de todos os meses")
        summary_card_usd("Totais gerais (USD)", tot_receita_usd, tot_desp_usd, tot_result_usd)

# ============================
# TAB 4 - GRÁFICOS (mantido)
# ============================
with tab4:
    st.subheader("Gráficos Mensais (USD como principal)")
    df_g = df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM gastos;")
    df_i = df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM investimentos;")
    df_r = df_sql("SELECT date(data) as data, valor_usd FROM amazon_receitas;").rename(columns={"valor_usd":"valor_usd"})
    df_r["valor_brl"]=0.0
    df_r["lucro"]=0.0

    if g_mes:
        df_g = apply_month_filter(df_g, g_mes)
        df_i = apply_month_filter(df_i, g_mes)
        df_r = apply_month_filter(df_r, g_mes)

    def monthly_sum(df, label):
        if df.empty:
            return pd.DataFrame(columns=["mes","tipo","BRL","USD","Lucro"])
        d=df.copy(); d["mes"]=pd.to_datetime(d["data"]).dt.to_period("M").astype(str)
        g=d.groupby("mes")[["valor_brl","valor_usd"]].sum().reset_index().rename(columns={"valor_brl":"BRL","valor_usd":"USD"})
        g["tipo"]=label; g["Lucro"]=0.0
        return g[["mes","tipo","BRL","USD","Lucro"]]

    agg=pd.concat([monthly_sum(df_r,"Receitas (Amazon)"),
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
        for c in ["Despesas (Gastos)","Despesas (Invest.)","Receitas (Amazon)"]:
            if c not in resu: resu[c]=0.0
        resu["Resultado"]=resu["Receitas (Amazon)"]-(resu["Despesas (Gastos)"]+resu["Despesas (Invest.)"])
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
    st.subheader("Cadastro e métricas por Produto (FBA)")

    # Form de produto
    with st.form("form_produto"):
        c1, c2 = st.columns([2,1])
        with c1:
            data_add = st.date_input("Data adicionada na Amazon", value=date.today(), format="DD/MM/YYYY")
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
                add_row("produtos", dict(
                    data_add=data_add.strftime("%Y-%m-%d"), nome=nome.strip(), sku=sku.strip(), upc=upc.strip(),
                    asin=asin.strip(), estoque=int(estoque), custo_base=custo_base, freight=freight, tax=tax,
                    quantidade=int(quantidade), prep=prep, sold_for=sold_for, amazon_fees=amazon_fees,
                    link_amazon=link_amz.strip(), link_fornecedor=link_for.strip()
                ))
                st.success("Produto salvo!")
                st.rerun()

    # Lista de produtos + métricas e lucro realizado (somatório)
    dfp_all = df_sql("""
        SELECT id, data_add, nome, sku, upc, asin, estoque,
               custo_base, freight, tax, quantidade, prep, sold_for, amazon_fees,
               link_amazon, link_fornecedor
        FROM produtos
        ORDER BY date(data_add) DESC, id DESC;
    """)
    dfp = apply_month_filter(dfp_all, g_mes, col="data_add") if g_mes else dfp_all

    if not dfp.empty:
        # métricas
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
            "Gross ROI": (dfv["roi"]*100).map(lambda x:f"{x:.2f}%"),
            "Margem %": (dfv["margin"]*100).map(lambda x:f"{x:.2f}%"),
            "Amazon": dfv["link_amazon"].fillna(""),
            "Fornecedor": dfv["link_fornecedor"].fillna(""),
        })
        st.markdown(df_to_clean_html(view, "del_prod", "tbl_prod"), unsafe_allow_html=True)

        # calcula lucro realizado no período a partir das vendas (amazon_receitas)
        if table_has_column("amazon_receitas","produto_id"):
            dr = df_sql("""SELECT id, data, produto_id, quantidade, sku
                           FROM amazon_receitas
                           ORDER BY date(data) DESC, id DESC;""")
        else:
            dr = df_sql("""SELECT id, data, quantidade, sku
                           FROM amazon_receitas
                           ORDER BY date(data) DESC, id DESC;""")
            dr["produto_id"] = None
        dr = apply_month_filter(dr, g_mes) if g_mes else dr

        total_lucro = 0.0
        if not dr.empty:
            prods_by_id = dfv.set_index("id").to_dict("index")
            prods_by_sku = dfv.set_index("sku").to_dict("index")
            for _, row in dr.iterrows():
                prod = None
                pid = row.get("produto_id")
                if pd.notna(pid):
                    prod = prods_by_id.get(int(pid))
                if not prod and "sku" in row and pd.notna(row["sku"]) and str(row["sku"]).strip():
                    prod = prods_by_sku.get(str(row["sku"]))
                if not prod:  # não achou produto
                    continue
                gp_u = gross_profit_unit(prod)
                total_lucro += gp_u * int(row["quantidade"])

        explic = (
            "<div style='font-size:20px; font-weight:800;'>"
            f"Lucro realizado no período selecionado: {escape(money_usd(total_lucro))}"
            "</div>"
            "<div style='margin-top:6px; font-weight:600;'>Como calculamos?</div>"
            "<div>Lucro = Σ (Gross Profit por unidade × quantidade vendida).</div>"
            "<div>Onde: Gross Profit = Sold for – Amazon Fees – PREP – "
            "(Price to Buy + (TAX + Frete) ÷ quantidade).</div>"
        )
        center_primary_badge(explic, margin_top_px=18)
    else:
        st.info("Cadastre produtos para ver as métricas.")

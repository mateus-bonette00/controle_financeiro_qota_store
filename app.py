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
APP_PASSWORD = os.getenv("APP_PASSWORD")  # defina no ambiente do Render para proteger o app

def require_login():
    if not APP_PASSWORD:
        return
    ok = st.session_state.get("authed", False)
    if not ok:
        st.title("Acesso restrito")
        pwd = st.text_input("Senha", type="password")
        if st.button("Entrar"):
            st.session_state.authed = (pwd == APP_PASSWORD)
        st.stop()

require_login()

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

    ensure_table("""
        CREATE TABLE IF NOT EXISTS produtos_compra (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            produto TEXT NOT NULL,
            sku TEXT,
            quantidade INTEGER NOT NULL DEFAULT 1,
            custo_unit REAL NOT NULL DEFAULT 0,
            taxa_unit REAL NOT NULL DEFAULT 0,
            prep_unit REAL NOT NULL DEFAULT 0,
            frete_total REAL NOT NULL DEFAULT 0,
            total_brl REAL NOT NULL DEFAULT 0,
            conta TEXT,
            quem TEXT,
            obs TEXT
        );
    """)
    add_column_if_missing("produtos_compra", "total_usd", "REAL NOT NULL DEFAULT 0")

    ensure_table("""
        CREATE TABLE IF NOT EXISTS amazon_receitas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            produto TEXT,
            sku TEXT,
            quantidade INTEGER NOT NULL DEFAULT 0,
            valor_brl REAL NOT NULL DEFAULT 0,
            quem TEXT,
            obs TEXT
        );
    """)
    add_column_if_missing("amazon_receitas", "valor_usd", "REAL NOT NULL DEFAULT 0")

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
        CREATE TABLE IF NOT EXISTS alocacoes_regra (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT UNIQUE,
            pct REAL NOT NULL
        );
    """)
    ensure_table("""
        CREATE TABLE IF NOT EXISTS alocacoes_execucao (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mes TEXT NOT NULL,
            nome TEXT NOT NULL,
            valor_brl REAL NOT NULL,
            valor_usd REAL NOT NULL DEFAULT 0
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
    for tbl in ["gastos", "investimentos", "receitas", "produtos_compra", "amazon_receitas", "amazon_saldos"]:
        try:
            df = df_sql(f"SELECT DISTINCT strftime('%Y-%m', date(data)) AS m FROM {tbl};")
            meses |= set(df["m"].dropna().tolist())
        except Exception:
            pass
    meses = sorted([m for m in meses if m])
    return meses

def apply_month_filter(df: pd.DataFrame, month: str) -> pd.DataFrame:
    if not month or df.empty or "data" not in df.columns:
        return df
    return df[df["data"].astype(str).str.startswith(month)].copy()

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
    help="Em branco = todos os meses. Este filtro afeta as abas Principal, Receitas (FBA), Fluxo de Caixa e Gráficos.",
    key="g_mes",
)
st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

style_tabs_center_big()
st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "🏠 Principal", "📦 Receitas (FBA)", "📊 Fluxo de Caixa", "📈 Gráficos", "🏦 Saldos (Amazon)", "🧮 Alocações"
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
            data_gasto = st.date_input("Data do gasto", value=date.today(), format="DD/MM/YYYY", key="g_data")
            categoria = st.selectbox("Categoria",
                                     ["Compra de Produto","Mensalidade/Assinatura","Contabilidade/Legal",
                                      "Taxas/Impostos","Frete/Logística","Outros"], key="g_categoria")
            desc = st.text_input("Descrição do gasto", key="g_desc")
            val_brl = st.number_input("Valor em BRL", min_value=0.0, step=0.01, format="%.2f", key="g_val_brl")
            val_usd = st.number_input("Valor em USD", min_value=0.0, step=0.01, format="%.2f", key="g_val_usd")
            metodo = st.selectbox("Método de pagamento", ["Pix","Cartão de Crédito","Boleto","Transferência","Dinheiro"], key="g_metodo")
            conta = st.selectbox("Conta/Banco", contas, key="g_conta")
            quem = st.selectbox("Quem pagou", pessoas, key="g_quem")
            if st.form_submit_button("Adicionar gasto"):
                add_row("gastos", dict(
                    data=data_gasto.strftime("%Y-%m-%d"), categoria=categoria, descricao=desc,
                    valor_brl=val_brl, valor_usd=val_usd, metodo=metodo, conta=conta, quem=quem
                ))
                for k, v in {"g_data": date.today(), "g_categoria": "Compra de Produto", "g_desc": "",
                             "g_val_brl": 0.0, "g_val_usd": 0.0, "g_metodo": "Pix",
                             "g_conta": contas[0], "g_quem": pessoas[0]}.items():
                    st.session_state[k] = v
                st.rerun()

    with col2:
        st.subheader("Investimentos")
        with st.form("form_invest"):
            data_inv = st.date_input("Data do investimento", value=date.today(), format="DD/MM/YYYY", key="i_data")
            inv_brl = st.number_input("Valor em BRL", min_value=0.0, step=0.01, format="%.2f", key="i_brl")
            inv_usd = st.number_input("Valor em USD", min_value=0.0, step=0.01, format="%.2f", key="i_usd")
            metodo_i = st.selectbox("Método de pagamento", ["Pix","Cartão de Crédito","Boleto","Transferência","Dinheiro"], key="i_metodo")
            conta_i = st.selectbox("Conta/Banco", contas, key="i_conta")
            quem_i = st.selectbox("Quem investiu/pagou", pessoas, key="i_quem")
            if st.form_submit_button("Adicionar investimento"):
                add_row("investimentos", dict(
                    data=data_inv.strftime("%Y-%m-%d"), valor_brl=inv_brl, valor_usd=inv_usd,
                    metodo=metodo_i, conta=conta_i, quem=quem_i
                ))
                for k, v in {"i_data": date.today(), "i_brl": 0.0, "i_usd": 0.0, "i_metodo": "Pix",
                             "i_conta": contas[0], "i_quem": pessoas[0]}.items():
                    st.session_state[k] = v
                st.rerun()

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
# TAB 2 - RECEITAS (FBA)
# ============================
with tab2:
    st.subheader("Operação FBA — Compras (USD) e Recebidos na Amazon (USD)")

    leftC, rightC = st.columns(2)
    with leftC:
        st.markdown("### Compras de Produto (custos em USD, frete não multiplica)")
        with st.form("form_produto_compra"):
            data_pc = st.date_input("Data da compra", value=date.today(), format="DD/MM/YYYY")
            prod = st.text_input("Nome do produto *", placeholder="Ex.: Garrafa Térmica 500ml")
            sku = st.text_input("SKU (opcional)", placeholder="Ex.: BTL-500-INOX")
            qty = st.number_input("Quantidade", min_value=1, step=1, value=1)
            custo_unit = st.number_input("Custo unitário (USD)", min_value=0.0, step=0.01, format="%.2f")
            taxa_unit = st.number_input("Taxa unitária (se tiver) (USD)", min_value=0.0, step=0.01, format="%.2f")
            prep_unit = st.number_input("Prep Center unitário (USD)", min_value=0.0, step=0.01, format="%.2f")
            frete_total = st.number_input("Frete total da compra (USD) — não multiplica", min_value=0.0, step=0.01, format="%.2f")
            conta_pc = st.selectbox("Conta/Banco", contas, key="conta_pc")
            quem_pc = st.selectbox("Quem comprou/lançou", pessoas, key="quem_pc")
            obs_pc = st.text_input("Observação (opcional)", placeholder="Lote Setembro, fornecedor X")
            subtotal = qty * (custo_unit + taxa_unit + prep_unit)
            total = subtotal + frete_total
            st.markdown(f"**Subtotal (qty × (custo + taxa + prep))**: {money_usd(subtotal)}")
            st.markdown(f"**Total da compra (subtotal + frete)**: {money_usd(total)}")
            if st.form_submit_button("Adicionar compra de produto"):
                if prod.strip():
                    add_row("produtos_compra", dict(
                        data=data_pc.strftime("%Y-%m-%d"), produto=prod.strip(), sku=sku.strip(),
                        quantidade=int(qty), custo_unit=custo_unit, taxa_unit=taxa_unit,
                        prep_unit=prep_unit, frete_total=frete_total, total_usd=total,
                        conta=conta_pc, quem=quem_pc, obs=obs_pc.strip()
                    ))
                    st.rerun()
                else:
                    st.warning("Informe o nome do produto.")

        df_pc_all = df_sql("""SELECT id, data, produto, sku, quantidade, custo_unit, taxa_unit, prep_unit, frete_total,
                                      COALESCE(total_usd, 0) as total_usd, conta, quem
                               FROM produtos_compra ORDER BY date(data) DESC, id DESC;""")
        df_pc = apply_month_filter(df_pc_all, g_mes)
        total_qtd = int(df_pc["quantidade"].sum()) if not df_pc.empty else 0
        total_usd = float(df_pc["total_usd"].sum()) if not df_pc.empty else 0.0
        st.markdown(f"**Totais de compras (USD)** — Quantidade: **{total_qtd}** · Valor: **{money_usd(total_usd)}**")
        if not df_pc.empty:
            df_pc_view = pd.DataFrame({
                "ID": df_pc["id"].astype(int),
                "Data": df_pc["data"],
                "Produto": df_pc["produto"], "SKU": df_pc["sku"],
                "Qtd": df_pc["quantidade"].astype(int),
                "Subtotal (USD)": (df_pc["quantidade"] * (df_pc["custo_unit"] + df_pc["taxa_unit"] + df_pc["prep_unit"])).map(money_usd),
                "Frete (USD)": df_pc["frete_total"].map(money_usd),
                "Total (USD)": df_pc["total_usd"].map(money_usd),
                "Conta": df_pc["conta"].fillna(""), "Quem": df_pc["quem"].fillna(""),
            })
            st.markdown(df_to_clean_html(df_pc_view, "del_pc", "tbl_pc"), unsafe_allow_html=True)
        else:
            st.info("Sem compras no filtro atual.")
        footer_total_badge(
            "Compras — Total de TODOS os meses",
            0.0,
            float(df_pc_all["total_usd"].sum()) if not df_pc_all.empty else 0.0,
            margin_top=16
        )

    with rightC:
        st.markdown("### Dinheiro recebido dentro da Amazon (USD)")
        with st.form("form_amz_receitas"):
            data_ar = st.date_input("Data do crédito", value=date.today(), format="DD/MM/YYYY", key="data_ar")
            prod_ar = st.text_input("Produto (opcional)", key="prod_ar")
            sku_ar = st.text_input("SKU (opcional)", key="sku_ar")
            qty_ar = st.number_input("Quantidade vendida (opcional)", min_value=0, step=1, value=0, key="qty_ar")
            val_ar = st.number_input("Valor recebido (USD) dentro da Amazon", min_value=0.0, step=0.01, format="%.2f", key="val_ar")
            quem_ar = st.selectbox("Quem lançou", pessoas, key="quem_ar")
            obs_ar = st.text_input("Observação (opcional)", key="obs_ar")
            if st.form_submit_button("Adicionar recebimento (Amazon)"):
                add_row("amazon_receitas", dict(
                    data=data_ar.strftime("%Y-%m-%d"), produto=prod_ar.strip(), sku=sku_ar.strip(),
                    quantidade=int(qty_ar), valor_usd=val_ar, quem=quem_ar, obs=obs_ar.strip()
                ))
                st.rerun()

        df_ar_all = df_sql("""SELECT id, data, produto, sku, quantidade, COALESCE(valor_usd, 0) as valor_usd, quem
                              FROM amazon_receitas ORDER BY date(data) DESC, id DESC;""")
        df_ar = apply_month_filter(df_ar_all, g_mes)
        tot_ar_usd = float(df_ar["valor_usd"].sum()) if not df_ar.empty else 0.0
        tot_ar_qty = int(df_ar["quantidade"].sum()) if not df_ar.empty else 0
        st.markdown(f"**Totais de recebidos (Amazon)** — Quantidade vendida: **{tot_ar_qty}** · Valor: **{money_usd(tot_ar_usd)}**")
        if not df_ar.empty:
            df_ar_view = pd.DataFrame({
                "ID": df_ar["id"].astype(int), "Data": df_ar["data"], "Produto": df_ar["produto"],
                "SKU": df_ar["sku"], "Qtd": df_ar["quantidade"].astype(int),
                "Valor (USD)": df_ar["valor_usd"].map(money_usd), "Quem": df_ar["quem"].fillna(""),
            })
            st.markdown(df_to_clean_html(df_ar_view, "del_ar", "tbl_ar"), unsafe_allow_html=True)
        else:
            st.info("Sem recebidos no filtro atual.")
        footer_total_badge(
            "Recebidos (Amazon) — Total de TODOS os meses",
            0.0,
            float(df_ar_all["valor_usd"].sum()) if not df_ar_all.empty else 0.0,
            margin_top=16
        )

    with st.expander("➕ Depósitos FBA (repasse para banco) — com cálculo de Lucro (USD)"):
        with st.form("form_receita_repasse"):
            c0, c1 = st.columns([1,2])
            with c0:
                data_r = st.date_input("Data do recebimento (repasse)", value=date.today(), format="DD/MM/YYYY")
            with c1:
                desc_r = st.text_input("Descrição", placeholder="Ex.: Depósito Amazon FBA, cycle 2025-09")
            col_a, col_b = st.columns(2)
            with col_a:
                bruto_usd = st.number_input("Bruto recebido (USD)", 0.0, step=0.01, format="%.2f")
                cogs_usd  = st.number_input("COGS (USD)", 0.0, step=0.01, format="%.2f")
                taxas_usd = st.number_input("Taxas Amazon (USD)", 0.0, step=0.01, format="%.2f")
            with col_b:
                ads_usd   = st.number_input("Anúncios/PPC (USD)", 0.0, step=0.01, format="%.2f")
                frete_usd = st.number_input("Frete/Logística (USD)", 0.0, step=0.01, format="%.2f")
                desc_usd  = st.number_input("Devoluções/Descontos (USD)", 0.0, step=0.01, format="%.2f")
            lucro_usd = bruto_usd - (cogs_usd + taxas_usd + ads_usd + frete_usd + desc_usd)
            st.markdown(f"**Lucro calculado (USD): {money_usd(lucro_usd)}**")
            metodo_r = st.selectbox("Método de recebimento", ["Pix","Transferência","Boleto","Cartão de Crédito","Dinheiro"])
            conta_r  = st.selectbox("Conta/Banco", contas, key="conta_rec")
            quem_r   = st.selectbox("Responsável (quem lançou)", pessoas, key="quem_rec")
            if st.form_submit_button("Adicionar depósito FBA (repasse)"):
                add_row("receitas", dict(
                    data=data_r.strftime("%Y-%m-%d"), origem="FBA", descricao=desc_r,
                    bruto=bruto_usd, cogs=cogs_usd, taxas_amz=taxas_usd, ads=ads_usd,
                    frete=frete_usd, descontos=desc_usd, lucro=lucro_usd,
                    valor_brl=0, valor_usd=bruto_usd, metodo=metodo_r, conta=conta_r, quem=quem_r
                ))
                st.rerun()

        df_r_all = df_sql("""SELECT id, data, descricao, bruto, cogs, taxas_amz, ads, frete, descontos, lucro,
                                    valor_usd, metodo, conta, quem
                             FROM receitas ORDER BY date(data) DESC, id DESC;""")
        df_r = apply_month_filter(df_r_all, g_mes)
        tot_bruto = float(df_r["bruto"].sum()) if not df_r.empty else 0.0
        tot_lucro = float(df_r["lucro"].sum()) if not df_r.empty else 0.0
        st.markdown(f"**Totais de Depósitos FBA (USD)** — Bruto: {money_usd(tot_bruto)} · Lucro: {money_usd(tot_lucro)}")
        if not df_r.empty:
            df_view_r = pd.DataFrame({
                "ID": df_r["id"].astype(int), "Data": df_r["data"], "Descrição": df_r["descricao"].fillna(""),
                "Bruto (USD)": df_r["bruto"].map(money_usd), "COGS (USD)": df_r["cogs"].map(money_usd),
                "Taxas AMZ (USD)": df_r["taxas_amz"].map(money_usd), "Ads (USD)": df_r["ads"].map(money_usd),
                "Frete (USD)": df_r["frete"].map(money_usd), "Descontos (USD)": df_r["descontos"].map(money_usd),
                "Lucro (USD)": df_r["lucro"].map(money_usd), "Método": df_r["metodo"].fillna(""),
                "Conta": df_r["conta"].fillna(""), "Quem": df_r["quem"].fillna(""),
            })
            st.markdown(df_to_clean_html(df_view_r, "del_rec", "tbl_rec"), unsafe_allow_html=True)
        else:
            st.info("Sem depósitos/repasse no filtro atual.")
        footer_total_badge(
            "Depósitos FBA — Total de TODOS os meses",
            0.0,
            float(df_r_all["bruto"].sum()) if not df_r_all.empty else 0.0,
            margin_top=16
        )

# ============================
# TAB 3 - FLUXO DE CAIXA
# ============================
with tab3:
    st.subheader("Fluxo de Caixa — Resumo Mensal e Total Geral (Receitas em USD)")

    df_g = df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM gastos;")
    df_i = df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM investimentos;")
    df_r = df_sql("SELECT date(data) as data, 0 as valor_brl, bruto as valor_usd, lucro FROM receitas;")

    # aplica filtro de mês global na base diária antes de agrupar
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

        # Totais gerais (sempre de TODOS os meses)
        p_usd_all = pd.concat([
            monthly(df_g,"Despesas (Gastos)")[["mes","tipo","usd"]],
            monthly(df_i,"Despesas (Invest.)")[["mes","tipo","usd"]],
            monthly(df_r,"Receitas (FBA)")[["mes","tipo","usd"]],
        ], ignore_index=True).pivot_table(index="mes", columns="tipo", values="usd", aggfunc="sum", fill_value=0).reset_index()
        for c in ["Despesas (Gastos)","Despesas (Invest.)","Receitas (FBA)"]:
            if c not in p_usd_all: p_usd_all[c]=0.0
        tot_receita_usd=float(p_usd_all["Receitas (FBA)"].sum())
        tot_desp_usd=float(p_usd_all["Despesas (Gastos)"].sum()+p_usd_all["Despesas (Invest.)"].sum())
        tot_result_usd=float(p_usd_all["Resultado"].sum()) if "Resultado" in p_usd_all else float((p_usd_all["Receitas (FBA)"]-(p_usd_all["Despesas (Gastos)"]+p_usd_all["Despesas (Invest.)"])).sum())
        st.markdown("### Totais Gerais (USD) — soma de todos os meses")
        summary_card_usd("Totais gerais (USD)", tot_receita_usd, tot_desp_usd, tot_result_usd)

        bruto_total=float(m_r["usd"].sum()) if not m_r.empty else 0.0
        lucro_total=float(df_r_f["lucro"].sum()) if not df_r_f.empty else 0.0
        margem_total=(lucro_total/bruto_total*100.0) if bruto_total>0 else 0.0
        st.markdown(f"**Margem total FBA (USD) no período filtrado:** {margem_total:.1f}% · **Lucro total:** {money_usd(lucro_total)} · **Bruto total:** {money_usd(bruto_total)}")

# ============================
# TAB 4 - GRÁFICOS
# ============================
with tab4:
    st.subheader("Gráficos Mensais (USD como principal)")

    df_g = df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM gastos;")
    df_i = df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM investimentos;")
    df_r = df_sql("SELECT date(data) as data, 0 as valor_brl, bruto as valor_usd, lucro FROM receitas;")

    # filtro global na base diária antes da agregação
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

        st.markdown("#### Margem (%) — FBA (USD)")
        mr = agg[agg["tipo"]=="Receitas (FBA)"][["mes","USD","Lucro"]].copy()
        if not mr.empty:
            mr["Margem"] = (mr["Lucro"] / mr["USD"]).replace([pd.NA, float("inf")], 0.0) * 100
            chart_m = alt.Chart(mr).mark_line(point=True).encode(
                x="mes:N", y=alt.Y("Margem:Q", title="Margem (%)"), tooltip=["mes","Margem"]
            )
            st.altair_chart(chart_m, use_container_width=True)

        st.markdown("#### BRL (opcional)")
        brl=agg[["mes","tipo","BRL"]].rename(columns={"BRL":"valor"})
        bars=alt.Chart(brl).mark_bar().encode(
            x=alt.X("mes:N",sort=alt.SortField("mes",order="ascending"),title="Mês"),
            y=alt.Y("valor:Q",title="Valor (BRL)"), color="tipo:N", tooltip=["mes","tipo","valor"]
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

    df_s = df_sql("SELECT id, data, disponivel, pendente, moeda FROM amazon_saldos ORDER BY date(data) DESC, id DESC;")
    if not df_s.empty:
        last = df_s.iloc[0]
        card = (f"Disponível: {money_usd(last['disponivel'])} · Pendente: {money_usd(last['pendente'])}") if last["moeda"]=="USD" \
               else (f"Disponível: {money_brl(last['disponivel'])} · Pendente: {money_brl(last['pendente'])}")
        st.markdown(f"**Último snapshot ({last['data']} - {last['moeda']}):** {card}")
        df_view_s = pd.DataFrame({
            "ID": df_s["id"].astype(int), "Data": df_s["data"],
            "Disponível": df_s.apply(lambda r: money_usd(r["disponivel"]) if r["moeda"]=="USD" else money_brl(r["disponivel"]), axis=1),
            "Pendente":  df_s.apply(lambda r: money_usd(r["pendente"])  if r["moeda"]=="USD" else money_brl(r["pendente"]),  axis=1),
            "Moeda": df_s["moeda"],
        })
        st.markdown(df_to_clean_html(df_view_s, "del_saldo", "tbl_saldo"), unsafe_allow_html=True)
    else:
        st.info("Sem snapshots cadastrados.")

# ============================
# TAB 6 - ALOCAÇÕES
# ============================
with tab6:
    st.subheader("Regras de Alocação (Profit First)")
    with st.form("form_regras"):
        colr1, colr2 = st.columns([2,1])
        with colr1: nome = st.text_input("Nome do balde", value="Profit")
        with colr2: pct = st.number_input("Percentual (0–100%)", min_value=0.0, max_value=100.0, value=10.0, step=1.0)
        if st.form_submit_button("Salvar/Atualizar regra"):
            try: add_row("alocacoes_regra", dict(nome=nome, pct=pct/100.0))
            except Exception: get_conn().execute("UPDATE alocacoes_regra SET pct=? WHERE nome=?;", (pct/100.0, nome)).connection.commit()
            st.rerun()

    df_regra = df_sql("SELECT nome, pct FROM alocacoes_regra ORDER BY nome;")
    if not df_regra.empty:
        df_view_rg = df_regra.copy(); df_view_rg["Percentual"]=(df_view_rg["pct"]*100).map(lambda x:f"{x:.0f}%")
        st.dataframe(df_view_rg[["nome","Percentual"]].rename(columns={"nome":"Balde"}), use_container_width=True, hide_index=True)
    else:
        st.info("Nenhuma regra cadastrada ainda. Adicione acima.")

    st.markdown("---")
    st.subheader("Distribuição sugerida do lucro por mês (USD)")
    meses = df_sql("SELECT DISTINCT strftime('%Y-%m', date(data)) as mes FROM receitas ORDER BY mes;")["mes"].tolist()
    if meses:
        mes_ref = st.selectbox("Escolha o mês", meses, index=len(meses)-1)
        tot_lucro_mes = df_sql(f"SELECT COALESCE(SUM(lucro),0) AS x FROM receitas WHERE strftime('%Y-%m', date(data))='{mes_ref}';").iloc[0]["x"]
        st.markdown(f"**Lucro do mês {mes_ref}: {money_usd(tot_lucro_mes)}**")
        df_regra2 = df_sql("SELECT nome, pct FROM alocacoes_regra ORDER BY nome;")
        if not df_regra2.empty:
            dist = {row["nome"]: round(float(tot_lucro_mes) * float(row["pct"]), 2) for _, row in df_regra2.iterrows()}
            for nome_b, val in dist.items():
                st.markdown(f"- **{nome_b}**: {money_usd(val)}")
            if st.button(f"Registrar alocação de {mes_ref}"):
                for nome_b, val in dist.items():
                    add_row("alocacoes_execucao", dict(mes=mes_ref, nome=nome_b, valor_brl=0, valor_usd=val))
                st.success("Alocação registrada!")
                st.rerun()
    else:
        st.info("Cadastre receitas FBA para escolher um mês.")

# app.py
from datetime import date
from decimal import Decimal
from html import escape
from pathlib import Path
from urllib.parse import urlencode
import base64
import sqlite3
import pandas as pd
import streamlit as st
import altair as alt
import os

# ----------------------------------
# Config
# ----------------------------------
st.set_page_config(page_title="Controle Financeiro Qota Store", layout="wide")
DB_PATH = os.getenv("DB_PATH", "finance.db")
PRIMARY = "#0053b0"

APP_PASSWORD = os.getenv("APP_PASSWORD")  # senha opcional via env

# ----------------------------------
# Auth (preserva ?auth=1 na URL)
# ----------------------------------
def require_login():
    if not APP_PASSWORD:
        return  # sem senha definida -> segue

    # lê query params
    try:
        q = dict(st.query_params)
    except Exception:
        q = st.experimental_get_query_params()
        q = {k: (v[0] if isinstance(v, list) else v) for k, v in q.items()}

    # se já tem auth=1 considera autenticado
    if q.get("auth") == "1":
        st.session_state.authed = True

    if st.session_state.get("authed"):
        return

    # tela de login
    st.title("Acesso restrito")
    pwd = st.text_input("Senha", type="password")
    if st.button("Entrar"):
        if pwd == APP_PASSWORD:
            st.session_state.authed = True
            try:
                st.query_params.update({"auth": "1"})
            except Exception:
                st.experimental_set_query_params(auth="1")
            st.rerun()
        else:
            st.error("Senha incorreta.")
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
    # segurança caso exista uma coluna literal chamada "REAL"
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
    # Gastos
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
    # Investimentos
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

    # Receitas FBA
    ensure_table("""
        CREATE TABLE IF NOT EXISTS receitas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            origem TEXT NOT NULL DEFAULT 'FBA',
            descricao TEXT,
            valor_brl REAL NOT NULL DEFAULT 0,  -- legado
            valor_usd REAL NOT NULL DEFAULT 0,  -- bruto (USD)
            metodo TEXT,
            conta TEXT,
            quem TEXT
        );
    """)
    add_column_if_missing("receitas", "bruto",     "REAL NOT NULL DEFAULT 0")
    add_column_if_missing("receitas", "cogs",      "REAL NOT NULL DEFAULT 0")
    add_column_if_missing("receitas", "taxas_amz", "REAL NOT NULL DEFAULT 0")
    add_column_if_missing("receitas", "ads",       "REAL NOT NULL DEFAULT 0")
    add_column_if_missing("receitas", "frete",     "REAL NOT NULL DEFAULT 0")
    add_column_if_missing("receitas", "descontos", "REAL NOT NULL DEFAULT 0")
    add_column_if_missing("receitas", "lucro",     "REAL NOT NULL DEFAULT 0")

    # Compras de produto (USD)
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

    # Recebidos Amazon (USD)
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

    # Saldos Amazon
    ensure_table("""
        CREATE TABLE IF NOT EXISTS amazon_saldos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            disponivel REAL NOT NULL DEFAULT 0,
            pendente REAL NOT NULL DEFAULT 0,
            moeda TEXT NOT NULL DEFAULT 'USD'
        );
    """)

    # Profit First
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
    try:
        v = Decimal(str(x))
    except Exception:
        return "R$ 0,00"
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def money_usd(x):
    try:
        v = Decimal(str(x))
    except Exception:
        return "$ 0.00"
    return f"$ {v:,.2f}"


def handle_query_deletions():
    # lê query params
    try:
        q = dict(st.query_params)
    except Exception:
        q = st.experimental_get_query_params()
        q = {k: (v[0] if isinstance(v, list) else v) for k, v in q.items()}

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
            try:
                delete_row(table, int(q.get(param)))
            except Exception:
                pass
            changed = True

    if changed:
        # preserva auth=1 após limpar params
        base = {"auth": "1"} if st.session_state.get("authed") else {}
        try:
            st.query_params.clear()
            if base:
                st.query_params.update(base)
        except Exception:
            st.experimental_set_query_params(**base)
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
        """,
        unsafe_allow_html=True,
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


def df_to_clean_html(df: pd.DataFrame, del_param: str, anchor: str) -> str:
    if "Data" in df:
        df["Data"] = pd.to_datetime(df["Data"]).dt.strftime("%d/%m/%Y")
    for col in df.columns:
        if col in {"ID", "Valor (BRL)", "Valor (USD)", "Lucro (USD)", "Subtotal (USD)", "Total (USD)", "Margem %"}:
            continue
        df[col] = df[col].astype(str).map(escape)

    # preserva query params existentes (ex.: auth=1)
    try:
        current_qs = dict(st.query_params)
    except Exception:
        current_qs = st.experimental_get_query_params()
        current_qs = {k: (v[0] if isinstance(v, list) else v) for k, v in current_qs.items()}

    def make_del_link(i: int) -> str:
        qs = dict(current_qs)
        qs[del_param] = int(i)
        href = f"?{urlencode(qs)}#{anchor}"
        return f'<a class="trash" href="{href}" title="Excluir">🗑️</a>'

    df["Ações"] = df["ID"].map(make_del_link)
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
        """,
        unsafe_allow_html=True,
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
            justify-content: center;
            gap: 48px;
            border-bottom: 0;
            margin-top: 6px;
        }}
        .stTabs [role="tab"] {{
            padding: 18px 30px !important;
            border-radius: 14px 14px 0 0 !important;
            border: 1px solid white !important;
            border-bottom: 3px solid transparent !important;
            background: rgba(255,255,255,0.04) !important;
            color: #FFFFFF !important;
        }}
        .stTabs [role="tab"] span,
        .stTabs [role="tab"] p,
        .stTabs [role="tab"] div {{
            font-size: 20px !important;
            font-weight: 800 !important;
            line-height: 1.15 !important;
            color: #FFFFFF !important;
            margin: 0 !important;
        }}
        .stTabs [role="tab"]:hover {{
            background: rgba(255,255,255,0.08) !important;
        }}
        .stTabs [role="tab"][aria-selected="true"] {{
            background: #0053b0 !important;
            border-color: white !important;
            border-bottom-color: {PRIMARY} !important;
            box-shadow: 0 2px 0 0 {PRIMARY} inset !important;
        }}
        .stTabs [role="tab"][aria-selected="true"] span,
        .stTabs [role="tab"][aria-selected="true"] p,
        .stTabs [role="tab"][aria-selected="true"] div {{
            color: #FFFFFF !important;
            font-weight: 900 !important;
        }}
        .stTabs [role="tab"] a,
        .stTabs [role="tab"] svg {{
            color: #FFFFFF !important;
            fill:  #FFFFFF !important;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )

# ---- Helpers de reset "adiado" de formulários ----
def schedule_reset(prefix: str):
    """Marca para resetar os campos com certo prefixo no próximo rerun."""
    st.session_state[f"{prefix}__reset"] = True

def apply_pending_reset(prefix: str, keys: list[str]):
    """Se houver reset pendente, apaga as keys de widget ANTES de desenhar o form."""
    if st.session_state.get(f"{prefix}__reset"):
        for k in keys:
            st.session_state.pop(f"{prefix}{k}", None)
        st.session_state[f"{prefix}__reset"] = False

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
style_tabs_center_big()
st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "🏠 Principal", "📦 Receitas (FBA)", "📊 Fluxo de Caixa", "📈 Gráficos", "🏦 Saldos (Amazon)", "🧮 Alocações"
])

contas = ["Nubank", "Nomad", "Wise", "Mercury Bank", "WesternUnion"]
pessoas = ["Bonette", "Daniel"]

# ============================
# TAB 1 - PRINCIPAL (gastos + investimentos)
# ============================
with tab1:
    col1, col2 = st.columns(2)

    # ------- Gastos -------
    with col1:
        st.subheader("Gastos")
        categorias = ["Compra de Produto", "Mensalidade/Assinatura", "Contabilidade/Legal",
                      "Taxas/Impostos", "Frete/Logística", "Outros"]

        # reset pendente (antes de desenhar o form)
        apply_pending_reset("g_", ["data","categoria","desc","brl","usd","metodo","conta","quem"])

        with st.form("form_gasto"):
            g_data = st.session_state.get("g_data", date.today())
            g_categoria = st.session_state.get("g_categoria", categorias[0])
            g_desc = st.session_state.get("g_desc", "")
            g_brl = st.session_state.get("g_brl", 0.0)
            g_usd = st.session_state.get("g_usd", 0.0)
            g_metodo = st.session_state.get("g_metodo", "Pix")
            g_conta = st.session_state.get("g_conta", contas[0])
            g_quem = st.session_state.get("g_quem", pessoas[0])

            data_gasto = st.date_input("Data do gasto", value=g_data, format="DD/MM/YYYY", key="g_data")
            categoria = st.selectbox("Categoria", categorias, index=categorias.index(g_categoria), key="g_categoria")
            desc = st.text_input("Descrição do gasto", value=g_desc, key="g_desc")
            val_brl = st.number_input("Valor em BRL", min_value=0.0, step=0.01, format="%.2f", value=g_brl, key="g_brl")
            val_usd = st.number_input("Valor em USD", min_value=0.0, step=0.01, format="%.2f", value=g_usd, key="g_usd")
            metodo = st.selectbox("Método de pagamento", ["Pix","Cartão de Crédito","Boleto","Transferência","Dinheiro"],
                                  index=["Pix","Cartão de Crédito","Boleto","Transferência","Dinheiro"].index(g_metodo), key="g_metodo")
            conta = st.selectbox("Conta/Banco", contas, index=contas.index(g_conta), key="g_conta")
            quem = st.selectbox("Quem pagou", pessoas, index=pessoas.index(g_quem), key="g_quem")

            if st.form_submit_button("Adicionar gasto"):
                add_row("gastos", dict(
                    data=data_gasto.strftime("%Y-%m-%d"), categoria=categoria, descricao=desc,
                    valor_brl=val_brl, valor_usd=val_usd, metodo=metodo, conta=conta, quem=quem
                ))
                schedule_reset("g_")   # marca o reset para o próximo rerun
                st.rerun()

    # ------- Investimentos -------
    with col2:
        st.subheader("Investimentos")

        apply_pending_reset("i_", ["data","brl","usd","metodo","conta","quem"])

        with st.form("form_invest"):
            i_data = st.session_state.get("i_data", date.today())
            i_brl = st.session_state.get("i_brl", 0.0)
            i_usd = st.session_state.get("i_usd", 0.0)
            i_metodo = st.session_state.get("i_metodo", "Pix")
            i_conta = st.session_state.get("i_conta", contas[0])
            i_quem = st.session_state.get("i_quem", pessoas[0])

            data_inv = st.date_input("Data do investimento", value=i_data, format="DD/MM/YYYY", key="i_data")
            inv_brl = st.number_input("Valor em BRL", min_value=0.0, step=0.01, format="%.2f", value=i_brl, key="i_brl")
            inv_usd = st.number_input("Valor em USD", min_value=0.0, step=0.01, format="%.2f", value=i_usd, key="i_usd")
            metodo_i = st.selectbox("Método de pagamento", ["Pix","Cartão de Crédito","Boleto","Transferência","Dinheiro"],
                                    index=["Pix","Cartão de Crédito","Boleto","Transferência","Dinheiro"].index(i_metodo), key="i_metodo")
            conta_i = st.selectbox("Conta/Banco", contas, index=contas.index(i_conta), key="i_conta")
            quem_i = st.selectbox("Quem investiu/pagou", pessoas, index=pessoas.index(i_quem), key="i_quem")

            if st.form_submit_button("Adicionar investimento"):
                add_row("investimentos", dict(
                    data=data_inv.strftime("%Y-%m-%d"), valor_brl=inv_brl, valor_usd=inv_usd,
                    metodo=metodo_i, conta=conta_i, quem=quem_i
                ))
                schedule_reset("i_")
                st.rerun()

    # Listas + totais
    left, right = st.columns(2)
    with left:
        st.markdown("### Gastos cadastrados")
        df_g = df_sql("""SELECT id, data, categoria, descricao, valor_brl, valor_usd, metodo, conta, quem
                         FROM gastos ORDER BY date(data) DESC, id DESC;""")
        tot_g_brl = float(df_g["valor_brl"].sum()) if not df_g.empty else 0.0
        tot_g_usd = float(df_g["valor_usd"].sum()) if not df_g.empty else 0.0
        totals_card("Totais de Gastos", tot_g_brl, tot_g_usd)
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
            st.info("Sem gastos cadastrados.")

    with right:
        st.markdown("### Investimentos cadastrados")
        df_i = df_sql("""SELECT id, data, valor_brl, valor_usd, metodo, conta, quem
                         FROM investimentos ORDER BY date(data) DESC, id DESC;""")
        tot_i_brl = float(df_i["valor_brl"].sum()) if not df_i.empty else 0.0
        tot_i_usd = float(df_i["valor_usd"].sum()) if not df_i.empty else 0.0
        totals_card("Totais de Investimentos", tot_i_brl, tot_i_usd)
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
            st.info("Sem investimentos cadastrados.")

# ============================
# TAB 2 - RECEITAS (FBA) — tudo USD
# ============================
with tab2:
    st.subheader("Operação FBA — Compras (USD) e Recebidos na Amazon (USD)")

    leftC, rightC = st.columns(2)

    # --------- Compras de produto ----------
    with leftC:
        st.markdown("### Compras de Produto (custos em USD, frete não multiplica)")

        apply_pending_reset("pc_", ["data","prod","sku","qty","custo","taxa","prep","frete","obs","conta","quem"])

        with st.form("form_produto_compra"):
            pc_data = st.session_state.get("pc_data", date.today())
            pc_prod = st.session_state.get("pc_prod", "")
            pc_sku = st.session_state.get("pc_sku", "")
            pc_qty = st.session_state.get("pc_qty", 1)
            pc_custo = st.session_state.get("pc_custo", 0.0)
            pc_taxa = st.session_state.get("pc_taxa", 0.0)
            pc_prep = st.session_state.get("pc_prep", 0.0)
            pc_frete = st.session_state.get("pc_frete", 0.0)
            pc_conta = st.session_state.get("pc_conta", contas[0])
            pc_quem = st.session_state.get("pc_quem", pessoas[0])
            pc_obs = st.session_state.get("pc_obs", "")

            data_pc = st.date_input("Data da compra", value=pc_data, format="DD/MM/YYYY", key="pc_data")
            prod = st.text_input("Nome do produto *", value=pc_prod, placeholder="Ex.: Garrafa Térmica 500ml", key="pc_prod")
            sku = st.text_input("SKU (opcional)", value=pc_sku, placeholder="Ex.: BTL-500-INOX", key="pc_sku")
            qty = st.number_input("Quantidade", min_value=1, step=1, value=pc_qty, key="pc_qty")
            custo_unit = st.number_input("Custo unitário (USD)", min_value=0.0, step=0.01, format="%.2f", value=pc_custo, key="pc_custo")
            taxa_unit = st.number_input("Taxa unitária (se tiver) (USD)", min_value=0.0, step=0.01, format="%.2f", value=pc_taxa, key="pc_taxa")
            prep_unit = st.number_input("Prep Center unitário (USD)", min_value=0.0, step=0.01, format="%.2f", value=pc_prep, key="pc_prep")
            frete_total = st.number_input("Frete total da compra (USD) — não multiplica", min_value=0.0, step=0.01, format="%.2f", value=pc_frete, key="pc_frete")

            conta_pc = st.selectbox("Conta/Banco", contas, index=contas.index(pc_conta), key="pc_conta")
            quem_pc = st.selectbox("Quem comprou/lançou", pessoas, index=pessoas.index(pc_quem), key="pc_quem")
            obs_pc = st.text_input("Observação (opcional)", value=pc_obs, placeholder="Lote Setembro, fornecedor X", key="pc_obs")

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
                    schedule_reset("pc_")
                    st.rerun()
                else:
                    st.warning("Informe o nome do produto.")

        df_pc = df_sql("""SELECT id, data, produto, sku, quantidade, custo_unit, taxa_unit, prep_unit, frete_total, 
                                 COALESCE(total_usd, 0) as total_usd, conta, quem
                          FROM produtos_compra ORDER BY date(data) DESC, id DESC;""")
        total_qtd = int(df_pc["quantidade"].sum()) if not df_pc.empty else 0
        total_usd = float(df_pc["total_usd"].sum()) if not df_pc.empty else 0.0
        st.markdown(f"**Totais de compras (USD)** — Quantidade: **{total_qtd}** · Valor: **{money_usd(total_usd)}**")
        if not df_pc.empty:
            df_pc_view = pd.DataFrame({
                "ID": df_pc["id"].astype(int),
                "Data": df_pc["data"],
                "Produto": df_pc["produto"],
                "SKU": df_pc["sku"],
                "Qtd": df_pc["quantidade"].astype(int),
                "Subtotal (USD)": (df_pc["quantidade"] * (df_pc["custo_unit"] + df_pc["taxa_unit"] + df_pc["prep_unit"])).map(money_usd),
                "Frete (USD)": df_pc["frete_total"].map(money_usd),
                "Total (USD)": df_pc["total_usd"].map(money_usd),
                "Conta": df_pc["conta"].fillna(""),
                "Quem": df_pc["quem"].fillna(""),
            })
            st.markdown(df_to_clean_html(df_pc_view, "del_pc", "tbl_pc"), unsafe_allow_html=True)
        else:
            st.info("Sem compras cadastradas.")

    # --------- Recebidos Amazon ----------
    with rightC:
        st.markdown("### Dinheiro recebido dentro da Amazon (USD)")

        apply_pending_reset("ar_", ["data","prod","sku","qty","val","quem","obs"])

        with st.form("form_amz_receitas"):
            ar_data = st.session_state.get("ar_data", date.today())
            ar_prod = st.session_state.get("ar_prod", "")
            ar_sku = st.session_state.get("ar_sku", "")
            ar_qty = st.session_state.get("ar_qty", 0)
            ar_val = st.session_state.get("ar_val", 0.0)
            ar_quem = st.session_state.get("ar_quem", pessoas[0])
            ar_obs = st.session_state.get("ar_obs", "")

            data_ar = st.date_input("Data do crédito", value=ar_data, format="DD/MM/YYYY", key="ar_data")
            prod_ar = st.text_input("Produto (opcional)", value=ar_prod, key="ar_prod")
            sku_ar = st.text_input("SKU (opcional)", value=ar_sku, key="ar_sku")
            qty_ar = st.number_input("Quantidade vendida (opcional)", min_value=0, step=1, value=ar_qty, key="ar_qty")
            val_ar = st.number_input("Valor recebido (USD) dentro da Amazon", min_value=0.0, step=0.01, format="%.2f",
                                     value=ar_val, key="ar_val")
            quem_ar = st.selectbox("Quem lançou", pessoas, index=pessoas.index(ar_quem), key="ar_quem")
            obs_ar = st.text_input("Observação (opcional)", value=ar_obs, key="ar_obs")

            if st.form_submit_button("Adicionar recebimento (Amazon)"):
                add_row("amazon_receitas", dict(
                    data=data_ar.strftime("%Y-%m-%d"), produto=prod_ar.strip(), sku=sku_ar.strip(),
                    quantidade=int(qty_ar), valor_usd=val_ar, quem=quem_ar, obs=obs_ar.strip()
                ))
                schedule_reset("ar_")
                st.rerun()

    # --------- Repasse (com lucro) ----------
    with st.expander("➕ Depósitos FBA (repasse para banco) — com cálculo de Lucro (USD)"):
        apply_pending_reset("r_", ["data","desc","bruto","cogs","taxas","ads","frete","descs","metodo","conta","quem"])

        with st.form("form_receita_repasse"):
            r_data = st.session_state.get("r_data", date.today())
            r_desc = st.session_state.get("r_desc", "")
            r_bruto = st.session_state.get("r_bruto", 0.0)
            r_cogs = st.session_state.get("r_cogs", 0.0)
            r_taxas = st.session_state.get("r_taxas", 0.0)
            r_ads = st.session_state.get("r_ads", 0.0)
            r_frete = st.session_state.get("r_frete", 0.0)
            r_descs = st.session_state.get("r_descs", 0.0)
            r_metodo = st.session_state.get("r_metodo", "Pix")
            r_conta = st.session_state.get("r_conta", contas[0])
            r_quem = st.session_state.get("r_quem", pessoas[0])

            c0, c1 = st.columns([1, 2])
            with c0:
                data_r = st.date_input("Data do recebimento (repasse)", value=r_data, format="DD/MM/YYYY", key="r_data")
            with c1:
                desc_r = st.text_input("Descrição", value=r_desc, placeholder="Ex.: Depósito Amazon FBA, cycle 2025-09", key="r_desc")

            col_a, col_b = st.columns(2)
            with col_a:
                bruto_usd = st.number_input("Bruto recebido (USD)", 0.0, step=0.01, format="%.2f", value=r_bruto, key="r_bruto")
                cogs_usd = st.number_input("COGS (USD)", 0.0, step=0.01, format="%.2f", value=r_cogs, key="r_cogs")
                taxas_usd = st.number_input("Taxas Amazon (USD)", 0.0, step=0.01, format="%.2f", value=r_taxas, key="r_taxas")
            with col_b:
                ads_usd = st.number_input("Anúncios/PPC (USD)", 0.0, step=0.01, format="%.2f", value=r_ads, key="r_ads")
                frete_usd = st.number_input("Frete/Logística (USD)", 0.0, step=0.01, format="%.2f", value=r_frete, key="r_frete")
                desc_usd = st.number_input("Devoluções/Descontos (USD)", 0.0, step=0.01, format="%.2f", value=r_descs, key="r_descs")

            lucro_usd = bruto_usd - (cogs_usd + taxas_usd + ads_usd + frete_usd + desc_usd)
            st.markdown(f"**Lucro calculado (USD): {money_usd(lucro_usd)}**")

            metodo_r = st.selectbox("Método de recebimento",
                                    ["Pix","Transferência","Boleto","Cartão de Crédito","Dinheiro"],
                                    index=["Pix","Transferência","Boleto","Cartão de Crédito","Dinheiro"].index(r_metodo),
                                    key="r_metodo")
            conta_r = st.selectbox("Conta/Banco", contas, index=contas.index(r_conta), key="r_conta")
            quem_r = st.selectbox("Responsável (quem lançou)", pessoas, index=pessoas.index(r_quem), key="r_quem")

            if st.form_submit_button("Adicionar depósito FBA (repasse)"):
                add_row("receitas", dict(
                    data=data_r.strftime("%Y-%m-%d"), origem="FBA", descricao=desc_r,
                    bruto=bruto_usd, cogs=cogs_usd, taxas_amz=taxas_usd, ads=ads_usd,
                    frete=frete_usd, descontos=desc_usd, lucro=lucro_usd,
                    valor_brl=0, valor_usd=bruto_usd,  # bruto em USD
                    metodo=metodo_r, conta=conta_r, quem=quem_r
                ))
                schedule_reset("r_")
                st.rerun()

        df_r = df_sql("""SELECT id, data, descricao, bruto, cogs, taxas_amz, ads, frete, descontos, lucro,
                                valor_usd, metodo, conta, quem
                         FROM receitas ORDER BY date(data) DESC, id DESC;""")
        tot_bruto = float(df_r["bruto"].sum()) if not df_r.empty else 0.0
        tot_lucro = float(df_r["lucro"].sum()) if not df_r.empty else 0.0
        st.markdown(f"**Totais de Depósitos FBA (USD)** — Bruto: {money_usd(tot_bruto)} · Lucro: {money_usd(tot_lucro)}")

        if not df_r.empty:
            df_view_r = pd.DataFrame({
                "ID": df_r["id"].astype(int),
                "Data": df_r["data"],
                "Descrição": df_r["descricao"].fillna(""),
                "Bruto (USD)": df_r["bruto"].map(money_usd),
                "COGS (USD)": df_r["cogs"].map(money_usd),
                "Taxas AMZ (USD)": df_r["taxas_amz"].map(money_usd),
                "Ads (USD)": df_r["ads"].map(money_usd),
                "Frete (USD)": df_r["frete"].map(money_usd),
                "Descontos (USD)": df_r["descontos"].map(money_usd),
                "Lucro (USD)": df_r["lucro"].map(money_usd),
                "Método": df_r["metodo"].fillna(""),
                "Conta": df_r["conta"].fillna(""),
                "Quem": df_r["quem"].fillna(""),
            })
            st.markdown(df_to_clean_html(df_view_r, "del_rec", "tbl_rec"), unsafe_allow_html=True)
        else:
            st.info("Sem depósitos/repasse cadastrados.")

# ============================
# TAB 3 - FLUXO DE CAIXA (mensal + TOTAL GERAL) — receitas em USD
# ============================
with tab3:
    st.subheader("Fluxo de Caixa — Resumo Mensal e Total Geral (Receitas em USD)")

    df_g = df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM gastos;")
    df_i = df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM investimentos;")
    df_r = df_sql("SELECT date(data) as data, 0 as valor_brl, bruto as valor_usd, lucro FROM receitas;")

    def monthly(df, kind, include_usd=True):
        if df.empty:
            x = pd.DataFrame(columns=["mes", "tipo", "brl", "usd"])
            if "lucro" in df.columns:
                x["lucro"] = []
            return x
        t = df.copy()
        t["mes"] = pd.to_datetime(t["data"]).dt.to_period("M").astype(str)
        agg = {"valor_brl": "sum", "valor_usd": "sum"} if include_usd else {"valor_brl": "sum"}
        g = t.groupby("mes").agg(agg)
        if "lucro" in t.columns:
            g["lucro"] = t.groupby("mes")["lucro"].sum()
        g = g.reset_index().rename(columns={"valor_brl": "brl", "valor_usd": "usd"})
        g["tipo"] = kind
        return g

    m_g = monthly(df_g, "Despesas (Gastos)")
    m_i = monthly(df_i, "Despesas (Invest.)")
    m_r = monthly(df_r, "Receitas (FBA)")

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
            st.markdown("#### BRL — por mês")
            dfv = p_brl.copy()
            for col in ["Receitas (FBA)","Despesas (Gastos)","Despesas (Invest.)","Resultado"]:
                dfv[col]=dfv[col].map(money_brl)
            st.dataframe(dfv.rename(columns={"mes":"Mês"}), use_container_width=True, hide_index=True)
        with c2:
            st.markdown("#### USD — por mês")
            dfv = p_usd.copy()
            for col in ["Receitas (FBA)","Despesas (Gastos)","Despesas (Invest.)","Resultado"]:
                dfv[col]=dfv[col].map(money_usd)
            st.dataframe(dfv.rename(columns={"mes":"Mês"}), use_container_width=True, hide_index=True)

        # Totais (USD)
        tot_receita_usd=float(p_usd["Receitas (FBA)"].sum())
        tot_desp_usd=float(p_usd["Despesas (Gastos)"].sum()+p_usd["Despesas (Invest.)"].sum())
        tot_result_usd=float(p_usd["Resultado"].sum())
        st.markdown("### Totais Gerais (USD) — soma de todos os meses")
        summary_card_usd("Totais gerais (USD)", tot_receita_usd, tot_desp_usd, tot_result_usd)

        # Margem total (USD)
        bruto_total=float(m_r["usd"].sum()) if not m_r.empty else 0.0
        lucro_total=float(df_r["lucro"].sum()) if not df_r.empty else 0.0
        margem_total=(lucro_total/bruto_total*100.0) if bruto_total>0 else 0.0
        st.markdown(f"**Margem total FBA (USD) no período:** {margem_total:.1f}% · **Lucro total:** {money_usd(lucro_total)} · **Bruto total:** {money_usd(bruto_total)}")

# ============================
# TAB 4 - GRÁFICOS — foco em USD
# ============================
with tab4:
    st.subheader("Gráficos Mensais (USD como principal)")
    df_g = df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM gastos;")
    df_i = df_sql("SELECT date(data) as data, valor_brl, valor_usd FROM investimentos;")
    df_r = df_sql("SELECT date(data) as data, 0 as valor_brl, bruto as valor_usd, lucro FROM receitas;")

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

    agg=pd.concat([monthly_sum(df_r,"Receitas (FBA)"),monthly_sum(df_g,"Despesas (Gastos)"),monthly_sum(df_i,"Despesas (Invest.)")],ignore_index=True)
    if agg.empty:
        st.info("Cadastre dados para visualizar os gráficos.")
    else:
        st.markdown("#### USD")
        usd=agg[["mes","tipo","USD"]].rename(columns={"USD":"valor"})
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
# TAB 6 - ALOCAÇÕES
# ============================
with tab6:
    st.subheader("Regras de Alocação (Profit First)")
    with st.form("form_regras"):
        colr1, colr2 = st.columns([2,1])
        with colr1:
            nome = st.text_input("Nome do balde", value="Profit")
        with colr2:
            pct = st.number_input("Percentual (0–100%)", min_value=0.0, max_value=100.0, value=10.0, step=1.0)
        if st.form_submit_button("Salvar/Atualizar regra"):
            try:
                add_row("alocacoes_regra", dict(nome=nome, pct=pct/100.0))
            except Exception:
                get_conn().execute("UPDATE alocacoes_regra SET pct=? WHERE nome=?;", (pct/100.0, nome)).connection.commit()
            st.rerun()

    df_regra = df_sql("SELECT nome, pct FROM alocacoes_regra ORDER BY nome;")
    if not df_regra.empty:
        df_view_rg = df_regra.copy()
        df_view_rg["Percentual"] = (df_view_rg["pct"]*100).map(lambda x: f"{x:.0f}%")
        st.dataframe(df_view_rg[["nome","Percentual"]].rename(columns={"nome":"Balde"}),
                     use_container_width=True, hide_index=True)
    else:
        st.info("Nenhuma regra cadastrada ainda. Adicione acima.")

    st.markdown("---")
    st.subheader("Distribuição sugerida do lucro por mês (USD)")
    meses = df_sql("SELECT DISTINCT strftime('%Y-%m', date(data)) as mes FROM receitas ORDER BY mes;")["mes"].tolist()
    if meses:
        mes_ref = st.selectbox("Escolha o mês", meses, index=len(meses)-1)
        tot_lucro_mes = df_sql(
            f"SELECT COALESCE(SUM(lucro),0) AS x FROM receitas WHERE strftime('%Y-%m', date(data))='{mes_ref}';"
        ).iloc[0]["x"]
        st.markdown(f"**Lucro do mês {mes_ref}: {money_usd(tot_lucro_mes)}**")

        df_regra2 = df_sql("SELECT nome, pct FROM alocacoes_regra ORDER BY nome;")
        if not df_regra2.empty:
            dist = {row["nome"]: round(float(tot_lucro_mes) * float(row["pct"]), 2) for _, row in df_regra2.iterrows()}
            for nome_b, val in dist.items():
                st.markdown(f"- **{nome_b}**: {money_usd(val)}")
            if st.button(f"Registrar alocação de {mes_ref}"):
                for nome_b, val in dist.items():
                    add_row("alocacoes_execucao", dict(mes=mes_ref, nome=nome_b, valor_brl=0, valor_usd=val))
                st.success("Alocação registrado!")
                st.rerun()
        else:
            st.info("Cadastre as regras de alocação acima para ver a distribuição.")
    else:
        st.info("Cadastre receitas FBA para escolher um mês.")

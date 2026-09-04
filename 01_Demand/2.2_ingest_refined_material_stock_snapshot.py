# Databricks notebook source
# DBTITLE 1,Propósito do Notebook
# MAGIC %md
# MAGIC # Snapshot Mensal de Estoque de Materiais
# MAGIC
# MAGIC **Camada**: Refined  
# MAGIC **Objetivo**: Capturar snapshot mensal de estoques, quantidades e preços de materiais SAP.  
# MAGIC **Modelo**: Append-only — um registro por chave de negócio por mês (sem overwrite).
# MAGIC
# MAGIC ## Premissas
# MAGIC
# MAGIC * Arquivos Excel (.xlsx) no Volume UC, mesma fonte do notebook SCD2 (2.2)
# MAGIC * Chave: `empresa` + `material` + `centro` + `reference_date`
# MAGIC * `reference_date` extraída do nome do arquivo (padrão `CAD{centro}_{yyyy}.{MM}.xlsx`)
# MAGIC * Proteção contra duplicatas: se `reference_date` já existe na tabela, a inserção é ignorada
# MAGIC * Campos numéricos convertidos para INT (estoques/quantidades) e FLOAT (preços)
# MAGIC * Coluna calculada `stock_total` = soma dos 5 tipos de estoque físico
# MAGIC * Este notebook **não move** arquivos para history/ — isso é responsabilidade do notebook SCD2 (2.2)
# MAGIC
# MAGIC ## Fonte e Destino
# MAGIC
# MAGIC **Fonte**: `/Volumes/parts_hdbk_sandbox/pr_cadastrao/sap_cadastraorefinado/current/`  
# MAGIC **Destino**: `parts_hdbk_sandbox.pr_cadastrao.material_inventory_history`

# COMMAND ----------

# DBTITLE 1,Guarda: verificar arquivos em current/
import os

SOURCE_DIR = "/Volumes/parts_hdbk_sandbox/pr_cadastrao/sap_cadastraorefinado/current/"

files = [f for f in os.listdir(SOURCE_DIR) if f.lower().endswith((".xls", ".xlsx"))]

if not files:
    print("Nenhum arquivo Excel em current/. Encerrando sem processar.")
    dbutils.notebook.exit("NO_FILES")
else:
    print(f"{len(files)} arquivo(s) detectado(s) em current/: {files}")

# COMMAND ----------

# DBTITLE 1,Função de Sanitização de Colunas
import re
import unicodedata

def sanitize_col_name(name):
    if not name:
        return "col_unknown"
    nfkd = unicodedata.normalize('NFKD', name)
    ascii_name = nfkd.encode('ASCII', 'ignore').decode('ASCII')
    clean = re.sub(r'[^a-zA-Z0-9_]', '_', ascii_name)
    clean = re.sub(r'_+', '_', clean).strip('_')
    return clean.lower()

# COMMAND ----------

# DBTITLE 1,Leitura dos arquivos Excel (.xlsx)
df_raw = (spark.read
    .format("excel")
    .option("header", "true")
    .option("inferSchema", "false")
    .load("/Volumes/parts_hdbk_sandbox/pr_cadastrao/sap_cadastraorefinado/current/")
)

print(f"Arquivos carregados: {len(df_raw.columns)} colunas, {df_raw.count()} linhas")

source_file_names = [
    row.file_name
    for row in df_raw.selectExpr("_metadata.file_name AS file_name").distinct().collect()
]
print(f"Arquivos fonte detectados: {source_file_names}")

# COMMAND ----------

# DBTITLE 1,Tratamento de header e sanitização de colunas
first_col = df_raw.columns[0]

if first_col.startswith("_c"):
    header_row = df_raw.first()
    original_names = [header_row[i] if header_row[i] else f"col_{i}" for i in range(len(df_raw.columns))]
    sanitized_names = [sanitize_col_name(n) for n in original_names]

    seen = {}
    for i, name in enumerate(sanitized_names):
        if name in seen:
            seen[name] += 1
            sanitized_names[i] = f"{name}_{seen[name]}"
        else:
            seen[name] = 0

    df = df_raw.toDF(*sanitized_names)

    header_values = [v for v in [header_row[0], header_row[1], header_row[2]] if v]
    df = df.filter(
        ~(
            (df[sanitized_names[0]] == header_values[0]) &
            (df[sanitized_names[1]] == header_values[1]) &
            (df[sanitized_names[2]] == header_values[2])
        )
    )
else:
    original_names = df_raw.columns
    sanitized_names = [sanitize_col_name(n) for n in original_names]

    seen = {}
    for i, name in enumerate(sanitized_names):
        if name in seen:
            seen[name] += 1
            sanitized_names[i] = f"{name}_{seen[name]}"
        else:
            seen[name] = 0

    df = df_raw.toDF(*sanitized_names)

print(f"DataFrame final: {df.count()} linhas, {len(df.columns)} colunas")

# COMMAND ----------

# DBTITLE 1,Snapshot mensal de inventário
from pyspark.sql import functions as F
from datetime import datetime
import uuid

SOURCE_PATH = "/Volumes/parts_hdbk_sandbox/pr_cadastrao/sap_cadastraorefinado/current/"
INVENTORY_TABLE = "parts_hdbk_sandbox.pr_cadastrao.material_inventory_history"

# --- Extrair data de referência do nome dos arquivos ---
ref_dates = set()
for fname in source_file_names:
    match = re.search(r'(\d{4})\.(\d{2})', fname)
    if match:
        ref_dates.add(datetime(int(match.group(1)), int(match.group(2)), 1))

if not ref_dates:
    raise ValueError(f"Não foi possível extrair data de referência dos arquivos: {source_file_names}")
if len(ref_dates) > 1:
    raise ValueError(f"Arquivos com datas diferentes na mesma carga: {ref_dates}")

REFERENCE_DATE = ref_dates.pop()

CURRENT_USER = spark.sql("SELECT current_user()").first()[0]
LOAD_ID = str(uuid.uuid4())
LOAD_TS = spark.sql("SELECT from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS ts").first()["ts"]

print(f"Data de referência do snapshot: {REFERENCE_DATE.strftime('%Y-%m-%d')}")

# --- Colunas de inventário ---
INVENTORY_COLUMNS = [
    "centro", "material", "empresa",
    "estoque_livre_no_centro", "estoque_disponivel_para_venda",
    "estoque_bloqueado", "estoque_em_transito",
    "estoque_em_poder_de_terceiros", "estoque_em_controle_qualidade",
    "estoque_devolucoes", "saldo_da_carteira_de_pedidos",
    "quantidade_em_pi", "quantidade_em_bo",
    "preco_de_rede_price_de_venda_liquida",
]

missing_inv = [c for c in INVENTORY_COLUMNS if c not in df.columns]
if missing_inv:
    raise ValueError(f"Colunas de inventário ausentes no DataFrame: {missing_inv}")

INTEGER_COLUMNS = [
    "estoque_livre_no_centro", "estoque_disponivel_para_venda",
    "estoque_bloqueado", "estoque_em_transito",
    "estoque_em_poder_de_terceiros", "estoque_em_controle_qualidade",
    "estoque_devolucoes", "saldo_da_carteira_de_pedidos",
    "quantidade_em_pi", "quantidade_em_bo",
]
FLOAT_COLUMNS = ["preco_de_rede_price_de_venda_liquida"]

inventory_df = df.select(*INVENTORY_COLUMNS).dropDuplicates(["empresa", "material", "centro"])

int_exprs = {
    c: F.regexp_replace(F.col(c), r"[^\d\-]", "").cast("int")
    for c in INTEGER_COLUMNS
}
float_exprs = {
    c: F.regexp_replace(F.regexp_replace(F.col(c), r"\.", ""), ",", ".").cast("float")
    for c in FLOAT_COLUMNS
}
inventory_df = inventory_df.withColumns({**int_exprs, **float_exprs})

STOCK_TOTAL_COLUMNS = [
    "estoque_livre_no_centro", "estoque_bloqueado",
    "estoque_em_transito", "estoque_em_poder_de_terceiros",
    "estoque_em_controle_qualidade",
]

inventory_df = inventory_df.withColumns({
    "stock_total": sum(F.coalesce(F.col(c), F.lit(0)) for c in STOCK_TOTAL_COLUMNS),
    "reference_date": F.lit(REFERENCE_DATE).cast("date"),
    "_ingested_at": F.lit(LOAD_TS),
    "_ingested_by": F.lit(CURRENT_USER),
    "_load_id": F.lit(LOAD_ID),
    "_source_file_path": F.lit(SOURCE_PATH),
})

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {INVENTORY_TABLE} (
        centro STRING, material STRING, empresa STRING,
        estoque_livre_no_centro INT, estoque_disponivel_para_venda INT,
        estoque_bloqueado INT, estoque_em_transito INT,
        estoque_em_poder_de_terceiros INT, estoque_em_controle_qualidade INT,
        estoque_devolucoes INT, saldo_da_carteira_de_pedidos INT,
        quantidade_em_pi INT, quantidade_em_bo INT,
        preco_de_rede_price_de_venda_liquida FLOAT,
        stock_total INT,
        reference_date DATE,
        _ingested_at TIMESTAMP, _ingested_by STRING,
        _load_id STRING, _source_file_path STRING
    ) USING DELTA
""")

existing_count = spark.sql(f"""
    SELECT COUNT(*) AS total FROM {INVENTORY_TABLE}
    WHERE reference_date = DATE '{REFERENCE_DATE.strftime("%Y-%m-%d")}'
""").first()["total"]

if existing_count > 0:
    print(f"Já existem {existing_count} registros para {REFERENCE_DATE.strftime('%Y-%m-%d')}. Pulando inserção.")
else:
    inventory_df.createOrReplaceTempView("vw_inventory_stage")
    spark.sql(f"INSERT INTO {INVENTORY_TABLE} SELECT * FROM vw_inventory_stage")

    inserted = inventory_df.count()
    total = spark.sql(f"SELECT COUNT(*) AS total FROM {INVENTORY_TABLE}").first()["total"]
    months = spark.sql(f"SELECT COUNT(DISTINCT reference_date) AS months FROM {INVENTORY_TABLE}").first()["months"]

    print(f"Tabela: {INVENTORY_TABLE}")
    print(f"Registros inseridos para {REFERENCE_DATE.strftime('%Y-%m-%d')}: {inserted}")
    print(f"Total acumulado: {total} registros em {months} mês(es)")

# COMMAND ----------

# DBTITLE 1,Metadados da tabela de inventário
INVENTORY_COMMENT = """
Snapshot mensal de estoques e preços de materiais SAP.

Modelo: Append-only (um registro por chave de negócio por mês)
Chave: empresa + material + centro + reference_date
Fonte: /Volumes/parts_hdbk_sandbox/pr_cadastrao/sap_cadastraorefinado/current/
"""

INVENTORY_COLUMN_COMMENTS = {
    "centro": "Código do centro/depósito SAP.",
    "material": "Código único do material/peça (partnumber SAP).",
    "empresa": "Código da empresa SAP (ex: 0200=2W, 0500=4W).",
    "estoque_livre_no_centro": "Quantidade de estoque livre (disponível) no centro.",
    "estoque_disponivel_para_venda": "Quantidade de estoque disponível para venda.",
    "estoque_bloqueado": "Quantidade de estoque bloqueado (indisponível).",
    "estoque_em_transito": "Quantidade de estoque em trânsito entre centros.",
    "estoque_em_poder_de_terceiros": "Quantidade de estoque em poder de terceiros.",
    "estoque_em_controle_qualidade": "Quantidade de estoque em inspeção de qualidade.",
    "estoque_devolucoes": "Quantidade de estoque em devoluções.",
    "saldo_da_carteira_de_pedidos": "Saldo pendente na carteira de pedidos de clientes.",
    "quantidade_em_pi": "Quantidade em Pedidos de Importação (PI).",
    "quantidade_em_bo": "Quantidade em Back Order (BO) - pedidos pendentes.",
    "preco_de_rede_price_de_venda_liquida": "Preço de venda líquida (rede).",
    "stock_total": "Estoque total físico: soma de estoque livre, bloqueado, em trânsito, poder de terceiros e controle de qualidade.",
    "reference_date": "Primeiro dia do mês de referência, extraído do nome do arquivo fonte.",
    "_ingested_at": "Data e hora da ingestão do snapshot.",
    "_ingested_by": "Usuário responsável pela execução da carga.",
    "_load_id": "Identificador único da execução da carga (UUID).",
    "_source_file_path": "Caminho do diretório fonte dos arquivos ingeridos.",
}

spark.sql(f"COMMENT ON TABLE {INVENTORY_TABLE} IS '{INVENTORY_COMMENT.replace(chr(39), chr(39)+chr(39))}'")

for col_name, comment in INVENTORY_COLUMN_COMMENTS.items():
    escaped = comment.replace("'", "''")
    spark.sql(f"COMMENT ON COLUMN {INVENTORY_TABLE}.{col_name} IS '{escaped}'")

spark.sql(f"""
    ALTER TABLE {INVENTORY_TABLE} SET TAGS (
        'domain' = 'materials', 'layer' = 'refined',
        'source' = 'sap', 'history_model' = 'monthly_snapshot',
        'data_classification' = 'internal'
    )
""")

spark.sql(f"""
    ALTER TABLE {INVENTORY_TABLE} SET TBLPROPERTIES (
        'business_owner' = 'Demand Planning',
        'technical_owner' = 'Andre Causs',
        'data_domain' = 'Materials Inventory',
        'source_system' = 'SAP',
        'refresh_frequency' = 'monthly_append',
        'natural_key' = 'empresa, material, centro, reference_date'
    )
""")

print(f"Metadados aplicados \u00e0 tabela {INVENTORY_TABLE}")
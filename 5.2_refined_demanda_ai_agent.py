# Databricks notebook source
# DBTITLE 1,Propósito do Notebook
# ==============================================================================
# NOTEBOOK: 5.2 - Demanda Refinada (rascunho / agente de IA)
# ==============================================================================
#
# PROPÓSITO:
#   Rascunho de uma variante do 5.1 (janela configurável via JANELA_MESES,
#   filtro por f-string em vez de spark.conf). Hoje o notebook só monta a
#   view intermediária vw_sales_orders — não gera nenhuma tabela
#   refined_demand_* ainda.
#
# ARQUITETURA:
#   ┌─────────────────────────────────────────────────────────────────────┐
#   │ INPUT: raw_sales_order, material_cadeia, knvv_sap, kna1_sap         │
#   └────────────────────────┬────────────────────────────────────────────┘
#                            │
#                            v
#   ┌─────────────────────────────────────────────────────────────────────┐
#   │ TRANSFORMAÇÃO:                                                      │
#   │   • JANELA_MESES define data_minima (dinâmico, igual ao 5.1_v2)     │
#   │   • vw_sales_orders: join de ordens + cadeia + centro + cliente     │
#   │     (filtro data >= data_minima via f-string, compatível serverless)│
#   └────────────────────────┬────────────────────────────────────────────┘
#                            │
#                            v
#   ┌─────────────────────────────────────────────────────────────────────┐
#   │ OUTPUT: nenhum ainda — só a TEMP VIEW vw_sales_orders               │
#   │   (rascunho incompleto: falta a parte de agregação/pivot)           │
#   └─────────────────────────────────────────────────────────────────────┘
#
# CONVENÇÕES:
#   • Mesmo padrão de vw_sales_orders do 5.1/5.1_v2
#
# DEPENDÊNCIAS:
#   • python-dateutil (relativedelta)
#
# EXECUÇÃO:
#   Notebook incompleto — não faz parte do pipeline de produção. Avaliar se
#   ainda está em uso antes de apagar ou de continuar o desenvolvimento.
#
# AUTOR: Andre Causs - Honda Peças - Planejamento
# ÚLTIMA ATUALIZAÇÃO: 2026-08-09
# ==============================================================================

print("📊 Notebook 5.2 (rascunho) carregado — só monta vw_sales_orders.")

# COMMAND ----------

# DBTITLE 1,⚙️ Parâmetros Configuráveis
# ==============================================================================
# PARÂMETROS CONFIGURÁVEIS
# ==============================================================================
# Ajuste estes valores conforme necessário para alterar o comportamento do pipeline
# ==============================================================================

# Janela de análise temporal (em meses)
# Define quantos meses fechados de histórico serão incluídos na análise
# Valor padrão: 48 meses (4 anos)
JANELA_MESES = 48

print(f"⚙️ Parâmetros configurados:")
print(f"   • Janela temporal: {JANELA_MESES} meses fechados")

# COMMAND ----------

# DBTITLE 1,Cálculo de Parâmetros Automáticos
from datetime import datetime
from dateutil.relativedelta import relativedelta

# ==============================================================================
# CÁLCULO DE PARÂMETROS TEMPORAIS
# ==============================================================================
# Define janela de análise automática baseada na data mais recente disponível
# nos dados de origem. A janela retroativa é definida pelo parâmetro JANELA_MESES.
# ==============================================================================

# Identifica a data mais recente nas ordens de venda
data_max_df = spark.table("parts_hdbk_sandbox.dt_sales_orders.raw_sales_order").agg({"data": "max"})
data_max_row = data_max_df.collect()[0]
data_max = data_max_row[0]  # Data mais recente

# Define ano e mês de referência com base na data mais recente
if data_max:
    # Converte para datetime se necessário
    if isinstance(data_max, str):
        data_referencia = datetime.strptime(data_max, "%Y-%m-%d")
    else:
        data_referencia = data_max
    
    ano = data_referencia.year
    mes = data_referencia.month
    
    # Calcula data_minima: primeiro dia do mês que inicia a janela de JANELA_MESES fechados
    # Ex: se último mês é junho/2026 e JANELA_MESES=48, então julho/2022 até junho/2026
    data_minima_dt = data_referencia - relativedelta(months=JANELA_MESES - 1)
    # Pega o primeiro dia do mês resultante
    data_minima_dt = data_minima_dt.replace(day=1)
    data_minima = data_minima_dt.strftime("%Y-%m-%d")
    
    # Define como variável Python para uso em células SQL via substituição
    # (spark.conf.set só aceita chaves pré-definidas do Spark)
    
    print(f"📅 Data de referência (mais recente): {data_referencia.strftime('%Y-%m-%d')}")
    print(f"📅 Ano: {ano}, Mês: {mes}")
    print(f"📅 Data mínima ({JANELA_MESES} meses atrás): {data_minima}")
else:
    raise ValueError("Não foi possível determinar a data mais recente das ordens de venda")

# COMMAND ----------

# DBTITLE 1,Sales Orders com Cadeia e Centro Original
# ==============================================================================
# VIEW: vw_sales_orders
# ==============================================================================
# Enriquece raw sales orders com hierarquia de cadeia de produtos e dados de
# cliente. Serve como camada base para todas as agregações de demanda.
#
# JOINS:
#   1. material_cadeia: mapeia SKU → item_principal_cadeia (família)
#   2. knvv_sap: obtém centro_original (distribuição) por cliente/org/canal
#   3. kna1_sap: obtém dados cadastrais do cliente (razão social, estado, país)
#
# FILTRO TEMPORAL: data >= data_minima (calculada dinamicamente)
# ==============================================================================

# Usa f-string Python para garantir interpolação correta do parâmetro data_minima
spark.sql(f"""  
CREATE OR REPLACE TEMP VIEW vw_sales_orders AS
SELECT
  rso.numero_ov,
  rso.data,
  rso.tipo_ov,
  rso.org_vendas,
  rso.canal_dist,
  rso.emissor_da_ordem,
  rso.centro,
  rso.material,  
  rso.quantidade,
  COALESCE(mc.item_principal_cadeia, rso.material) AS item_principal_cadeia,
  k.cen AS centro_original,
  kna.razao_social,
  kna.estado,
  kna.pais
FROM parts_hdbk_sandbox.dt_sales_orders.raw_sales_order rso
LEFT JOIN parts_hdbk_sandbox.pr_cadastrao.material_cadeia mc
  ON rso.material = mc.material
  AND rso.org_vendas = mc.empresa
LEFT JOIN parts_hdbk_sandbox.dm_customers.knvv_sap k
  ON rso.emissor_da_ordem = k.cliente
  AND rso.org_vendas = k.orgv
  AND rso.canal_dist = k.cdst
  AND rso.setor_ativ = k.sa
LEFT JOIN parts_hdbk_sandbox.dm_customers.kna1_sap kna
  ON rso.emissor_da_ordem = kna.cliente
WHERE rso.data >= '{data_minima}'
""")

print(f"✓ View vw_sales_orders criada com filtro: data >= {data_minima}")
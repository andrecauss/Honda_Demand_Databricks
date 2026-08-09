# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "openpyxl",
#   "xlsxwriter",
# ]
# ///
# DBTITLE 1,Export Demand Tables to CSV
# ==============================================================================
# NOTEBOOK: 6.2 - Exportação de Demandas para Excel
# ==============================================================================
#
# PROPÓSITO:
#   Exportar todas as tabelas refinadas do schema parts_hdbk_sandbox.pr_demand
#   como arquivos Excel (.xlsx) para o volume Unity Catalog de exportação.
#
# ARQUITETURA:
#   ┌─────────────────────────────────────────────────────────────────────┐
#   │ INPUT: parts_hdbk_sandbox.pr_demand (schema)                        │
#   │   • refined_demand_fechada_novos_modelos                            │
#   │   • refined_demand_aberta                                           │
#   │   • refined_demand_linha                                            │
#   │   • refined_demand_mi, refined_demand_me                            │
#   │   • refined_demand_zpug, refined_demand_zpug_cliente                │
#   │   • refined_demand_distribuicao                                     │
#   └────────────────────────┬────────────────────────────────────────────┘
#                            │
#                            v
#   ┌─────────────────────────────────────────────────────────────────────┐
#   │ TRANSFORMAÇÃO:                                                      │
#   │   • Conversão Spark DataFrame → Pandas DataFrame                    │
#   │   • Detecção de colunas 'file' e 'sheet' para organização           │
#   │   • Criação de workbooks Excel com múltiplas abas quando aplicável  │
#   │   • Limpeza de colunas de controle (file/sheet) antes da exportação │
#   └────────────────────────┬────────────────────────────────────────────┘
#                            │
#                            v
#   ┌─────────────────────────────────────────────────────────────────────┐
#   │ OUTPUT: /Volumes/parts_hdbk_sandbox/pr_demand/                      │
#   │         demand_refined_exportfiles/*.xlsx                           │
#   └─────────────────────────────────────────────────────────────────────┘
#
# DIMENSÕES DE ANÁLISE:
#   • Mercado: MI (Mercado Interno) vs ME (Mercado Externo)
#   • Segmento: HDA (Honda Automóveis) vs HAB (Honda Motos)
#   • Centros: 0203, 0209, 0232 (HDA) e 0503, 0505 (HAB)
#   • Tipo de Demanda: Fechada, Aberta, Linha, ZPUG
#   • Granularidade: Item Principal Cadeia (família de produtos)
#
# CONVENÇÕES:
#   • Tabelas com colunas 'file'/'sheet': um workbook por 'file', uma aba
#     por 'sheet'
#   • Tabelas simples: um workbook com uma única aba
#   • Nomes de arquivos: padrão "{SEGMENTO} {ANO} {MÊS} {TIPO}.xlsx"
#   • Limite de 31 caracteres para nomes de abas Excel
#
# DEPENDÊNCIAS:
#   • openpyxl: leitura/escrita de arquivos .xlsx
#   • xlsxwriter: engine otimizado para pandas.to_excel()
#   • pandas: manipulação de dados tabulares
#
# EXECUÇÃO:
#   1. Execute todas as células sequencialmente (Run All)
#   2. Volume de destino será limpo antes da exportação
#   3. Arquivos .xlsx serão criados no volume especificado
#   4. Verificar logs de sucesso/erro ao final
#
# AUTOR: Andre Causs - Honda Peças - Planejamento
# ÚLTIMA ATUALIZAÇÃO: 2026-08-09
# ==============================================================================

print("📊 Notebook 6.2 - Exportação de Demandas para Excel carregado.")
print("✓ Pronto para processar.")

# COMMAND ----------

# DBTITLE 1,List tables in schema
# ------------------------------------------------------------------------------
# Descoberta de Tabelas do Schema
# ------------------------------------------------------------------------------
# Lista todas as tabelas disponíveis no schema pr_demand para exportação.
# Usado posteriormente para relatório de resumo e validação.
# ------------------------------------------------------------------------------
tables = spark.sql("SHOW TABLES IN parts_hdbk_sandbox.pr_demand").collect()
table_names = [row.tableName for row in tables]

print(f"Total de tabelas encontradas: {len(table_names)}")

# COMMAND ----------

# DBTITLE 1,Install openpyxl library
# ------------------------------------------------------------------------------
# Instalação de Dependências
# ------------------------------------------------------------------------------
# Instala bibliotecas necessárias para manipulação de arquivos Excel:
# - openpyxl: Leitura e escrita de arquivos .xlsx (formato Office Open XML)
# - xlsxwriter: Engine otimizado para pandas.to_excel() com melhor performance
#   e suporte a formatação avançada
# ------------------------------------------------------------------------------
%pip install openpyxl xlsxwriter

# COMMAND ----------

# DBTITLE 1,Funções Auxiliares de Exportação
# ==============================================================================
# FUNÇÕES AUXILIARES DE EXPORTAÇÃO
# ==============================================================================
# Define funções reutilizáveis para exportação de tabelas Delta para
# formato Excel, com suporte a múltiplas abas e organização por arquivo.
# ==============================================================================
import pandas as pd
import tempfile
import os

# Caminho do volume de destino para armazenamento dos arquivos exportados
VOLUME_PATH = "/Volumes/parts_hdbk_sandbox/pr_demand/demand_refined_exportfiles"

# Diretório temporário local usado durante a criação dos arquivos Excel
# antes de copiá-los para o volume Unity Catalog
LOCAL_TMP = tempfile.mkdtemp()

print(f"Volume de destino: {VOLUME_PATH}")
print(f"Diretorio temporario: {LOCAL_TMP}")


def export_table_to_excel(table_name, schema="parts_hdbk_sandbox.pr_demand"):
    """
    Exporta uma tabela Delta para formato Excel (.xlsx).
    
    A função detecta automaticamente a estrutura da tabela e aplica a
    lógica de exportação apropriada:
    
    - Tabelas com colunas 'file' e 'sheet': organizadas em múltiplos workbooks,
      onde cada valor único de 'file' gera um arquivo .xlsx, e cada valor
      único de 'sheet' dentro desse arquivo se torna uma aba separada.
    
    - Tabelas simples (sem file/sheet): exportadas como um único arquivo
      Excel com uma única aba.
    
    Args:
        table_name (str): Nome da tabela a ser exportada (sem o prefixo de
            schema/catalog).
        schema (str, optional): Schema completo no formato catalog.schema.
            Defaults to "parts_hdbk_sandbox.pr_demand".
    
    Returns:
        None
    
    Raises:
        Exception: Propaga qualquer erro ocorrido durante a exportação,
            após registrá-lo no console.
    
    Side Effects:
        - Cria arquivos .xlsx temporários no sistema de arquivos local
        - Copia arquivos para o volume Unity Catalog especificado
        - Imprime mensagens de progresso e status no console
    
    Example:
        >>> export_table_to_excel("refined_demand_fechada_novos_modelos")
        [OK] refined_demand_fechada_novos_modelos -> HDA 2026 Jul Demanda Fechada.xlsx (1250 linhas, 5 abas)
    
    Note:
        - Nomes de abas Excel são truncados em 31 caracteres (limitação do formato)
        - Colunas 'file' e 'sheet' são removidas dos dados exportados
        - Utiliza xlsxwriter como engine para melhor performance
    """
    full_table_name = f"{schema}.{table_name}"
    
    try:
        # Carrega DataFrame e cacheia schema ANTES de qualquer iteração
        # (otimização Spark Connect - evita múltiplas chamadas Analyze RPC)
        df = spark.table(full_table_name)
        columns_list = df.columns
        has_file_sheet = "file" in columns_list and "sheet" in columns_list
        
        # Ação Spark: converte DataFrame para pandas DENTRO do try/except
        # para capturar erros de execução distribuída (Spark Connect lazy evaluation)
        pdf = df.toPandas()

        if has_file_sheet:
            # Tabelas organizadas: um workbook por 'file', uma aba por 'sheet'
            # dentro de cada workbook
            for file_name, file_group in pdf.groupby("file"):
                local_path = os.path.join(LOCAL_TMP, f"{file_name}.xlsx")
                with pd.ExcelWriter(local_path, engine='xlsxwriter') as writer:
                    for sheet_name, sheet_group in file_group.groupby("sheet"):
                        # Remove colunas de controle (metadados de organização)
                        # antes de exportar os dados de negócio
                        data = sheet_group.drop(columns=["file", "sheet"])
                        
                        # Excel impõe limite de 31 caracteres para nomes de abas
                        data.to_excel(writer, sheet_name=str(sheet_name)[:31], index=False)

                # Copia arquivo local para volume Unity Catalog
                output_path = f"{VOLUME_PATH}/{file_name}.xlsx"
                dbutils.fs.cp(f"file:{local_path}", output_path)
                
                n_sheets = file_group["sheet"].nunique()
                print(f"[OK] {table_name} -> {file_name}.xlsx ({len(file_group)} linhas, {n_sheets} abas)")
        else:
            # Tabelas simples: um workbook com uma única aba
            local_path = os.path.join(LOCAL_TMP, f"{table_name}.xlsx")
            pdf.to_excel(local_path, index=False, engine='xlsxwriter')

            output_path = f"{VOLUME_PATH}/{table_name}.xlsx"
            dbutils.fs.cp(f"file:{local_path}", output_path)
            print(f"[OK] {table_name}.xlsx: {len(pdf)} linhas")

    except Exception as e:
        print(f"[ERRO] Erro ao exportar {table_name}: {str(e)}")
        raise

# COMMAND ----------

# DBTITLE 1,Limpar Volume de Exportação
# ==============================================================================
# LIMPEZA DO VOLUME DE EXPORTAÇÃO
# ==============================================================================
# Remove todos os arquivos .xlsx existentes no volume de destino antes da
# nova exportação.
# ==============================================================================
print("\n" + "="*70)
print("LIMPANDO VOLUME DE EXPORTAÇÃO")
print("="*70)

try:
    # Lista todos os arquivos atualmente presentes no volume
    files = dbutils.fs.ls(VOLUME_PATH)
    
    if len(files) == 0:
        print("Volume já está vazio.")
    else:
        print(f"\nEncontrados {len(files)} arquivos no volume.")
        print("Removendo todos os arquivos...\n")
        
        # Remove cada arquivo individualmente
        for file_info in files:
            file_path = file_info.path
            dbutils.fs.rm(file_path)
            print(f"[REMOVIDO] {file_info.name}")
        
        print(f"\n[OK] Volume limpo com sucesso! {len(files)} arquivos removidos.")
        
except Exception as e:
    # Falha na limpeza não deve bloquear a exportação
    print(f"[AVISO] Erro ao limpar volume: {str(e)}")
    print("Continuando com a exportação...")

# COMMAND ----------

# DBTITLE 1,Bloco: Demandas Gerais
# ===========================================================================
# BLOCO 1: DEMANDAS FECHADAS - NOVOS MODELOS
# ===========================================================================
# Exporta tabelas de demanda fechada para novos modelos
# ===========================================================================

print("\n" + "="*70)
print("BLOCO 1: DEMANDAS FECHADAS - NOVOS MODELOS")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Exportar Demanda Fechada
# ------------------------------------------------------------------------------
# Exporta Demanda Fechada - Novos Modelos
# ------------------------------------------------------------------------------
# Tabela: refined_demand_fechada_novos_modelos
# Estrutura: Múltiplos arquivos Excel com colunas 'file' e 'sheet'
# ------------------------------------------------------------------------------
export_table_to_excel("refined_demand_fechada_novos_modelos")

# COMMAND ----------

# DBTITLE 1,Bloco: Demandas Gerais
# ===========================================================================
# BLOCO 2: DEMANDAS GERAIS
# ===========================================================================
# Exporta tabelas de demanda aberta e linha (contagem de SKUs)
# ===========================================================================

print("\n" + "="*70)
print("BLOCO 2: DEMANDAS GERAIS")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Exportar Demanda Aberta
# ------------------------------------------------------------------------------
# Exporta Demanda Aberta
# ------------------------------------------------------------------------------
# Tabela: refined_demand_aberta
# Estrutura: Múltiplos arquivos Excel com colunas 'file' e 'sheet'
# ------------------------------------------------------------------------------
export_table_to_excel("refined_demand_aberta")

# COMMAND ----------

# DBTITLE 1,Exportar Demanda Linha
# ------------------------------------------------------------------------------
# Exporta Demanda Linha
# ------------------------------------------------------------------------------
# Tabela: refined_demand_linha
# Estrutura: Múltiplos arquivos Excel com colunas 'file' e 'sheet'
# ------------------------------------------------------------------------------
export_table_to_excel("refined_demand_linha")

# COMMAND ----------

# DBTITLE 1,Bloco: Demandas por Mercado
# ===========================================================================
# BLOCO 3: DEMANDAS POR MERCADO
# ===========================================================================
# Exporta tabelas segmentadas por Mercado Interno (MI) e Mercado Externo (ME)
# ===========================================================================

print("\n" + "="*70)
print("BLOCO 3: DEMANDAS POR MERCADO")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Exportar Demanda MI
# ------------------------------------------------------------------------------
# Exporta Demanda MI
# ------------------------------------------------------------------------------
# Tabela: refined_demand_mi
# Estrutura: Múltiplos arquivos Excel com colunas 'file' e 'sheet'
# ------------------------------------------------------------------------------
export_table_to_excel("refined_demand_mi")

# COMMAND ----------

# DBTITLE 1,Exportar Demanda ME
# ------------------------------------------------------------------------------
# Exporta Demanda ME
# ------------------------------------------------------------------------------
# Tabela: refined_demand_me
# Estrutura: Múltiplos arquivos Excel com colunas 'file' e 'sheet'
# ------------------------------------------------------------------------------
export_table_to_excel("refined_demand_me")

# COMMAND ----------

# DBTITLE 1,Bloco: Pedidos ZPUG
# ===========================================================================
# BLOCO 4: PEDIDOS ZPUG
# ===========================================================================
# Exporta pedidos do tipo ZPUG (tipo_ov = 'ZPUG'), agregados e por cliente
# ===========================================================================

print("\n" + "="*70)
print("BLOCO 4: PEDIDOS ZPUG")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Exportar Pedidos ZPUG
# ------------------------------------------------------------------------------
# Exporta Pedidos ZPUG Agregados
# ------------------------------------------------------------------------------
# Tabela: refined_demand_zpug
# Estrutura: Múltiplos arquivos Excel com colunas 'file' e 'sheet'
# ------------------------------------------------------------------------------
export_table_to_excel("refined_demand_zpug")

# COMMAND ----------

# DBTITLE 1,Exportar Pedidos ZPUG por Cliente
# ------------------------------------------------------------------------------
# Exporta Pedidos ZPUG por Cliente
# ------------------------------------------------------------------------------
# Tabela: refined_demand_zpug_cliente
# Estrutura: Múltiplos arquivos Excel com colunas 'file' e 'sheet'
# ------------------------------------------------------------------------------
export_table_to_excel("refined_demand_zpug_cliente")

# COMMAND ----------

# DBTITLE 1,Bloco: Distribuição
# ===========================================================================
# BLOCO 5: DISTRIBUICAO POR CENTRO E MERCADO
# ===========================================================================
# Exporta tabela de distribuicao unificada HDA + HAB com percentuais por
# centro e mercado
# ===========================================================================

print("\n" + "="*70)
print("BLOCO 5: DISTRIBUICAO POR CENTRO E MERCADO")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Exportar Distribuição
# ------------------------------------------------------------------------------
# Exporta Distribuição
# ------------------------------------------------------------------------------
# Tabela: refined_demand_distribuicao
# Estrutura: Arquivo Excel sem colunas 'file'/'sheet' (tabela simples)
# ------------------------------------------------------------------------------
export_table_to_excel("refined_demand_distribuicao")

# COMMAND ----------

# DBTITLE 1,Resumo da Exportação
# ==============================================================================
# RESUMO DA EXPORTAÇÃO
# ==============================================================================
# Exibe estatísticas finais e confirmação de conclusão bem-sucedida.
# ==============================================================================
print("\n" + "="*70)
print("✅ EXPORTAÇÃO CONCLUÍDA COM SUCESSO!")
print("="*70)
print(f"\n📁 Local: {VOLUME_PATH}")
print(f"📊 Total de tabelas exportadas: {len(table_names)}")
print("\nℹ️  Os arquivos Excel estão prontos para compartilhamento.")
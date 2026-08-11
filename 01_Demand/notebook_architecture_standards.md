# 📘 Padrões de Arquitetura para Notebooks Databricks

**Autor:** André Causs - Demand Planning - Honda Parts Division  
**Última Atualização:** 2026-08-09  
**Versão:** 1.0

---

## 📋 Sumário

1. [Visão Geral](#visão-geral)
2. [Estrutura de Célula Inicial](#estrutura-de-célula-inicial)
3. [Padrões de Comentários](#padrões-de-comentários)
4. [Documentação de Funções](#documentação-de-funções)
5. [Células de Processamento](#células-de-processamento)
6. [Convenções de Nomenclatura](#convenções-de-nomenclatura)
7. [Exemplos Práticos](#exemplos-práticos)

---

## 🎯 Visão Geral

Este documento define padrões de arquitetura para notebooks Databricks, garantindo:

✅ **Legibilidade** - Código auto-documentado e fácil de entender  
✅ **Manutenibilidade** - Estrutura consistente facilita atualizações  
✅ **Rastreabilidade** - Inputs, transformações e outputs claramente documentados  
✅ **Onboarding** - Novos desenvolvedores entendem o código rapidamente  
✅ **Qualidade** - Padrões profissionais de engenharia de dados

---

## 📄 Estrutura de Célula Inicial

Todo notebook deve começar com uma célula de propósito completa:

```python
# ==============================================================================
# NOTEBOOK: [Número] - [Nome Descritivo]
# ==============================================================================
#
# PROPÓSITO:
#   [Descrição clara e concisa do que o notebook faz, incluindo contexto de
#   negócio e por que ele existe]
#
# ARQUITETURA:
#   ┌─────────────────────────────────────────────────────────────────────┐
#   │ INPUT: [fonte1], [fonte2], [fonte3]                                 │
#   └────────────────────────┬────────────────────────────────────────────┘
#                            │
#                            v
#   ┌─────────────────────────────────────────────────────────────────────┐
#   │ TRANSFORMAÇÃO:                                                      │
#   │   • [Transformação 1]                                               │
#   │   • [Transformação 2]                                               │
#   │   • [Transformação 3]                                               │
#   └────────────────────────┬────────────────────────────────────────────┘
#                            │
#                            v
#   ┌─────────────────────────────────────────────────────────────────────┐
#   │ OUTPUT: [destino1], [destino2]                                      │
#   └─────────────────────────────────────────────────────────────────────┘
#
# DIMENSÕES DE ANÁLISE:
#   • [Dimensão 1]: [Descrição]
#   • [Dimensão 2]: [Descrição]
#   • [Dimensão 3]: [Descrição]
#
# CONVENÇÕES:
#   • [Convenção de nomenclatura 1]
#   • [Convenção de nomenclatura 2]
#   • [Formato de dados]
#
# DEPENDÊNCIAS:
#   • [biblioteca1]
#   • [biblioteca2]
#   • [serviço externo]
#
# EXECUÇÃO:
#   [Instruções de como executar o notebook]
#
# AUTOR: [Nome] - [Departamento] - [Divisão]
# ÚLTIMA ATUALIZAÇÃO: [YYYY-MM-DD]
# ==============================================================================

print("📊 Notebook [Nome] carregado.")
print("✓ Pronto para processar.")
```

### 🔑 Elementos Essenciais:

1. **PROPÓSITO** - O "por quê" do notebook existe
2. **ARQUITETURA** - Fluxo visual (INPUT → TRANSFORMAÇÃO → OUTPUT)
3. **DIMENSÕES DE ANÁLISE** - Granularidade e métricas
4. **CONVENÇÕES** - Nomenclaturas e padrões específicos
5. **DEPENDÊNCIAS** - Bibliotecas e serviços necessários
6. **EXECUÇÃO** - Como rodar o notebook
7. **AUTOR** - Sempre incluir o nome do desenvolvedor

---

## 💬 Padrões de Comentários

### 🔹 Separadores de Seção

Use separadores visuais consistentes:

```python
# ==============================================================================
# SEÇÃO PRINCIPAL (Configuração, Transformação, Output)
# ==============================================================================
# Descrição detalhada da seção, explicando seu propósito e contexto.
# Pode ocupar múltiplas linhas.
# ==============================================================================
```

```python
# ------------------------------------------------------------------------------
# SUBSEÇÃO (Função específica, célula de processamento)
# ------------------------------------------------------------------------------
# Descrição concisa da subseção.
# ------------------------------------------------------------------------------
```

### 🔹 Comentários Inline

```python
# Comentário explicando a lógica de negócio
variavel = valor  # Comentário breve sobre esta linha específica
```

### 🔹 Comentários de Contexto de Negócio

Sempre inclua o "por quê" quando relevante:

```python
# Exclui ZESP (Pedido Inicial de Exportação) para isolar demanda de
# novos modelos sem pedidos de lançamento em mercados externos
df_filtrado = df.filter(df.tipo_ov != "ZESP")
```

### 🔹 Evite Comentários Óbvios

❌ **Ruim:**
```python
# Incrementa contador
contador = contador + 1
```

✅ **Bom:**
```python
# Rastreia número de lotes processados para logging de progresso
contador = contador + 1
```

---

## 📚 Documentação de Funções

### 🔹 Formato Google Docstring

```python
def funcao_exemplo(parametro1, parametro2, parametro3="default"):
    """
    Breve descrição de uma linha do que a função faz.
    
    Descrição mais detalhada explicando o contexto de negócio,
    casos de uso, e comportamentos importantes. Pode ocupar
    múltiplas linhas.
    
    Args:
        parametro1 (str): Descrição do parâmetro 1
        parametro2 (DataFrame): Descrição do parâmetro 2
        parametro3 (str, optional): Descrição do parâmetro opcional.
            Defaults to "default".
    
    Returns:
        DataFrame: Descrição do retorno
    
    Raises:
        ValueError: Quando ocorre [condição específica]
    
    Side Effects:
        - Persiste dados em [destino]
        - Exibe mensagens no console
    
    Example:
        >>> resultado = funcao_exemplo('valor1', df)
        >>> print(resultado.count())
        1000
    
    Note:
        Observações importantes sobre uso ou limitações.
    """
    pass
```

### 🔹 Documentação Mínima (Funções Simples)

```python
def funcao_simples(x, y):
    """
    Soma dois números.
    
    Args:
        x (int): Primeiro número
        y (int): Segundo número
    
    Returns:
        int: Soma de x e y
    """
    return x + y
```

---

## 🔄 Células de Processamento

### 🔹 Estrutura Padrão

```python
# ------------------------------------------------------------------------------
# [SEGMENTO] - [TIPO DE DEMANDA]
# ------------------------------------------------------------------------------
# Agregação: [coluna(s) de agrupamento]
# Operação: [soma/contagem/média/etc]
# Centros: [lista de centros processados]
# Filtros: [condições aplicadas]
# Saída: [nome da tabela de destino]
# Nota: [Contexto de negócio ou decisão técnica importante]
# ------------------------------------------------------------------------------

# Código da célula aqui
```

### 🔹 Exemplo Completo

```python
# ------------------------------------------------------------------------------
# HDA - DEMANDA FECHADA
# ------------------------------------------------------------------------------
# Agregação: item_principal_cadeia (família de produtos)
# Operação: Soma de quantidades
# Centros: TTL, 0203, 0209, 0232
# Filtros: org_vendas='0200' (2W Motos)
# Saída: refined_demand_fechada_HDA_{centro}
# ------------------------------------------------------------------------------
org_vendas = "0200"
arquivo = _nome_arquivo(org_vendas, "Demanda Fechada")

df_base = df.filter((df.data >= data_minima) & (df.org_vendas == org_vendas))

for aba in ABAS_2W:
    df_aba = df_base if aba == "TTL" else df_base.filter(df_base.centro_original == aba)
    df_pivot = _pivot(df_aba, ["item_principal_cadeia"])
    df_pivot = df_pivot.withColumn("file", lit(arquivo)).withColumn("sheet", lit(aba))
    
    sufixo = f"HDA_{aba}"
    tabela_completa = f"refined_demand_fechada_{sufixo}"
    _append(df_pivot, tabela_completa)
```

---

## 🏷️ Convenções de Nomenclatura

### 🔹 Variáveis e Funções

| Tipo | Padrão | Exemplo |
|------|--------|--------|
| Função privada/auxiliar | `_snake_case` | `_pivot()`, `_append()` |
| Função pública | `snake_case` | `process_data()` |
| Variável | `snake_case` | `df_base`, `org_vendas` |
| Constante | `UPPER_SNAKE_CASE` | `SCHEMA`, `ABAS_2W` |
| DataFrame | `df_*` | `df_pivot`, `df_resultado` |

### 🔹 Tabelas

```python
# Padrão: {tipo}_{descrição}_{segmento}_{centro}
refined_demand_fechada_HDA_TTL
refined_demand_aberta_HAB_0503
raw_sales_order
dim_customers
```

### 🔹 Colunas

```python
# Colunas de negócio: Pascal Case (para exportação)
"Item Principal Cadeia"
"Material"
"Cliente"

# Colunas técnicas: snake_case (para processamento interno)
item_principal_cadeia
centro_original
data_aaaa_mm
```

### 🔹 Arquivos de Exportação

```python
# Padrão: {SEGMENTO} {ANO} {MÊS_ABREVIADO} {TIPO}
"HDA 2026 Jul Demanda Fechada"
"HAB 2026 Ago Distribuição"
```

---

## 📖 Exemplos Práticos

### 🔹 Exemplo 1: Célula SQL com Documentação

```sql
%sql
-- ==============================================================================
-- VIEW: vw_sales_orders
-- ==============================================================================
-- Enriquece raw sales orders com hierarquia de cadeia de produtos e dados de
-- cliente. Serve como camada base para todas as agregações de demanda.
--
-- JOINS:
--   1. material_cadeia: mapeia SKU → item_principal_cadeia (família)
--   2. knvv_sap: obtém centro_original (distribuição) por cliente/org/canal
--   3. kna1_sap: obtém dados cadastrais do cliente (razão social, estado, país)
--
-- FILTRO TEMPORAL: data >= data_minima (calculada dinamicamente: -48 meses)
-- ==============================================================================

CREATE OR REPLACE TEMP VIEW vw_sales_orders AS
SELECT
  rso.numero_ov,
  rso.data,
  rso.material,
  COALESCE(mc.item_principal_cadeia, rso.material) AS item_principal_cadeia,
  k.cen AS centro_original
FROM raw_sales_order rso
LEFT JOIN material_cadeia mc ON rso.material = mc.material
LEFT JOIN knvv_sap k ON rso.emissor_da_ordem = k.cliente
WHERE rso.data >= '${spark.conf.data_minima}'
```

### 🔹 Exemplo 2: Célula de Limpeza

```python
# ==============================================================================
# LIMPEZA PRÉVIA DO SCHEMA DE SAÍDA
# ==============================================================================
# Remove todas as tabelas refined_demand_* existentes para garantir consistência
# e evitar acumulação de dados entre execuções. Esta é uma operação destrutiva
# que deve ser executada apenas em ambientes controlados.
# ==============================================================================

_limpar_tabelas()
```

### 🔹 Exemplo 3: Célula de Separação de Seção

```python
# ==============================================================================
# SEGMENTO 2W - HDA (MOTOS)
# ==============================================================================
# org_vendas: 0200
# Centros de Distribuição:
#   • 0203 - Sumaré (SP)
#   • 0209 - Jaboatão (PE)
#   • 0232 - Manaus (AM)
#   • TTL  - Total consolidado dos 3 centros
#
# Este bloco gera 9 conjuntos de tabelas refinadas:
#   1. Demanda Fechada (item_principal_cadeia)
#   2. Demanda Fechada Novos Modelos (sem tipo_ov='ZESP')
#   3. Demanda Aberta (material/SKU)
#   4. Demanda Linha (contagem por item_principal_cadeia)
#   5. Demanda MI - Mercado Interno (canal_dist='01')
#   6. Demanda ME - Mercado Externo (canal_dist='02')
#   7. Pedido ZPUG (tipo_ov='ZPUG')
#   8. Pedido ZPUG por Cliente (tipo_ov='ZPUG' + emissor_da_ordem)
#   9. Distribuição (análise percentual por centro e mercado)
# ==============================================================================

print("🏍️ Iniciando processamento HDA (2W Motos)...")
```

---

## ✅ Checklist de Revisão

Antes de finalizar um notebook, verifique:

- [ ] Célula inicial de propósito completa e atualizada
- [ ] Todas as funções documentadas com docstrings
- [ ] Células de processamento com headers padronizados
- [ ] Comentários explicam "por quê", não apenas "o quê"
- [ ] Nomenclaturas consistentes (variáveis, funções, tabelas)
- [ ] Separadores visuais usados corretamente
- [ ] Contexto de negócio documentado onde relevante
- [ ] Autor e data de atualização corretos
- [ ] Emojis usados para melhorar escaneabilidade (opcional)

---

## 🚀 Dicas de Produtividade

### 1️⃣ Use Templates

Crie snippets para estruturas comuns:

```python
# Template de função
def _funcao_nome():
    """
    [Descrição breve]
    
    Args:
        param (type): descrição
    
    Returns:
        type: descrição
    """
    pass
```

### 2️⃣ Mantenha Consistência

Se um notebook usa um padrão, todos os notebooks do projeto devem usar o mesmo.

### 3️⃣ Documente Decisões Técnicas

Quando você faz uma escolha técnica não óbvia, explique o raciocínio:

```python
# Usamos .withColumns() em vez de múltiplos .withColumn() para evitar
# execução plan profundamente aninhado que degrada performance
df = df.withColumns({
    "col1": expr1,
    "col2": expr2
})
```

### 4️⃣ Evite Comentários Markdown

Prefira comentários Python (`#`) em vez de células Markdown (`%md`) para:
- Melhor versionamento
- Facilita busca de texto
- Mantém código e documentação juntos

---

## 📚 Referências

- [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html)
- [PEP 257 - Docstring Conventions](https://www.python.org/dev/peps/pep-0257/)
- [Databricks Best Practices](https://docs.databricks.com/notebooks/best-practices.html)

---

## 📝 Notas de Versão

### v1.0 (2026-08-09)
- Versão inicial do guia de padrões
- Baseado em refinamentos aplicados ao notebook 5.1
- Inclui exemplos práticos do projeto Honda Demand Analytics

---

**Fim do Documento**

*Este é um documento vivo. Atualize-o conforme novos padrões são estabelecidos.*
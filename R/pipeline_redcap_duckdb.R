#!/usr/bin/env Rscript

# Garante uso do ambiente do projeto (renv) antes de carregar dependências.
if (requireNamespace("renv", quietly = TRUE)) {
  renv::activate()
} else {
  stop("Pacote 'renv' não encontrado no ambiente atual.")
}

# Dependências são chamadas por namespace (ex.: `pkg::fun`) para evitar poluir o
# ambiente global e explicitar a origem de cada função.

# Lê variável de ambiente obrigatória ou interrompe a execução com erro claro.
get_env <- function(name) {
  v <- trimws(Sys.getenv(name, ""))
  if (v == "") stop(sprintf("Variável de ambiente ausente: %s", name))
  v
}

# Extrai registros do REDCap via API.
# Permite filtrar por lógica, campos e instrumentos/formulários quando necessário.
extract_redcap <- function(
api_url, token, filter_logic = NULL, fields = NULL, forms = NULL
) {
  args <- list(
    redcap_uri = api_url,
    token = token,
    raw_or_label = "raw"
  )
  if (!is.null(filter_logic) && nzchar(filter_logic)) args$filter_logic <- filter_logic
  if (!is.null(fields) && length(fields)) args$fields <- fields
  if (!is.null(forms) && length(forms)) args$forms <- forms

  do.call(REDCapR::redcap_read, args)$data
}

# Normaliza o dataset para carga estável em DuckDB:
# - converte colunas para texto quando possível
# - adiciona metadados de auditoria (timestamp de ingestão e hash da linha)
transform_redcap <- function(df) {
  # garante data.frame base para evitar problemas com classes especiais
  df <- as.data.frame(df, stringsAsFactors = FALSE)

  df |>
    dplyr::mutate(
      dplyr::across(dplyr::everything(), ~ ifelse(is.na(.x), NA_character_, as.character(.x))),
      ._redcap_ingested_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      ._row_hash = vapply(
        seq_len(nrow(df)),
        function(i) digest::digest(df[i, ], algo = "xxhash64"),
        character(1)
      )
    )
}

# Abre conexão com DuckDB. Garante pasta do arquivo de banco quando necessário.
init_duckdb <- function(path) {
  db_dir <- dirname(path)
  if (nzchar(db_dir) && db_dir != "." && !dir.exists(db_dir)) {
    dir.create(db_dir, recursive = TRUE, showWarnings = FALSE)
  }
  DBI::dbConnect(duckdb::duckdb(), dbdir = path)
}

# Cria tabela de log de execuções, se ainda não existir.
ensure_log_table <- function(con) {
  DBI::dbExecute(
    con,
    "CREATE TABLE IF NOT EXISTS redcap_pipeline_log (
       run_id VARCHAR,
       started_at TIMESTAMP,
       finished_at TIMESTAMP,
       status VARCHAR,
       n_rows BIGINT,
       table_name VARCHAR,
       message VARCHAR
     )"
  )
}

# Registra sucesso/erro de cada execução no log interno do DuckDB.
log_run <- function(con, run_id, started_at, status, n_rows, table_name, message = NULL) {
  ensure_log_table(con)
  DBI::dbWriteTable(
    con,
    "redcap_pipeline_log",
    data.frame(
      run_id = run_id,
      started_at = started_at,
      finished_at = Sys.time(),
      status = status,
      n_rows = as.integer(n_rows),
      table_name = table_name,
      message = ifelse(is.null(message), NA_character_, message),
      stringsAsFactors = FALSE
    ),
    append = TRUE
  )
}

# Carrega o dataset no DuckDB.
# Se a tabela existe, faz append alinhando o schema:
# - adiciona colunas em falta na tabela alvo;
# - preenche colunas ausentes no dataframe com NA;
# - reordena as colunas para bater com a ordem do destino.
# Se a tabela não existe, cria a tabela com overwrite.
ensure_table_schema <- function(con, table_name, df) {
  if (!DBI::dbExistsTable(con, table_name)) {
    return(df)
  }

  quoted_table <- DBI::dbQuoteIdentifier(con, table_name)
  pragma_sql <- sprintf("PRAGMA table_info(%s)", quoted_table)
  table_cols <- DBI::dbGetQuery(con, pragma_sql)$name

  # adiciona colunas novas na tabela destino para permitir append seguro
  cols_missing_in_target <- setdiff(names(df), table_cols)
  if (length(cols_missing_in_target)) {
    for (col_name in cols_missing_in_target) {
      quoted_col <- DBI::dbQuoteIdentifier(con, col_name)
      alter_sql <- sprintf("ALTER TABLE %s ADD COLUMN %s VARCHAR", quoted_table, quoted_col)
      DBI::dbExecute(con, alter_sql)
    }
    table_cols <- c(table_cols, cols_missing_in_target)
  }

  # garante todas as colunas esperadas no dataframe
  cols_missing_in_df <- setdiff(table_cols, names(df))
  if (length(cols_missing_in_df)) {
    df[cols_missing_in_df] <- NA_character_
  }

  df[, table_cols, drop = FALSE]
}

load_to_duckdb <- function(con, df, table_name) {
  if (DBI::dbExistsTable(con, table_name)) {
    df <- ensure_table_schema(con, table_name, df)
    DBI::dbWriteTable(con, table_name, df, append = TRUE, row.names = FALSE)
  } else {
    DBI::dbWriteTable(con, table_name, df, overwrite = TRUE, row.names = FALSE)
  }
  nrow(df)
}

# Orquestra o fluxo: lê configs, conecta ao banco, extrai, transforma e grava.
# Em caso de erro, grava evento no log e relança a falha.
run_pipeline <- function() {
  started_at <- Sys.time()
  run_id <- format(started_at, "%Y%m%d_%H%M%S")

  api_url <- get_env("REDCAP_API_URL")
  token <- get_env("REDCAP_API_TOKEN")
  duckdb_path <- Sys.getenv("DUCKDB_PATH", "data/redcap.duckdb")
  table_name <- Sys.getenv("DUCKDB_TABLE", "redcap_data")
  filter_logic <- Sys.getenv("REDCAP_FILTER_LOGIC", "")

  con <- init_duckdb(duckdb_path)
  # Fecha conexão apenas ao final do pipeline (evita "Invalid connection" em operações subsequentes).
  on.exit({
    if (DBI::dbIsValid(con)) {
      DBI::dbDisconnect(con, shutdown = TRUE)
    }
  }, add = TRUE)

  tryCatch(
    {
      message("Extrair REDCap...")
      raw <- extract_redcap(api_url, token, filter_logic = filter_logic)

      message("Transformar dados...")
      data_clean <- transform_redcap(raw)

      message("Gravar no DuckDB...")
      n <- load_to_duckdb(con, data_clean, table_name)

      log_run(
        con = con, run_id = run_id, started_at = started_at,
        status = "ok", n_rows = n, table_name = table_name
      )
      message(sprintf("OK - %d linhas em %s", n, table_name))
      invisible(TRUE)
    },
    error = function(e) {
      log_run(
        con = con, run_id = run_id, started_at = started_at,
        status = "error", n_rows = 0L, table_name = table_name,
        message = conditionMessage(e)
      )
      stop(e)
    }
  )
}

# Ponto de entrada do script.
run_pipeline()

#!/usr/bin/env Rscript
# Discord → Supabase via Discord BOT API (no Selenium)
# Reads messages from a channel using a bot token and uploads to Supabase.

suppressPackageStartupMessages({
  library(httr2)
  library(dplyr)
  library(purrr)
  library(tidyr)
  library(lubridate)
  library(readr)
  library(DBI)
  library(RPostgres)
  library(glue)
})

# ---------- helpers ----------
getv <- function(name, default = NULL, required = FALSE) {
  v <- Sys.getenv(name, unset = NA_character_)
  if (!is.na(v) && nzchar(v)) return(v)
  if (required) stop(glue("Missing required environment variable: {name}"), call. = FALSE)
  default
}

discord_get <- function(url, token) {
  req <- request(url) |>
    req_headers(
      Authorization = paste0("Bot ", token),
      "User-Agent"  = "DiscordBot (github-actions, 1.0)"
    )

  resp <- req_perform(req)

  # Handle basic 429 backoff
  if (resp_status(resp) == 429) {
    body <- resp_body_json(resp, simplifyVector = TRUE)
    wait <- as.numeric(body$retry_after %||% 1)
    Sys.sleep(wait + 0.2)
    resp <- req_perform(req)
  }

  stop_for_status(resp)
  resp_body_json(resp, simplifyVector = TRUE)
}

fetch_channel_messages <- function(channel_id, token, max_messages = 5000) {
  out <- list()
  before <- NULL
  fetched <- 0L

  repeat {
    base <- glue("https://discord.com/api/v10/channels/{channel_id}/messages?limit=100")
    url  <- if (is.null(before)) base else paste0(base, "&before=", before)

    dat <- discord_get(url, token)
    if (!length(dat)) break

    out <- c(out, dat)
    fetched <- fetched + length(dat)

    if (length(dat) < 100 || fetched >= max_messages) break
    before <- dat[[length(dat)]]$id  # paginate using the last message id
    Sys.sleep(0.2)                    # gentle pacing
  }

  out
}

# ---------- config from env ----------
CHANNEL_ID        <- getv("DISCORD_CHANNEL_ID", required = TRUE)   # e.g. 712035664161538089
DISCORD_BOT_TOKEN <- getv("DISCORD_BOT_TOKEN", required = TRUE)    # "Bot …" without the "Bot " prefix
MAX_MESSAGES      <- as.integer(getv("MAX_MESSAGES", "5000"))

SB_HOST <- getv("SUPABASE_HOST", required = TRUE)
SB_PORT <- as.integer(getv("SUPABASE_PORT", "6543"))
SB_DB   <- getv("SUPABASE_DB",   required = TRUE)
SB_USER <- getv("SUPABASE_USER", required = TRUE)
SB_PWD  <- getv("SUPABASE_PWD",  required = TRUE)

dir.create("artifacts", showWarnings = FALSE)

# ---------- fetch ----------
message(glue("⬇️  Fetching up to {MAX_MESSAGES} messages from channel {CHANNEL_ID} via Discord API…"))
raw <- fetch_channel_messages(CHANNEL_ID, DISCORD_BOT_TOKEN, MAX_MESSAGES)

# ---------- normalize ----------
# Each element is one message JSON. We extract:
# - message_id (Discord snowflake, keep as TEXT)
# - poster_user (author$username)
# - time_iso (timestamp)
# - message_text (content)
# - is_reply + reply_user + reply_snippet (from referenced_message, if any)
df <- map_dfr(raw, function(m) {
  author <- m$author
  ref    <- m$referenced_message
  tibble(
    message_id    = m$id %||% NA_character_,
    poster_user   = author$username %||% NA_character_,
    time_text     = m$timestamp %||% NA_character_,
    time_iso      = m$timestamp %||% NA_character_,
    message_text  = m$content %||% NA_character_,
    is_reply      = !is.null(ref),
    reply_user    = if (!is.null(ref)) (ref$author$username %||% NA_character_) else NA_character_,
    reply_snippet = if (!is.null(ref)) (ref$content %||% NA_character_) else NA_character_
  )
})

# Save snapshot for debugging
write_csv(df, "artifacts/discord_messages_raw.csv")
message(glue("📦 Pulled {nrow(df)} messages."))

# ---------- upload to Supabase (new table name to avoid schema clashes) ----------
# We use message_id (TEXT) as the stable primary key.
con <- DBI::dbConnect(
  RPostgres::Postgres(),
  host     = SB_HOST,
  port     = SB_PORT,
  dbname   = SB_DB,
  user     = SB_USER,
  password = SB_PWD,
  sslmode  = "require"
)
on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

DBI::dbExecute(con, "
  CREATE TABLE IF NOT EXISTS discord_messages_api (
    message_id     text PRIMARY KEY,
    poster_user    text,
    time_text      text,
    time_iso       timestamptz,
    message_text   text,
    is_reply       boolean,
    reply_user     text,
    reply_snippet  text
  );
")

# Cast ISO → timestamptz (UTC)
df_upload <- df |>
  mutate(
    time_iso = suppressWarnings(lubridate::ymd_hms(time_iso, tz = 'UTC'))
  )

DBI::dbWriteTable(con, "tmp_discord_messages_api", df_upload,
                  temporary = TRUE, overwrite = TRUE)

DBI::dbExecute(con, "
  INSERT INTO discord_messages_api AS d
        (message_id, poster_user, time_text, time_iso,
         message_text, is_reply, reply_user, reply_snippet)
  SELECT message_id, poster_user, time_text, time_iso,
         message_text, is_reply, reply_user, reply_snippet
  FROM tmp_discord_messages_api
  ON CONFLICT (message_id) DO UPDATE
    SET poster_user    = EXCLUDED.poster_user,
        time_text      = EXCLUDED.time_text,
        time_iso       = EXCLUDED.time_iso,
        message_text   = EXCLUDED.message_text,
        is_reply       = EXCLUDED.is_reply,
        reply_user     = EXCLUDED.reply_user,
        reply_snippet  = EXCLUDED.reply_snippet;
")

DBI::dbExecute(con, "DROP TABLE IF EXISTS tmp_discord_messages_api;")
message(glue("✅ Upload complete: {nrow(df_upload)} rows (upserted) → discord_messages_api"))


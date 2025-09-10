#!/usr/bin/env Rscript
# CI-friendly Discord scraper → Supabase (hardened, no readyState waits)

suppressPackageStartupMessages({
  library(RSelenium)
  library(rvest)
  library(stringr)
  library(dplyr)
  library(tidyr)
  library(lubridate)
  library(DBI)
  library(RPostgres)
  library(readr)
  library(glue)
})

# --- harden networking in CI (avoid odd proxies) ---
Sys.unsetenv(c("http_proxy","https_proxy","HTTP_PROXY","HTTPS_PROXY"))

dir.create("artifacts", showWarnings = FALSE)

# ---------- helpers ----------
getv <- function(name, default = NULL, required = FALSE) {
  v <- Sys.getenv(name, unset = NA_character_)
  if (!is.na(v) && nzchar(v)) return(v)
  if (required) stop(glue("Missing required environment variable: {name}"), call. = FALSE)
  default
}

wait_css <- function(remDr, css, timeout = 15000, interval = 500) {
  deadline <- Sys.time() + timeout/1000
  repeat {
    el <- tryCatch(remDr$findElement("css selector", css), error = function(e) NULL)
    if (!is.null(el)) return(el)
    if (Sys.time() > deadline) stop(paste("Timeout waiting for:", css))
    Sys.sleep(interval/1000)
  }
}

wait_any_css <- function(remDr, css_list, timeout = 15000, interval = 400) {
  deadline <- Sys.time() + timeout/1000
  repeat {
    for (css in css_list) {
      el <- tryCatch(remDr$findElement("css selector", css), error = function(e) NULL)
      if (!is.null(el)) return(list(css = css, element = el))
    }
    if (Sys.time() > deadline) return(NULL)
    Sys.sleep(interval/1000)
  }
}

save_debug <- function(remDr, stem) {
  html <- tryCatch(remDr$getPageSource()[[1]], error = function(e) "")
  if (nzchar(html)) writeLines(html, file.path("artifacts", paste0(stem, ".html")))
  try(remDr$screenshot(file = file.path("artifacts", paste0(stem, ".png"))), silent = TRUE)
}

switch_to_latest_window <- function(remDr) {
  h <- tryCatch(remDr$getWindowHandles()[[1]], error = function(e) character())
  if (length(h) > 0) try(remDr$switchToWindow(h[[length(h)]]), silent = TRUE)
}

# ---------- Selenium open (realistic caps; no --headless=new) ----------
open_selenium <- function() {
  message("🔌 Connecting to Selenium at localhost:4444 ...")
  ua <- "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
  caps <- list(
    "goog:chromeOptions" = list(
      args = c(
        # The standalone-chrome container already runs with a virtual display,
        # so we don't *need* headless. If you insist, add "--headless".
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--window-size=1366,768",
        "--disable-blink-features=AutomationControlled",
        "--lang=en-US,en",
        glue("--user-agent={ua}")
      )
    ),
    "pageLoadStrategy" = "eager"  # don't block on full load
  )
  for (pth in c("/wd/hub", "")) {
    pth_msg <- if (pth == "") "/" else pth
    message(sprintf("… trying path '%s'", pth_msg))
    drv <- RSelenium::remoteDriver(
      remoteServerAddr = "localhost",
      port             = 4444L,
      browserName      = "chrome",
      path             = pth,
      extraCapabilities = caps
    )
    ok <- tryCatch({ drv$open(); TRUE }, error = function(e) {
      message("   failed: ", conditionMessage(e)); FALSE
    })
    if (ok) return(drv)
  }
  stop("Could not connect to Selenium on localhost:4444 (tried /wd/hub and /).")
}

# ---------- configuration (env-driven) ----------
CHANNEL_URL      <- getv("DISCORD_CHANNEL_URL", required = TRUE)
DISCORD_EMAIL    <- getv("DISCORD_EMAIL", required = TRUE)
DISCORD_PASSWORD <- getv("DISCORD_PASSWORD", required = TRUE)

N_SCROLLS        <- as.integer(getv("N_SCROLLS", "600"))
SCROLL_PX        <- as.integer(getv("SCROLL_PX", "-800"))
SCROLL_SLEEP_MS  <- as.integer(getv("SCROLL_SLEEP_MS", "1200"))
TIMEOUT_MS       <- as.integer(getv("TIMEOUT_MS", "15000"))

# Supabase
SB_HOST <- getv("SUPABASE_HOST", required = TRUE)
SB_PORT <- as.integer(getv("SUPABASE_PORT", "6543"))
SB_DB   <- getv("SUPABASE_DB", required = TRUE)
SB_USER <- getv("SUPABASE_USER", required = TRUE)
SB_PWD  <- getv("SUPABASE_PWD", required = TRUE)

# ---------- connect ----------
remDr <- open_selenium()

# ---------- login flow ----------
message("➡️  Navigating to Discord login…")
remDr$navigate("https://discord.com/login")
switch_to_latest_window(remDr)
save_debug(remDr, "login_after_nav")

# Cookie consent (best-effort)
try({
  btns <- remDr$findElements("css selector", "button, [role='button']")
  for (b in btns) {
    txt <- tryCatch(b$getElementText()[[1]], error = function(e) "")
    if (grepl("Accept|Allow|Aceptar|Agree", txt, ignore.case = TRUE)) { b$clickElement(); break }
  }
}, silent = TRUE)

# Look for either the login inputs OR obvious challenge markers
first_hit <- wait_any_css(
  remDr,
  css_list = c(
    "input[name='email'], input[type='email']",
    "div#challenge-stage",
    "iframe[src*='captcha'], iframe[title*='captcha']",
    "#app-mount"  # Discord root (as a last resort indicator page rendered)
  ),
  timeout = TIMEOUT_MS * 2
)

if (is.null(first_hit) || !grepl("input", first_hit$css, fixed = TRUE)) {
  save_debug(remDr, "no_email_input")
  stop("Login form not found (maybe Cloudflare/captcha or layout change). See artifacts/no_email_input.*", call. = FALSE)
}

email_box <- first_hit$element
pwd_box   <- wait_css(remDr, "input[name='password'], input[type='password']", TIMEOUT_MS)

email_box$clearElement(); email_box$sendKeysToElement(list(DISCORD_EMAIL))
pwd_box$clearElement();   pwd_box$sendKeysToElement(list(DISCORD_PASSWORD))

login_btn <- wait_css(remDr, "button[type='submit']", TIMEOUT_MS)
login_btn$clickElement()

# Wait for app shell (servers sidebar)
message("⏳ Waiting for app UI…")
ok <- FALSE
deadline <- Sys.time() + 30
repeat {
  els <- remDr$findElements("css selector", "[data-list-id='guildsnav'], nav[aria-label='Servers']")
  if (length(els) > 0) { ok <- TRUE; break }
  Sys.sleep(0.5)
  if (Sys.time() > deadline) break
}
save_debug(remDr, if (ok) "after_login_ok" else "after_login_failed")
if (!ok) stop("Login did not complete (captcha/2FA or selector changed). See artifacts/after_login_failed.*", call. = FALSE)

# Go to target channel
message("➡️  Opening target channel…")
remDr$navigate(CHANNEL_URL)
switch_to_latest_window(remDr)
save_debug(remDr, "channel_after_nav")

# ---------- scrolling & capture ----------
message(glue("🖱️  Scrolling {N_SCROLLS} iterations, step {SCROLL_PX}px, delay {SCROLL_SLEEP_MS}ms…"))
SCROLLER_CSS <- ".managedReactiveScroller_d125d2"    # may drift over time
MSG_LI_CSS   <- "li.messageListItem__5126c"

df_containers_all <- tibble(container_html = character())

for (i in seq_len(N_SCROLLS)) {
  if (i %% 25 == 0) message(glue("… scroll loop {i}/{N_SCROLLS}"))
  scroller <- tryCatch(wait_css(remDr, SCROLLER_CSS, 8000), error = function(e) NULL)
  if (is.null(scroller)) { message("⚠️  Could not find scroller container; breaking."); break }

  msgs <- remDr$findElements("css selector", MSG_LI_CSS)
  if (length(msgs) > 0) {
    raw_vec <- vapply(msgs, function(el) {
      tryCatch(el$getElementAttribute("outerHTML")[[1]], error = function(e) NA_character_)
    }, character(1))
    df_containers_all <- bind_rows(df_containers_all, tibble(container_html = raw_vec))
  }

  remDr$executeScript("arguments[0].scrollBy(0, arguments[1]);", list(scroller, SCROLL_PX))
  Sys.sleep(SCROLL_SLEEP_MS/1000)
}

# Deduplicate
df_containers_all <- df_containers_all %>% distinct(container_html, .keep_all = TRUE)
message(glue("📦 Captured unique containers: {nrow(df_containers_all)}"))
write_csv(df_containers_all, "artifacts/df_containers_all.csv")

# ---------- parsing ----------
parseDiscordMessage <- function(html_snippet) {
  doc <- read_html(html_snippet)

  reply_node <- doc %>% html_node("div.repliedMessage_c19a55")
  is_reply <- !is.na(html_text(reply_node, trim = TRUE))

  reply_user <- NA_character_
  reply_snippet <- NA_character_
  if (is_reply) {
    tmp_user <- reply_node %>% html_node("span.username_c19a55") %>% html_text(trim = TRUE)
    if (!is.na(tmp_user)) reply_user <- tmp_user
    tmp_snippet <- reply_node %>% html_node("div.repliedTextContent_c19a55") %>% html_text(trim = TRUE)
    if (!is.na(tmp_snippet)) reply_snippet <- tmp_snippet
  }

  poster_user <- doc %>% html_node("span.headerText_c19a55 span.username_c19a55") %>% html_text(trim = TRUE)
  if (is.na(poster_user)) poster_user <- NA_character_

  time_node <- doc %>% html_node("span.timestamp_c19a55 time")
  time_text <- NA_character_
  time_iso  <- NA_character_
  if (!is.na(html_text(time_node, trim = TRUE))) {
    time_text <- time_node %>% html_text(trim = TRUE)
    time_iso  <- time_node %>% html_attr("datetime")
  }

  msg_node <- doc %>% html_node("div.contents_c19a55 div.markup__75297.messageContent_c19a55")
  message_text <- NA_character_
  if (!is.na(html_text(msg_node, trim = TRUE))) {
    message_text <- msg_node %>% html_text(trim = TRUE)
  }

  tibble(
    poster_user   = poster_user,
    time_text     = time_text,
    time_iso      = time_iso,
    message_text  = message_text,
    is_reply      = is_reply,
    reply_user    = if (is_reply) reply_user else NA_character_,
    reply_snippet = if (is_reply) reply_snippet else NA_character_
  )
}

clean_df <- df_containers_all %>% filter(!is.na(container_html) & nzchar(container_html))
df_parsed <- bind_rows(lapply(clean_df$container_html, parseDiscordMessage))
df_parsed <- df_parsed %>% tidyr::fill(poster_user, .direction = "down")
write_csv(df_parsed, "artifacts/df_parsed.csv")

# ---------- upload to Supabase ----------
message("🛫 Uploading to Supabase…")

df_upload <- df_parsed %>%
  mutate(
    msg_id   = row_number(),
    time_iso = suppressWarnings(ymd_hms(time_iso, tz = "UTC"))
  ) %>%
  select(msg_id, poster_user, time_text, time_iso, message_text, is_reply, reply_user, reply_snippet)

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
  CREATE TABLE IF NOT EXISTS discord_messages (
    msg_id        integer PRIMARY KEY,
    poster_user   text,
    time_text     text,
    time_iso      timestamptz,
    message_text  text,
    is_reply      boolean,
    reply_user    text,
    reply_snippet text
  );
")

DBI::dbWriteTable(con, "tmp_discord_messages", df_upload, temporary = TRUE, overwrite = TRUE)
DBI::dbExecute(con, "
  INSERT INTO discord_messages AS d
        (msg_id, poster_user, time_text, time_iso, message_text, is_reply, reply_user, reply_snippet)
  SELECT msg_id, poster_user, time_text, time_iso, message_text, is_reply, reply_user, reply_snippet
  FROM tmp_discord_messages
  ON CONFLICT (msg_id) DO UPDATE
    SET poster_user   = EXCLUDED.poster_user,
        time_text     = EXCLUDED.time_text,
        time_iso      = EXCLUDED.time_iso,
        message_text  = EXCLUDED.message_text,
        is_reply      = EXCLUDED.is_reply,
        reply_user    = EXCLUDED.reply_user,
        reply_snippet = EXCLUDED.reply_snippet;
")
DBI::dbExecute(con, "DROP TABLE IF EXISTS tmp_discord_messages;")

message("✅ Done. Uploaded rows: ", nrow(df_upload))

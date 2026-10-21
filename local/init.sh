#!/bin/bash
set -e

# Target download directory
TARGET_DIR="../common/dockerfiles/tmp"
mkdir -p "$TARGET_DIR"

# List of files (deduplicated)
URLS=(
  "https://jdbc.postgresql.org/download/postgresql-42.6.0.jar"
  "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar"
  "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar"
  "https://repo1.maven.org/maven2/javax/activation/javax.activation-api/1.2.0/javax.activation-api-1.2.0.jar"
  "https://repo1.maven.org/maven2/javax/xml/bind/jaxb-api/2.3.1/jaxb-api-2.3.1.jar"
  "https://repo1.maven.org/maven2/com/sun/xml/bind/jaxb-impl/2.3.1/jaxb-impl-2.3.1.jar"
  "https://archive.apache.org/dist/spark/spark-4.1.3/spark-4.1.3-bin-hadoop3.tgz"
  "https://repo1.maven.org/maven2/org/apache/hive/hcatalog/hive-hcatalog-core/3.1.2/hive-hcatalog-core-3.1.2.jar"
  "https://repo1.maven.org/maven2/org/apache/hive/hive-exec/3.1.3/hive-exec-3.1.3.jar"
  # Iceberg runtime MUST match the Spark above (4.1.x): use the *-4.1_2.13 artifact.
  # Iceberg 1.11 has no Spark 4.2 runtime yet — don't bump Spark past 4.1 until it does.
  # hadoop-aws MUST equal Spark's bundled hadoop-client (3.4.2 in Spark 4.1.3), and the
  # AWS SDK bundle MUST be the one hadoop-aws 3.4.2 was built with (2.29.52).
  "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.11.0/iceberg-spark-runtime-4.1_2.13-1.11.0.jar"
  "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.11.0/iceberg-aws-bundle-1.11.0.jar"
)


# A file is complete only if it unpacks: a cut-off download (slow line, Ctrl-C) must not be
# "already exists, skipping" next time — the image build then fails with "unexpected end of file".
complete() {
  case "$1" in
    *.tgz) gzip -t "$1" 2>/dev/null ;;
    *.jar) python3 -c 'import sys, zipfile; sys.exit(zipfile.ZipFile(sys.argv[1]).testzip() is not None)' "$1" 2>/dev/null ;;
    *)     [ -s "$1" ] ;;
  esac
}

echo "Downloading files to $TARGET_DIR ..."

for url in "${URLS[@]}"; do
  filename=$(basename "$url")
  filepath="$TARGET_DIR/$filename"

  if [ -f "$filepath" ] && complete "$filepath"; then
    echo "✅ $filename already exists, skipping."
    continue
  fi
  [ -f "$filepath" ] && echo "⚠️  $filename is incomplete (an earlier download was cut off) — downloading it again." \
    && mv "$filepath" "$filepath.part"
  echo "⬇️  Downloading $filename ..."
  # no total time limit (Spark is ~570 MB): give up only when stalled under 1 KB/s for a minute;
  # -C - resumes the .part file, and it becomes the real name only once it's complete
  curl -fL --retry 5 --retry-delay 3 -C - --speed-limit 1024 --speed-time 60 --progress-bar \
    -o "$filepath.part" "$url"
  if ! complete "$filepath.part"; then
    rm -f "$filepath.part"
    echo "❌ $filename downloaded but is damaged — run 'make init' again." >&2
    exit 1
  fi
  mv "$filepath.part" "$filepath"
done

echo "🎉 All downloads complete. Files saved in $TARGET_DIR"

# ---------------------------------------------------------------------------
# AdventureWorks (OLTP) sample database for the local Postgres.
# Downloads the Microsoft OLTP CSVs + the lorint Postgres port, fixes the CSVs
# for Postgres (needs `ruby` — ships with macOS), and stages them where the
# Postgres init script (03_adventureworks.sh) loads them into the
# `adventureworks` database on first `make up`. Staged data is gitignored
# (~110 MB). Skips if already prepared.
# ---------------------------------------------------------------------------
AW_DIR="init_scripts/postgres/adventureworks"
if [ -f "$AW_DIR/install.sql" ] && ls "$AW_DIR"/*.csv >/dev/null 2>&1; then
  echo "✅ AdventureWorks sample already prepared in $AW_DIR, skipping."
elif ! command -v ruby >/dev/null 2>&1; then
  echo "⚠️  ruby not found — skipping AdventureWorks prep (it fixes the CSVs)."
  echo "    Install ruby and re-run 'make init' to enable the adventureworks DB."
else
  echo "⬇️  Preparing AdventureWorks (Postgres) sample database ..."
  mkdir -p "$AW_DIR"
  tmp="$(mktemp -d)"
  curl --max-time 300 -L --progress-bar -o "$tmp/data.zip" \
    "https://github.com/microsoft/sql-server-samples/releases/download/adventureworks/AdventureWorks-oltp-install-script.zip"
  curl --max-time 120 -L --progress-bar -o "$tmp/script.zip" \
    "https://github.com/lorint/AdventureWorks-for-Postgres/archive/master.zip"
  unzip -oq "$tmp/data.zip"  -d "$AW_DIR"        # OLTP CSVs land directly in AW_DIR
  unzip -oq "$tmp/script.zip" -d "$tmp/script"
  cp "$tmp"/script/AdventureWorks-for-Postgres-master/install.sql     "$AW_DIR"/
  cp "$tmp"/script/AdventureWorks-for-Postgres-master/update_csvs.rb  "$AW_DIR"/
  ( cd "$AW_DIR" && ruby update_csvs.rb >/dev/null )   # fix CSVs for Postgres COPY
  rm -f "$AW_DIR/update_csvs.rb"; rm -rf "$tmp"
  echo "🎉 AdventureWorks prepared in $AW_DIR ($(ls "$AW_DIR"/*.csv | wc -l | tr -d ' ') CSVs)."
fi

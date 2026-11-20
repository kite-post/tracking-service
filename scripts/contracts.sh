#!/usr/bin/env bash
# Підключає kite-contracts за тегом: make contracts V=0.2.0 розставляє версію в
# Makefile і pom.xml і тягне пакет. Без V — просто підтягує поточну версію.
# Цим самим викликом користується CI, коли kite-contracts випускає новий тег.
set -euo pipefail

v="${1:?вкажи версію: scripts/contracts.sh 0.2.0}"
printf '%s' "$v" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$' \
  || { echo "contracts: версія «$v» має бути вигляду 1.2.3" >&2; exit 1; }
mvn="mvn"
[ -x ./mvnw ] && mvn="./mvnw"

# Правка атомарна: якщо тег не знайдеться, файли повертаються як були, і
# Makefile не розійдеться з маніфестом.
backup="$(mktemp -d)"
for f in Makefile pom.xml; do [ -f "$f" ] && cp "$f" "$backup/"; done
restore() { for f in Makefile pom.xml; do [ -f "$backup/$f" ] && cp "$backup/$f" "$f"; done; }
trap 'rc=$?; [ $rc -ne 0 ] && restore; exit $rc' EXIT

perl -pi -e "s|^CONTRACTS_VERSION \?= .*|CONTRACTS_VERSION ?= $v|" Makefile
perl -pi -e "s|<kite-contracts.version>[^<]*</kite-contracts.version>|<kite-contracts.version>$v</kite-contracts.version>|" pom.xml

$mvn -B -ntp -q dependency:resolve
echo "ua.kite:kite-contracts:$v на місці: моделі, інтерфейси API і класи подій."
echo "Схеми лежать у тому самому jar — /kite/contracts/openapi і /kite/contracts/events."

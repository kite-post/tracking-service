#!/usr/bin/env bash
# Перевіряє, що сервіс підключає kite-contracts як залежність, закріплену за
# тегом, і не тримає копій схем у себе (5.4, п. 6). Цю ціль проганяє CI.
set -euo pipefail

want="${1:?вкажи версію: scripts/contracts-check.sh 0.1.0}"
mvn="mvn"
[ -x ./mvnw ] && mvn="./mvnw"

fail() {
  echo "contracts-check: $1" >&2
  exit 1
}

# 1. Версія в pom.xml — та сама, що в Makefile.
pom_version="$(sed -n 's|.*<kite-contracts.version>\(.*\)</kite-contracts.version>.*|\1|p' pom.xml | head -1)"
[ -n "$pom_version" ] || fail "у pom.xml немає властивості kite-contracts.version"
[ "$pom_version" = "$want" ] || fail "pom.xml закріплений за $pom_version, а Makefile — за $want"

# 2. Версія закріплена, а не плаваюча.
case "$pom_version" in
  *LATEST* | *SNAPSHOT* | *RELEASE* | *'['* | *'('*)
    fail "версія $pom_version не закріплена: контракти підключаються тільки закріпленим тегом" ;;
esac

# 3. Залежність справді оголошена.
grep -q "<artifactId>kite-contracts</artifactId>" pom.xml \
  || fail "залежності ua.kite:kite-contracts немає в pom.xml"

# 4. Своїх копій схем у репозиторії бути не повинно.
copies="$(find . -path ./target -prune -o \
  \( -name '*.schema.json' -o -name 'openapi.yaml' -o -name 'openapi.yml' -o -name 'asyncapi.yaml' \) \
  -print 2>/dev/null || true)"
[ -z "$copies" ] || fail "у репозиторії лежать копії схем, а мають приходити залежністю:
$copies"

# 5. Залежність резолвиться — отже тег існує і доступний.
$mvn -B -ntp -q dependency:resolve >/dev/null \
  || fail "ua.kite:kite-contracts:$want не резолвиться — чи є такий тег і чи налаштований доступ до GitHub Packages?"

echo "contracts-check: kite-contracts $want підключений за тегом, копій схем немає."

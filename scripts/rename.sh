#!/usr/bin/env bash
# Перетворює шаблон у репозиторій конкретного сервіса: ім'я артефакта, пакет,
# клас застосунку та ім'я образу. Запускається один раз після створення репо.
set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "Вкажи ім'я сервіса, наприклад shipment-service" >&2
  exit 1
fi
name="$1"

if [[ ! "$name" =~ ^[a-z][a-z0-9-]*$ ]]; then
  echo "Ім'я сервіса — малі літери, цифри і дефіси: shipment-service" >&2
  exit 1
fi

# shipment-service -> shipment -> ua.kite.shipment -> ShipmentApplication
short="${name%-service}"
pkg_leaf="$(printf '%s' "$short" | tr -d '-')"
class_base="$(printf '%s' "$short" | awk -F- '{for (i=1;i<=NF;i++) printf toupper(substr($i,1,1)) substr($i,2)}')"

echo "Сервіс:  $name"
echo "Пакет:   ua.kite.$pkg_leaf"
echo "Клас:    ${class_base}Application"
echo "База:    $pkg_leaf (як на стенді kite-infra)"

old_dir="src/main/java/ua/kite/servicetemplate"
new_dir="src/main/java/ua/kite/$pkg_leaf"
old_test_dir="src/test/java/ua/kite/servicetemplate"
new_test_dir="src/test/java/ua/kite/$pkg_leaf"

if [ -d "$old_dir" ]; then
  mkdir -p "$(dirname "$new_dir")"
  git mv "$old_dir" "$new_dir" 2>/dev/null || mv "$old_dir" "$new_dir"
fi
if [ -d "$old_test_dir" ]; then
  mkdir -p "$(dirname "$new_test_dir")"
  git mv "$old_test_dir" "$new_test_dir" 2>/dev/null || mv "$old_test_dir" "$new_test_dir"
fi

# Файли класів
while IFS= read -r f; do
  base="$(basename "$f")"
  [[ "$base" == ServiceTemplate* ]] || continue
  target="$(dirname "$f")/${base/ServiceTemplate/$class_base}"
  git mv "$f" "$target" 2>/dev/null || mv "$f" "$target"
done < <(find src -name 'ServiceTemplate*.java')

# Вміст: пакет, класи, артефакт, ім'я застосунку
while IFS= read -r f; do
  perl -pi -e "
    s/ua\.kite\.servicetemplate/ua.kite.$pkg_leaf/g;
    s/\bservicetemplate\b/$pkg_leaf/g;
    s/ServiceTemplateApplication/${class_base}Application/g;
    s/service-template/$name/g;
    s/ServiceTemplate/$class_base/g;
  " "$f"
done < <(find . -type f \
  \( -name '*.java' -o -name '*.xml' -o -name '*.yml' -o -name '*.yaml' -o -name '*.md' \) \
  -not -path './target/*' -not -path './.git/*')

echo
echo "Готово. Далі:"
echo "  make lint test   # перевірити, що нічого не поїхало"
echo "  git add -A && git commit -m \"Перейменувати шаблон у $name\""

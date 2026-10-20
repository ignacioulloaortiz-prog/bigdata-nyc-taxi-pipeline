#!/usr/bin/env bash
# Crea un issue por tarea de docs/plan.csv y un tablero GitHub Projects con ellos.
# Requisitos: GitHub CLI (gh) autenticado con permiso de proyectos:
#   gh auth login && gh auth refresh -s project
# Uso (desde la raíz del repo):  bash docs/crear_kanban_github.sh <usuario_github> <nombre_repo>
set -euo pipefail
OWNER="$1"; REPO="$2"
PROJECT_NUM=$(gh project create --owner "$OWNER" --title "Pipeline Big Data NYC Taxi" --format json --jq .number)
echo "Proyecto #$PROJECT_NUM creado"
for L in "Planificación" "Ingesta" "Procesamiento" "Arquitectura" "Análisis" "Modelamiento" "Repositorio" "Documentación" "Entrega"; do
  gh label create "$L" -R "$OWNER/$REPO" --force >/dev/null
done
tail -n +2 docs/plan.csv | while IFS=, read -r id tarea fase rol responsable inicio fin estado hito; do
  body="**Responsable:** $responsable · **Rol:** $rol
**Inicio:** $inicio · **Fin:** $fin
**Hito:** ${hito:--}"
  url=$(gh issue create -R "$OWNER/$REPO" --title "$id · $tarea" --body "$body" --label "$fase")
  gh project item-add "$PROJECT_NUM" --owner "$OWNER" --url "$url" >/dev/null
  if [ "$estado" = "Hecho" ]; then gh issue close "$url" >/dev/null; fi
  echo "$id -> $url ($estado)"
done
echo "Listo: https://github.com/users/$OWNER/projects/$PROJECT_NUM"

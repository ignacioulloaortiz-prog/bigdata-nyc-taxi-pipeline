"""Uso: python build_notebook.py pipeline_source.py pipeline_bigdata_nyc_taxi.ipynb
Convierte un .py con celdas '# %%' en un notebook .ipynb."""
import sys, nbformat
src, dst = sys.argv[1], sys.argv[2]
cells, cur, kind = [], [], None
def flush():
    if kind is None: return
    text = "\n".join(cur).strip("\n")
    if kind == "md":
        text = "\n".join(l[2:] if l.startswith("# ") else l.lstrip("#") for l in text.splitlines())
        cells.append(nbformat.v4.new_markdown_cell(text))
    else:
        cells.append(nbformat.v4.new_code_cell(text))
for line in open(src, encoding="utf-8").read().splitlines():
    if line.startswith("# %% [markdown]"):
        flush(); cur, kind = [], "md"
    elif line.startswith("# %%"):
        flush(); cur, kind = [], "code"
    else:
        cur.append(line)
flush()
nb = nbformat.v4.new_notebook(cells=cells)
nb.metadata = {"kernelspec": {"name": "python3", "display_name": "Python 3", "language": "python"},
               "language_info": {"name": "python"}, "colab": {"provenance": []}}
nbformat.write(nb, dst)
print(len(cells), "celdas ->", dst)

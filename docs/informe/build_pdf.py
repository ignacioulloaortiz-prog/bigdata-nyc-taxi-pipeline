"""Genera el PDF del informe:  python build_pdf.py https://github.com/<usuario>/<repo> [salida.pdf]"""
import sys, asyncio
from pathlib import Path
from playwright.async_api import async_playwright

HERE = Path(__file__).resolve().parent
url = sys.argv[1] if len(sys.argv) > 1 else "https://github.com/ignacioulloaortiz-prog/bigdata-nyc-taxi-pipeline"
out = Path(sys.argv[2] if len(sys.argv) > 2 else HERE / "IUlloa_MVargas.pdf").resolve()
html = (HERE / "informe.html").read_text(encoding="utf-8").replace("{{GITHUB_URL}}", url)
tmp = HERE / "_render.html"; tmp.write_text(html, encoding="utf-8")

async def main():
    async with async_playwright() as p:
        b = await p.chromium.launch()
        pg = await b.new_page()
        await pg.goto(tmp.as_uri()); await pg.wait_for_load_state("networkidle")
        await pg.pdf(path=str(out), format="A4", print_background=True, display_header_footer=True,
                     header_template="<span></span>",
                     footer_template='<div style="font-size:8px;color:#6b7280;width:100%;padding:0 16mm;display:flex;justify-content:space-between">'
                                     '<span>Big Data TI3025 · Evaluación 2 · Ignacio Ulloa · Monserratt Vargas</span><span><span class="pageNumber"></span>/<span class="totalPages"></span></span></div>',
                     margin={"top": "16mm", "bottom": "16mm", "left": "16mm", "right": "16mm"})
        await b.close()
asyncio.run(main()); tmp.unlink()
print("PDF:", out)

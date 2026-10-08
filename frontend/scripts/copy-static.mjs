// After `vite build`: copy dist/ into the Python package (src/databricks_cluster_log_analyzer/api/static), so a
// machine without Node can run the UI with only `pip install -e .` and `dbx-log-analyzer ui`.
import { cpSync, existsSync, rmSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const dist = resolve(here, '..', 'dist');
const target = resolve(here, '..', '..', 'src', 'databricks_cluster_log_analyzer', 'api', 'static');
if (!existsSync(resolve(dist, 'index.html'))) {
  console.error(`copy-static: ${dist}/index.html not found; run vite build first`);
  process.exit(1);
}
rmSync(target, { recursive: true, force: true });
cpSync(dist, target, { recursive: true });
console.log(`copy-static: ${dist} -> ${target}`);

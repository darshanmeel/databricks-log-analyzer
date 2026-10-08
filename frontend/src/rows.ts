// A stage's rows in and out, from what Spark recorded: in = rows read from storage + rows read from the shuffle of its
// parent stages; out = rows it wrote to the shuffle, else rows it wrote to a table. Null when the logs have none.

export interface StageRowsIn {
  input_records?: number | null; shuffle_read_records?: number | null; shuffle_write_records?: number | null; output_records?: number | null;
  rdd_scopes?: string[] | null;
}

export interface StageRowsFlow {
  read: number; fromShuffle: number; in: number | null; out: number | null; outTo: 'shuffle' | 'table' | null; grew: number | null;
  /** why it grew: an explode (Generate: explode(), or a MERGE with Change Data Feed writing change rows next to the
   * data), else a join; unknown when the stage's operators are not at hand */
  grewBy: 'explode' | 'join' | null;
}

const n = (v: number | null | undefined) => (typeof v === 'number' && Number.isFinite(v) ? v : null);

export function stageRows(s: StageRowsIn | null | undefined): StageRowsFlow {
  const read = n(s?.input_records) ?? 0;
  const fromShuffle = n(s?.shuffle_read_records) ?? 0;
  const anyIn = n(s?.input_records) !== null || n(s?.shuffle_read_records) !== null;
  const sw = n(s?.shuffle_write_records);
  const ow = n(s?.output_records);
  const out = sw ? sw : ow ? ow : sw ?? ow;
  const outTo = sw ? 'shuffle' : ow ? 'table' : null;
  const inn = anyIn ? read + fromShuffle : null;
  // a join that matched many rows per key (or a cross join) puts out far more rows than came in
  const grew = inn && out && out >= 1.5 * inn ? out / inn : null;
  const scopes = s?.rdd_scopes;
  const grewBy = !grew ? null : scopes ? (scopes.some((x) => /^Generate/.test(x)) ? 'explode' : 'join') : null;
  return { read, fromShuffle, in: inn, out, outTo, grew, grewBy };
}

/** "×2.2" */
export const fmtGrow = (g: number) => `×${g >= 10 ? Math.round(g) : g.toFixed(1)}`;

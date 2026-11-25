// A background re-read of the Timeline fetches only the newest page. Laying
// that page over the rows already loaded, rather than replacing them, is what
// keeps a reader three pages down where they were: new events go on top, a
// row the server changed takes its new copy, a row the new page no longer
// holds inside its own time window is dropped, and everything older than the
// page stays with the cursor that continues it.

type Row = { adapter: string; event_id: string; occurred_at: string };
export type TimelinePage<T extends Row> = { items: T[]; cursor?: string; hasMore: boolean };

const key = (row: Row) => `${row.adapter}|${row.event_id}`;

export function mergeFirstPage<T extends Row>(current: TimelinePage<T>, page: TimelinePage<T>): TimelinePage<T> {
  if (current.items.length === 0 || page.items.length === 0) return page;
  const known = new Map(current.items.map((row) => [key(row), row]));
  // No row in common: more arrived than one page holds, and splicing the old
  // rows under the new would hide the events in between.
  if (!page.items.some((row) => known.has(key(row)))) return page;

  const oldest = Math.min(...page.items.map((row) => Date.parse(row.occurred_at)));
  const inPage = new Set(page.items.map(key));
  // Keep the object the list already renders when the server's copy is the
  // same, so FlatList does not re-render (or re-measure) an unchanged row.
  const head = page.items.map((row) => {
    const old = known.get(key(row));
    return old && JSON.stringify(old) === JSON.stringify(row) ? old : row;
  });
  const tail = current.items.filter((row) => !inPage.has(key(row)) && Date.parse(row.occurred_at) < oldest);
  // A last page is the whole list; nothing older survives it.
  if (tail.length === 0 || !page.hasMore) return { ...page, items: head };
  return { items: [...head, ...tail], cursor: current.cursor, hasMore: current.hasMore };
}

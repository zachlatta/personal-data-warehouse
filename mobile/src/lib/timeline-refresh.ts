// A background re-read of the Timeline fetches only the newest page. Laying
// that page over the rows already loaded, rather than replacing them, is what
// keeps a reader three pages down where they were: new events go on top, a
// row the server changed takes its new copy, a row the new page no longer
// holds inside its own window is dropped, and everything older than the page
// stays with the cursor that continues it.
//
// "Older than the page" is the server's own order, (occurred_at, seq)
// descending (app/internal/server/timeline.go), measured from the page's last
// row: comparing timestamps alone dropped a loaded row that shared the last
// row's timestamp but sat below it, while the kept cursor already pointed past
// it, so it vanished until a pull.

type Row = { adapter: string; event_id: string; occurred_at: string; seq: number };
export type TimelinePage<T extends Row> = { items: T[]; cursor?: string; hasMore: boolean };

const key = (row: Row) => `${row.adapter}|${row.event_id}`;

// True when a sorts after (is older than) b in the server's order.
function olderThan(a: Row, b: Row): boolean {
  const at = Date.parse(a.occurred_at);
  const bt = Date.parse(b.occurred_at);
  return at < bt || (at === bt && a.seq < b.seq);
}

export function mergeFirstPage<T extends Row>(current: TimelinePage<T>, page: TimelinePage<T>): TimelinePage<T> {
  if (current.items.length === 0 || page.items.length === 0) return page;
  const known = new Map(current.items.map((row) => [key(row), row]));
  // No row in common: more arrived than one page holds, and splicing the old
  // rows under the new would hide the events in between.
  if (!page.items.some((row) => known.has(key(row)))) return page;

  const last = page.items[page.items.length - 1];
  const inPage = new Set(page.items.map(key));
  // Keep the object the list already renders when the server's copy is the
  // same, so FlatList does not re-render (or re-measure) an unchanged row.
  const head = page.items.map((row) => {
    const old = known.get(key(row));
    return old && JSON.stringify(old) === JSON.stringify(row) ? old : row;
  });
  const tail = current.items.filter((row) => !inPage.has(key(row)) && olderThan(row, last));
  // A last page is the whole list; nothing older survives it.
  if (tail.length === 0 || !page.hasMore) return { ...page, items: head };
  return { items: [...head, ...tail], cursor: current.cursor, hasMore: current.hasMore };
}

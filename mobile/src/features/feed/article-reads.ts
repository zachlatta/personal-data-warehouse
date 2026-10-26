// Reads of the article under a card, shared by url for this run of the app.
// A card starts its read when a finger lands on it, so the read is under way
// before the tap opens the detail view, and the view shows a finished read
// on its first frame instead of a spinner (on 2026-10-25 the article came
// 155 to 829 ms after the view opened). A failed read is forgotten, so the
// next open tries again.

export type ArticleRead<A> = { article: A; stored: boolean };

export type ArticleReads<A, C> = {
  // The read of url, starting it (with ctx) if none is under way or done.
  read: (url: string, ctx: C) => Promise<ArticleRead<A>>;
  // The finished read of url, if there is one.
  peek: (url: string) => ArticleRead<A> | undefined;
};

export function createArticleReads<A, C>(load: (url: string, ctx: C) => Promise<ArticleRead<A>>, keep = 6): ArticleReads<A, C> {
  const pending = new Map<string, Promise<ArticleRead<A>>>();
  // Finished reads, oldest first; articles carry their photos, so few are kept.
  const done = new Map<string, ArticleRead<A>>();
  return {
    read(url, ctx) {
      const hit = done.get(url);
      if (hit) {
        done.delete(url);
        done.set(url, hit);
        return Promise.resolve(hit);
      }
      let p = pending.get(url);
      if (p) return p;
      p = load(url, ctx);
      pending.set(url, p);
      p.then(
        (r) => {
          pending.delete(url);
          done.set(url, r);
          while (done.size > keep) done.delete(done.keys().next().value as string);
        },
        () => pending.delete(url),
      );
      return p;
    },
    peek: (url) => done.get(url),
  };
}

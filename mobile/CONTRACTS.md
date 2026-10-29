# Phone app product contracts

The app helps its reader discover interesting things, keep reading across
interruptions, and review an agent's proposed work. Its aspiration is to become
the preferred starting point for browsing. Interest and preference require
evidence from the reader; screen time alone cannot establish either.

These contracts describe the experience on Android and iOS. They complement
the warehouse contracts in the root AGENTS.md.

| Promise | Enforcement and verification |
| --- | --- |
| Content never waits for analytics. | The durable event outbox uploads independently of Feed reads. `refresh.test.mjs` exercises a stalled uploader. |
| An unreachable service produces an actionable state. | Both API clients use `requestJson`; reads have an 8-second deadline including the response body. Writes have a 20-second deadline, are never retried automatically, and explain uncertain outcomes. `request.test.mjs` covers stalled headers, stalled bodies, errors, and writes. Screens keep existing content and provide retry controls. |
| A background refresh does not move the card being read. | Away from the top, keep the current cards and their heights; stage the latest page for the new-cards pill. Pull-to-refresh explicitly replaces the page. `refresh.test.mjs` checks ordering and object identity; validate the pill and return position on Android. |
| A local action survives a refresh that beats its upload. | Save and like overlays remain until a server read agrees, and unsent actions replay from the durable outbox after restart. Dismissals pending before or during a read filter its result. `saved-queue.test.mjs` and `refresh.test.mjs` cover these races. |
| Saved reading is prepared immediately. | Saving from the Feed or reader starts article/photo prefetch. The reader shows “Saved offline” only for a stored article. Videos are not downloaded. Validate a saved article after disconnecting the network. |
| Review edits survive leaving and reopening a pending request during the app session. | Email, Slack, and text composers use the same memory-only draft store, keyed by request/mutation and server baseline. A changed baseline invalidates an old local edit; sign-out clears drafts. `review-drafts.test.mjs` checks retention, invalidation, and cleanup. Use Save without sending to persist a draft to the server before closing the app. |
| A decision targets the request on screen, exactly once. | Each request has a separately keyed review screen. The decision holder ignores repeated taps for a held/in-flight request. `undo-decision.test.mjs` covers repeat taps, delayed commitment, Undo, and failures; validate next-request content on Android. A timeout says the result could not be confirmed. |
| Links open their corresponding screen. | `app-links.test.mjs` checks route mapping and native/server declarations. Android App Links also require release-signature verification on the installed phone. |
| Improvements can be judged from real use. | Feed loads, first card paint, article/card opening, interactions, and review paint are recorded through the durable event outbox. Test devices remain excluded from user-facing engagement marts. First-card paint starts at screen mount in JavaScript, not at the native launcher tap. |

## Release checks

Run `npm run typecheck`, `npm run lint`, and `npm test` in `mobile/`, plus the
repository's required `uv run pytest`. Server or catalog changes also require
the Go suite. CI publishes JavaScript changes to the preview channel; a native
fingerprint change requires a new APK.

Validate with an isolated Android package and a fixture API: cached entry,
stalled fetch and retry, a delayed analytics upload, article reading offline,
open/back continuity, edited-review navigation, and sequential decisions with
Undo. Never generate real Feed impressions or approve a live proposal as a test.

Keep private usage evidence and screenshots outside public repository history.
Do not mark perceived content quality, browsing replacement, release-signature
verification, or live latency green based solely on these unit tests.

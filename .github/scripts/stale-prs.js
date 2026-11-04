'use strict';

// Labels and closes pull requests that have had no human activity.
//
// The policy is a pure function, plan(), over normalised PR data, so it can be
// tested without the API (stale-prs.test.js). run() is the adapter that
// .github/workflows/stale_prs.yaml calls through actions/github-script, and the
// CLI entry at the bottom runs the same thing locally as a dry run:
//
//   GITHUB_TOKEN=$(gh auth token) node .github/scripts/stale-prs.js --repo bsv-blockchain/teranode
//
// The local client has no write methods, so the CLI cannot label, comment on
// or close anything.

const DAY_MS = 24 * 60 * 60 * 1000;

const DEFAULTS = Object.freeze({
  staleDays: 90,
  closeDays: 14,
  closeCap: 25,
  staleLabel: 'stale',
  keepOpenLabel: 'keep-open',
  marker: '<!-- stale-prs -->',
});

const TEAM_ASSOCIATIONS = new Set(['OWNER', 'MEMBER', 'COLLABORATOR']);
const EXTERNAL_ASSOCIATIONS = new Set(['CONTRIBUTOR', 'FIRST_TIME_CONTRIBUTOR', 'FIRST_TIMER', 'NONE']);
const TEAM_PERMISSIONS = new Set(['admin', 'maintain', 'write']);

// Timeline events that count as human activity when a User performs them.
// Label changes are deliberately absent.
const ACTIVITY_EVENTS = new Set(['reopened', 'force_pushed', 'ready_for_review']);

function config(overrides) {
  return { ...DEFAULTS, ...(overrides || {}) };
}

function staleMessage(cfg) {
  return (
    `No activity on this PR for ${cfg.staleDays} days. It will be closed in ${cfg.closeDays} days ` +
    'unless there is a new commit, comment or review. ' +
    `Add the \`${cfg.keepOpenLabel}\` label to exempt it.\n\n${cfg.marker}`
  );
}

function closeMessage(cfg) {
  return (
    `Closed after ${cfg.staleDays + cfg.closeDays} days without activity. ` +
    'Reopen it when you pick this back up. If the branch was force-pushed since, open a new PR instead.' +
    `\n\n${cfg.marker}`
  );
}

// ---------------------------------------------------------------------------
// Decision logic (pure)
// ---------------------------------------------------------------------------

function ts(value) {
  if (!value) return null;
  const t = Date.parse(value);
  return Number.isNaN(t) ? null : t;
}

function isHuman(actor) {
  return Boolean(actor && actor.isUser && actor.login);
}

function maxTs(values) {
  let best = null;
  for (const v of values) {
    if (v !== null && (best === null || v > best)) best = v;
  }
  return best;
}

// Latest human activity on the PR, as epoch ms, or null when there is none.
// Counts: PR creation by a human, the head commit date, comments, reviews and
// review comments by User actors, and reopen / force-push / ready-for-review
// events by User actors. Bot actors and this job's own comments never count.
function lastHumanActivity(pr, cfg) {
  const times = [ts(pr.headCommitDate)];
  if (isHuman(pr.author)) times.push(ts(pr.createdAt));
  for (const c of pr.comments) {
    if (isHuman(c.author) && !(c.body || '').includes(cfg.marker)) times.push(ts(c.createdAt));
  }
  for (const r of pr.reviews) {
    if (isHuman(r.author)) times.push(ts(r.submittedAt));
  }
  for (const c of pr.reviewComments) {
    if (isHuman(c.author)) times.push(ts(c.createdAt));
  }
  for (const e of pr.events) {
    if (ACTIVITY_EVENTS.has(e.type) && isHuman(e.actor)) times.push(ts(e.createdAt));
  }
  return maxTs(times);
}

// When the stale label was last added, from the timeline. Any actor counts.
function staleLabeledAt(pr, cfg) {
  return maxTs(pr.events.filter((e) => e.type === 'labeled' && e.label === cfg.staleLabel).map((e) => ts(e.createdAt)));
}

// team | external | exempt, with a reason. Team means the author's
// association is OWNER/MEMBER/COLLABORATOR, or they have write access to the
// repository (authorPermission). The permission check only ever upgrades an
// author to team: it exists because the Actions token can report private org
// members as CONTRIBUTOR.
function classifyAuthor(pr) {
  if (!pr.author || !pr.author.login) return { kind: 'exempt', reason: 'author account missing' };
  if (pr.author.isBot) return { kind: 'exempt', reason: 'bot author' };
  if (!pr.author.isUser) return { kind: 'exempt', reason: 'author is not a user account' };
  if (TEAM_ASSOCIATIONS.has(pr.authorAssociation)) return { kind: 'team', via: pr.authorAssociation };
  if (TEAM_PERMISSIONS.has(pr.authorPermission)) {
    return { kind: 'team', via: `write access, reported ${pr.authorAssociation}`, upgraded: true };
  }
  if (EXTERNAL_ASSOCIATIONS.has(pr.authorAssociation)) return { kind: 'external', via: pr.authorAssociation };
  return { kind: 'exempt', reason: `unknown author association ${pr.authorAssociation || '(none)'}` };
}

// For an external PR: is it waiting on its author? True only when changes
// were requested and the author has not pushed, commented or replied since
// the latest human CHANGES_REQUESTED review.
function waitingOnAuthor(pr) {
  if (pr.reviewDecision !== 'CHANGES_REQUESTED') return { waiting: false, reason: 'waiting on a reviewer' };
  const login = pr.author.login;
  const requestedAt = maxTs(
    pr.reviews
      .filter((r) => r.state === 'CHANGES_REQUESTED' && isHuman(r.author) && r.author.login !== login)
      .map((r) => ts(r.submittedAt)),
  );
  if (requestedAt === null) return { waiting: false, reason: 'changes requested, but the review was not found' };
  const byAuthor = (a) => isHuman(a) && a.login === login;
  const authorSince = [
    ts(pr.headCommitDate),
    ...pr.comments.filter((c) => byAuthor(c.author)).map((c) => ts(c.createdAt)),
    ...pr.reviews.filter((r) => byAuthor(r.author)).map((r) => ts(r.submittedAt)),
    ...pr.reviewComments.filter((c) => byAuthor(c.author)).map((c) => ts(c.createdAt)),
  ].some((t) => t !== null && t > requestedAt);
  if (authorSince) return { waiting: false, reason: 'author replied after changes were requested; waiting on a reviewer' };
  return { waiting: true };
}

// Why this PR must never be labelled or closed, or null.
function exemptionReason(pr, cfg) {
  if (pr.error) return `data error: ${pr.error}`;
  if (pr.labels.includes(cfg.keepOpenLabel)) return `\`${cfg.keepOpenLabel}\` label`;
  if (pr.reviewDecision === 'APPROVED') return 'approved';
  const author = classifyAuthor(pr);
  if (author.kind === 'exempt') return author.reason;
  if (author.kind === 'external') {
    const w = waitingOnAuthor(pr);
    if (!w.waiting) return `external author, ${w.reason}`;
  }
  return null;
}

function days(ms) {
  return Math.floor(ms / DAY_MS);
}

function iso(t) {
  return t === null ? null : new Date(t).toISOString();
}

// Decide one PR. Actions:
//   stale   - add the stale label and post the stale comment
//   close   - post the close comment and close (the branch is never deleted)
//   unstale - remove the stale label, no comment
//   wait    - labelled, close not due yet
//   active  - human activity within staleDays
//   exempt  - never labelled or closed (reason says why)
function decide(pr, now, cfgIn) {
  const cfg = config(cfgIn);
  const author = classifyAuthor(pr);
  const base = {
    number: pr.number,
    title: pr.title,
    author: pr.author ? pr.author.login : null,
    authorKind: author.kind,
    authorVia: author.via || null,
    authorUpgraded: Boolean(author.upgraded),
    authorAssociation: pr.authorAssociation || null,
    authorPermission: pr.authorPermission || null,
    isDraft: Boolean(pr.isDraft),
  };
  const hasStale = pr.labels.includes(cfg.staleLabel);

  const exempt = exemptionReason(pr, cfg);
  if (exempt) {
    // A PR that became exempt after it was labelled (keep-open added, approved)
    // loses the label. Data errors leave everything as it is.
    if (hasStale && !pr.error) return { ...base, action: 'unstale', reason: `exempt: ${exempt}` };
    return { ...base, action: 'exempt', reason: exempt };
  }

  const last = lastHumanActivity(pr, cfg);
  if (last === null) return { ...base, action: 'exempt', reason: 'no human activity dates found' };
  const idleDays = days(now - last);
  const withIdle = { ...base, lastActivity: iso(last), idleDays };

  if (hasStale) {
    const labeledAt = staleLabeledAt(pr, cfg);
    if (labeledAt === null) {
      return { ...withIdle, action: 'exempt', reason: `\`${cfg.staleLabel}\` label present but no label event found` };
    }
    const labeledDays = days(now - labeledAt);
    const out = { ...withIdle, labeledAt: iso(labeledAt), labeledDays };
    if (last > labeledAt) return { ...out, action: 'unstale', reason: 'human activity after the label' };
    if (now - labeledAt >= cfg.closeDays * DAY_MS) {
      return { ...out, action: 'close', reason: `labelled ${labeledDays} days ago, no human activity since` };
    }
    return { ...out, action: 'wait', reason: `labelled ${labeledDays} days ago, closes in ${cfg.closeDays - labeledDays} days` };
  }

  if (now - last >= cfg.staleDays * DAY_MS) {
    return {
      ...withIdle,
      action: 'stale',
      reason: `no human activity for ${idleDays} days`,
      // A dry run never labels, so it reports what the close would be: a PR
      // idle for staleDays + closeDays today is closed closeDays after
      // labelling if nothing happens in between.
      closeAfterLabel: now - last >= (cfg.staleDays + cfg.closeDays) * DAY_MS,
    };
  }
  return { ...withIdle, action: 'active', reason: `human activity ${idleDays} days ago` };
}

// Decide every PR in ascending number order. When closeCap closes have been
// planned the run stops: the remaining PRs are listed as not processed.
function plan(prs, now, cfgIn) {
  const cfg = config(cfgIn);
  const sorted = [...prs].sort((a, b) => a.number - b.number);
  const decisions = [];
  const notProcessed = [];
  let closes = 0;
  let capHit = false;
  for (const pr of sorted) {
    if (capHit) {
      notProcessed.push(pr.number);
      continue;
    }
    const d = decide(pr, now, cfg);
    decisions.push(d);
    if (d.action === 'close') {
      closes += 1;
      if (closes >= cfg.closeCap) capHit = true;
    }
  }
  return { decisions, notProcessed, capHit, config: cfg, now: iso(now) };
}

// ---------------------------------------------------------------------------
// Summary
// ---------------------------------------------------------------------------

function cell(s) {
  return String(s === null || s === undefined ? '' : s).replace(/\|/g, '\\|').replace(/\n/g, ' ');
}

function prRef(d, repo) {
  return `[${repo}#${d.number}](https://github.com/${repo}/pull/${d.number})`;
}

function table(header, rows) {
  if (rows.length === 0) return '_None._\n';
  const head = `| ${header.join(' | ')} |\n|${header.map(() => ' --- ').join('|')}|\n`;
  return head + rows.map((r) => `| ${r.map(cell).join(' | ')} |`).join('\n') + '\n';
}

function renderSummary(result, opts) {
  const { live, repo, writes = [], failures = [], permissionErrors = [], permissionLookups = 0 } = opts;
  const cfg = result.config;
  const by = (a) => result.decisions.filter((d) => d.action === a);
  const date = (s) => (s ? s.slice(0, 10) : '');
  const who = (d) => `${d.author || '(none)'} (${d.authorKind}${d.authorVia ? ` via ${d.authorVia}` : ''})${d.isDraft ? ', draft' : ''}`;
  const out = [];

  out.push(`## Stale PRs: ${live ? 'live run' : 'dry run'}, ${date(result.now)}\n`);
  out.push(
    live
      ? 'Labels, comments and closes below were applied.\n'
      : 'Dry run: nothing was labelled, commented on or closed. Writes need the repository variable `STALE_PRS_ENABLED=true` and a non-dry run.\n',
  );
  out.push(
    `Stale after ${cfg.staleDays} days without human activity; closed ${cfg.closeDays} days after the \`${cfg.staleLabel}\` label ` +
      `if nothing happens. At most ${cfg.closeCap} closes per run.\n`,
  );

  const stale = by('stale');
  out.push(`### ${live ? 'Labelled' : 'Would label'} \`${cfg.staleLabel}\` now (${stale.length})\n`);
  out.push(
    table(
      ['PR', 'Author', 'Last human activity', 'Idle days', `Would close ${cfg.closeDays} days after labelling, if nothing happens`],
      stale.map((d) => [prRef(d, repo), who(d), date(d.lastActivity), d.idleDays, d.closeAfterLabel ? 'yes' : `no (idle < ${cfg.staleDays + cfg.closeDays} days)`]),
    ),
  );

  const close = by('close');
  out.push(`### ${live ? 'Closed' : 'Would close'} (${close.length})\n`);
  out.push(
    table(
      ['PR', 'Author', 'Labelled', 'Last human activity'],
      close.map((d) => [prRef(d, repo), who(d), date(d.labeledAt), date(d.lastActivity)]),
    ),
  );

  const unstale = by('unstale');
  out.push(`### ${live ? 'Un-staled' : 'Would un-stale'} (${unstale.length})\n`);
  out.push(table(['PR', 'Author', 'Reason'], unstale.map((d) => [prRef(d, repo), who(d), d.reason])));

  const wait = by('wait');
  out.push(`### Labelled, close not due yet (${wait.length})\n`);
  out.push(table(['PR', 'Author', 'Reason'], wait.map((d) => [prRef(d, repo), who(d), d.reason])));

  const exempt = by('exempt');
  out.push(`### Exempt (${exempt.length})\n`);
  out.push(table(['PR', 'Author', 'Reason'], exempt.map((d) => [prRef(d, repo), who(d), d.reason])));

  out.push(`### Active (${by('active').length})\n`);
  out.push(`Human activity within ${cfg.staleDays} days; nothing to do.\n`);

  const upgraded = new Set(result.decisions.filter((d) => d.authorUpgraded).map((d) => d.author));
  out.push(
    `### Author classification\n\n${permissionLookups} repository permission lookups for authors not reported as ` +
      `OWNER/MEMBER/COLLABORATOR; ${upgraded.size} upgraded to team by write access; ${permissionErrors.length} failed.\n`,
  );

  if (result.capHit) {
    out.push(
      `### Close cap reached\n\nStopped after ${cfg.closeCap} closes. ${result.notProcessed.length} PRs not processed this run: ` +
        `${result.notProcessed.map((n) => `${repo}#${n}`).join(', ')}.\n`,
    );
  }
  if (permissionErrors.length) {
    out.push(
      `### Permission lookups failed (${permissionErrors.length})\n\n` +
        'These authors were classified by `authorAssociation` alone:\n\n' +
        permissionErrors.map((e) => `- ${e.login}: ${e.error}`).join('\n') +
        '\n',
    );
  }
  if (live) {
    out.push(`### Writes (${writes.length})\n`);
    out.push(writes.length ? writes.map((w) => `- ${w}`).join('\n') + '\n' : '_None._\n');
  }
  if (failures.length) {
    out.push(`### Failures (${failures.length})\n`);
    out.push(failures.map((f) => `- ${f}`).join('\n') + '\n');
  }
  return out.join('\n');
}

// ---------------------------------------------------------------------------
// GitHub adapter
// ---------------------------------------------------------------------------

const ACTOR = '__typename login';
const PRS_QUERY = `
query($owner: String!, $repo: String!, $after: String) {
  repository(owner: $owner, name: $repo) {
    pullRequests(states: OPEN, first: 25, after: $after, orderBy: {field: CREATED_AT, direction: ASC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        number title isDraft createdAt authorAssociation reviewDecision
        author { ${ACTOR} }
        labels(first: 50) { nodes { name } }
        commits(last: 1) { nodes { commit { committedDate } } }
        comments(last: 50) { nodes { createdAt body author { ${ACTOR} } } }
        reviews(last: 50) { nodes { state submittedAt author { ${ACTOR} } } }
        reviewThreads(last: 30) { nodes { comments(last: 10) { nodes { createdAt author { ${ACTOR} } } } } }
        timelineItems(last: 50, itemTypes: [LABELED_EVENT, UNLABELED_EVENT, REOPENED_EVENT, HEAD_REF_FORCE_PUSHED_EVENT, READY_FOR_REVIEW_EVENT]) {
          nodes {
            __typename
            ... on LabeledEvent { createdAt label { name } actor { ${ACTOR} } }
            ... on UnlabeledEvent { createdAt label { name } actor { ${ACTOR} } }
            ... on ReopenedEvent { createdAt actor { ${ACTOR} } }
            ... on HeadRefForcePushedEvent { createdAt actor { ${ACTOR} } }
            ... on ReadyForReviewEvent { createdAt actor { ${ACTOR} } }
          }
        }
      }
    }
  }
}`;

const EVENT_TYPES = {
  LabeledEvent: 'labeled',
  UnlabeledEvent: 'unlabeled',
  ReopenedEvent: 'reopened',
  HeadRefForcePushedEvent: 'force_pushed',
  ReadyForReviewEvent: 'ready_for_review',
};

function actor(a) {
  if (!a) return null;
  return { login: a.login, isUser: a.__typename === 'User', isBot: a.__typename === 'Bot' };
}

function nodes(conn) {
  return (conn && conn.nodes ? conn.nodes : []).filter(Boolean);
}

// GraphQL PR node -> the shape decide() reads.
function normalize(node) {
  const head = nodes(node.commits)[0];
  return {
    number: node.number,
    title: node.title,
    isDraft: node.isDraft,
    createdAt: node.createdAt,
    author: actor(node.author),
    authorAssociation: node.authorAssociation,
    authorPermission: null,
    reviewDecision: node.reviewDecision,
    labels: nodes(node.labels).map((l) => l.name),
    headCommitDate: head && head.commit ? head.commit.committedDate : null,
    comments: nodes(node.comments).map((c) => ({ createdAt: c.createdAt, body: c.body, author: actor(c.author) })),
    reviews: nodes(node.reviews).map((r) => ({ state: r.state, submittedAt: r.submittedAt, author: actor(r.author) })),
    reviewComments: nodes(node.reviewThreads).flatMap((t) =>
      nodes(t.comments).map((c) => ({ createdAt: c.createdAt, author: actor(c.author) })),
    ),
    events: nodes(node.timelineItems)
      .filter((e) => EVENT_TYPES[e.__typename])
      .map((e) => ({
        type: EVENT_TYPES[e.__typename],
        label: e.label ? e.label.name : null,
        createdAt: e.createdAt,
        actor: actor(e.actor),
      })),
  };
}

async function fetchOpenPRs(github, owner, repo) {
  const prs = [];
  let after = null;
  for (;;) {
    const res = await github.graphql(PRS_QUERY, { owner, repo, after });
    const conn = res.repository.pullRequests;
    prs.push(...nodes(conn).map(normalize));
    if (!conn.pageInfo.hasNextPage) return prs;
    after = conn.pageInfo.endCursor;
  }
}

// Look up repository permission for authors whose association does not
// already make them team. Failures are recorded, never fatal.
async function resolvePermissions(github, owner, repo, prs) {
  const cache = new Map();
  const errors = [];
  for (const pr of prs) {
    if (!pr.author || !pr.author.isUser || TEAM_ASSOCIATIONS.has(pr.authorAssociation)) continue;
    const login = pr.author.login;
    if (!cache.has(login)) {
      try {
        const { data } = await github.rest.repos.getCollaboratorPermissionLevel({ owner, repo, username: login });
        cache.set(login, data.permission || null);
      } catch (err) {
        cache.set(login, null);
        errors.push({ login, error: `${err.status || ''} ${err.message}`.trim() });
      }
    }
    pr.authorPermission = cache.get(login);
  }
  return { errors, lookups: cache.size };
}

async function ensureLabels(github, owner, repo, cfg) {
  const want = [
    { name: cfg.staleLabel, color: 'cfd3d7', description: `No human activity for ${cfg.staleDays} days` },
    { name: cfg.keepOpenLabel, color: '0e8a16', description: 'Exempt from the stale PR job' },
  ];
  for (const label of want) {
    try {
      await github.rest.issues.getLabel({ owner, repo, name: label.name });
    } catch (err) {
      if (err.status !== 404) throw err;
      await github.rest.issues.createLabel({ owner, repo, ...label });
    }
  }
}

async function apply(github, owner, repo, d, cfg) {
  const issue_number = d.number;
  const ref = `${owner}/${repo}#${d.number}`;
  if (d.action === 'stale') {
    await github.rest.issues.addLabels({ owner, repo, issue_number, labels: [cfg.staleLabel] });
    await github.rest.issues.createComment({ owner, repo, issue_number, body: staleMessage(cfg) });
    return `labelled ${ref}`;
  }
  if (d.action === 'close') {
    await github.rest.issues.createComment({ owner, repo, issue_number, body: closeMessage(cfg) });
    await github.rest.pulls.update({ owner, repo, pull_number: d.number, state: 'closed' });
    return `closed ${ref}`;
  }
  if (d.action === 'unstale') {
    await github.rest.issues.removeLabel({ owner, repo, issue_number, name: cfg.staleLabel });
    return `un-staled ${ref}`;
  }
  return null;
}

// Entry point for actions/github-script. Writes only when live is true.
async function run({ github, context, core, live = false, now = Date.now(), cfg: cfgIn, log = console.log }) {
  const cfg = config(cfgIn);
  const { owner, repo } = context.repo;
  const full = `${owner}/${repo}`;

  const prs = await fetchOpenPRs(github, owner, repo);
  const { errors: permissionErrors, lookups: permissionLookups } = await resolvePermissions(github, owner, repo, prs);
  const result = plan(prs, now, cfg);

  const writes = [];
  const failures = [];
  if (live) {
    await ensureLabels(github, owner, repo, cfg);
    for (const d of result.decisions) {
      try {
        const w = await apply(github, owner, repo, d, cfg);
        if (w) writes.push(w);
      } catch (err) {
        failures.push(`${full}#${d.number} (${d.action}): ${err.status || ''} ${err.message}`.trim());
      }
    }
  }

  const md = renderSummary(result, { live, repo: full, writes, failures, permissionErrors, permissionLookups });
  log(md);
  if (core && core.summary) await core.summary.addRaw(md).write();
  if (core && failures.length) core.setFailed(`${failures.length} write(s) failed; see the job summary`);
  return { result, writes, failures, permissionErrors, markdown: md };
}

// ---------------------------------------------------------------------------
// Local dry run
// ---------------------------------------------------------------------------

// A client with a GraphQL query method and one REST read, and nothing else.
// run() would throw on any write, and the CLI never passes live: true.
function readOnlyClient(token) {
  const headers = {
    Authorization: `bearer ${token}`,
    Accept: 'application/vnd.github+json',
    'User-Agent': 'stale-prs-dry-run',
    'X-GitHub-Api-Version': '2022-11-28',
  };
  const counts = { graphqlQueries: 0, restGets: 0 };
  async function check(res) {
    if (res.ok) return res.json();
    const err = new Error(`${res.status} ${await res.text()}`);
    err.status = res.status;
    throw err;
  }
  return {
    counts,
    async graphql(query, variables) {
      if (/^\s*mutation\b/.test(query)) throw new Error('read-only client: mutations are not allowed');
      counts.graphqlQueries += 1;
      const body = await check(
        await fetch('https://api.github.com/graphql', { method: 'POST', headers, body: JSON.stringify({ query, variables }) }),
      );
      if (body.errors) throw new Error(JSON.stringify(body.errors));
      return body.data;
    },
    rest: {
      repos: {
        async getCollaboratorPermissionLevel({ owner, repo, username }) {
          counts.restGets += 1;
          const url = `https://api.github.com/repos/${owner}/${repo}/collaborators/${encodeURIComponent(username)}/permission`;
          return { data: await check(await fetch(url, { method: 'GET', headers })) };
        },
      },
    },
  };
}

async function main(argv) {
  const args = { repo: 'bsv-blockchain/teranode', now: null, json: null };
  for (let i = 0; i < argv.length; i += 1) {
    if (argv[i] === '--repo') args.repo = argv[++i];
    else if (argv[i] === '--now') args.now = argv[++i];
    else if (argv[i] === '--json') args.json = argv[++i];
    else throw new Error(`unknown argument ${argv[i]}`);
  }
  const token = process.env.GITHUB_TOKEN;
  if (!token) throw new Error('set GITHUB_TOKEN, e.g. GITHUB_TOKEN=$(gh auth token)');
  const [owner, repo] = args.repo.split('/');
  const github = readOnlyClient(token);
  const now = args.now ? Date.parse(args.now) : Date.now();
  const out = await run({ github, context: { repo: { owner, repo } }, core: null, live: false, now });
  if (args.json) require('node:fs').writeFileSync(args.json, JSON.stringify(out.result, null, 2));
  console.log(
    `\nRequests: ${github.counts.graphqlQueries} GraphQL queries, ${github.counts.restGets} REST GETs, 0 writes (the client has no write methods).`,
  );
}

if (require.main === module) {
  main(process.argv.slice(2)).catch((err) => {
    console.error(err.message);
    process.exit(1);
  });
}

module.exports = {
  DEFAULTS,
  config,
  staleMessage,
  closeMessage,
  lastHumanActivity,
  classifyAuthor,
  waitingOnAuthor,
  decide,
  plan,
  renderSummary,
  normalize,
  run,
};

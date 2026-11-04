'use strict';

// Run with: node --test .github/scripts/stale-prs.test.js

const test = require('node:test');
const assert = require('node:assert/strict');

const { decide, plan, renderSummary, normalize, run, DEFAULTS } = require('./stale-prs.js');

const DAY = 24 * 60 * 60 * 1000;
const NOW = Date.parse('2026-11-04T06:00:00Z');
const ago = (days) => new Date(NOW - days * DAY).toISOString();

const user = (login) => ({ login, isUser: true, isBot: false });
const bot = (login) => ({ login, isUser: false, isBot: true });

const AUTHOR = 'alice';
const REVIEWER = 'bob';

function pr(overrides = {}) {
  return {
    number: 1,
    title: 'test PR',
    isDraft: false,
    createdAt: ago(200),
    author: user(AUTHOR),
    authorAssociation: 'MEMBER',
    authorPermission: null,
    reviewDecision: null,
    labels: [],
    headCommitDate: ago(200),
    comments: [],
    reviews: [],
    reviewComments: [],
    events: [],
    ...overrides,
  };
}

const staleLabel = (daysAgo, actor = bot('github-actions')) => ({ type: 'labeled', label: 'stale', createdAt: ago(daysAgo), actor });
const comment = (author, daysAgo, body = 'a comment') => ({ author, createdAt: ago(daysAgo), body });
const review = (author, state, daysAgo) => ({ author, state, submittedAt: ago(daysAgo) });

// Member PRs

test('member PR idle 90 days is labelled stale', () => {
  const d = decide(pr({ createdAt: ago(95), headCommitDate: ago(90) }), NOW);
  assert.equal(d.action, 'stale');
  assert.equal(d.idleDays, 90);
  assert.equal(d.pastCloseThreshold, false);
  assert.equal(d.closesOn, ago(-14));
});

test('member PR idle 89 days is active', () => {
  assert.equal(decide(pr({ createdAt: ago(95), headCommitDate: ago(89) }), NOW).action, 'active');
});

test('member PR idle 104 days is reported as past the close threshold', () => {
  const d = decide(pr({ createdAt: ago(104), headCommitDate: ago(104) }), NOW);
  assert.equal(d.action, 'stale');
  assert.equal(d.pastCloseThreshold, true);
});

test('drafts are in scope', () => {
  assert.equal(decide(pr({ isDraft: true }), NOW).action, 'stale');
});

test('member PR labelled 14 days ago with no activity since is closed', () => {
  const d = decide(pr({ labels: ['stale'], events: [staleLabel(14)] }), NOW);
  assert.equal(d.action, 'close');
});

test('member PR labelled 13 days ago waits', () => {
  const d = decide(pr({ labels: ['stale'], events: [staleLabel(13)] }), NOW);
  assert.equal(d.action, 'wait');
  assert.match(d.reason, /closes in 1 days/);
});

test('human comment after the label un-stales', () => {
  const d = decide(pr({ labels: ['stale'], events: [staleLabel(20)], comments: [comment(user('carol'), 5)] }), NOW);
  assert.equal(d.action, 'unstale');
});

test('new commit after the label un-stales', () => {
  const d = decide(pr({ labels: ['stale'], events: [staleLabel(20)], headCommitDate: ago(1) }), NOW);
  assert.equal(d.action, 'unstale');
});

test('human review after the label un-stales', () => {
  const d = decide(pr({ labels: ['stale'], events: [staleLabel(20)], reviews: [review(user(REVIEWER), 'COMMENTED', 2)] }), NOW);
  assert.equal(d.action, 'unstale');
});

test('human review comment after the label un-stales', () => {
  const d = decide(pr({ labels: ['stale'], events: [staleLabel(20)], reviewComments: [{ author: user(REVIEWER), createdAt: ago(2) }] }), NOW);
  assert.equal(d.action, 'unstale');
});

test('reopening a closed PR un-stales it instead of closing it again', () => {
  const d = decide(
    pr({ labels: ['stale'], events: [staleLabel(30), { type: 'reopened', createdAt: ago(1), actor: user(AUTHOR) }] }),
    NOW,
  );
  assert.equal(d.action, 'unstale');
});

test('opening a PR counts as activity even when its commits are old', () => {
  assert.equal(decide(pr({ createdAt: ago(1), headCommitDate: ago(300) }), NOW).action, 'active');
});

// Bot activity

test('bot comments, bot reviews and bot events do not reset the clock', () => {
  const d = decide(
    pr({
      comments: [comment(bot('sonarqubecloud'), 1), comment(bot('github-actions'), 2)],
      reviews: [review(bot('claude'), 'COMMENTED', 1)],
      reviewComments: [{ author: bot('copilot'), createdAt: ago(1) }],
      events: [{ type: 'force_pushed', createdAt: ago(1), actor: bot('renovate') }],
    }),
    NOW,
  );
  assert.equal(d.action, 'stale');
  assert.equal(d.idleDays, 200);
});

test('bot comment after the label does not un-stale', () => {
  const d = decide(pr({ labels: ['stale'], events: [staleLabel(20)], comments: [comment(bot('sonarqubecloud'), 1)] }), NOW);
  assert.equal(d.action, 'close');
});

test("the job's own comment does not reset the clock even if posted by a user token", () => {
  const d = decide(
    pr({ labels: ['stale'], events: [staleLabel(20)], comments: [comment(user('someone'), 19, `No activity...\n\n${DEFAULTS.marker}`)] }),
    NOW,
  );
  assert.equal(d.action, 'close');
});

test('label changes do not count as activity', () => {
  const d = decide(
    pr({ events: [{ type: 'labeled', label: 'needs-review', createdAt: ago(1), actor: user(REVIEWER) }] }),
    NOW,
  );
  assert.equal(d.action, 'stale');
});

// Exemptions

test('approved PRs are exempt', () => {
  const d = decide(pr({ reviewDecision: 'APPROVED' }), NOW);
  assert.equal(d.action, 'exempt');
  assert.equal(d.reason, 'approved');
});

test('keep-open PRs are exempt', () => {
  const d = decide(pr({ labels: ['keep-open'] }), NOW);
  assert.equal(d.action, 'exempt');
  assert.match(d.reason, /keep-open/);
});

test('keep-open added after the stale label removes the stale label', () => {
  const d = decide(pr({ labels: ['stale', 'keep-open'], events: [staleLabel(20)] }), NOW);
  assert.equal(d.action, 'unstale');
  assert.match(d.reason, /^exempt: /);
});

test('external PR waiting on a reviewer is exempt', () => {
  const d = decide(pr({ authorAssociation: 'CONTRIBUTOR' }), NOW);
  assert.equal(d.action, 'exempt');
  assert.match(d.reason, /waiting on a reviewer/);
});

test('external PR whose author replied after changes were requested is exempt', () => {
  const d = decide(
    pr({
      authorAssociation: 'CONTRIBUTOR',
      reviewDecision: 'CHANGES_REQUESTED',
      reviews: [review(user(REVIEWER), 'CHANGES_REQUESTED', 150)],
      comments: [comment(user(AUTHOR), 140)],
    }),
    NOW,
  );
  assert.equal(d.action, 'exempt');
  assert.match(d.reason, /author replied/);
});

test('external PR whose author pushed after changes were requested is exempt', () => {
  const d = decide(
    pr({
      authorAssociation: 'FIRST_TIME_CONTRIBUTOR',
      reviewDecision: 'CHANGES_REQUESTED',
      reviews: [review(user(REVIEWER), 'CHANGES_REQUESTED', 150)],
      headCommitDate: ago(140),
    }),
    NOW,
  );
  assert.equal(d.action, 'exempt');
});

test('external PR waiting on its author goes stale and then closes', () => {
  const waiting = {
    authorAssociation: 'CONTRIBUTOR',
    reviewDecision: 'CHANGES_REQUESTED',
    reviews: [review(user(REVIEWER), 'CHANGES_REQUESTED', 120)],
  };
  assert.equal(decide(pr(waiting), NOW).action, 'stale');
  assert.equal(decide(pr({ ...waiting, labels: ['stale'], events: [staleLabel(15)] }), NOW).action, 'close');
});

test('external PR with CHANGES_REQUESTED but no visible review is exempt', () => {
  const d = decide(pr({ authorAssociation: 'NONE', reviewDecision: 'CHANGES_REQUESTED' }), NOW);
  assert.equal(d.action, 'exempt');
  assert.match(d.reason, /review was not found/);
});

test('write access makes a CONTRIBUTOR-reported author team', () => {
  const d = decide(pr({ authorAssociation: 'CONTRIBUTOR', authorPermission: 'admin' }), NOW);
  assert.equal(d.authorKind, 'team');
  assert.equal(d.action, 'stale');
});

test('read access does not make an author team', () => {
  const d = decide(pr({ authorAssociation: 'CONTRIBUTOR', authorPermission: 'read' }), NOW);
  assert.equal(d.authorKind, 'external');
  assert.equal(d.action, 'exempt');
});

// Fail safe

test('unknown association is exempt', () => {
  const d = decide(pr({ authorAssociation: 'MANNEQUIN' }), NOW);
  assert.equal(d.action, 'exempt');
  assert.match(d.reason, /unknown author association/);
});

test('missing author is exempt', () => {
  assert.equal(decide(pr({ author: null }), NOW).action, 'exempt');
});

test('bot author is exempt', () => {
  assert.equal(decide(pr({ author: bot('dependabot') }), NOW).action, 'exempt');
});

test('no activity dates at all is exempt', () => {
  assert.equal(decide(pr({ author: user(AUTHOR), createdAt: null, headCommitDate: null }), NOW).action, 'exempt');
});

test('stale label without a label event is never closed', () => {
  const d = decide(pr({ labels: ['stale'] }), NOW);
  assert.equal(d.action, 'exempt');
  assert.match(d.reason, /no label event/);
});

test('a PR with a data error is exempt and keeps its labels', () => {
  const d = decide(pr({ labels: ['stale'], events: [staleLabel(20)], error: 'fetch failed' }), NOW);
  assert.equal(d.action, 'exempt');
});

// Close cap

test('the run stops once the close cap is reached', () => {
  const prs = Array.from({ length: 30 }, (_, i) => pr({ number: i + 1, labels: ['stale'], events: [staleLabel(20)] }));
  const result = plan(prs, NOW, { closeCap: 25 });
  assert.equal(result.decisions.filter((d) => d.action === 'close').length, 25);
  assert.equal(result.capHit, true);
  assert.deepEqual(result.notProcessed, [26, 27, 28, 29, 30]);
  assert.match(renderSummary(result, { live: false, repo: 'o/r' }), /Close cap reached/);
});

test('the cap does not trigger below the limit', () => {
  const prs = Array.from({ length: 3 }, (_, i) => pr({ number: i + 1, labels: ['stale'], events: [staleLabel(20)] }));
  const result = plan(prs, NOW, { closeCap: 25 });
  assert.equal(result.capHit, false);
  assert.deepEqual(result.notProcessed, []);
});

// Adapter

function graphqlNode(overrides = {}) {
  return {
    number: 7,
    title: 'old work',
    isDraft: false,
    createdAt: ago(200),
    authorAssociation: 'CONTRIBUTOR',
    reviewDecision: null,
    author: { __typename: 'User', login: AUTHOR },
    labels: { nodes: [] },
    commits: { nodes: [{ commit: { committedDate: ago(200) } }] },
    comments: { nodes: [{ createdAt: ago(1), body: 'ping', author: { __typename: 'Bot', login: 'sonarqubecloud' } }] },
    reviews: { nodes: [] },
    reviewThreads: { nodes: [] },
    timelineItems: { nodes: [{ __typename: 'LabeledEvent', createdAt: ago(1), label: { name: 'x' }, actor: { __typename: 'User', login: REVIEWER } }] },
    ...overrides,
  };
}

test('normalize maps GraphQL actor types', () => {
  const n = normalize(graphqlNode());
  assert.equal(n.author.isUser, true);
  assert.equal(n.comments[0].author.isBot, true);
  assert.equal(n.events[0].type, 'labeled');
  assert.equal(n.headCommitDate, ago(200));
});

function fakeGithub({ node, permission, permissionError }) {
  const writes = [];
  const record = (name) => async (args) => {
    writes.push({ name, args });
    if (name === 'getLabel') {
      const err = new Error('Not Found');
      err.status = 404;
      throw err;
    }
    return { data: {} };
  };
  return {
    writes,
    async graphql() {
      return { repository: { pullRequests: { pageInfo: { hasNextPage: false, endCursor: null }, nodes: [node] } } };
    },
    rest: {
      repos: {
        async getCollaboratorPermissionLevel() {
          if (permissionError) {
            const err = new Error('Resource not accessible by integration');
            err.status = 403;
            throw err;
          }
          return { data: { permission } };
        },
      },
      issues: {
        getLabel: record('getLabel'),
        createLabel: record('createLabel'),
        addLabels: record('addLabels'),
        createComment: record('createComment'),
        removeLabel: record('removeLabel'),
      },
      pulls: { update: record('update') },
    },
  };
}

const context = { repo: { owner: 'o', repo: 'r' } };

test('a dry run makes no write calls', async () => {
  const github = fakeGithub({ node: graphqlNode(), permission: 'write' });
  const out = await run({ github, context, core: null, live: false, now: NOW, log: () => {} });
  assert.equal(out.result.decisions[0].action, 'stale');
  assert.deepEqual(github.writes, []);
  assert.match(out.markdown, /1 repository permission lookups .*; 1 upgraded to team by write access; 0 failed/);
  assert.match(out.markdown, /alice \(team via write access, reported CONTRIBUTOR\)/);
});

test('a live run labels and comments, and creates missing labels', async () => {
  const github = fakeGithub({ node: graphqlNode(), permission: 'write' });
  const out = await run({ github, context, core: null, live: true, now: NOW, log: () => {} });
  const names = github.writes.map((w) => w.name);
  assert.deepEqual(names, ['getLabel', 'createLabel', 'getLabel', 'createLabel', 'addLabels', 'createComment']);
  assert.match(github.writes[5].args.body, /No activity on this PR for 90 days/);
  assert.deepEqual(out.writes, ['labelled o/r#7']);
});

test('a live close comments then closes, and never touches the branch', async () => {
  const node = graphqlNode({
    authorAssociation: 'MEMBER',
    labels: { nodes: [{ name: 'stale' }] },
    timelineItems: { nodes: [{ __typename: 'LabeledEvent', createdAt: ago(15), label: { name: 'stale' }, actor: { __typename: 'Bot', login: 'github-actions' } }] },
  });
  const github = fakeGithub({ node });
  await run({ github, context, core: null, live: true, now: NOW, log: () => {} });
  const tail = github.writes.slice(-2);
  assert.equal(tail[0].name, 'createComment');
  assert.match(tail[0].args.body, /Closed after 200 days without activity/);
  assert.equal(tail[1].name, 'update');
  assert.deepEqual(tail[1].args, { owner: 'o', repo: 'r', pull_number: 7, state: 'closed' });
});

test('a failed permission lookup falls back to authorAssociation and is reported', async () => {
  const github = fakeGithub({ node: graphqlNode(), permissionError: true });
  const out = await run({ github, context, core: null, live: false, now: NOW, log: () => {} });
  assert.equal(out.result.decisions[0].action, 'exempt');
  assert.equal(out.permissionErrors.length, 1);
  assert.match(out.markdown, /Permission lookups failed/);
});

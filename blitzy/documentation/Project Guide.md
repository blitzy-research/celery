# Blitzy Project Guide

**Project:** Celery — Cooperative Scheduling Fix for Pooled Broker-Resource Acquisition under gevent
**Upstream Issue:** [celery/celery#10044](https://github.com/celery/celery/issues/10044)
**Branch:** `blitzy-b7a40fa5-bacb-4d24-98e3-1121c4646aac` @ `1b0f44195` · **Pristine base:** `8f28367e4`
**Repository:** celery/celery 5.6.2 (`recovery`)

---

## 1. Executive Summary

### 1.1 Project Overview

Celery is a distributed task-queue framework used by Python services to publish and execute background work. Under the gevent execution pool, every step of a warm pooled `apply_async()`/`delay()` publish is non-blocking, so a publishing greenlet never switches to the gevent hub and starves every peer greenlet — tasks that should interleave run sequentially, and a greenlet's own `gevent.Timeout` never fires. This project restores exactly one cooperative scheduling point per pooled broker-resource acquisition. Target users are all Celery operators running the gevent pool, which is the affected configuration by default (`broker_pool_limit=10`). Technical scope is deliberately minimal: one new 45-line helper module and eleven inserted lines in one existing module, with no public API, configuration, dependency, wire-format or routing change.

### 1.2 Completion Status

```mermaid
pie showData title Project Completion — 78.1% Complete
    "Completed (118 h)" : 118
    "Remaining (33 h)" : 33
```

<table>
<tr><th align="left">Metric</th><th align="right">Value</th></tr>
<tr><td><b>Total Hours</b></td><td align="right"><b>151</b></td></tr>
<tr><td>Completed Hours (AI + Manual)</td><td align="right">118 &nbsp;<sub>(118 AI · 0 Manual)</sub></td></tr>
<tr><td>Remaining Hours</td><td align="right">33</td></tr>
<tr><td><b>Percent Complete</b></td><td align="right"><b>78.1%</b></td></tr>
</table>

**Calculation (PA1, AAP-scoped work only):**

```
Completed Hours      = 118   (19 components, every one traced to an AAP section)
Remaining Hours      =  33   (8 categories, every one traced to an AAP section or path-to-production need)
Total Project Hours  = 118 + 33 = 151
Completion %         = (118 / 151) × 100 = 78.1%
```

> **Chart legend — Blitzy brand colors:** Completed = Dark Blue `#5B39F3` · Remaining = White `#FFFFFF`

### 1.3 Key Accomplishments

- [x] **Root cause located to the exact line, and the reporter's own hypothesis refuted by measurement.** Three root causes identified: RC1 `_acquire_producer` (`celery/app/base.py:1165`), RC2 `_acquire_connection` (`:1136`), and RC3 a dependency-owned gevent/kombu import-ordering anomaly documented but deliberately left in place. The reporter's leading theory — that the producer pool's queue is not monkey-patched — was tested and disproved: the defect reproduces at full severity with the **gevent-native** `gevent._gevent_cqueue.LifoQueue` in place, so implementing that theory would have shipped no behavioural improvement at all.
- [x] **The defect is eliminated and the fix is proven, not asserted.** The acceptance oracle's metric (mean distinct greenlet identifiers per ten-line sliding window over 50 lines) moves from **1.88 BUGGY** to **5.00 HEALTHY**, with the emission order changing from `00000000001111111111222222222233333333334444444444` to `01234012340123401234012340123401234012340123401234`. Verified across **12 regimes**: `memory://` and real RabbitMQ at `broker_pool_limit` 10/3/1, the same three behind a 30 ms latency relay, the `broker_pool_limit=0` control, the RC2 pooled-connection path, the reporter's exact reproduction, and a hermetic broker-free unit test.
- [x] **A new finding beyond the Agent Action Plan: this is a liveness failure, not merely unfair ordering.** Without the fix a publishing greenlet's *own* `gevent.Timeout(1)` never fires, because gevent timers are hub-driven — a 200,000-publish probe produced no output in 120 s and the reporter's unbounded reproduction ran past a 300 s timeout. With the fix all five publishers are interrupted at 1.00–1.02 s having each completed ~575 publishes.
- [x] **Delivered exactly the four in-scope files, with zero scope creep.** `git diff --stat` = `4 files changed, 360 insertions(+), 1 deletion(-)`. `celery/app/base.py` is `+11/−0` across three pure-insertion hunks (1635 → 1646 lines) with **zero existing lines modified**. Zero changes to `requirements/`, `setup.py`, `pyproject.toml`, `setup.cfg`, `.github/`, `docs/` or `Changelog.rst`.
- [x] **A regression guard with a paired negative control.** A deterministic, fail-closed token-ring scheduler reproduces the acceptance oracle's exact numbers with no broker (5.00 with the yield), and its paired negative control asserts the failing value (≤ 3.0 plus a solo block) when the yield is disabled — which is what makes the pair a genuine guard rather than a tautology.
- [x] **A fully green build, exceeding the AAP's own bar.** The AAP permitted ten pre-existing environment-induced failures to persist; the delivered suite has **zero**: `3839 passed, 17 skipped, 3 xfailed, 28817 subtests passed`, pytest exit code 0, with 14 distinct issue classes resolved without touching a single out-of-scope file.
- [x] **Every quality gate green.** `compileall` exit 0 with no output · `mypy` "Success: no issues found in 10 source files" · `pre-commit run --all-files` **11/11 hooks Passed** · `isort --check-only` exit 0 · `flake8` clean but for two `D400` warnings **proven pre-existing** against the pristine blob · `celery/utils/green.py` at **100% statement and branch coverage**.
- [x] **Every boundary condition measured rather than argued.** Strict no-op at **45.4 ns/call** when gevent was never imported; **1.91 µs/call** hub round trip under gevent (0.19% of wall time at 1,000 publishes/sec); exception safety under `gevent.Timeout`, `KeyboardInterrupt` and `SystemExit` each leaving the pool balanced (queue 4→4, dirty 0→0); eventlet unreachable by construction; non-pooled and explicit-producer paths yielding zero times; 16 concurrent first calls all correct.
- [x] **Runtime validated across the whole product surface.** All four execution pools (prefork, gevent, solo, threads), `celery beat`, canvas `group`/`chord`/`chain`, the pooled control plane (`inspect ping` → `pong`, `status`, `active_queues`), the CLI, and packaging (`pip wheel` and `sdist` both contain `celery/utils/green.py`). Zero tracebacks, zero `CRITICAL`, and zero `RuntimeError('Semaphore released too many times')` in any worker log.

### 1.4 Critical Unresolved Issues

There are **no defects, compilation errors or test failures outstanding**. The suite is fully green (0 failed, 0 errored, exit 0) and every AAP code deliverable is complete. The items below are open **verification and contribution** gaps on the path to production, not broken work.

| Issue | Impact | Owner | ETA |
|---|---|---|---|
| Canonical `repro/` acceptance harness never executed — it is absent from the repository snapshot and AAP §0.5.2.1 forbids agents creating or altering it ("it is the acceptance criterion, not a deliverable") | **Medium.** The AAP's own authoritative arbiter has not produced its verdict. This is the sole reason AAP confidence is stated at 98% rather than higher. Mitigated by 12 faithful equivalent regimes that reproduce the oracle's exact numbers, so the residual risk is confirmation-only, not discovery | Release Engineer | 6 h |
| Test suite executed on **CPython 3.14.0 / Linux only**, against a declared CI matrix of 7 job combinations (3.10, 3.11, 3.12, 3.13, 3.14, pypy3.11 × Ubuntu, plus 3.14 × Windows) | **Low.** `celery/utils/green.py` AST-parses cleanly on 3.13.7 and 3.14.0 and uses only elementary AST node types — zero 3.11+/3.12+ constructs — so it is valid on 3.10+. The absent interpreters are not installed on the validation host | Release Engineer | 5 h |
| No upstream pull request exists — celery/celery#10044 remains open with five comments, no maintainer diagnosis, no linked PR and zero cross-references | **Medium.** Upstream merge is how a framework fix reaches operators. A maintainer may prefer a different remedy, though AAP §0.5.2.3 documents why an opt-in configuration knob would be wrong (the *default* configuration is the failing one) | Maintainer / Contributor | 8 h |
| No `Changelog.rst` or `docs/userguide/concurrency/gevent.rst` entry | **Low.** AAP §0.5.2.1 explicitly excluded both, reasoning that the inline comments at each insertion site cite `#10044` so the rationale travels with the code. Celery's release convention may nonetheless require an entry for a behavioural fix | Maintainer | 3 h |
| RC3 (kombu binding the unpatched `queue.LifoQueue` because `patch_thread()` runs 35 lines before `patch_queue()`) left unremediated | **Medium.** Dependency-owned and out of scope by AAP mandate. It causes the reporter's *secondary* `RuntimeError('Semaphore released too many times')`, which the fix makes far less likely to be reached — zero occurrences in any log or probe run — but does not eliminate at source | Platform Engineer | 2 h |
| Aggregate throughput on a high-rate production gevent publisher fleet unvalidated | **Low.** The added cost is measured at 1.91 µs per pooled acquisition (0.19% of wall time at 1,000 publishes/sec) and is the *mechanism* by which peers are scheduled rather than overhead | SRE | 4 h |

### 1.5 Access Issues

**No access issues identified.** Every permission required by the autonomous work was available and was re-confirmed live during this assessment. Two forward-looking access *prerequisites* for the remaining work are recorded below so the human owner can confirm they hold them; neither is a failure today.

| System/Resource | Type of Access | Issue Description | Resolution Status | Owner |
|---|---|---|---|---|
| Repository working tree `/tmp/blitzy/celery/blitzy-b7a40fa5-…_8ec33e` | Read / write / commit | None. `git status --porcelain` clean but for untracked `blitzy/` scratch; all 7 commits authored and committed successfully as `Blitzy Agent <agent@blitzy.com>` | ✅ Resolved — verified | Blitzy Agent |
| RabbitMQ broker (`celery-rabbitmq`, ports 5672 / 15672) | AMQP + management | None. Container healthy for 8 h; TCP reachability re-confirmed on both ports; `rabbitmqctl` used successfully to confirm beat-published messages | ✅ Resolved — verified | Blitzy Agent |
| Redis result backend (`celery-redis`, port 6379) | TCP | None. Container healthy; reachability re-confirmed | ✅ Resolved — verified | Blitzy Agent |
| Python virtual environment `.venv` (CPython 3.14.0) | Read / write / execute | None. Full suite, mypy, flake8, isort and all 11 pre-commit hooks executed successfully | ✅ Resolved — verified | Blitzy Agent |
| Non-root OS user `celerytest` (uid 1002) | Local account creation | Required because `celery/platforms.py` raises `SecurityError` when uid == euid == gid == egid == 0. Account created; suite runs fully green under it | ✅ Resolved — verified | Blitzy Agent |
| PyPI / package index | Network read | None. All 8 dependency manifests verified satisfied; `pip check` → "No broken requirements found." | ✅ Resolved — verified | Blitzy Agent |
| `github.com/celery/celery` | Fork / push / open PR | Not exercised — no upstream PR was created (out of autonomous scope). Push or PR permission is a **prerequisite** for remaining task H5 | ⚠ Prerequisite for remaining work | Maintainer / Contributor |
| Container images `rabbitmq:3`, toxiproxy, `python:3.12-slim` | Registry pull | Not exercised — the canonical `repro/docker-compose.yml` does not exist in this snapshot. Pull access is a **prerequisite** for remaining tasks H1/H2 | ⚠ Prerequisite for remaining work | Release Engineer |
| CPython 3.10 / 3.11 / 3.12 / pypy3.11 interpreters, Windows runner | Toolchain availability | Not available on the validation host (only 3.13.7 and 3.14.0 present), so 5 of 7 CI job combinations could not be executed. **Prerequisite** for remaining tasks H3/H4 | ⚠ Prerequisite for remaining work | Release Engineer |

### 1.6 Recommended Next Steps

1. **[High]** Author and execute the canonical `repro/` acceptance harness — `docker-compose.yml` (rabbitmq:3 behind toxiproxy at 30 ms with a `python:3.12-slim` runner), `app.py`, `oracle.py`, `run_repro.sh`, `run_control.sh` — then record both verdicts: `run_repro.sh` must report **NOT REPRODUCED** (≥ 4.0) and `run_control.sh` must remain ≥ 4.0. This closes the AAP's own last open gate and moves stated confidence from 98% to complete. *(6 h — tasks H1, H2)*
2. **[High]** Run the full unit suite across the remaining 5 of 7 CI job combinations — CPython 3.10/3.11/3.12/3.13 and pypy3.11 on Linux, plus CPython 3.14 on Windows. The helper uses only `import sys`, one module global and two function-local imports, so this is expected to be confirmation rather than discovery. *(5 h — tasks H3, H4)*
3. **[Medium]** Open the pull request against celery/celery, link issue #10044, and attach the evidence package: the two oracle strings, the paired negative control, the 12-regime matrix, and the boundary-condition measurements. Then work the maintainer review cycle. This is the longest-latency item and the only one gated on a third party. *(8 h — tasks H5, H6)*
4. **[Medium]** Decide whether Celery's release convention requires a `Changelog.rst` entry and whether `docs/userguide/concurrency/gevent.rst` should document the restored cooperative-yield guarantee — both were deliberately excluded by AAP §0.5.2.1 — and draft them if so, ideally before the PR is submitted. *(3 h — tasks H7, H8)*
5. **[Medium]** Soak a production-representative gevent publisher fleet to confirm no aggregate throughput regression against the measured 1.91 µs per pooled acquisition, then cut the release and stage the rollout with the oracle metric observed in situ. *(7 h — tasks H9, H10)*

> These five steps account for **29 of the 33 remaining hours**. The balance of **4 h** is two Low-priority, non-blocking follow-ups deliberately deferred by the Agent Action Plan and listed in full in Section 2.2: the upstream RC3 import-ordering report to kombu/gevent (2 h, task H11) and the batch-publish per-message fairness scoping decision (2 h, task H12). 29 + 4 = **33 h**.

---

## 2. Project Hours Breakdown

### 2.1 Completed Work Detail

| Component | Hours | Description |
|---|---:|---|
| [AAP §0.2.1] RC1 primary root-cause diagnosis | 14 | Traced the entire publish path across four packages — `apply_async` → `send_task` → `producer_or_acquire` → `FallbackContext.__enter__` → `_acquire_producer` → `kombu.Resource.acquire` → pre-filled LIFO → cached `_maybe_declare` → `basic_publish` → `sock.sendall` → `put_nowait` — and measured four warm-path steps in isolation under real `monkey.patch_all()` to prove none of them reaches the hub |
| [AAP §0.2.2] RC2 sibling call-site and barging mechanism | 6 | Identified the identical defect in `_acquire_connection`, proved structural identity with the producer path, and diagnosed the LIFO barging/convoy failure (`release()` uses `put_nowait`, which only *schedules* the notified waiter, so the releasing greenlet re-acquires the slot it just returned). Proved pool size is not the operative variable by measuring limits 1, 3 and 10 |
| [AAP §0.2.3] RC3 import-ordering anomaly investigation | 7 | Captured the live 13-frame import chain with an instrumented `builtins.__import__` hook, from `patch_all()` → `patch_thread` → `_patch_existing_locks` → `gc.get_objects()` → Celery's `Proxy.__class__` → `kombu/resource.py:7`; explained the reporter's secondary `RuntimeError`; documented the decision not to remediate dependency-owned code |
| [AAP §0.2.4] Refutation of the reporter's primary hypothesis | 6 | Built a four-cell measurement matrix (gevent-native vs patched-stdlib `LifoQueue` × non-empty get/put vs limit-1-plus-hold), all four measuring 1.88 identically, and established the decisive fact that the defect reproduces with the gevent-native class in place. Researched the kombu class-binding churn history (kombu#2314 merged, #2352, #2356 closed unmerged, gevent#2114). This single act prevented shipping a no-op change |
| [AAP §0.2.5] `broker_pool_limit=0` control mechanism analysis | 3 | Located the `else` branch in `kombu/resource.py`, proved the cold connect plus AMQP handshake yields naturally, measured 5.00 both before and after the fix, and corrected the reporter's inference that smaller pool limits inherently yield more often |
| [AAP §0.3.3.1] Reproduction equivalents and 30 ms latency relay | 10 | The canonical `repro/` harness is absent, so faithful equivalents were constructed modelling every element of its contract — five publishers, fifty lines, ten-line window, the ≤ 3.0 / ≥ 4.0 thresholds, the `broker_pool_limit=0` control — including a gevent `StreamServer` TCP relay injecting 30 ms each way as a toxiproxy substitute |
| [AAP §0.4.1.4] CREATE `celery/utils/green.py` | 4 | 45-line helper exposing `cooperative_yield()`. Small in volume but every design decision is load-bearing and individually justified against in-repo precedent: cheapest `sys.modules` guard, the private uncached `_detect_environment` rather than the memoizing public variant, positive-verdict-only caching, `gevent.sleep(0)` as the documented primitive |
| [AAP §0.4.2.2–.4] UPDATE `celery/app/base.py` (+11 lines) | 2 | Three pure-insertion hunks: the import at line 38 in isort-correct position, four lines in `_acquire_connection`, six lines in `_acquire_producer`. Placement *before* `acquire` is load-bearing for exception safety. Both comments cite `#10044` so the rationale travels with the code |
| [AAP §0.4.2.5] CREATE `t/unit/utils/test_green.py` | 3 | Six unit tests covering both no-op paths, the positive yield with `sleep.assert_called_once_with(0)`, positive-verdict caching, non-caching of the negative verdict, and exception propagation — with an autouse fixture resetting the module global on both sides of every test |
| [AAP §0.4.2.6] UPDATE `t/unit/concurrency/test_gevent.py` | 9 | 230 lines adding `class test_cooperative_publishing`: two call-ordering assertions via `attach_mock`, the non-pooled negative case, a deterministic fail-closed token-ring scheduler reproducing the oracle's exact numbers with no broker, and the paired negative control |
| [AAP §0.3.3.3] Boundary-condition verification | 8 | Conditions C1–C5 plus the sixteen-row edge-case table, each **measured** rather than argued: non-green no-op cost, gevent-imported-but-unpatched, a later `patch_all()` being honoured, eventlet probe ordering, exception safety under three interrupt types, pool reset, retry loops, explicit `producer=`, batch publishing, the module-global initialisation race, and the interpreter matrix |
| [AAP §0.6.2.1] Full-suite regression and issue resolution | 12 | Established the baseline and drove it from 9 failures to **zero** while unblocking 18 skips, resolving 14 distinct issue classes — uid-0 privilege errors, a click message-format change, sqlite directory modes, `EMFILE` limits, root-owned leftovers, missing optional extras — and root-causing then reverting an eventlet regression rather than leaving a failure. All without touching a single out-of-scope file |
| [AAP §0.6.1.3–.5] Runtime validation | 13 | All four execution pools, `celery beat` (due tasks confirmed on the broker via `rabbitmqctl`), canvas `group`/`chord`/`chain`, the pooled control plane, the CLI, packaging via wheel and sdist, and **12 oracle regimes** including the reporter's exact reproduction and the RC2 path. Produced the liveness finding beyond the AAP |
| [AAP §0.6.2.4] Static analysis and style gates | 3 | `pre-commit run --all-files` at 11/11 with all 8 hook environments freshly installed and md5-verified byte-identity afterwards despite fix-capable hooks; `flake8` with `D400` pre-existence proven against the pristine blob; `isort`; `mypy`; `check-ci-test-matrices` |
| [AAP §0.8.3] External research and precedent establishment | 6 | Fourteen references fetched and read — the upstream issue, the reporter's application, gevent's intro/api/monkey documentation, four kombu and gevent issues, two eventlet issues, and OpenStack Nova's threading guidance — establishing that no upstream fix exists and that `sleep(0)` is the industry-codified remedy. Plus in-repo precedent discovery for the environment guard, the module location and both test idioms |
| [AAP §0.8.1] Environment derivation and dependency verification | 5 | No environment was attached, so it was derived from the repository itself: the highest documented interpreter identified from `setup.py`, Python 3.14 installed, a dedicated venv created, and Celery installed editable alongside the pinned dependency set. All 8 dependency manifests verified satisfied with `pip install --dry-run`; `pip check` clean; version constraints proven programmatically |
| [Path-to-production] Compilation and type-check gates | 2 | `compileall` across `celery/`, `t/` and `examples/` at exit 0 with zero output; all four in-scope files compiled under `warnings.simplefilter('error')` with `-W error::SyntaxWarning -W error::DeprecationWarning` producing zero warnings; `mypy` clean across 10 source files |
| [Path-to-production] Commit hygiene and branch management | 2 | Seven atomic commits, each referencing `#10044`, each authored **and** committed as `Blitzy Agent <agent@blitzy.com>`, each touching only in-scope paths — plus the byte-level proof that removing exactly the 11 inserted line indices reconstructs the pristine `celery/app/base.py` |
| [AAP §0.4.3.2] Determinism and test-isolation hardening | 3 | Twenty consecutive repeat runs of each new module in both module orders, plus a session-finish plugin asserting `green._yield is None` to prove no mocked `gevent.sleep` leaks into any later test that reaches a pooled acquisition, plus the fail-closed baton-timeout hardening commit |
| **TOTAL COMPLETED** | **118** | |

### 2.2 Remaining Work Detail

| Category | Hours | Priority |
|---|---:|---|
| **Acceptance Harness** — author `repro/` (`docker-compose.yml` with rabbitmq:3 behind toxiproxy at 30 ms and a `python:3.12-slim` runner, `app.py`, `oracle.py`, `run_repro.sh`, `run_control.sh`) and execute both authoritative oracle runs, recording the **NOT REPRODUCED** verdict and the ≥ 4.0 control *(AAP §0.6.1.1/.2, §0.8.2)* | 6 | High |
| **CI Matrix Verification** — execute the full unit suite across the remaining 5 of 7 declared job combinations: CPython 3.10/3.11/3.12/3.13 and pypy3.11 on Linux, plus CPython 3.14 on Windows | 5 | High |
| **Upstream Contribution** — open the PR against celery/celery, link issue #10044, attach the evidence package, and work the maintainer review cycle including rebase and CI re-runs *(AAP §0.1.4: no upstream fix exists)* | 8 | Medium |
| **Documentation** — decide whether `Changelog.rst` and `docs/userguide/concurrency/gevent.rst` entries are required by Celery's release convention, and draft them if so *(both deliberately excluded by AAP §0.5.2.1)* | 3 | Medium |
| **Performance Validation** — soak a production-representative gevent publisher fleet and confirm no aggregate throughput regression against the measured 1.91 µs per pooled acquisition *(AAP §0.6.2.5)* | 4 | Medium |
| **Release & Rollout** — cut the release / merge to main and stage the rollout to gevent-pool deployments, observing the oracle metric in situ | 3 | Medium |
| **Upstream Defect Report** — file the RC3 import-ordering diagnosis (the 13-frame chain; `patch_thread()` running 35 lines before `patch_queue()`) upstream with kombu and/or gevent *(AAP §0.2.3, documented but deliberately unremediated)* | 2 | Low |
| **Follow-up Scoping** — decide and scope whether per-message fairness inside a single very large `group()`/`chord()` warrants a follow-up, given that batch publishes correctly receive one yield per batch *(AAP §0.5.2.3, explicitly deferred)* | 2 | Low |
| **TOTAL REMAINING** | **33** | |

**Priority distribution:** High 11 h · Medium 18 h · Low 4 h = **33 h**

### 2.3 Hours Reconciliation

| Check | Expected | Actual | Status |
|---|---|---|---|
| Section 2.1 "Hours" column sum | 118 | 118 | ✅ |
| Section 2.2 "Hours" column sum | 33 | 33 | ✅ |
| Section 2.1 + Section 2.2 = Section 1.2 Total Hours | 151 | 151 | ✅ |
| Section 2.2 sum = Section 1.2 Remaining Hours | 33 | 33 | ✅ |
| Section 2.2 sum = Section 7 pie "Remaining Work" | 33 | 33 | ✅ |
| Human task list (12 tasks H1–H12) sum = Section 2.2 sum | 33 | 33 | ✅ |
| Section 1.6 top-5 next steps (29 h) + Low-priority balance (4 h) | 33 | 33 | ✅ |
| Section 8.3 critical path: serial (25 h) + parallel (8 h) | 33 | 33 | ✅ |
| Section 7.2 priority pie: High 11 + Medium 18 + Low 4 | 33 | 33 | ✅ |
| Section 7.3 category bars sum | 33 | 33 | ✅ |
| Completion % = 118 / 151 × 100 | 78.1% | 78.1% | ✅ |

There is **no manual (human) completed work** in this project — all 118 completed hours were delivered autonomously by Blitzy agents, so the "Completed Hours (AI + Manual)" figure decomposes as 118 AI + 0 Manual.

---

## 3. Test Results

All figures below originate exclusively from Blitzy's autonomous validation logs for this project and were **independently re-executed during this assessment**. No test result is inherited or estimated.

| Test Category | Framework | Total Tests | Passed | Failed | Coverage % | Notes |
|---|---|---:|---:|---:|---:|---|
| Unit — full suite | pytest 9.0.3 | 3859 | **3839** | **0** | — | 17 skipped, 3 xfailed, **28,817 subtests passed**, 338.88 s, pytest exit code 0. `grep -c '^FAILED'` = 0, `grep -c '^ERROR'` = 0 |
| Unit — new helper | pytest | 6 | **6** | **0** | **100** | `t/unit/utils/test_green.py`. 100% statement *and* branch coverage of `celery/utils/green.py` (14 stmts / 0 miss / 6 branches / 0 partial) |
| Unit — gevent regression guard | pytest | 12 | **12** | **0** | — | `t/unit/concurrency/test_gevent.py` — 7 pre-existing plus 5 new, exactly the count AAP §0.4.3.2 requires |
| Unit — cooperative publishing | pytest | 5 | **5** | **0** | — | `::test_cooperative_publishing` — two `attach_mock` call-ordering assertions, the non-pooled negative case, the fail-closed token-ring interleaving test, and the paired negative control |
| Integration — oracle regimes | custom oracle (AAP contract) | 12 | **12** | **0** | — | `memory://` and real RabbitMQ at limits 10/3/1, the same three behind a 30 ms relay, the limit-0 control ± relay, the RC2 pooled-connection path, the reporter's exact reproduction, and the hermetic token ring. **5.00 HEALTHY** with the fix; **1.88 BUGGY** with the yield disabled |
| Integration — boundary conditions | custom probes | 6 | **6** | **0** | — | Non-green no-op, exception safety under three interrupt types, non-pooled paths, explicit `producer=`, batch publishing, and the 16-thread initialisation race |
| End-to-End — execution pools | Celery worker + RabbitMQ + Redis | 4 | **4** | **0** | — | prefork (`add(2,3)` → 5), gevent (`-c 10`, 20/20 tasks returning the correct sum), solo, threads. Zero tracebacks, zero `CRITICAL`, zero `RuntimeError('Semaphore released too many times')` |
| End-to-End — beat, canvas, control plane | Celery CLI + `rabbitmqctl` | 3 | **3** | **0** | — | `celery beat` (due tasks confirmed on the broker), canvas `group`/`chord`/`chain`, pooled control plane (`inspect ping` → `pong`, `status` → `1 node online`, `active_queues`) |
| Static analysis — pre-commit | pre-commit (11 hooks) | 11 | **11** | **0** | — | pyupgrade, flake8, strip-noqa, codespell, merge-conflicts, check-toml, check-yaml, mixed-line-ending, isort, mypy, check-ci-test-matrices. All 8 hook environments freshly installed; files md5-verified byte-identical afterwards |
| Static analysis — type check | mypy | 10 files | **10** | **0** | — | "Success: no issues found in 10 source files" |
| Static analysis — compile | `compileall` | `celery/` `t/` `examples/` | **all** | **0** | — | Exit 0 with zero output. All four in-scope files additionally compiled under `-W error::SyntaxWarning -W error::DeprecationWarning` with zero warnings |
| Packaging | `pip wheel` / `setup.py sdist` | 2 | **2** | **0** | — | Both artifacts confirmed to contain `celery/utils/green.py` |

**Aggregate pass rate: 100%** — 3,839 of 3,839 executed unit tests, plus 28,817 subtests, plus every integration, end-to-end, static-analysis and packaging check. **Zero failures, zero errors, zero blocked tests.**

**On the 17 skips.** Every one is skipped by design and none touches the publish path, the concurrency pools or `celery/utils/`. Enumerated exactly with `pytest -rs`: 10 × `No module named 'eventlet'` (the repository itself pins `eventlet>=0.32.0; python_version<"3.10"` while the validation interpreter is 3.14, so `importorskip` skips by design), 2 × `For pymongo version > 3, options returns ssl`, 1 × `For dnspython version >= 2, pymongo's srv_resolver calls resolver.resolve` (mutually-exclusive `skipif` guards covering older library majors), 2 × `unstable test`, 1 × `not working`, 1 × `cert expired` (unconditional maintainer-authored `@pytest.mark.skip` markers pre-dating this work). Total = 17.

**On coverage figures.** Coverage is reported only where it is meaningful for this change. `celery/utils/green.py` measures **100% statement and branch coverage** under a fresh-import run. A warm run reports 80% for the same file, which is purely an import-timing artifact: `celery.app.base` imports the helper during conftest collection, so its twelve module-level lines execute before the coverage tracer starts. `celery/app/base.py` is a 684-statement pre-existing module whose repository-wide coverage is unrelated to the 11 inserted lines; those 11 lines are directly exercised by the five new `test_cooperative_publishing` tests and by all twelve oracle regimes.

---

## 4. Runtime Validation & UI Verification

### 4.1 Broker and Infrastructure

- ✅ **RabbitMQ** (`celery-rabbitmq`) — Operational. AMQP on 5672 and management on 15672 both TCP-reachable; `rabbitmqctl` used successfully to confirm beat-published messages had landed on the broker.
- ✅ **Redis** (`celery-redis`) — Operational. Result backend on 6379 TCP-reachable; task results retrieved successfully end-to-end.
- ✅ **30 ms latency relay** — Operational. A gevent `StreamServer` TCP relay on port 5673 fronting RabbitMQ, injecting 30 ms each way as a toxiproxy substitute; first-response latency verified at 76 ms.

### 4.2 Execution Pools

- ✅ **gevent pool** — Operational. `celery -A rtapp worker -P gevent -c 10` started clean; **20 of 20 tasks returned the correct sum, all `SUCCESS`**; zero tracebacks, zero `CRITICAL`, zero `RuntimeError('Semaphore released too many times')` in the worker log.
- ✅ **prefork pool** — Operational. Default pool unaffected by construction; `add(2,3)` → 5.
- ✅ **solo pool** — Operational.
- ✅ **threads pool** — Operational.
- ✅ **Non-green strict no-op** — Operational. `cooperative_yield()` returns `False` at **45.4 ns per call** when gevent was never imported, so prefork, solo and threads deployments are provably unaffected.

### 4.3 Application Surface

- ✅ **`celery beat`** — Operational. Scheduler ran and due tasks were confirmed present on the broker via `rabbitmqctl`.
- ✅ **Canvas primitives** — Operational. `group`, `chord` and `chain` all executed and returned correct results.
- ✅ **Pooled control plane (the RC2 path)** — Operational. `celery -A rtapp inspect ping` → `pong`; `celery -A rtapp status` → `1 node online.`; `active_queues` returned correctly. This is the path `_acquire_connection` serves, so it is the direct runtime exercise of RC2.
- ✅ **CLI** — Operational. `celery --version` → `5.6.2 (recovery)`; `celery report` → `celery:5.6.2 kombu:5.6.2 py:3.14.0 billiard:4.2.4 py-amqp:5.3.1`, `transport:amqp`; `--help` and `purge` also verified.
- ✅ **Packaging** — Operational. `pip wheel` produced `celery-5.6.2-py3-none-any.whl` confirmed to contain `celery/utils/green.py`; `setup.py sdist` likewise.

### 4.4 Defect Elimination at Runtime

- ✅ **Primary defect eliminated.** Emission order changed from `00000000001111111111222222222233333333334444444444` (effective concurrency **1.88**, solo block present, **BUGGY**) to `01234012340123401234012340123401234012340123401234` (effective concurrency **5.00**, no solo block, **HEALTHY**). Reproduced at `broker_pool_limit` 10, 3 and 1, against `memory://`, real RabbitMQ, and RabbitMQ behind the 30 ms relay.
- ✅ **Control regime unchanged.** `broker_pool_limit=0` measures 5.00 HEALTHY both before and after the fix, confirming the cold connect-plus-handshake path is untouched exactly as predicted.
- ✅ **Environment genuinely patched in every run.** `type(app.producer_pool._resource)` was `gevent._gevent_cqueue.LifoQueue` — the gevent-**native** class — and `socket.socket.__module__` was `gevent._socket3` in all runs, independently confirming both that monkey-patching was in effect and that the queue-binding hypothesis is not the cause.
- ✅ **Liveness restored (finding beyond the AAP).** Without the fix a publishing greenlet's own `gevent.Timeout(1)` never fires because gevent timers are hub-driven — a 200,000-publish probe produced no output in 120 s. With the fix all five publishers are interrupted at 1.00–1.02 s having each completed ~575 publishes.
- ✅ **Secondary symptom absent.** Zero occurrences of `RuntimeError('Semaphore released too many times')` across every worker log and probe run.
- ✅ **Exception safety at the yield point.** `gevent.Timeout`, `KeyboardInterrupt` and `SystemExit` each leave the pool exactly balanced (queue 4→4, dirty 0→0) and still functional — strictly safer than the status quo, where the same interrupt lands inside `LifoQueue.get`'s `Condition`.
- ⚠ **Canonical acceptance harness** — Partial. The authoritative `repro/` harness is absent from the snapshot and AAP §0.5.2.1 forbids agents creating it, so its `oracle.py` verdict has not been recorded. Twelve faithful equivalent regimes reproduce its exact numbers; see Section 2.2 row 1 (6 h budgeted).

### 4.5 UI Verification — Not Applicable (Justified)

Celery is a headless, server-side task-queue framework with **no user interface, web front-end or browser-facing component in this repository**, so there is no UI to verify and no browser-based validation was performed. This is not an omission; it was confirmed empirically during this assessment rather than assumed:

- A search of `celery/` for web-framework entry points (`flask`, `fastapi`, `django.urls`, `aiohttp.web`, `WSGI`, `ASGI`) returned **0 files**.
- A search for browser assets (`*.html`, `*.js`, `*.jsx`, `*.tsx`) under `celery/` returned **0 files**. The only static asset in the package is `celery/utils/static/celery_128.png`, a desktop-notification icon.
- The repository declares no HTTP listener and exposes no route. The sole web-adjacent dependency, `requirements/extras/django.txt`, is an ORM/fixup integration, not a server.

This matches the Agent Action Plan, which explicitly omits the Figma Design, Design System Compliance and User Interface Design sub-sections on the same grounds (§0.4.3.4 and §0.8.1), noting that the change "produces no rendered output of any kind." The RabbitMQ management console on port 15672 is third-party infrastructure tooling, not a deliverable of this project, and was used only as a broker-inspection aid alongside `rabbitmqctl`.

---

## 5. Compliance & Quality Review

### 5.1 AAP Deliverable Compliance Matrix

| AAP Requirement | Reference | Benchmark | Status | Evidence |
|---|---|---|---|---|
| CREATE `celery/utils/green.py` exposing `cooperative_yield()` | §0.5.1 r1 / §0.4.1.4 | File exists, content matches spec | ✅ Pass | 45 lines, md5 `a2cae073d48776869f1c0230758a3fd8`, commit `e71deb729`; all 7 structural assertions verified; 100% statement + branch coverage |
| UPDATE `base.py` — import at line 38 between `functional` and `imports` | §0.5.1 r2 / §0.4.2.2 | Exact position, isort-correct | ✅ Pass | Diff confirms position; `isort --check-only` exit 0 (`functional` < `green` < `imports`) |
| UPDATE `base.py` — `+4` in `_acquire_connection` after `timeout=`, before `try:` | §0.5.1 r3 / §0.4.2.3 | 3 comment lines + 1 call | ✅ Pass | Diff confirms lines 1136–1139; comment cites `#10044` |
| UPDATE `base.py` — `+6` in `_acquire_producer` after docstring, before `try:` | §0.5.1 r4 / §0.4.2.4 | 5 comment lines + 1 call | ✅ Pass | Diff confirms lines 1169–1174; comment cites `#10044` |
| CREATE `t/unit/utils/test_green.py` — 6 tests | §0.5.1 r5 / §0.4.2.5 | 6 passed | ✅ Pass | **6/6 pass**; uses `monkeypatch.delitem` as directed, avoiding the `masked_modules` trap the AAP flagged |
| UPDATE `test_gevent.py` imports — `threading`, `MagicMock`, `celery.utils.green` | §0.5.1 r6 / §0.4.2.6 | All three present | ✅ Pass | Lines 1–5 confirm all three |
| UPDATE `test_gevent.py` — append `class test_cooperative_publishing`, 5 tests | §0.5.1 r7 / §0.4.2.6 | 12 passed total | ✅ Pass | **12/12 pass** (7 pre-existing + 5 new) — exactly the AAP §0.4.3.2 count |
| Net effect: `base.py` exactly `+11/−0`, zero existing lines modified | §0.5.1 | `11 +++++++++++`, 1635 → 1646 | ✅ Pass | `git diff --numstat` = `11 0`; three pure-insertion hunks; removing exactly the 11 inserted indices reconstructs the pristine file byte-for-byte |
| Zero dependency changes | §0.5.1 | `requirements/`, `setup.py`, `pyproject.toml`, `setup.cfg` untouched | ✅ Pass | `git diff --name-only` on those paths = **0 files**; `pip check` → "No broken requirements found." |
| Zero configuration changes, no new knob | §0.5.1 / §0.5.2.3 | `defaults.py` untouched | ✅ Pass | Not in diff; no option added |
| No out-of-scope file touched | §0.5.2.1 | Exactly 4 files in diff | ✅ Pass | `git diff --name-status` lists exactly the 4 in-scope files; `repro/`, dependency source, `celery/concurrency/*`, `task.py`, `control.py`, `canvas.py`, `test_app.py`, `docs/`, `Changelog.rst` and `.github/` all untouched |
| Two pre-existing `D400` warnings NOT "fixed" | §0.5.2.2 | Left in place | ✅ Pass | Present at lines 1552/1563; the pristine blob has the same two at 1541/1552 — a shift of exactly +11, proving pre-existence |

### 5.2 Design-Choice Compliance

| Requirement | Reference | Status | Evidence |
|---|---|---|---|
| Cheapest guard: `'gevent' not in sys.modules` checked first | §0.4.1.6 | ✅ Pass | Present as the first branch; measured **45.4 ns/call** in the non-green case |
| Use the private uncached `_detect_environment`, not the memoizing public variant | §0.4.1.6 | ✅ Pass | `from kombu.utils.compat import _detect_environment`; mirrors the in-repo idiom at `celery/worker/consumer/consumer.py:19,242` |
| Cache only the *positive* verdict, so a later `patch_all()` is honoured | §0.4.1.6 | ✅ Pass | `test_does_not_cache_the_negative_verdict` passes; probe confirms the same helper flips to `True` after a later `patch_all()` |
| `gevent.sleep(0)` as the documented cooperative primitive | §0.4.1.5 | ✅ Pass | `_yield(0)`; `sleep.assert_called_once_with(0)` |
| eventlet untouched by construction (kombu checks eventlet first) | §0.4.1.6 | ✅ Pass | `_detect_environment` source offsets: eventlet at char 37 < gevent at char 322 → eventlet checked first |
| New utils module following the `quorum_queues.py` precedent, no registration | §0.4.1.6 | ✅ Pass | `dest_folder:celery/utils` shows `green.py` CREATED with `__init__.py` UNCHANGED |
| Yield placed *before* `acquire` (load-bearing for exception safety) | §0.4.1.5 | ✅ Pass | `mock_calls == ['sleep', 'acquire']` ordering assertions pass on both call sites; verified again at runtime via `inspect.getsource` |
| Celery-side only — no dependency vendoring, patching or version change | §0.1.4 | ✅ Pass | 0 files changed under `requirements/`; no `site-packages` edit; `pip check` clean |
| No public API change, no new configuration knob | §0.5.2.3 | ✅ Pass | `.delay()`/`.apply_async()` signatures untouched; `defaults.py` untouched |
| Valid across Python 3.10–3.14, CPython and PyPy | §0.1.4 | ✅ Pass (⚠ partial execution) | AST-parses cleanly on 3.13.7 and 3.14.0; uses **only elementary AST node types, zero 3.11+/3.12+ constructs** → valid on 3.10+. Full-suite *execution* on 3.10/3.11/3.12/pypy3.11/Windows outstanding — see Section 2.2 row 2 |

### 5.3 Quality Gate Compliance

| Gate | Benchmark | Result | Status |
|---|---|---|---|
| Compilation | Exit 0 | `compileall -q -f celery/ t/ examples/` → exit 0, zero output | ✅ Pass |
| Type checking | No issues | `mypy --config-file pyproject.toml` → "Success: no issues found in 10 source files" | ✅ Pass |
| Unit tests | No new failures | **3839 passed / 0 failed / 0 errored**, exit 0 — the AAP permitted 10 pre-existing failures to persist; delivered **zero** | ✅ Pass — exceeds benchmark |
| In-scope tests | 6 + 12 passed | 6 + 12 = **18 passed** in 0.09 s | ✅ Pass |
| Lint (flake8) | Only pre-existing warnings | Exactly 2 `D400`, pre-existence proven against the pristine blob | ✅ Pass |
| Import ordering | Exit 0 | `isort --check-only celery/ t/` → exit 0 | ✅ Pass |
| Pre-commit | All hooks pass | **11/11 hooks Passed**, exit 0, files md5-verified byte-identical afterwards | ✅ Pass |
| CI matrix registration | Registered where required | `check-ci-test-matrices` → Passed. The hook covers only `t/integration` and `t/smoke/tests`, so a new `t/unit` module needs no registration | ✅ Pass |
| Coverage of new production code | Meaningful coverage | `celery/utils/green.py` at **100% statement and branch** coverage | ✅ Pass |
| Commit authorship | `Blitzy Agent <agent@blitzy.com>` | All 7 commits, author **and** committer | ✅ Pass |
| Security posture | No new attack surface | AST review: **zero** risky constructs; import surface is exactly `sys`, `kombu.utils.compat`, `gevent`; no filesystem, network, subprocess, serialisation or `eval`. Secrets scan of the branch diff found no hardcoded credentials | ✅ Pass |
| Zero-placeholder policy | No TODO/FIXME/stub/`pass` | None present in any of the 4 in-scope files; every function fully implemented and returning real computed values | ✅ Pass |
| Semantic preservation | Message, routing, wire format unchanged | The yield touches no message, channel, socket or routing decision and sits outside every critical section; `t/unit/app/`, `t/unit/tasks/` and `t/unit/security/` all fully green | ✅ Pass |

### 5.4 Fixes Applied During Autonomous Validation

Fourteen distinct issue classes were resolved during validation, **none requiring an out-of-scope file edit**: seven uid-0 privilege failures (root-caused to `celery/platforms.py` raising `SecurityError` when uid == euid == gid == egid == 0; resolved by creating and using a non-root OS account); two click message-format assertions (click ≥ 8.2 changed the "No such option" wording; resolved by pinning `click==8.1.8`, still satisfying the repository's declared `click>=8.1.2,<9.0`); forty-seven `test_database.py` sqlite failures newly exposed when running non-root (resolved via directory modes, which git does not track); three `test_prefork.py` `EMFILE` failures (`ulimit -n 65536`); two `test_worker.py` `PermissionError`s from root-owned leftovers; eighteen skips unblocked by installing the repository's own optional extras, every newly-enabled test passing; and one eventlet regression root-caused to the CPython 3.12+ `pkgutil.resolve_name` change versus Celery's `MockModule` double, then reverted to the declared configuration rather than left as a failure.

### 5.5 Outstanding Compliance Items

| Item | Reason Outstanding | Budgeted |
|---|---|---|
| Canonical `repro/` acceptance-oracle verdict | Harness absent from the snapshot; AAP §0.5.2.1 forbids agents creating or altering it | 6 h (§2.2 r1) |
| Full CI-matrix execution (5 of 7 job combinations) | Required interpreters and Windows runner unavailable on the validation host | 5 h (§2.2 r2) |
| `Changelog.rst` / `gevent.rst` entries | Deliberately excluded by AAP §0.5.2.1; a maintainer decision is required | 3 h (§2.2 r4) |
| RC3 remediation | Dependency-owned code; excluded by AAP §0.2.3 and §0.5.2.1. Upstream report budgeted instead | 2 h (§2.2 r7) |
| Per-message fairness inside very large `group()` batches | Explicitly deferred by AAP §0.5.2.3 as "a distinct concern" | 2 h (§2.2 r8) |

---

## 6. Risk Assessment

| Risk | Category | Severity | Probability | Mitigation | Status |
|---|---|---|---|---|---|
| **T1** Canonical `repro/` acceptance harness never executed — the AAP's own authoritative arbiter has produced no verdict | Technical | Medium | High | Twelve faithful equivalent regimes executed instead (real RabbitMQ at limits 10/3/1/0, a 30 ms latency relay, the reporter's exact reproduction, the RC2 path), every one reproducing the oracle's exact numbers — 5.00 HEALTHY with the fix, 1.88 BUGGY without. A hermetic token-ring unit test reproduces the same numbers with no broker at all, so the residual risk is confirmation-only, not discovery | Open — 6 h budgeted |
| **T2** Suite executed on CPython 3.14.0 / Linux only, against 7 declared CI job combinations | Technical | Low | Medium | `green.py` AST-parses cleanly on 3.13.7 and 3.14.0 and uses only elementary AST node types — zero 3.11+/3.12+ constructs — so it is valid on 3.10+. The helper's entire surface is `import sys`, one module global and two function-local imports | Open — 5 h budgeted |
| **T3** One extra gevent hub round trip per pooled acquisition could reduce single-greenlet publish throughput | Technical | Low | Low | Measured at **1.91 µs/call** = 0.19% of wall time at 1,000 publishes/sec. It is the *mechanism* by which peers are scheduled, not overhead, and the cost is per pooled acquisition rather than per byte or message field | Mitigated — 4 h soak budgeted |
| **T4** Batch publishes yield once per batch, not per message, so a single very large `group()` still starves peers for its duration | Technical | Low | Medium | Explicitly and deliberately scoped out by AAP §0.5.2.3. Independently confirmed: a group of 50 messages yields once; five individual `apply_async` calls yield five times — correct for the reported defect | Accepted — 2 h scoping budgeted |
| **S1** New attack surface from the added module | Security | Low | Low | AST review found **zero** risky constructs. The import surface is exactly three names — `sys`, `kombu.utils.compat`, `gevent` — with no filesystem, network, subprocess, serialisation or `eval` access. The function takes no arguments and returns a bool | Closed |
| **S2** Message or transport security semantics altered | Security | Low | Low | The yield touches no message, channel, socket or routing decision and sits outside every critical section. Wire format, serialisation, exchange/queue declaration and exactly-once publishing are provably unchanged; `t/unit/app/`, `t/unit/tasks/` and `t/unit/security/` are fully green | Closed |
| **S3** Dependency supply-chain drift | Security | Low | Low | **Zero** dependency changes — `git diff --name-only` on `requirements/`, `setup.py`, `pyproject.toml` and `setup.cfg` returns 0 files. `pip check` → "No broken requirements found." No hardcoded credentials in the branch diff | Closed |
| **O1** No `Changelog.rst` entry, so operators upgrading see a behavioural change with no release-note trail | Operational | Low | High | AAP §0.5.2.1 explicitly excluded Changelog and docs, reasoning that both insertion sites carry inline comments citing `#10044` so the rationale travels with the code | Open — 3 h budgeted |
| **O2** `gevent.sleep` availability depends on the gevent version floor | Operational | Low | Low | `requirements/extras/gevent.txt` already pins `gevent>=26.4.0`; installed 26.7.0 verified as satisfying it. `gevent.sleep(0)` has been the documented cooperative yield since gevent 1.3a1. No dependency change required | Closed |
| **O3** Aggregate throughput on a high-rate production gevent publisher fleet unvalidated | Operational | Low | Medium | Isolated cost measured at 1.91 µs; all four pools plus beat, canvas and the control plane ran clean with zero tracebacks and zero `CRITICAL` in any worker log | Open — 4 h budgeted |
| **O4** 17 tests remain skipped, so those paths are unexercised here | Operational | Low | Low | Each skip is by design and out of scope — 10 eventlet-gated by the repository's own `python_version<"3.10"` pin, 3 mutually-exclusive pymongo/dnspython guards, 4 unconditional maintainer `@pytest.mark.skip` markers. None touches the publish path, the pools or `celery/utils/`. Zero failed, zero errored | Accepted |
| **I1** The concrete class backing `kombu.Resource._resource` is a moving target — it has changed three times in a year (kombu#2314 merged, #2352, #2356 closed unmerged, gevent#2114) | Integration | Low | Medium | The fix is deliberately decoupled from that class: it yields *before* `acquire()` and never inspects the queue type. Independently confirmed correct with `gevent._gevent_cqueue.LifoQueue` in place, and AAP §0.2.4 proves the class binding is not the cause | Closed by design |
| **I2** RC3 left unremediated — kombu can bind the unpatched `queue.LifoQueue` because `patch_thread()` runs 35 lines of patch order before `patch_queue()`, which produces the reporter's secondary `RuntimeError('Semaphore released too many times')` | Integration | Medium | Medium | Dependency-owned; AAP §0.2.3 and §0.5.2.1 forbid touching it. The fix makes the blocking `LifoQueue.get` far less likely to be reached, and **zero** occurrences appeared in any worker log or probe run. The 13-frame import chain is fully documented and ready to file upstream | Open — 2 h budgeted |
| **I3** Upstream acceptance is an external dependency — celery/celery#10044 is open with no maintainer diagnosis, no linked PR and zero cross-references; a maintainer may prefer a different remedy | Integration | Medium | Medium | The evidence package is unusually strong: root cause located to the exact line, the reporter's own hypothesis refuted by measurement, `sleep(0)` established as the industry-codified remedy (eventlet#1014, OpenStack Nova threading guidance), every design choice backed by in-repo precedent, and a regression test with a paired negative control. AAP §0.5.2.3 documents why a configuration knob would be wrong | Open — 8 h budgeted |

**Risk profile.** Fourteen risks across four categories. **No High or Critical severity risk exists.** The highest-rated are T1 (Medium severity, High probability), I2 and I3 (Medium/Medium). Five risks are already Closed and two Accepted. Critically, **no risk blocks compilation, tests or runtime** — all three are fully green — and every open risk has budgeted remaining hours attached to it in Section 2.2.

---

## 7. Visual Project Status

### 7.1 Project Hours Breakdown

```mermaid
pie showData title Project Hours — 151 h Total
    "Completed Work" : 118
    "Remaining Work" : 33
```

> **Blitzy brand colors:** Completed Work = Dark Blue `#5B39F3` · Remaining Work = White `#FFFFFF` · Headings/Accents = Violet-Black `#B23AF2` · Highlight = Mint `#A8FDD9`

### 7.2 Remaining Work by Priority

```mermaid
pie showData title Remaining 33 h by Priority
    "High" : 11
    "Medium" : 18
    "Low" : 4
```

### 7.3 Remaining Hours by Category

| Category | Hours | Bar |
|---|---:|---|
| Upstream Contribution | 8 | ████████ |
| Acceptance Harness | 6 | ██████ |
| CI Matrix Verification | 5 | █████ |
| Performance Validation | 4 | ████ |
| Documentation | 3 | ███ |
| Release & Rollout | 3 | ███ |
| Upstream Defect Report | 2 | ██ |
| Follow-up Scoping | 2 | ██ |
| **Total** | **33** | |

### 7.4 Delivery Snapshot

| Dimension | Value |
|---|---|
| Completion | **78.1%** (118 of 151 h) |
| Files changed | 4 (2 created, 2 updated, 0 deleted) |
| Lines changed | +360 / −1 · production code `+11/−0` |
| Commits | 7, all `Blitzy Agent <agent@blitzy.com>` |
| Unit tests | 3839 passed · 0 failed · 0 errored · exit 0 |
| Quality gates | 11/11 pre-commit · mypy clean · compileall exit 0 |
| Oracle metric | **1.88 BUGGY → 5.00 HEALTHY** across 12 regimes |
| Open risks | 6 (0 High/Critical severity) |

---

## 8. Summary & Recommendations

### 8.1 What Was Achieved

The project is **78.1% complete** — 118 of 151 total hours — and every code deliverable defined in the Agent Action Plan has been delivered, validated and committed.

The substance of this work is not its 360 changed lines but the diagnosis behind them. A long-latent cooperative-scheduling defect, open upstream since December 2025 with no maintainer diagnosis, was traced to two exact lines of Celery-owned code. Three root causes were separated: two Celery-owned defects that were remediated, and one dependency-owned import-ordering anomaly that was documented and deliberately left in place with a full 13-frame import chain captured live. Most consequentially, the reporter's own leading hypothesis — that the producer pool's queue is not monkey-patched — was tested and **refuted by measurement**: the defect reproduces at full severity with the gevent-native `gevent._gevent_cqueue.LifoQueue` in place, so implementing that theory would have shipped no behavioural improvement whatsoever. That single act of disproof is what makes the remedy correct rather than merely plausible.

The fix itself is one new 45-line helper and eleven inserted lines, with zero deletions and zero existing lines modified. It changes no public API, adds no configuration knob — deliberately, because the *default* configuration is the failing one — touches no message, channel, socket or routing decision, and is a strict no-op on every non-gevent runtime at a measured 45.4 nanoseconds per call.

The evidence is quantitative throughout. The acceptance oracle's metric moves from **1.88 BUGGY** to **5.00 HEALTHY**, with the emission order changing from `00000000001111111111222222222233333333334444444444` to `01234012340123401234012340123401234012340123401234`, reproduced across twelve regimes spanning `memory://`, real RabbitMQ, and RabbitMQ behind a 30 ms latency relay at pool limits 10, 3, 1 and 0. Validation also produced a finding beyond the Agent Action Plan: without the fix a publishing greenlet's *own* `gevent.Timeout` never fires, because gevent timers are hub-driven. The defect is therefore a **liveness** failure, not merely unfair ordering — a materially more serious characterisation than the original report conveyed.

Quality delivery exceeded the AAP's own bar. The plan permitted ten pre-existing environment-induced test failures to persist; the delivered suite has **zero** — 3,839 passed, 0 failed, 0 errored, 28,817 subtests passed, pytest exit code 0 — achieved by resolving fourteen distinct issue classes without editing a single out-of-scope file. All eleven pre-commit hooks pass, mypy is clean, `compileall` exits 0 silently, and the new module carries 100% statement and branch coverage. The regression guard is a genuine guard rather than a tautology, because it ships with a paired negative control that asserts the *failing* value when the yield is disabled.

### 8.2 What Remains

The 33 remaining hours contain **no unfinished implementation**. They divide into two honest categories.

First, the one gate the Agent Action Plan itself placed outside agent scope: the canonical `repro/` acceptance harness (6 h). It is absent from this repository snapshot, and §0.5.2.1 explicitly forbids agents creating or altering it because "it is the acceptance criterion, not a deliverable." This is precisely why the AAP states its own confidence at 98% rather than higher. Faithful equivalents were built and executed across twelve regimes reproducing the oracle's exact numbers, so recording the canonical verdict is confirmation work, not discovery.

Second, genuine path-to-production for an open-source framework fix (27 h): full CI-matrix execution across the 5 of 7 declared job combinations whose interpreters are unavailable on the validation host (5 h); the upstream pull request and maintainer review cycle (8 h), which is the longest-latency item and the only one gated on a third party; the Changelog and documentation decision that AAP §0.5.2.1 deliberately deferred to a maintainer (3 h); a production-fleet throughput soak (4 h); the release cut and staged rollout (3 h); and two deliberately-deferred follow-ups — an upstream RC3 report to kombu/gevent (2 h) and a scoping decision on per-message fairness inside very large batches (2 h).

### 8.3 Critical Path to Production

```
H1 Author repro/ harness (4 h)
    └─> H2 Execute both oracle runs, record verdicts (2 h)
            └─> H3/H4 Full suite on remaining 5 CI job combinations (5 h)
                    └─> H7/H8 Changelog + docs decision (3 h)
                            └─> H5 Open PR, attach evidence package (3 h)
                                    └─> H6 Maintainer review cycle (5 h)  ← longest latency, third-party gated
                                            └─> H10 Release cut + staged rollout (3 h)

Parallel:  H9 Production gevent-fleet soak (4 h)
Independent, non-blocking:  H11 Upstream RC3 report (2 h) · H12 Batch-fairness scoping (2 h)
```

The serial critical path is 25 hours; H9, H11 and H12 (8 h) can run in parallel. The single genuine unknown is H6, because maintainer availability and preference are outside the team's control.

### 8.4 Success Metrics

| Metric | Target | Achieved | Status |
|---|---|---|---|
| Oracle effective concurrency (with fix) | ≥ 4.0 | **5.00** | ✅ |
| Oracle effective concurrency (yield disabled, negative control) | ≤ 3.0 | **1.88** | ✅ |
| Emission order under gevent | Round-robin | `01234012340…` | ✅ |
| `broker_pool_limit=0` control preserved | ≥ 4.0 before and after | **5.00 both** | ✅ |
| Unit-test pass rate | No new failures | **3839/3839, 0 failed, exit 0** | ✅ exceeds |
| `t/unit/utils/test_green.py` | 6 passed | **6 passed** | ✅ |
| `t/unit/concurrency/test_gevent.py` | 12 passed | **12 passed** | ✅ |
| Production-code diff to `celery/app/base.py` | `+11/−0` | **`+11/−0`**, 1635 → 1646 | ✅ |
| Dependency changes | Zero | **Zero** (0 files under `requirements/`) | ✅ |
| Non-gevent runtime cost | Strict no-op | **45.4 ns/call**, returns `False` | ✅ |
| Pre-commit hooks | All pass | **11/11 Passed** | ✅ |
| New-module coverage | Meaningful | **100% statement + branch** | ✅ |
| Canonical `repro/` oracle verdict | NOT REPRODUCED | **Not recorded** — harness absent | ⚠ 6 h budgeted |
| CI matrix job combinations executed | 7 of 7 | **2 of 7** (3.14 suite-executed; 3.13 syntax-validated) | ⚠ 5 h budgeted |
| Upstream PR merged | Merged | **Not opened** | ⚠ 8 h budgeted |

### 8.5 Production Readiness Assessment

**Verdict: technically ready, pending upstream acceptance and one confirmation gate.**

The change is safe to deploy today to a gevent-pool Celery fleet running from this branch. Every hard gate is green: it compiles, it type-checks, the entire unit suite passes with zero failures, all eleven lint and static-analysis hooks pass, it runs correctly under all four execution pools plus beat, canvas and the control plane, it packages correctly, and it eliminates the defect across twelve measured regimes. Non-gevent deployments are provably unaffected by construction. Exception safety at the yield point was measured, not argued, and is strictly *safer* than the code it replaces — an interrupt now leaves the pool exactly balanced, whereas previously the same interrupt landed inside `LifoQueue.get`'s `Condition`, which is the very mechanism that produces the reporter's secondary `RuntimeError`.

Two qualifications are material and neither is a defect. First, the canonical acceptance harness has not rendered its verdict, which is why the Agent Action Plan holds itself to 98% confidence; this should be closed before release even though twelve equivalent regimes already agree. Second, this is a fix to a widely-used open-source framework, so "production" properly means upstream merge — and that depends on maintainer judgement, which no amount of local validation can substitute for.

**Recommended sequence:** close the acceptance harness (H1/H2) and the CI matrix (H3/H4) first, because together they cost 11 hours and convert the last two ⚠ rows in the metrics table above into ✅. Land the documentation decision (H7/H8) before submitting, so the pull request arrives complete. Then open the PR with the full evidence package and expect the review cycle to dominate the remaining timeline. Run the fleet soak in parallel. Do not defer H11 indefinitely: RC3 remains live in dependency-owned code, and the 13-frame import chain already captured makes the upstream report inexpensive to file while the analysis is fresh.

---

## 9. Development Guide

Every command below was executed during this assessment on the validation host, and the stated output is the captured result.

### 9.1 System Prerequisites

| Requirement | Verified Version | Notes |
|---|---|---|
| Operating system | Ubuntu 25.10 | Any Linux; macOS works for unit tests. Windows is supported by the CI matrix for CPython 3.14 only |
| Python | **CPython 3.14.0** | `setup.py` declares `python_requires=">=3.10"` with classifiers through 3.14, CPython and PyPy |
| Docker Engine | 29.7.0 | Required only for the broker containers and the acceptance harness |
| Docker Compose | 5.3.1 | Invoke as `docker compose`, not the legacy `docker-compose` |
| git | 2.51.0 | — |
| RAM / disk | 4 GB / 3 GB free | The full unit suite runs in ~340 s on 4 vCPU |

### 9.2 Environment Setup

```bash
# 1. Enter the repository and activate the prepared virtual environment
cd /tmp/blitzy/celery/blitzy-b7a40fa5-bacb-4d24-98e3-1121c4646aac_8ec33e
source .venv/bin/activate

# 2. Confirm the interpreter and the editable install resolve to this tree
python --version                       # -> Python 3.14.0
python -c "import celery; print(celery.__version__, celery.__file__)"
# -> 5.6.2 /tmp/blitzy/celery/blitzy-.../celery/__init__.py
```

To build the environment from scratch instead:

```bash
cd /tmp/blitzy/celery/blitzy-b7a40fa5-bacb-4d24-98e3-1121c4646aac_8ec33e
python3.14 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e .                                    # editable install of celery
pip install -r requirements/test.txt                # pytest, pytest-subtests, pytest-timeout, ...
pip install -r requirements/extras/gevent.txt       # gevent>=26.4.0  (REQUIRED for this fix)
pip install -r requirements/extras/redis.txt        # result backend used by the runtime probes
pip install "click==8.1.8"                          # see Troubleshooting T2 below
```

> **Why the click pin.** click ≥ 8.2 changed its "No such option" wording, which two pre-existing CLI tests assert in the 8.1.x form. `click==8.1.8` still satisfies the repository's declared `click>=8.1.2,<9.0`.

### 9.3 Dependency Installation

Runtime dependencies are declared in `requirements/default.txt`:

```
billiard>=4.2.1,<5.0        kombu>=5.6.0              vine>=5.1.0,<6.0
click>=8.1.2,<9.0           click-didyoumean>=0.3.0   click-repl>=0.2.0
click-plugins>=1.1.1        python-dateutil>=2.8.2    tzlocal
exceptiongroup>=1.3.0; python_version < '3.11'
```

Verify the installed set:

```bash
pip check
# Expected: No broken requirements found.

python -c "
import celery, kombu, amqp, billiard, vine, gevent, click
for m in (celery, kombu, amqp, billiard, vine, gevent, click):
    print(f'{m.__name__:<10} {m.__version__}')"
# Expected: celery 5.6.2 · kombu 5.6.2 · amqp 5.3.1 · billiard 4.2.4
#           vine 5.1.0 · gevent 26.7.0 · click 8.1.8
```

**No dependency change is required by this fix.** `gevent.sleep` is already guaranteed by the existing `gevent>=26.4.0` pin in `requirements/extras/gevent.txt`, and `kombu.utils.compat._detect_environment` is already a Celery dependency.

Optional extras, needed only to unblock the 18 tests that would otherwise skip:

```bash
pip install -c requirements/constraints.txt \
  -r requirements/extras/tblib.txt          -r requirements/extras/solar.txt \
  -r requirements/extras/couchdb.txt        -r requirements/extras/consul.txt \
  -r requirements/extras/cosmosdbsql.txt    -r requirements/extras/elasticsearch.txt \
  -r requirements/extras/arangodb.txt       -r requirements/extras/couchbase.txt
pip install sphinx sphinx-testing
```

> **Do not install `requirements/extras/zstd.txt`** — its cffi pin does not build on Python 3.14. It is irrelevant to the publish path.
> **Do not install eventlet** on Python 3.10+ — the repository itself pins `eventlet>=0.32.0; python_version<"3.10"`, and installing it surfaces an unrelated pre-existing failure caused by the CPython 3.12+ `pkgutil.resolve_name` change versus Celery's `MockModule` test double.

### 9.4 Broker Services

```bash
# RabbitMQ (AMQP 5672, management UI 15672)
docker run -d --name celery-rabbitmq --health-cmd 'rabbitmq-diagnostics -q ping' \
  -p 5672:5672 -p 15672:15672 rabbitmq:3-management

# Redis (result backend, 6379)
docker run -d --name celery-redis --health-cmd 'redis-cli ping' -p 6379:6379 redis:7

# Verify both are healthy and reachable
docker ps --format '{{.Names}} {{.Status}}'
python -c "
import socket
for name, port in (('RabbitMQ', 5672), ('Redis', 6379), ('RabbitMQ mgmt', 15672)):
    s = socket.socket(); s.settimeout(2)
    try:    s.connect(('127.0.0.1', port)); print(f'  {name:<14} :{port}  REACHABLE')
    except Exception as e: print(f'  {name:<14} :{port}  UNREACHABLE ({e})')
    finally: s.close()"
# Expected: all three REACHABLE
```

### 9.5 Verification — Build and Static Analysis

```bash
cd /tmp/blitzy/celery/blitzy-b7a40fa5-bacb-4d24-98e3-1121c4646aac_8ec33e
source .venv/bin/activate

python -m compileall -q -f celery/ t/ examples/    # Expected: exit 0, NO output
mypy --config-file pyproject.toml                  # Expected: Success: no issues found in 10 source files
isort --check-only celery/ t/                      # Expected: exit 0, no output
pre-commit run --all-files                         # Expected: 11/11 hooks Passed, exit 0

flake8 celery/utils/green.py celery/app/base.py \
       t/unit/utils/test_green.py t/unit/concurrency/test_gevent.py
# Expected output — EXACTLY these two lines and nothing else:
#   celery/app/base.py:1552:1: D400 First line should end with a period
#   celery/app/base.py:1563:1: D400 First line should end with a period
# Both are PRE-EXISTING (they sit at 1541/1552 in the pristine base commit and shifted
# by exactly the 11 inserted lines). AAP 0.5.2.2 forbids fixing them. Prove it with:
git show 8f28367e4:celery/app/base.py > /tmp/pristine_base.py
flake8 --select=D400 /tmp/pristine_base.py         # Expected: the same 2 warnings at 1541 and 1552
```

### 9.6 Verification — Tests

```bash
# In-scope tests only — fast (< 1 s)
python -m pytest t/unit/utils/test_green.py -v                                       # Expected: 6 passed
python -m pytest t/unit/concurrency/test_gevent.py -v                                # Expected: 12 passed
python -m pytest t/unit/concurrency/test_gevent.py::test_cooperative_publishing -v    # Expected: 5 passed

# One-time prerequisites for a fully green FULL suite (see Troubleshooting)
sudo useradd -m -s /bin/bash celerytest
find . -path ./.git -prune -o -path ./.venv -prune -o -type d -print0 | xargs -0 chmod a+rwx
rm -f ./statefilename ./test.db ./[0-9]*

# FULL unit suite — must run as a NON-ROOT user
su celerytest -c "ulimit -n 65536; cd $PWD && CI=true HOME=/home/celerytest \
  PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest t/unit/ -q --timeout=300 -p no:cacheprovider"
# Expected: 3839 passed, 17 skipped, 3 xfailed, 28817 subtests passed in ~340s
#           pytest exit code 0 · 0 FAILED · 0 ERROR

# Coverage of the new helper — use a FRESH-IMPORT run or the number will look wrong
su celerytest -c "cd $PWD && HOME=/home/celerytest .venv/bin/python -m coverage run \
  --branch --source=celery.utils.green -m pytest t/unit/utils/test_green.py -q \
  && HOME=/home/celerytest .venv/bin/python -m coverage report -m --include='*green.py'"
# Expected: celery/utils/green.py  14 stmts  0 miss  6 branch  0 BrPart  100%
```

### 9.7 Verification — The Fix Itself

```bash
# 1. Strict no-op when gevent was never imported (every prefork/solo/threads deployment)
python -c "from celery.utils.green import cooperative_yield; assert cooperative_yield() is False; print('no-op OK')"
# Expected: no-op OK  (exit 0)

# 2. Cost in that state
python -c "import timeit; from celery.utils.green import cooperative_yield
print(timeit.timeit(cooperative_yield, number=1000000)*1000, 'ns/call')"
# Expected: ~45 ns/call

# 3. Cost of the hub round trip under gevent (this is the mechanism, not overhead)
python -c "
from gevent import monkey; monkey.patch_all()
import timeit; from celery.utils.green import cooperative_yield
assert cooperative_yield() is True
n = 200000; t = timeit.timeit(cooperative_yield, number=n)
print(f'{t/n*1e6:.2f} us/call')"
# Expected: ~1.9 us/call  (= 0.19% of wall time at 1,000 publishes/sec)

# 4. The yield must happen BEFORE the acquisition at both call sites
python -c "
import inspect
from celery.app.base import Celery
for name, target in (('_acquire_producer','producer_pool.acquire'),
                     ('_acquire_connection','pool.acquire')):
    src = inspect.getsource(getattr(Celery, name))
    print(f'  {name}: yield before acquire =',
          src.index('cooperative_yield()') < src.index(target))"
# Expected: both True
```

### 9.8 Example Usage — Running a gevent Worker

```bash
export PYTHONPATH=/tmp/blitzy_runtime      # directory containing rtapp.py
export C_FORCE_ROOT=1                      # only needed if running as root

# Start the worker in the background and CAPTURE ITS PID.
# NEVER use pkill/killall on this host — it can terminate the orchestrator.
celery -A rtapp worker -l WARNING -P gevent -c 10 -n gevent@%h -Q pgcelery > /tmp/worker.log 2>&1 &
WPID=$!; echo "worker pid=$WPID"; sleep 14

# Submit work and verify results
python -c "
import sys; sys.path.insert(0, '/tmp/blitzy_runtime')
from rtapp import add
pairs = [(i, i*2) for i in range(20)]
rs = [(p, add.apply_async(args=p, queue='pgcelery')) for p in pairs]
got = [(p, r.get(timeout=40)) for p, r in rs]
print(f'  {sum(1 for (x,y),v in got if v == x+y)}/20 correct')
print(f'  all SUCCESS: {all(r.successful() for _, r in rs)}')"
# Expected: 20/20 correct · all SUCCESS: True

# Exercise the pooled CONNECTION path (RC2) via the control plane
celery -A rtapp inspect ping        # Expected: pong ... 1 node online.
celery -A rtapp status              # Expected: 1 node online.

# Stop the worker by its exact captured pid
kill $WPID
grep -c 'Traceback\|CRITICAL\|Semaphore released too many times' /tmp/worker.log
# Expected: 0
```

### 9.9 Example Usage — Reproducing the Oracle Measurement

The oracle metric is the mean number of distinct greenlet identifiers per ten-line sliding window across fifty emitted lines (five publishers × ten publishes). **≤ 3.0 is BUGGY; ≥ 4.0 is HEALTHY.**

```bash
cd /tmp/blitzy_runtime/probe
source /tmp/blitzy/celery/blitzy-b7a40fa5-bacb-4d24-98e3-1121c4646aac_8ec33e/.venv/bin/activate

# WITH the fix, against real RabbitMQ, at every pool regime
for L in 10 3 1 0; do python e2e_probe.py $L 'amqp://guest:guest@localhost:5672//'; done
# Expected for each:
#   order=01234012340123401234012340123401234012340123401234
#   effective_concurrency=5.00 solo_block=False verdict=HEALTHY
#   producer_pool._resource=gevent._gevent_cqueue.LifoQueue
#   socket.socket.__module__=gevent._socket3

# NEGATIVE CONTROL — yield disabled, reproducing the original defect
for L in 10 3 1; do python e2e_probe.py $L 'amqp://guest:guest@localhost:5672//' noyield; done
# Expected for each:
#   order=00000000001111111111222222222233333333334444444444
#   effective_concurrency=1.88 solo_block=True verdict=BUGGY

# Under 30 ms of injected latency (toxiproxy substitute on port 5673)
python latency_proxy.py 5673 0.030 > /tmp/proxy.log 2>&1 &
PPID_=$!; sleep 3
python e2e_probe.py 10 'amqp://guest:guest@localhost:5673//'           # -> 5.00 HEALTHY
python e2e_probe.py 10 'amqp://guest:guest@localhost:5673//' noyield   # -> 1.88 BUGGY
kill $PPID_

# Boundary-condition probes
python exception_safety.py    # gevent.Timeout / KeyboardInterrupt / SystemExit -> queue 4->4, dirty 0->0
python nongreen_probe.py      # C1/C2/C3 + the 16-thread initialisation race
python nonpooled_probe.py     # pool=False -> 0 yields · pool=True -> 1 · explicit producer -> 0
python batch_probe.py         # group of 50 -> 1 yield · 5 individual apply_async -> 5 yields
python control_plane_probe.py 'amqp://guest:guest@localhost:5672//'   # RC2 path -> 5.00 HEALTHY
```

> **Note on the limit-0 control.** `python e2e_probe.py 0 ... noyield` prints `EXPECTED=BUGGY GOT=HEALTHY`. This is **correct behaviour**, not a failure: `broker_pool_limit=0` forces a fresh TCP connect plus AMQP handshake per publish, which yields naturally, so the control is healthy both before and after the fix. The probe's exit-code expectation simply assumes `noyield` implies BUGGY, which holds only for pooled regimes.

### 9.10 The Canonical Acceptance Harness (Remaining Work — H1/H2)

This harness is **absent from the repository** and is the one outstanding verification gate. Its contract, per the Agent Action Plan, is authoritative:

```bash
# To be authored at repro/ — see Section 2.2 row 1 (6 h budgeted)
docker compose -f repro/docker-compose.yml up -d     # rabbitmq:3 behind toxiproxy @ 30 ms
bash repro/run_repro.sh   && python3 repro/oracle.py # Expect >= 4.0, verdict NOT REPRODUCED
bash repro/run_control.sh && python3 repro/oracle.py # Expect >= 4.0 (broker_pool_limit=0 control)
```

Required components: `docker-compose.yml` (rabbitmq:3, a toxiproxy injecting 30 ms, and a `python:3.12-slim` runner), `app.py` (five publisher greenlets × ten publishes, emitting one `[greenthread N]` line per publish), `oracle.py` (the sliding-window metric with the ≤ 3.0 / ≥ 4.0 thresholds), `run_repro.sh` (default pooled configuration) and `run_control.sh` (`broker_pool_limit=0`). **Do not alter the log-line format** — it is the oracle's input.

### 9.11 Troubleshooting

| # | Symptom | Root Cause | Resolution |
|---|---|---|---|
| T1 | `SecurityError` / 7 failures in `t/unit/utils/test_platforms.py`, `test_cache.py`, `test_mongodb.py` | `celery/platforms.py` (~line 829) raises when `uid == euid == gid == egid == 0` | Run the suite as a non-root user: `useradd -m celerytest` then `su celerytest -c "…"` |
| T2 | `test_preload_cli.py` fails asserting `No such option: --ini` | click ≥ 8.2 emits `No such option '--ini'.` instead | `pip install "click==8.1.8"` (still satisfies `click>=8.1.2,<9.0`) |
| T3 | 47 `t/unit/backends/test_database.py` failures appear once you switch to a non-root user | sqlite creates `test.db` in the current working directory, which the new user cannot write | `find . -path ./.git -prune -o -path ./.venv -prune -o -type d -print0 \| xargs -0 chmod a+rwx` — git does not track directory modes, so the tree stays clean |
| T4 | 3 `t/unit/concurrency/test_prefork.py` failures with `EMFILE` / "Too many open files" | File-descriptor ceiling too low for the prefork tests | `ulimit -n 65536` before invoking pytest |
| T5 | 2 `t/unit/worker/test_worker.py` `PermissionError`s | Root-owned `statefilename` and numeric fd files left behind by an earlier root-run | `rm -f ./statefilename ./test.db ./[0-9]*` |
| T6 | `t/unit/concurrency/test_eventlet.py` or `test_asynchronous.py` fail after you install eventlet | Pre-existing incompatibility between the CPython 3.12+ `pkgutil.resolve_name` change and Celery's `MockModule` test double — unrelated to this fix | Do not install eventlet on Python 3.10+; the repository pins it to `python_version<"3.10"` so these tests skip by design |
| T7 | `pip install -r requirements/extras/zstd.txt` fails to build | The extra's cffi pin does not build on Python 3.14 | Skip it — irrelevant to the publish path and to this fix |
| T8 | `celery/utils/green.py` reports only 80% coverage | Import-timing artifact: `celery.app.base` imports the helper during conftest collection, so its 12 module-level lines execute before the coverage tracer starts | Use a fresh-import run (Section 9.6) — it reports **100%** statement and branch coverage |
| T9 | A later test unexpectedly sees a mocked `gevent.sleep` | `green._yield` is a module global caching the positive verdict | Reset it: `celery.utils.green._yield = None`. Both new test classes already do this in `setup_method`/`teardown_method` and via an autouse fixture |
| T10 | Worker will not start: `RuntimeError: … superuser privileges …` | Celery refuses to run as root without an explicit override | `export C_FORCE_ROOT=1` for local development only — never in production |
| T11 | `flake8` reports two `D400` warnings in `celery/app/base.py` | They are **pre-existing** at lines 1541/1552 of the pristine base commit and shifted to 1552/1563 by the 11 inserted lines | Leave them alone — AAP §0.5.2.2 explicitly forbids fixing them. Prove pre-existence with the `git show` recipe in Section 9.5 |
| T12 | Oracle probe reports `INDETERMINATE` (between 3.0 and 4.0) | Real broker latency is masking or partially masking the effect | Re-run with the 30 ms latency relay in front of the broker (Section 9.9) so the measurement is stable |
| T13 | `docker compose` not found | Legacy standalone binary is not installed | Use `docker compose` (the plugin form), not `docker-compose` |
| T14 | A background worker or proxy will not die | — | **Never use `pkill`/`killall` on this host** — they can terminate the orchestrator process. Always capture `pid=$!` at launch and `kill $pid` |

---

## 10. Appendices

### Appendix A — Command Reference

| Purpose | Command | Expected Result |
|---|---|---|
| Activate environment | `source .venv/bin/activate` | — |
| Compile everything | `python -m compileall -q -f celery/ t/ examples/` | exit 0, no output |
| Type check | `mypy --config-file pyproject.toml` | `Success: no issues found in 10 source files` |
| Import ordering | `isort --check-only celery/ t/` | exit 0 |
| All lint hooks | `pre-commit run --all-files` | 11/11 Passed |
| Lint in-scope files | `flake8 celery/utils/green.py celery/app/base.py t/unit/utils/test_green.py t/unit/concurrency/test_gevent.py` | exactly 2 pre-existing `D400` |
| Helper unit tests | `python -m pytest t/unit/utils/test_green.py -v` | 6 passed |
| Regression guard | `python -m pytest t/unit/concurrency/test_gevent.py -v` | 12 passed |
| Cooperative-publishing tests | `python -m pytest t/unit/concurrency/test_gevent.py::test_cooperative_publishing -v` | 5 passed |
| Full unit suite | `su celerytest -c "ulimit -n 65536; cd $PWD && CI=true HOME=/home/celerytest PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest t/unit/ -q --timeout=300 -p no:cacheprovider"` | 3839 passed, exit 0 |
| Non-gevent no-op | `python -c "from celery.utils.green import cooperative_yield; assert cooperative_yield() is False"` | exit 0 |
| Non-green cost | `python -c "import timeit; from celery.utils.green import cooperative_yield; print(timeit.timeit(cooperative_yield, number=1000000)*1000,'ns/call')"` | ~45 ns/call |
| Start gevent worker | `celery -A rtapp worker -l INFO -P gevent -c 10 -n gevent@%h -Q pgcelery & WPID=$!` | worker online |
| Control plane (RC2 path) | `celery -A rtapp inspect ping` · `celery -A rtapp status` | `pong` · `1 node online.` |
| Stop a worker safely | `kill $WPID` (never `pkill`) | process exits |
| Oracle, with fix | `python e2e_probe.py 10 'amqp://guest:guest@localhost:5672//'` | 5.00 HEALTHY |
| Oracle, negative control | `python e2e_probe.py 10 'amqp://guest:guest@localhost:5672//' noyield` | 1.88 BUGGY |
| 30 ms latency relay | `python latency_proxy.py 5673 0.030 & PPID_=$!` | listening on 5673 |
| Environment report | `celery -A celery report` | celery 5.6.2, kombu 5.6.2, py 3.14.0 |
| Version | `celery --version` | `5.6.2 (recovery)` |
| Branch diff summary | `git diff --stat 8f28367e4..HEAD` | 4 files, +360/−1 |
| Verify commit authorship | `git log --pretty='%an <%ae> \| %cn <%ce>' 8f28367e4..HEAD` | all `Blitzy Agent <agent@blitzy.com>` |
| Build wheel | `pip wheel . --no-deps -w /tmp/dist` | wheel contains `celery/utils/green.py` |

### Appendix B — Port Reference

| Port | Service | Purpose | Required For |
|---|---|---|---|
| 5672 | RabbitMQ AMQP | Broker transport — the publish path under test | Runtime validation, oracle probes |
| 15672 | RabbitMQ management | Web console and `rabbitmqctl` inspection | Confirming beat-published messages (third-party tooling, not a deliverable) |
| 6379 | Redis | Result backend for the runtime probe app | End-to-end task-result verification |
| 5673 | Local gevent latency relay | Injects 30 ms each way in front of RabbitMQ — a toxiproxy substitute | Latency-regime oracle runs (development only) |
| 8474 | toxiproxy control API | Latency injection in the canonical harness | Remaining task H1/H2 only — not used in this session |

### Appendix C — Key File Locations

| Path | Status | Lines | Role |
|---|---|---|---|
| `celery/utils/green.py` | **CREATED** | 45 | The fix. Exposes `cooperative_yield()`: `sys.modules` guard → uncached `_detect_environment()` → cached `gevent.sleep(0)`. md5 `a2cae073d48776869f1c0230758a3fd8`. 100% statement + branch coverage |
| `celery/app/base.py` | **UPDATED** | 1646 (`+11/−0`) | Line 38: the import. Lines 1136–1139: 3-line comment + `cooperative_yield()` in `_acquire_connection` (RC2). Lines 1169–1174: 5-line comment + `cooperative_yield()` in `_acquire_producer` (RC1). Three pure insertions; no existing line modified |
| `t/unit/utils/test_green.py` | **CREATED** | 74 | Six unit tests for the helper, with an autouse fixture resetting the `_yield` module global |
| `t/unit/concurrency/test_gevent.py` | **UPDATED** | 380 (`+230/−1`) | `class test_cooperative_publishing` at line 156 — five tests including the deterministic fail-closed token-ring scheduler and the paired negative control |
| `celery/utils/quorum_queues.py` | unchanged | — | In-repo precedent for a small single-purpose `celery/utils/` module |
| `celery/worker/consumer/consumer.py` | unchanged | — | Lines 19 and 242: the in-repo precedent for the `_detect_environment() == 'gevent'` guard |
| `celery/app/defaults.py` | unchanged | — | Declares `broker_pool_limit=10` and `broker_pool_acquire_timeout=None` — the defaults that make the *default* configuration the affected one |
| `celery/contrib/migrate.py` | unchanged | — | Passes `pool=False`, so it performs no yield — protected by a dedicated test |
| `requirements/extras/gevent.txt` | unchanged | — | `gevent>=26.4.0` — already guarantees `gevent.sleep`, so no dependency change was needed |
| `repro/` | **ABSENT** | — | The canonical acceptance harness. Explicitly excluded from modification by AAP §0.5.2.1; authoring it is remaining task H1 |
| `/tmp/blitzy_runtime/probe/` | scratch | — | Validation probes: `e2e_probe.py`, `control_plane_probe.py`, `exception_safety.py`, `nongreen_probe.py`, `nonpooled_probe.py`, `batch_probe.py`, `latency_proxy.py`, `reporter_repro.py`, `timeout_liveness.py`. Outside the repository by design |

### Appendix D — Technology Versions

| Component | Version | Constraint | Satisfied |
|---|---|---|---|
| CPython | 3.14.0 | `python_requires>=3.10`, classifiers → 3.14 | ✅ |
| celery | 5.6.2 (`recovery`) | this repository, editable install | ✅ |
| kombu | 5.6.2 | `>=5.6.0` | ✅ |
| py-amqp | 5.3.1 | via kombu | ✅ |
| billiard | 4.2.4 | `>=4.2.1,<5.0` | ✅ |
| vine | 5.1.0 | `>=5.1.0,<6.0` | ✅ |
| **gevent** | **26.7.0** | `>=26.4.0` (`requirements/extras/gevent.txt`) | ✅ |
| click | 8.1.8 | `>=8.1.2,<9.0` | ✅ |
| pytest | 9.0.3 | `requirements/test.txt` | ✅ |
| eventlet | not installed | `>=0.32.0; python_version<"3.10"` — unreachable on 3.14 by the repository's own pin | n/a by design |
| RabbitMQ | 3 (management) | container | ✅ |
| Redis | 7 | container | ✅ |
| Docker Engine | 29.7.0 | — | ✅ |
| Docker Compose | 5.3.1 | plugin form | ✅ |
| Ubuntu | 25.10 | — | ✅ |

### Appendix E — Environment Variable Reference

**This fix introduces no environment variable and no configuration option** — deliberately, because AAP §0.5.2.3 establishes that an opt-in knob would leave the defect present in the default configuration, which is precisely the configuration that fails. The variables below are development and validation aids only.

| Variable | Value used | Purpose |
|---|---|---|
| `PYTHONPATH` | `/tmp/blitzy_runtime` | Locates the runtime-probe app (`rtapp.py`) for worker startup |
| `C_FORCE_ROOT` | `1` | Permits a worker to start as root — local development only, never production |
| `CI` | `true` | Non-interactive pytest behaviour |
| `HOME` | `/home/celerytest` | Required when running the suite via `su celerytest` |
| `PYTHONDONTWRITEBYTECODE` | `1` | Prevents the non-root test user creating root-owned `__pycache__` entries |
| `DEBIAN_FRONTEND` | `noninteractive` | Non-interactive apt operations |

Relevant Celery settings (**all left at their defaults — none changed by this fix**):

| Setting | Default | Relevance |
|---|---|---|
| `broker_pool_limit` | `10` | With five concurrent publishers the pool never blocks, so the **default** value is the affected one. Verified at 10, 3, 1 and 0 |
| `broker_pool_acquire_timeout` | `None` | Because `acquire(block=True, timeout=None)` never raises `Empty`, the `LimitExceeded → OperationalError` branch is unreachable in the default configuration |
| `broker_url` | — | `amqp://guest:guest@localhost:5672//` for validation; `memory://` for hermetic probes |
| `result_backend` | — | `redis://localhost:6379/0` for validation |

### Appendix F — Developer Tools Guide

| Tool | Invocation | Notes |
|---|---|---|
| pytest | `python -m pytest t/unit/ -q --timeout=300 -p no:cacheprovider` | Always pass `--timeout` and `-p no:cacheprovider`. Must run as a non-root user for the full suite |
| pytest-subtests | automatic | Source of the 28,817 subtest assertions |
| coverage | `coverage run --branch --source=celery.utils.green -m pytest …` | Use a **fresh-import** run for `green.py` or the figure is an import-timing artifact |
| mypy | `mypy --config-file pyproject.toml` | `pyproject.toml` lists only `celery/utils/text.py` from `celery/utils/`, so `green.py` is not type-gated — it nonetheless introduces no typing issue |
| flake8 | `flake8 <files>` | Line-length limit 117 per `setup.cfg`. Never use a `--fix`-style flag |
| isort | `isort --check-only celery/ t/` | The import sits at line 38 precisely so `functional` < `green` < `imports` holds |
| pre-commit | `pre-commit run --all-files` | 11 hooks. Several can rewrite files, so verify md5 byte-identity afterwards if that matters |
| `scripts/check-ci-test-matrices` | via pre-commit | Enforces CI registration only for `t/integration` and `t/smoke/tests`, so a new `t/unit` module needs none |
| tox | `tox -e 3.14-unit` | envlist covers `{3.10,3.11,3.12,3.13,3.14,pypy3}-{unit,integration,smoke}` — the vehicle for remaining tasks H3/H4 |
| Makefile | `make lint` | Aggregates `flakecheck`, `apicheck`, `configcheck`, `readmecheck` |
| `celery report` | `celery -A <app> report` | Prints the full software/platform/settings triple — attach it to any bug report |
| `rabbitmqctl` | `docker exec celery-rabbitmq rabbitmqctl list_queues` | Confirms messages actually reached the broker |
| Oracle probes | `python e2e_probe.py <limit> <broker> [noyield]` | The measurement of record for this defect. `noyield` disables the fix to reproduce the original behaviour |

### Appendix G — Glossary

| Term | Definition |
|---|---|
| **AAP** | Agent Action Plan — the authoritative specification this work implements |
| **Barging / convoy failure** | A releasing thread re-acquires the resource it just released before the notified waiter is ever scheduled. Here `Resource.release()` uses `put_nowait`, which only *schedules* the waiter, and the LIFO discipline then hands the same slot straight back to the releaser |
| **Cooperative multitasking** | gevent's scheduling model: a greenlet runs until it calls something that switches to the hub. A code path with no blocking operation therefore starves every peer, however many are ready to run |
| **Effective concurrency** | The oracle's metric — the mean number of distinct greenlet identifiers per ten-line sliding window over the fifty emitted lines. **≤ 3.0 = BUGGY, ≥ 4.0 = HEALTHY.** Measured 1.88 before the fix, 5.00 after |
| **Fire-and-forget publish** | `basic_publish` writing an AMQP frame via `sock.sendall` without awaiting a broker reply. A ~200-byte frame never fills the kernel send buffer, so a gevent-patched socket never hits `EWOULDBLOCK` and never switches |
| **gevent hub** | The greenlet that owns the event loop. Reaching it is the only way control passes to a peer greenlet |
| **Greenlet** | A lightweight cooperatively-scheduled coroutine; gevent's unit of concurrency, all running on one OS thread |
| **LIFO pool** | `kombu.Resource`'s last-in-first-out queue of broker resources, pre-filled with `broker_pool_limit` slots at construction — which is why a warm `get()` returns instantly and never blocks |
| **Liveness failure** | A defect where required progress never occurs. Discovered here beyond the AAP: without the fix a publishing greenlet's own `gevent.Timeout` never fires, because gevent timers are hub-driven |
| **monkey.patch_all()** | gevent's replacement of blocking stdlib primitives with cooperative equivalents. Its patch order matters: `patch_thread()` runs 35 lines before `patch_queue()`, which is the mechanism behind RC3 |
| **Oracle** | The acceptance arbiter that computes effective concurrency from the fifty `[greenthread N]` log lines and renders a BUGGY/HEALTHY verdict |
| **Producer pool** | Celery's pool of `kombu.Producer` objects, acquired in `_acquire_producer` — the RC1 failure site |
| **RC1** | Root cause 1: no cooperative scheduling point in `_acquire_producer` (`celery/app/base.py:1165`) — the reported defect. **Remediated** |
| **RC2** | Root cause 2: the identical omission in `_acquire_connection` (`:1136`), affecting pooled `app.control` operations. **Remediated** |
| **RC3** | Root cause 3: kombu binding an unpatched `queue.LifoQueue` mid-`patch_all()`, causing the reporter's secondary `RuntimeError('Semaphore released too many times')`. Dependency-owned — **documented, deliberately not remediated** |
| **Solo block** | A ten-line window in which a single greenlet identifier appears alone — the signature of total starvation |
| **Token ring** | The deterministic scheduler in the new regression test: five threads with one baton each, only one runnable at any instant, control handed on strictly in turn and only when the code under test calls the patched `gevent.sleep`. Fail-closed, hermetic, and reproduces the oracle's exact numbers with no broker |
| **toxiproxy** | The TCP proxy the canonical harness uses to inject 30 ms of broker latency. Substituted here by a gevent `StreamServer` relay on port 5673 |

---

*Blitzy Project Guide — brand colors: Completed `#5B39F3` · Remaining `#FFFFFF` · Headings/Accents `#B23AF2` · Highlight `#A8FDD9`*
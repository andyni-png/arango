# Prompt: run `antithesis-setup` for the ArangoDB Agency

You are scaffolding an Antithesis harness for the **ArangoDB Agency**, the 3-node Raft
consensus store built into `arangod`. The working directory is `~/arangodb`. Your job is
**infrastructure only**: a 3-agent cluster that comes up in Antithesis, elects a leader,
sits idle and ready, and proves the SDK path works with one trivial assertion. The workload
and the real properties come later, in `antithesis-feature-workload`. Don't start them here.

## Read these first (source of truth)

1. `~/arangodb/k-scope.md`: K's email word for word, the bug scope, agent flags, auth scheme,
   and topology.
2. `~/arangodb/properties.md`: the property catalog P1–P7 and S1–S6. Read it for context only.
   **Implement none of it in this step.**
3. The `antithesis-setup` skill (`SKILL.md` and each file in `references/`, in order).

If anything in this prompt conflicts with the skill, the skill wins on Antithesis mechanics
(compose rules, SDK, validate). This prompt wins on what the SUT is and what's in scope.
Tell the user about any such conflict. Don't resolve it silently.

## Step 0: satisfy the research prerequisite without running research

The skill stops if `antithesis/scratchbook/sut-analysis.md` and
`deployment-topology.md` are missing. We decided with the user to **skip
`antithesis-research`**, because the scope and properties were already worked out by hand.
So create those files yourself, drawing only on `k-scope.md`, `properties.md` and the
source in `arangod/Agency`:

- `antithesis/scratchbook/sut-analysis.md`: what the Agency is, the components that matter
  (`Agent.cpp`, `State.cpp`, `Constituent.cpp`, `Store.cpp`, `RestAgencyHandler.cpp`,
  `AgentCallback.cpp`), the HTTP surface (`/_api/agency/{write,read,transact,state,stores,config}`),
  JWT auth, and the risky path: **a snapshot sent inside appendEntries**
  (`Agent.cpp:604-706`). Include `Assumptions` and `Open Questions` sections. At minimum,
  the open questions are reading the compaction snapshot from outside the agent, and
  whether the `devel` line numbers match 3.12.
- `antithesis/scratchbook/deployment-topology.md`: the topology below.
- `antithesis/scratchbook/property-catalog.md`: a copy of `properties.md` with frontmatter
  added, so `antithesis-workload` finds the catalog where it expects it.

Every file starts with the provenance frontmatter from
`antithesis-research/references/scratchbook-setup.md`:

```yaml
---
sut_path: /home/exedev/arangodb
commit: 0ced0d52ca82969240e115707899223c06ba2030
updated: <today>
external_references:
  - path: docker.io/arangodb/enterprise:3.12.12
    why: The binary under test. The source checkout is devel and is used for reading only.
  - path: /home/exedev/arangodb/k-scope.md
    why: Customer scope and demo setup from K's email.
---
```

When the skill asks you to describe provenance and confirm the target, say plainly that the
source is a sparse `devel` checkout that's **only read**, and that the thing under test is
the prebuilt `arangodb/enterprise:3.12.12` image. Then ask the user to confirm.

## Topology to build

| Service | Image | Role |
|---|---|---|
| `agent1`, `agent2`, `agent3` | thin Dockerfile `FROM arangodb/enterprise:3.12.12` | one `arangod` agent each |
| `workload` | small Python image with the Antithesis Python SDK | idle client; holds the bootstrap assertion and `setup_complete` |

**Agents**
- Use K's flags from `k-scope.md`, with two changes. Replace `[::1]` with hostnames and
  use a single port in every container (for example `tcp://agent1:8531`, `agent2:8531`,
  `agent3:8531`). Use `--server.endpoint tcp://0.0.0.0:8531`.
- `--agency.size 3 --agency.activate true`, three `--agency.endpoint`,
  `--agency.my-address` set to the container's own hostname.
- Auth: `--server.jwt-secret-keyfile` pointing to a secret file that the thin Dockerfile
  bakes into the image (`hello world`, the same as K's). Do **not** turn auth off.
- `--database.directory` on a **named volume per agent**, so data survives kill and
  restart.
- **Low compaction** so snapshots actually travel through appendEntries during a run:
  `--agency.compaction-step-size` and `--agency.compaction-keep-size` in the tens to low
  hundreds. Before using any flag, check its exact name and default against
  `arangod --help-all` inside the image, and write down the values you chose and why.
- Leave everything that affects durability at its default. In particular, **don't** set
  `--agency.wait-for-sync false`. Faking durability would create false positives for P2.
- Override the image's entrypoint so `arangod` runs directly as an agent. The stock
  entrypoint starts a single server.
- Healthcheck: authenticated `GET /_api/version` on the agent's own port.
  - Before writing it, check what the image actually contains (`curl`? `wget`? only
    `arangosh`?).
  - If nothing fits, add a tiny tool in the thin Dockerfile.
  - Create the JWT once, for example with a small script, and reuse it everywhere. K's
    token from the email is valid for the secret `hello world`.

**Workload container**
- Python 3 plus `antithesis` (the Python SDK) plus `requests` or an equivalent HTTP client.
- `depends_on` all three agents with `condition: service_healthy`.
- Entrypoint:
  1. Wait until `GET /_api/agency/config` on some agent reports a leader.
  2. Do one authenticated write and read back through `/_api/agency/write` and `/read`,
     following 307 redirects to the leader.
  3. Fire the bootstrap assertion: `reachable("agency cluster elected a leader and served a
     write")`, with the string inline and constant.
  4. Emit `setup_complete`.
  5. Then `sleep infinity`.
- Create `antithesis/test/<name>/` as an **empty** placeholder for later test templates.

**Instrumentation decision (write it down, don't try to fix it)**
`arangod` is a prebuilt, closed enterprise binary. Coverage instrumentation would mean
rebuilding ArangoDB from source with the Antithesis C++ flags, and that's out of scope for
this step. Record the agents as **uninstrumented, reason: prebuilt vendor binary**, and the
workload container as SDK-integrated (Python, cataloged). Add a note that an instrumented
`devel` build is a possible follow-up if K wants coverage-guided exploration.

## Compose rules (the skill checks these, so get them right the first time)

For every service:
- `platform: linux/amd64`
- `hostname` equal to `container_name`, with no underscores
- `init: true`
- `NO_COLOR=1`

Also:
- Local images use `build:`.
- `depends_on` uses `service_healthy` everywhere.
- No `logging:`, no `internal: true` network, no `pull_policy:`.

## Definition of done

1. `snouty validate antithesis/config/` passes.
2. `docker compose build && docker compose up` on this VM, using the engine that
   `snouty doctor` reports:
   - all 3 agents turn healthy
   - `/_api/agency/config` shows **the same leader from all three agents**
   - the workload logs a successful write and read
   - the `setup_complete` step runs
3. Kill one agent and restart it (`docker compose restart agent2`). It rejoins, and the
   leader is still agreed. This is a quick sanity check that the volumes and hostnames work
   under the faults Antithesis will inject.
4. Every item in the skill's Self-Review section is checked, and you report the result.
5. `docker compose down -v` at the end, so the VM is left clean.

## Do NOT

- Don't run `antithesis-research`. The scratchbook files above replace it.
- Don't write workload logic, property checkers, or any assertion other than the one
  bootstrap `reachable`. P1–P7 and S1–S6 belong to the next skill.
- Don't clone the rest of the ArangoDB repo, build `arangod`, or install a C++ toolchain.
- Don't run all three agents in one container, as K's demo does. Antithesis has to be able
  to partition and kill agents independently.
- Don't use `latest`. Pin `arangodb/enterprise:3.12.12`, and confirm with
  `/_api/version` that the running version is 3.12.12.
- Don't disable auth, wait-for-sync, or anything else that weakens Raft guarantees to make
  things easier.
- Don't enable or configure Supervision, coordinators, or DB servers. The agency runs
  alone.
- Don't run `snouty launch`. Launching is a separate step through `antithesis-launch`, and
  only when the user asks.
- Don't commit anything, and don't touch files outside `antithesis/` or the scratchbook.
  `k-scope.md` and `properties.md` stay as they are.
- If you hit a blocker you can't resolve from the code or the image (for example a flag
  that doesn't exist, or the image won't run an agent-only process), **stop and report
  it**. Don't guess a workaround.

## Report back with

- The file tree you created under `antithesis/`.
- The final agent command line.
- The compaction values you chose, with their justification.
- The local smoke-test output: health, leader agreement, the write and read, and the
  restart.
- The results of the self-review checklist.
- Anything that should go into `properties.md` open questions for the workload step.

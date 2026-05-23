# Branch Protection Recommendations

Recommended GitHub branch protection rules for `fatemabranch` and `main`.

## Apply via: Settings → Branches → Add rule

---

## `main` — Production Branch

| Setting | Value |
|---|---|
| Require a pull request before merging | ✅ Enabled |
| Required approvals | 1 |
| Dismiss stale PR approvals when new commits are pushed | ✅ Enabled |
| Require status checks to pass before merging | ✅ Enabled |
| Required status checks | `CI / Python — Lint & Format`, `CI / Python — pytest`, `CI / Docker — Compose Config` |
| Require branches to be up to date before merging | ✅ Enabled |
| Require conversation resolution before merging | ✅ Enabled |
| Do not allow force pushes | ✅ Enabled |
| Do not allow deletions | ✅ Enabled |

---

## `fatemabranch` — Development Branch

| Setting | Value |
|---|---|
| Require a pull request before merging | ✅ Enabled |
| Required approvals | 1 |
| Require status checks to pass before merging | ✅ Enabled |
| Required status checks | `CI / Python — Lint & Format`, `CI / Python — pytest` |
| Require branches to be up to date before merging | ✅ Recommended |
| Do not allow force pushes | ✅ Enabled |
| Do not allow deletions | ✅ Enabled |

---

## How to configure via GitHub CLI

```bash
# Protect main
gh api repos/{owner}/{repo}/branches/main/protection \
  --method PUT \
  --field required_status_checks='{"strict":true,"contexts":["CI / Python — Lint & Format","CI / Python — pytest","CI / Docker — Compose Config"]}' \
  --field enforce_admins=false \
  --field required_pull_request_reviews='{"required_approving_review_count":1,"dismiss_stale_reviews":true}' \
  --field restrictions=null \
  --field allow_force_pushes=false \
  --field allow_deletions=false

# Protect fatemabranch
gh api repos/{owner}/{repo}/branches/fatemabranch/protection \
  --method PUT \
  --field required_status_checks='{"strict":true,"contexts":["CI / Python — Lint & Format","CI / Python — pytest"]}' \
  --field enforce_admins=false \
  --field required_pull_request_reviews='{"required_approving_review_count":1}' \
  --field restrictions=null \
  --field allow_force_pushes=false \
  --field allow_deletions=false
```

Replace `{owner}/{repo}` with the actual GitHub repository path.

---

## CI status check names

The exact names to enter in the required status checks field are the `name` and `job name` from the workflow files, joined with ` / `:

| Workflow file | Job id | Display name |
|---|---|---|
| `ci.yml` | `python-lint` | `CI / Python — Lint & Format` |
| `ci.yml` | `frontend-lint` | `CI / React — ESLint` |
| `ci.yml` | `python-tests` | `CI / Python — pytest` |
| `ci.yml` | `docker-validate` | `CI / Docker — Compose Config` |
| `ci.yml` | `streamlit-syntax` | `CI / Streamlit — Syntax Check` |
| `docker-build.yml` | `compose-validate` | `Docker Build Validation / Compose — Syntax Validation` |
| `docker-build.yml` | `dockerfile-check` | `Docker Build Validation / Compose — Dockerfile Existence` |
| `docker-build.yml` | `docker-build` | `Docker Build Validation / Docker — Build Images` |
| `code-quality.yml` | `black` | `Code Quality / Black — Format Check` |
| `code-quality.yml` | `flake8` | `Code Quality / flake8 — Style Check` |
| `code-quality.yml` | `credentials-scan` | `Code Quality / Security — Hardcoded Credentials` |
| `code-quality.yml` | `import-check` | `Code Quality / Python — Import Validation` |

> **Note:** Status check names only appear in the dropdown after at least one CI run has completed on the repository.

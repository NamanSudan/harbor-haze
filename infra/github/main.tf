# The repository is created once with `gh repo create` (so the owner is its creator) and then
# adopted with `make gh-import`; every setting after that lives in this file.

resource "github_repository" "repo" {
  name        = var.repo_name
  description = "Does ship traffic near the Port of LA / Long Beach help predict hourly air quality?"
  visibility  = var.visibility

  has_issues   = true
  has_projects = true
  has_wiki     = false

  allow_merge_commit     = false
  allow_squash_merge     = true
  allow_rebase_merge     = false
  delete_branch_on_merge = true

  lifecycle {
    prevent_destroy = true
  }
}

resource "github_repository_collaborator" "member" {
  for_each   = var.collaborators
  repository = github_repository.repo.name
  username   = each.key
  permission = each.value
}

# dev = integration and testing, main = the deliverable. New work branches off dev and lands
# through a PR; dev is promoted to main through a PR.
resource "github_branch" "dev" {
  repository    = github_repository.repo.name
  branch        = "dev"
  source_branch = "main"
}

resource "github_branch_default" "default" {
  repository = github_repository.repo.name
  branch     = github_branch.dev.branch
}

# GitHub only enforces branch protection on private repos with a paid plan (Pro / Team). Until then the same
# rules are enforced by .github/workflows/ci.yml (branch-policy job) and the pre-commit/pre-push hooks.
resource "github_branch_protection" "protected" {
  for_each       = var.enable_branch_protection ? toset(var.protected_branches) : toset([])
  repository_id  = github_repository.repo.node_id
  pattern        = each.key
  enforce_admins = true

  required_pull_request_reviews {
    required_approving_review_count = var.required_approvals
    dismiss_stale_reviews           = true
  }

  required_status_checks {
    strict   = true
    contexts = var.required_checks
  }

  required_linear_history = true
  allows_force_pushes     = false
  allows_deletions        = false
}

resource "github_issue_label" "label" {
  for_each   = var.labels
  repository = github_repository.repo.name
  name       = each.key
  color      = each.value
}

resource "github_repository_vulnerability_alerts" "repo" {
  repository = github_repository.repo.name
  enabled    = true
}

variable "owner" {
  description = "GitHub account that owns the repository"
  type        = string
}

variable "repo_name" {
  type    = string
  default = "harbor-haze"
}

variable "visibility" {
  type    = string
  default = "public"
  validation {
    condition     = contains(["private", "public"], var.visibility)
    error_message = "visibility must be private or public"
  }
}

variable "collaborators" {
  description = "GitHub username => permission (pull, triage, push, maintain, admin)"
  type        = map(string)
  default     = {}
}

variable "required_checks" {
  description = "CI job names that must pass before a PR can merge"
  type        = list(string)
  default     = ["branch-policy", "lint-and-test", "docker-build", "terraform"]
}

variable "required_approvals" {
  type    = number
  default = 1
}

variable "labels" {
  description = "Issue/PR labels: name => color (hex, no #)"
  type        = map(string)
  default = {
    "lane: ingestion" = "1d76db"
    "lane: streaming" = "0e8a16"
    "lane: warehouse" = "5319e7"
    "lane: modeling"  = "fbca04"
    "lane: ui"        = "c5def5"
    "infra"           = "000000"
    "data-quality"    = "d93f0b"
    "report"          = "bfdadc"
  }
}

variable "protected_branches" {
  type    = list(string)
  default = ["main", "dev"]
}

variable "enable_branch_protection" {
  description = "Free on public repos; a private repo needs GitHub Pro/Team"
  type        = bool
  default     = true
}

variable "enforce_for_admins" {
  description = "Apply the rules to repo admins too (false lets the owner merge without waiting)"
  type        = bool
  default     = false
}

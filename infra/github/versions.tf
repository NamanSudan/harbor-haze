terraform {
  required_version = ">= 1.5"
  required_providers {
    github = {
      source  = "integrations/github"
      version = "~> 6.6"
    }
  }
}

# Auth: GITHUB_TOKEN from the repo owner's `gh auth token` (see Makefile `gh-*` targets).
provider "github" {
  owner = var.owner
}

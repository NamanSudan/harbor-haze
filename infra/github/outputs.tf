output "repo_url" {
  value = github_repository.repo.html_url
}

output "pending_invitations" {
  description = "Collaborators must accept the e-mailed invitation before they can push"
  value       = { for k, v in github_repository_collaborator.member : k => v.invitation_id }
}

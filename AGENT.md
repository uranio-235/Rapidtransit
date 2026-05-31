# AGENT.md

## Scope

These instructions apply to the Rapidtransit repository.

## General behavior

- Before making changes, inspect the current repository state and the latest relevant files.
- Prefer the smallest focused change that satisfies the request.
- Do not overwrite user changes unless the user explicitly asks for it.
- After any edit, validate the change with the cheapest meaningful check available.

## Release workflow

When the user says something like `haz un release`, follow this workflow:

1. Read this file first.
2. Check the working tree is clean.
3. Read the latest Git tag.
4. Decide the next version using SemVer.
5. Create a new annotated tag for the release.
6. Push the tag to GitHub.
7. Confirm the GitHub Actions workflow will build, test, and publish the NuGet package.

## Versioning rules

- Use tags in the form `vX.Y.Z` for normal releases.
- Use tags like `vX.Y.Z-alpha.N` only for prereleases.
- The tag name is the source of truth for the package version.
- Do not edit version numbers manually in the project files unless the user explicitly asks for a new baseline.

## Release decision rules

- If the user does not specify the next version, inspect the latest tag and propose the next version before tagging.
- If the user does specify the version, use that exact version.
- If the working tree has uncommitted changes, stop and report that a commit is needed before releasing.
- Never create a release tag from an unclean tree.

## Expected release commands

- Get the latest tag: `git describe --tags --abbrev=0`
- Create an annotated tag: `git tag -a vX.Y.Z -m "Release vX.Y.Z"`
- Push the tag: `git push origin vX.Y.Z`

## Notes for NuGet publishing

- The GitHub Actions workflow is responsible for packing and publishing to NuGet.org.
- The workflow expects the release tag to start with `v`.
- The package version should match the tag without the leading `v`.

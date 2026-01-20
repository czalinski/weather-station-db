# Claude Code Instructions

## Git Workflow

When adding a set of features requested by the user:

1. **Create a feature branch** from `main` before starting work
   - Use descriptive branch names (e.g., `feature/add-alerting`, `feature/csv-compression`)

2. **Work on the feature branch** for all related changes

3. **On "commit and close the branch"** instruction:
   - Commit all changes with a descriptive message
   - Merge the branch into `main` (or create a PR if requested)
   - Delete the feature branch after successful merge

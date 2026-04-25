# KiwiFS — Tasks

---

## Remaining

### Protocol testing (NFS, S3, WebDAV, FUSE)

- [ ] Docker NFS mount: `docker run --mount type=nfs,source=/,target=/mnt/kiwi,o=addr=kiwifs-host`
- [ ] K8s NFS PersistentVolume
- [ ] macOS Finder NFS, Windows Map Network Drive
- [ ] boto3, MinIO client, rclone against S3 API
- [ ] Docker FUSE: `--device /dev/fuse --cap-add SYS_ADMIN`
- [ ] Windows/macOS WebDAV mount

### Follow-up hardening

- [ ] NFS multi-export per space (deferred — NFS v3 exposes a single root per server; multi-export needs one `nfs.Serve` per space on different ports)

### OSS distribution

- [ ] Publish npm package to registry
- [ ] Store SSH key and AWS creds as GitHub secrets

---

## JetRun integration (detailed checklist)

All of these are in the JetRun monorepo (`services/orchestrator/`), not in
kiwifs itself. Prerequisites: Feature #6 (metadata index) and Feature #8
(provenance) deployed on the KiwiFS server.

### 1. Set `KIWI_URL` in ECS task definition

- [ ] Open the ECS task definition for the orchestrator service (AWS Console or Terraform/CDK)
- [ ] Add environment variable: `KIWI_URL=http://18.209.226.85:3333`
- [ ] Add environment variable: `KIWI_API_KEY=<generate a key and set in KiwiFS too>`
- [ ] Update the security group for the orchestrator's ECS tasks to allow outbound TCP to `18.209.226.85:3333`
- [ ] Update the KiwiFS EC2 security group (`i-02811687a240d1ef1`) to allow inbound TCP 3333 from the ECS task security group
- [ ] Deploy a new revision of the task definition
- [ ] Verify: SSH into a running ECS task, `curl http://18.209.226.85:3333/health` returns `{"status":"ok"}`

### 2. Auto-write project index on repo import

When a user imports a GitHub repo into JetRun, the orchestrator should
automatically create the project's knowledge base with a `project-index.md`.

**File:** `services/orchestrator/app/routes/projects/codebase.py`

- [ ] Identify the endpoint/function that runs after a repo is successfully linked to a project
- [ ] After repo link succeeds, call `kiwi.write(f"projects/{project_id}/project-index.md", content)`
- [ ] Also create the directory structure (`workflows`, `runs`, `failures`, `concepts`)
- [ ] Wrap in try/except — KiwiFS being down should not block repo import
- [ ] Test: import a repo, verify `project-index.md` exists in KiwiFS

### 3. Write run record after each execution

After the agent completes a run, write a structured run record to KiwiFS.

**File:** `services/orchestrator/app/runtime/turns.py`

- [ ] Find the function that runs after a turn completes successfully
- [ ] After turn completes, build and write the run record with frontmatter
- [ ] Extract relevant data (project_id, commit_sha, elapsed_seconds, summary, workflows)
- [ ] Wrap in try/except — run record failure should not crash the turn
- [ ] Test: complete a turn, verify run record appears, frontmatter is queryable

### 4. Update coverage strategy after each run

**File:** `services/orchestrator/app/runtime/turns.py` (same post-turn hook)

- [ ] After writing the run record, read/create coverage-strategy.md
- [ ] If exists, update frontmatter fields and append to history table
- [ ] Wrap in try/except
- [ ] Test: complete 2 runs, verify coverage-strategy.md has both entries

### 5. Write failure pattern on new error class

**File:** `services/orchestrator/app/runtime/turns.py` (or a new `knowledge.py` helper)

- [ ] After a failed run, extract error information
- [ ] Check if error class already has a pattern file — append or create
- [ ] Helper function `append_occurrence()` for existing patterns
- [ ] Wrap in try/except
- [ ] Test: trigger failing run, verify pattern created; trigger again, verify count incremented

### 6. Knowledge panel in web UI thread view

**Files:** `apps/web/src/components/console/threads.tsx`, new `KnowledgePanel.tsx`

- [ ] Create `KnowledgePanel.tsx` — tree, file viewer, search
- [ ] Wire into thread view sidebar (tab/toggle next to workspace)
- [ ] Add knowledge proxy routes if missing
- [ ] Show recent changes (SSE or poll)
- [ ] Show provenance links (run → files, file → run)
- [ ] Test: open thread, verify knowledge panel loads and search works

---

## Deploy cheat sheet

```bash
# Build and push
cd kiwifs
docker build --platform linux/amd64 -t 093955289594.dkr.ecr.us-east-1.amazonaws.com/kiwifs:latest .
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 093955289594.dkr.ecr.us-east-1.amazonaws.com
docker push 093955289594.dkr.ecr.us-east-1.amazonaws.com/kiwifs:latest

# Deploy
ssh -i ~/.ssh/server-key.pem ec2-user@18.209.226.85 \
  "sudo aws ecr get-login-password --region us-east-1 | sudo docker login --username AWS --password-stdin 093955289594.dkr.ecr.us-east-1.amazonaws.com && \
   sudo docker pull 093955289594.dkr.ecr.us-east-1.amazonaws.com/kiwifs:latest && \
   sudo docker stop kiwifs && sudo docker rm kiwifs && \
   sudo docker run -d --name kiwifs --restart always -p 3333:3333 -v /data/knowledge:/data 093955289594.dkr.ecr.us-east-1.amazonaws.com/kiwifs:latest"
```

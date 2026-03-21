# Stacks

## Deploy

```bash
deploy stacks -r    # always needs rebuild (no volume-mounted code)
```

Stacks is part of the media Docker Compose stack at `/volume2/docker/media/compose.yaml`. The deploy tool rsyncs source to `/volume2/docker/repos/stacks/`, builds the image there, and restarts the stacks service within the media stack.
